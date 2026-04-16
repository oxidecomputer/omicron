// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use anyhow::Context;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_fm as fm;
use nexus_types::internal_api::background::FmAnalysisStatus;
use nexus_types::internal_api::background::fm_analysis as status;
use nexus_types::inventory;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct FmAnalysis {
    datastore: Arc<DataStore>,
    sitrep_rx: watch::Receiver<Option<CurrentSitrep>>,
    inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
    sitrep_loader: Activator,
    sitrep_gc: Activator,
    nexus_id: OmicronZoneUuid,
}

/// This is just because I don't like it when a constructor takes multiple
/// positional arguments of the same type...
pub struct Activators {
    pub sitrep_loader: Activator,
    pub sitrep_gc: Activator,
}

impl BackgroundTask for FmAnalysis {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    let err = format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&err)
                    );
                    json!({ "error": err })
                }
            }
        })
    }
}

impl FmAnalysis {
    pub fn new(
        datastore: Arc<DataStore>,
        sitrep_rx: watch::Receiver<Option<CurrentSitrep>>,
        inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
        activators: Activators,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        let Activators { sitrep_loader, sitrep_gc } = activators;
        Self {
            datastore,
            sitrep_rx,
            inv_rx,
            sitrep_loader,
            sitrep_gc,
            nexus_id,
        }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> FmAnalysisStatus {
        let parent_sitrep = self.sitrep_rx.borrow_and_update().clone();
        let parent_sitrep_id = parent_sitrep.as_ref().map(|s| s.1.id());
        let Some(inv) = self.inv_rx.borrow_and_update().clone() else {
            slog::debug!(
                opctx.log,
                "fault management analysis waiting for inventory to be loaded"
            );
            return FmAnalysisStatus {
                parent_sitrep_id,
                inv_collection_id: None,
                outcome: status::Outcome::WaitingForInventory,
            };
        };
        let inv_collection_id = inv.id;
        let opctx = opctx.child(
            [
                (
                    "parent_sitrep_id".to_string(),
                    format!("{parent_sitrep_id:?}"),
                ),
                (
                    "inv_collection_id".to_string(),
                    inv_collection_id.to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        // Prepare analysis inputs.
        let (inputs, prep_status) = match self
            .prepare_inputs(&opctx, parent_sitrep, inv)
            .await
        {
            Ok(inputs) => inputs,
            Err(err) => {
                let error = InlineErrorChain::new(&*err);
                slog::error!(opctx.log, "preparing analysis inputs failed"; &error);
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    outcome: status::Outcome::PreparationError(
                        error.to_string(),
                    ),
                };
            }
        };

        // Okay, actually run analysis and generate a new sitrep.
        let outcome = self.analyze(&opctx, inputs).await;

        FmAnalysisStatus {
            parent_sitrep_id,
            inv_collection_id: Some(inv_collection_id),
            outcome: status::Outcome::RanAnalysis {
                prep_status,
                analysis_status: outcome,
            },
        }
    }

    async fn prepare_inputs(
        &mut self,
        opctx: &OpContext,
        parent_sitrep: Option<CurrentSitrep>,
        inv: Arc<inventory::Collection>,
    ) -> anyhow::Result<(fm::analysis_input::Input, status::PreparationStatus)>
    {
        let mut builder =
            fm::analysis_input::Input::builder(parent_sitrep, inv);
        let mut errors = Vec::new();
        self.load_new_ereports(opctx, &mut builder, &mut errors)
            .await
            .context("failed to load new ereports")?;

        let (input, report) = builder.build();
        Ok((input, status::PreparationStatus { errors, report }))
    }

    async fn load_new_ereports(
        &mut self,
        opctx: &OpContext,
        builder: &mut fm::analysis_input::Builder,
        errors: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        let mut paginator = Paginator::new(
            nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let prev_total = builder.num_ereports();
            let batch = self
                .datastore
                .ereports_list_unmarked(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|e| {
                (e.restart_id.into_untyped_uuid(), e.ena)
            });
            let loaded = batch.len();
            let mut invalid = 0;
            builder.add_unmarked_ereports(batch.into_iter().filter_map(
                |ereport| {
                    let ereport = match fm::Ereport::try_from(ereport) {
                        Ok(ereport) => ereport,
                        Err(e) => {
                            invalid += 1;
                            errors.push(e.to_string());
                            return None;
                        }
                    };

                    Some(ereport)
                },
            ));

            let total = builder.num_ereports();
            let new = total - prev_total;
            if invalid > 0 {
                slog::warn!(
                    &opctx.log,
                    "loaded {loaded} ereports, {new} new, {invalid} invalid"
                );
            } else {
                slog::debug!(&opctx.log, "loaded {loaded} ereports, {new} new");
            }
        }

        Ok(())
    }

    async fn analyze(
        &mut self,
        opctx: &OpContext,
        inputs: fm::analysis_input::Input,
    ) -> status::AnalysisStatus {
        let start_time = Utc::now();
        let mut sitrep_builder = fm::SitrepBuilder::new(&opctx.log, &inputs);
        let result = fm::diagnosis::analyze(&inputs, &mut sitrep_builder);
        let end_time = Utc::now();
        let (sitrep, report) = sitrep_builder.build(self.nexus_id, end_time);

        // Did it work?
        if let Err(e) = result {
            let err = InlineErrorChain::new(&*e);
            slog::error!(&opctx.log, "fault management analysis failed"; "err" => %err);
            return status::AnalysisStatus {
                start_time,
                end_time,
                report,
                outcome: status::AnalysisOutcome::Error(e.to_string()),
            };
        }

        // TODO(eliza): diff the sitrep against the parent, and return
        // `Unchanged` if it's the same.
        let unchanged = true;
        if unchanged {
            slog::info!(
                &opctx.log,
                "fault management analysis produced no changes from the \
                 current sitrep"
            );
            return status::AnalysisStatus {
                start_time,
                end_time,
                report,
                outcome: status::AnalysisOutcome::Unchanged,
            };
        }

        let sitrep_id = sitrep.id();
        match self.datastore.fm_sitrep_insert(opctx, sitrep).await {
            Ok(()) => {
                slog::info!(&opctx.log, "updated the current sitrep!");
                // If we committed a new sitrep, we ought to go ahead and load it
                // now...
                self.sitrep_loader.activate();
                status::AnalysisStatus {
                    start_time,
                    end_time,
                    report,
                    outcome: status::AnalysisOutcome::Committed { sitrep_id },
                }
            }
            Err(datastore::fm::InsertSitrepError::ParentNotCurrent(_)) => {
                slog::info!(
                    &opctx.log,
                    "new sitrep was not committed as the parent sitrep was \
                     out of date";
                );
                // We are behind, activate the sitrep loader to try and catch up!
                self.sitrep_loader.activate();
                // Also, we should probably clean up after ourselves...
                self.sitrep_gc.activate();

                status::AnalysisStatus {
                    start_time,
                    end_time,
                    report,
                    outcome: status::AnalysisOutcome::NotCommitted {
                        sitrep_id,
                    },
                }
            }
            Err(datastore::fm::InsertSitrepError::Other(e)) => {
                let err = InlineErrorChain::new(&e);
                slog::error!(&opctx.log, "failed to insert sitrep"; "err" => %err);
                status::AnalysisStatus {
                    start_time,
                    end_time,
                    report,
                    outcome: status::AnalysisOutcome::CommitFailed {
                        sitrep_id,
                        error: e.to_string(),
                    },
                }
            }
        }
    }
}
