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
    activators: Activators,
    nexus_id: OmicronZoneUuid,
}

/// This is just because I don't like it when a constructor takes multiple
/// positional arguments of the same type...
#[derive(Clone)]
pub struct Activators {
    pub inventory_loader: Activator,
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
        Self { datastore, sitrep_rx, inv_rx, activators, nexus_id }
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

        // First, ensure that we are operating on an inventory collection which
        // is newer than the inventory included in the parent sitrep. It is
        // necessary to check for this because *this Nexus*'s `inventory_load`
        // background task may be have loaded an older inventory collection than
        // the Nexus that produced the parent sitrep, and we wish to avoid
        // generating a sitrep with stale inventory data, since this could cause
        // us to output a sitrep based on an earlier state of the system than
        // the current one.
        let parent_inv_id =
            parent_sitrep.as_ref().map(|s| s.1.metadata.inv_collection_id);
        if let Some(parent_inv_id) = parent_inv_id
            && parent_inv_id != inv_collection_id
        {
            let loaded_inv = inv.metadata();
            match self
                .datastore
                .inventory_collection_read_metadata(&opctx, parent_inv_id)
                .await
            {
                // If the loaded inventory collection is not strictly newer than
                // the parent sitrep's inventory collection (i.e. collecting it
                // started after the parent collection finished), we must wait
                // for a newer collection to be loaded before performing
                // analysis.
                Ok(Some(parent_inv))
                    if !loaded_inv.is_strictly_newer_than(&parent_inv) =>
                {
                    slog::info!(
                        opctx.log,
                        "refusing to perform fault management analysis based \
                         on an inventory collection that is older than the \
                         collection in the parent sitrep";
                        "inv_collection_time_started" =>
                            %loaded_inv.time_started,
                        "inv_collection_time_done" => %loaded_inv.time_done,
                        "parent_inv_id" => %parent_inv.id,
                        "parent_inv_time_started" =>  %parent_inv.time_started,
                        "parent_inv_time_done" =>  %parent_inv.time_done,
                    );
                    // Activate the inventory loader so that it will
                    // (hopefully) load a newer collection.
                    self.activators.inventory_loader.activate();
                    return FmAnalysisStatus {
                        parent_sitrep_id,
                        inv_collection_id: Some(inv_collection_id),
                        outcome: status::Outcome::InventoryStale {
                            parent_inv,
                            loaded_inv,
                        },
                    };
                }
                // Otherwise, the loaded inventory collection is newer than the
                // one in the parent sitrep. This is the case if
                // `inventory_collection_read_metadata` returns `None`, since if
                // the parent sitrep's inventory collection has been garbage
                // collected, it is definitely older than the loaded one.
                Ok(_) => {}
                Err(error) => {
                    let error = InlineErrorChain::new(&error);
                    const MSG: &str = "failed to read parent sitrep's \
                         inventory collection metadata";
                    slog::error!(
                        opctx.log,
                        "{MSG}";
                        "parent_inv_id" => %parent_inv_id,
                        &error,
                    );
                    return FmAnalysisStatus {
                        parent_sitrep_id,
                        inv_collection_id: Some(inv_collection_id),
                        outcome: status::Outcome::PreparationError(format!(
                            "{MSG}: {error}"
                        )),
                    };
                }
            }
        }

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
                self.activators.sitrep_loader.activate();
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
                self.activators.sitrep_loader.activate();
                // Also, we should probably clean up after ourselves...
                self.activators.sitrep_gc.activate();

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
