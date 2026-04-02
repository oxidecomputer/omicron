// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use anyhow::Context;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_fm as fm;
use nexus_types::internal_api::background::FmAnalysisStatus;
use nexus_types::internal_api::background::fm_analysis as status;
use nexus_types::inventory;
use omicron_uuid_kinds::GenericUuid;
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
        sitrep_loader: Activator,
    ) -> Self {
        Self { datastore, sitrep_rx, inv_rx, sitrep_loader }
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
        let outcome = self
            .analyze(&opctx, inputs)
            .await
            .unwrap_or_else(|err| {
                let error = InlineErrorChain::new(&*err);
                slog::error!(opctx.log, "fault management analysis failed!"; &error);
                status::AnalysisOutcome::Error(error.to_string())
            });

        if let status::AnalysisOutcome::Committed { .. } = &outcome {
            // If we commmitted a new sitrep, we ought to go ahead and load it
            // now...
            self.sitrep_loader.activate();
        }

        FmAnalysisStatus {
            parent_sitrep_id,
            inv_collection_id: Some(inv_collection_id),
            outcome: status::Outcome::RanAnalysis { prep_status, outcome },
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

        let (input, report) = builder.finish();
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
        _opctx: &OpContext,
        _inputs: fm::analysis_input::Input,
    ) -> anyhow::Result<status::AnalysisOutcome> {
        anyhow::bail!("FM analysis is not yet implemented")
    }
}
