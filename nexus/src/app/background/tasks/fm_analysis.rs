// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::FmAnalysisStatus;
use nexus_types::internal_api::background::fm_analysis;
use nexus_types::inventory;
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
            let status = self.analyze(opctx).await;
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

    async fn analyze(&mut self, opctx: &OpContext) -> FmAnalysisStatus {
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
                outcome: fm_analysis::Outcome::WaitingForInventory,
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
        let outcome = self
            .actually_analyze(&opctx, parent_sitrep, inv)
            .await
            .unwrap_or_else(|err| {
                let error = InlineErrorChain::new(&*err);
                slog::error!(opctx.log, "fault management analysis failed!"; "error" => &error);
                fm_analysis::Outcome::Error(error.to_string())
            });

        FmAnalysisStatus {
            parent_sitrep_id,
            inv_collection_id: Some(inv_collection_id),
            outcome,
        }
    }

    async fn actually_analyze(
        &mut self,
        opctx: &OpContext,
        parent_sitrep: Option<CurrentSitrep>,
        inv: Arc<inventory::Collection>,
    ) -> anyhow::Result<fm_analysis::Outcome> {
        anyhow::bail!("todo: eliza draw the rest of the owl")
    }
}
