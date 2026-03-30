// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::fm;
use nexus_types::internal_api::background::FmAnalysisStatus;
use nexus_types::internal_api::background::fm_analysis;
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
                prep: fm_analysis::PreparationStatus::default(),
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

        let mut prep_status = fm_analysis::PreparationStatus::default();
        let new_ereports = match self
            .load_new_ereports(
                &opctx,
                parent_sitrep.as_ref().map(|s| &s.1),
                &mut prep_status,
            )
            .await
        {
            Ok(ereports) => ereports,
            Err(err) => {
                let error = InlineErrorChain::new(&*err);
                slog::error!(opctx.log, "failed to load new ereports!"; &error);
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    prep: prep_status,
                    outcome: fm_analysis::Outcome::Error(format!(
                        "failed to load new ereports: {error}"
                    )),
                };
            }
        };

        let outcome = self
            .analyze(&opctx, parent_sitrep, inv, new_ereports)
            .await
            .unwrap_or_else(|err| {
                let error = InlineErrorChain::new(&*err);
                slog::error!(opctx.log, "fault management analysis failed!"; &error);
                fm_analysis::Outcome::Error(error.to_string())
            });

        FmAnalysisStatus {
            parent_sitrep_id,
            inv_collection_id: Some(inv_collection_id),
            prep: prep_status,
            outcome,
        }
    }

    async fn load_new_ereports(
        &mut self,
        opctx: &OpContext,
        parent_sitrep: Option<&fm::Sitrep>,
        prep_status: &mut fm_analysis::PreparationStatus,
    ) -> anyhow::Result<IdOrdMap<fm::Ereport>> {
        let mut ereports = IdOrdMap::default();
        let mut paginator = Paginator::new(
            nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let prev_total = ereports.len();
            let batch = self
                .datastore
                .ereports_list_unseen(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|e| {
                (e.restart_id.into_untyped_uuid(), e.ena)
            });
            let loaded = batch.len();
            let mut invalid = 0;
            ereports.extend(batch.into_iter().filter_map(|ereport| {
                let ereport = match fm::Ereport::try_from(ereport) {
                    Ok(ereport) => ereport,
                    Err(e) => {
                        invalid += 1;
                        prep_status.errors.push(e.to_string());
                        return None;
                    }
                };

                if let Some(sitrep) = parent_sitrep {
                    if sitrep.ereports_by_id.contains_key(ereport.id()) {
                        prep_status.ereports_in_parent_sitrep_not_marked += 1;
                        return None;
                    }
                }
                prep_status.new_ereports.insert(*ereport.id());
                Some(ereport)
            }));

            let total = ereports.len();
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

        Ok(ereports)
    }

    async fn analyze(
        &mut self,
        _opctx: &OpContext,
        _parent_sitrep: Option<CurrentSitrep>,
        _inv: Arc<inventory::Collection>,
        _new_ereports: IdOrdMap<fm::Ereport>,
    ) -> anyhow::Result<fm_analysis::Outcome> {
        anyhow::bail!("FM analysis is not yet implemented")
    }
}
