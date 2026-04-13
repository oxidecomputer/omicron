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

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use nexus_types::fm::SitrepVersion;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::SitrepUuid;

    fn activators() -> Activators {
        let a = Activators {
            inventory_loader: Activator::new(),
            sitrep_loader: Activator::new(),
            sitrep_gc: Activator::new(),
        };
        a.inventory_loader.mark_wired_up().unwrap();
        a.sitrep_loader.mark_wired_up().unwrap();
        a.sitrep_gc.mark_wired_up().unwrap();
        a
    }

    /// Create a [`CurrentSitrep`] whose metadata references the given
    /// inventory collection ID.
    fn make_current_sitrep(inv_collection_id: CollectionUuid) -> CurrentSitrep {
        let id = SitrepUuid::new_v4();
        Arc::new((
            SitrepVersion { id, version: 1, time_made_current: Utc::now() },
            Sitrep {
                metadata: SitrepMetadata {
                    id,
                    parent_sitrep_id: None,
                    inv_collection_id,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep".to_string(),
                    time_created: Utc::now(),
                },
                cases: Default::default(),
                ereports_by_id: Default::default(),
            },
        ))
    }

    #[tokio::test]
    async fn test_inventory_staleness_check() {
        let logctx = dev::test_setup_log("test_inventory_staleness_check");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Build two inventory collections.  `CollectionBuilder` uses
        // `now()` for timestamps, so building them sequentially gives us
        // collections whose timestamps may overlap.  Force explicit
        // timestamps to guarantee the ordering we need:
        //
        //   older:  [time_started ............. time_done]
        //   newer:                                         [time_started ... time_done]
        //
        // `newer.time_started > older.time_done`  ⟹  newer is *strictly*
        // newer than older.
        let older = CollectionBuilder::new("test").build();
        let mut newer = CollectionBuilder::new("test").build();
        newer.time_started = older.time_done + chrono::Duration::seconds(1);
        newer.time_done = newer.time_started + chrono::Duration::seconds(1);

        // Also build a collection that *overlaps* with `newer`: it started
        // during newer's collection window, so it is NOT strictly newer.
        //
        //   newer:       [time_started ............ time_done]
        //   overlapping:        [time_started ........................ time_done]
        let mut overlapping = CollectionBuilder::new("test").build();
        overlapping.time_started =
            newer.time_started + chrono::Duration::milliseconds(500);
        overlapping.time_done = newer.time_done + chrono::Duration::seconds(1);

        // Insert all three into the database so that
        // `inventory_collection_read_metadata` can find them.
        datastore
            .inventory_insert_collection(opctx, &older)
            .await
            .expect("inserted older collection");
        datastore
            .inventory_insert_collection(opctx, &newer)
            .await
            .expect("inserted newer collection");
        datastore
            .inventory_insert_collection(opctx, &overlapping)
            .await
            .expect("inserted overlapping collection");

        let older = Arc::new(older);
        let newer = Arc::new(newer);
        let overlapping = Arc::new(overlapping);

        // If our "currently loaded" inventory was collected before the one in
        // the parent sitrep, we should refuse to run analysis and return
        // `InventoryStale`.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(newer.id)));
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            match &result.outcome {
                status::Outcome::InventoryStale { parent_inv, loaded_inv } => {
                    assert_eq!(parent_inv.id, newer.id);
                    assert_eq!(loaded_inv.id, older.id);
                }
                other => panic!("expected InventoryStale, got {other:?}"),
            }
        }

        // If the parent sitrep's inventory collection is strictly newer than
        // the one in the parent sitrep, analysis should proceed.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(older.id)));
            let (_inv_tx, inv_rx) = watch::channel(Some(newer.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(newer.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::InventoryStale { .. }
                ),
                "expected analysis to proceed, got: {:?}",
                result.outcome,
            );
        }

        // If the parent sitrep references an inventory collection ID that no
        // longer exists in the database, we assume that it has been deleted and
        // must therefore be older than ours.
        {
            let parent_inv_id = CollectionUuid::new_v4();
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(parent_inv_id)));
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::InventoryStale { .. }
                ),
                "expected analysis to proceed when parent sitrep's inventory \
                 collection does not exist in CRDB, got: {:?}",
                result.outcome,
            );
        }

        // When there is no parent sitrep, the staleness check is
        // skipped entirely and analysis proceeds.
        {
            let (_sitrep_tx, sitrep_rx) = watch::channel(None);
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::InventoryStale { .. }
                ),
                "expected analysis to proceed with no parent sitrep, \
                 got: {:?}",
                result.outcome,
            );
        }

        // When the parent sitrep's inventory collection and the currently
        // loaded collection are the same, we should still proceed with FM
        // analysis (i.e. if we were triggered by ereports rather than an
        // inventory change...)
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(older.id)));
            let (_inv_tx, inv_rx) = watch::channel(Some(older.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(older.id));
            assert!(
                !matches!(
                    &result.outcome,
                    status::Outcome::InventoryStale { .. }
                ),
                "expected analysis to proceed with same inventory \
                 collection ID, got: {:?}",
                result.outcome,
            );
        }

        // If our "currently loaded" inventory collection overlaps with the
        // parent sitrep's inventory collection --- i.e. it started while the
        // parent's collection was still in progress --- it is NOT strictly
        // newer, so we should refuse to run analysis.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(newer.id)));
            let (_inv_tx, inv_rx) = watch::channel(Some(overlapping.clone()));
            let mut task = FmAnalysis::new(
                datastore.clone(),
                sitrep_rx,
                inv_rx,
                activators(),
                OmicronZoneUuid::new_v4(),
            );

            let result = task.actually_activate(opctx).await;
            assert_eq!(result.inv_collection_id, Some(overlapping.id));
            match &result.outcome {
                status::Outcome::InventoryStale { parent_inv, loaded_inv } => {
                    assert_eq!(parent_inv.id, newer.id);
                    assert_eq!(loaded_inv.id, overlapping.id);
                }
                other => panic!(
                    "expected InventoryStale for overlapping collections, \
                     got {other:?}"
                ),
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
