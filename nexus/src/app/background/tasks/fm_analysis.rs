// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use anyhow::Context;
use chrono::Utc;
use fm::analysis_input::InvalidInputs;
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
        // Snapshot the static known-classes set once, up front, so it's
        // reported in the activation status regardless of which outcome
        // variant fires.
        let known_classes: Vec<String> = fm::diagnosis::known_ereport_classes()
            .iter()
            .map(|s| (*s).to_string())
            .collect();

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
                known_classes,
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
            Err(PreparationError::Other(err)) => {
                let error = InlineErrorChain::new(&*err);
                slog::error!(
                    opctx.log,
                    "fault management analysis preparation failed";
                    &error,
                );
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    known_classes,
                    outcome: status::Outcome::PreparationError(
                        error.to_string(),
                    ),
                };
            }
            Err(PreparationError::InvalidInputs(
                InvalidInputs::InventoryStale {
                    parent_inv_id,
                    next_inv_min_time_started,
                    input_inv_time_started,
                },
            )) => {
                slog::info!(
                    opctx.log,
                    "fault management analysis: waiting for a newer inventory \
                     to be loaded";
                    "parent_sitrep_inv_id" => %parent_inv_id,
                    "next_inv_min_time_started" => %next_inv_min_time_started,
                    "input_inv_time_started" => %input_inv_time_started,
                );
                self.activators.inventory_loader.activate();
                return FmAnalysisStatus {
                    parent_sitrep_id,
                    inv_collection_id: Some(inv_collection_id),
                    known_classes,
                    outcome: status::Outcome::WaitingForNewerInventory {
                        parent_inv_id,
                        next_inv_min_time_started,
                        input_inv_time_started,
                    },
                };
            }
        };

        // Okay, actually run analysis and generate a new sitrep.
        let outcome = self.analyze(&opctx, inputs).await;

        FmAnalysisStatus {
            parent_sitrep_id,
            inv_collection_id: Some(inv_collection_id),
            known_classes,
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
    ) -> Result<
        (fm::analysis_input::Input, status::PreparationStatus),
        PreparationError,
    > {
        let mut builder =
            fm::analysis_input::Input::builder(parent_sitrep, inv)?;
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
        // Only surface ereports a diagnosis engine will consume.
        let classes = fm::diagnosis::known_ereport_classes();
        let mut paginator = Paginator::new(
            nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let prev_total = builder.num_ereports();
            let batch = self
                .datastore
                .ereports_list_unmarked(opctx, classes, &p.current_pagparams())
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

        if let Some(parent) = inputs.parent_sitrep() {
            if parent.compare_state() == sitrep {
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

#[derive(Debug, thiserror::Error)]
enum PreparationError {
    #[error(transparent)]
    InvalidInputs(#[from] InvalidInputs),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
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

    /// Create a `CurrentSitrep` as though it was produced from the given
    /// inventory collection.
    fn make_current_sitrep(inv: &inventory::Collection) -> CurrentSitrep {
        let id = SitrepUuid::new_v4();
        Arc::new((
            SitrepVersion { id, version: 1, time_made_current: Utc::now() },
            Sitrep {
                metadata: SitrepMetadata {
                    id,
                    parent_sitrep_id: None,
                    inv_collection_id: inv.id,
                    next_inv_min_time_started: inv.time_done,
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

        // Build three inventory collections. We will manually set the
        // timestamps on these collections after building them, as
        // `CollectionBuilder` produces timestamps using `Utc::now()`.
        //
        // This is the oldest one:
        let older = {
            let mut older = CollectionBuilder::new("test (oldest)").build();
            older.time_done += chrono::Duration::seconds(1);
            Arc::new(older)
        };

        // This one started after `older` finished:
        let newer = {
            let mut newer = CollectionBuilder::new("test (new)").build();
            newer.time_started = older.time_done + chrono::Duration::seconds(1);
            newer.time_done = newer.time_started + chrono::Duration::seconds(1);
            Arc::new(newer)
        };

        // This one started after `older` started, but before `older` finished:
        let overlapping = {
            let mut overlapping =
                CollectionBuilder::new("test (overlapping)").build();
            overlapping.time_started =
                older.time_started + chrono::Duration::milliseconds(500);
            overlapping.time_done =
                older.time_done + chrono::Duration::seconds(1);
            Arc::new(overlapping)
        };

        // If our "currently loaded" inventory started before the parent
        // sitrep's `next_inv_min_time_started`, we should refuse to run
        // analysis and return `WaitingForNewerInventory`.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(&newer)));
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
                status::Outcome::WaitingForNewerInventory {
                    parent_inv_id,
                    next_inv_min_time_started,
                    input_inv_time_started,
                } => {
                    assert_eq!(*parent_inv_id, newer.id);
                    assert_eq!(*next_inv_min_time_started, newer.time_done,);
                    assert_eq!(*input_inv_time_started, older.time_started,);
                }
                other => {
                    panic!("expected WaitingForNewerInventory, got {other:?}")
                }
            }
        }

        // If the loaded inventory collection started at or after the parent
        // sitrep's `next_inv_min_time_started`, analysis should proceed.
        {
            let (_sitrep_tx, sitrep_rx) =
                watch::channel(Some(make_current_sitrep(&older)));
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
                    status::Outcome::WaitingForNewerInventory { .. }
                ),
                "expected analysis to proceed, got: {:?}",
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
                    status::Outcome::WaitingForNewerInventory { .. }
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
                watch::channel(Some(make_current_sitrep(&older)));
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
                    status::Outcome::WaitingForNewerInventory { .. }
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
                watch::channel(Some(make_current_sitrep(&older)));
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
                status::Outcome::WaitingForNewerInventory {
                    parent_inv_id,
                    next_inv_min_time_started,
                    input_inv_time_started,
                } => {
                    assert_eq!(*parent_inv_id, older.id);
                    assert_eq!(*next_inv_min_time_started, older.time_done,);
                    assert_eq!(
                        *input_inv_time_started,
                        overlapping.time_started,
                    );
                }
                other => panic!(
                    "expected WaitingForNewerInventory for overlapping \
                     collections, got {other:?}"
                ),
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
