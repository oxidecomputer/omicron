// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for fault management sitrep garbage collection.

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::datastore::fm::GcOrphansResult;
use nexus_types::internal_api::background::SitrepGcStatus as Status;
use nexus_types::internal_api::background::fm_sitrep_gc as status;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::num::NonZeroU32;
use std::sync::Arc;

pub struct SitrepGc {
    datastore: Arc<DataStore>,
    history_limit: NonZeroU32,
    batch_size: NonZeroU32,
}

impl BackgroundTask for SitrepGc {
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

impl SitrepGc {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self {
            datastore,
            history_limit: Self::DEFAULT_HISTORY_LIMIT,
            batch_size: datastore::SQL_BATCH_SIZE,
        }
    }

    // Limits for abandoning sitreps from the history table.
    pub const DEFAULT_HISTORY_LIMIT: NonZeroU32 =
        NonZeroU32::new(2000).unwrap();

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        // First, try to prune the sitrep history if it's over the threshold.
        // Doing this first ensures that we will then attempt to GC any rows
        // abandoned by pruning the history. However, if this fails, we still
        // want to try to GC rows that were already orphaned, so don't `?` this!
        let mut history_pruning = status::HistoryPruningStatus::default();
        let pruning_params = datastore::fm::HistoryPruningParams {
            limit: self.history_limit,
            batch_size: self.batch_size,
        };
        let history_pruning_outcome = loop {
            match self.datastore.fm_sitrep_history_prune(&opctx, params).await {
                Ok(status::HistoryPruningStatus::NotPruned { count }) => {
                    slog::debug!(
                        &opctx.log,
                        "sitrep history depth is below the limit, no entries were \
                         pruned";
                        "sitrep_history_count" => count,
                        "sitrep_history_limit" => self.history_limit.get(),
                    );
                    Ok(status::HistoryPruningStatus::NotPruned { count })
                }
                Ok(status::HistoryPruningStatus::Pruned {
                    n_pruned,
                    newest_version_pruned,
                }) => {
                    slog::info!(
                        &opctx.log,
                        "pruned old sitreps from the end of the history table";
                        "sitrep_history_limit" => self.history_limit.get(),
                        "sitreps_pruned" => n_pruned,
                        "newest_version_pruned" => newest_version_pruned

                    );
                    Ok(status::HistoryPruningStatus::Pruned {
                        n_pruned,
                        newest_version_pruned,
                    })
                }
                Err(e) => {
                    let error = InlineErrorChain::new(&e);
                    slog::error!(
                        &opctx.log,
                        "pruning the sitrep history table failed";
                        "sitrep_history_limit" => self.history_limit.get(),
                        &error
                    );
                    break status::HistoryPruningOutcome::Error(
                        error.to_string(),
                    );
                }
            };
        };

        let mut status = Status {
            history_pruning_status: pruning_status,
            history_pruning_outcome,
            orphaned_sitreps_deleted: 0,
            sitrep_metadata_batches: 0,
            batch_size: self.batch_size.get(),
            child_tables: Default::default(),
            errors: Vec::new(),
        };

        match self.datastore.fm_sitrep_gc_orphans(&opctx, self.batch_size).await
        {
            Ok(GcOrphansResult {
                sitreps_deleted,
                sitrep_metadata_batches,
                child_tables,
            }) => {
                status.orphaned_sitreps_deleted = sitreps_deleted;
                status.sitrep_metadata_batches = sitrep_metadata_batches;
                status.child_tables = child_tables
                    .into_iter()
                    .map(|(table, stats)| {
                        (
                            table.table_name().to_string(),
                            status::ChildTableGcStats {
                                rows_deleted: stats.rows_deleted,
                                batches: stats.batches,
                            },
                        )
                    })
                    .collect();
            }
            Err(err) => {
                let error = InlineErrorChain::new(&err);
                const MSG: &str = "failed to GC orphaned sitreps";
                slog::error!(&opctx.log, "{MSG}"; &error);
                status.errors.push(format!("{MSG}: {error}"));
            }
        }

        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::fm::SitrepModel;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_orphaned_sitrep_gc() {
        let logctx = dev::test_setup_log("test_orphaned_sitrep_gc");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut task = SitrepGc::new(datastore.clone());
        let mut model = SitrepModel::new(datastore.clone());

        // First, insert an initial sitrep (v1).
        model.insert_history(opctx, 1).await;

        // Now, create some orphaned sitreps which also have no parent.
        for _ in 0..4 {
            model.insert_orphan(opctx, None).await;
        }

        // Next, create a new current sitrep (v2), which descends from v1.
        model.insert_history(opctx, 1).await;

        // Now, create some orphaned sitreps which also descend from the
        // (no longer current) v1.
        let stale_parent = Some(model.history[0]);
        for _ in 0..3 {
            model.insert_orphan(opctx, stale_parent).await;
        }

        // Make sure everything, including the orphans, exists.
        model.assert_matches(opctx).await;

        // Activate the background task. The orphans should all be gone,
        // while the current sitrep and its ancestor remain.
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        // Independently of the model's simulation: all 7 orphans were
        // deleted, and the (well under-limit) history was not pruned.
        assert_eq!(status.orphaned_sitreps_deleted, 7);
        assert_eq!(
            status.history_pruning_status,
            Ok(status::HistoryPruningStatus::NotPruned { count: 2 })
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests that the GC task prunes the sitrep history table down to (at
    /// most) `history_limit` entries, and that the sitreps whose history
    /// entries were pruned are garbage-collected as orphans *in the same
    /// activation* (i.e., pruning runs before the orphan sweep).
    #[tokio::test]
    async fn test_history_pruning() {
        let logctx = dev::test_setup_log("test_history_pruning");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const LIMIT: u32 = 5;
        let mut task = SitrepGc::new(datastore.clone());
        task.history_limit = NonZeroU32::new(LIMIT).unwrap();

        let mut model = SitrepModel::new(datastore.clone());

        // Below the limit: nothing should be pruned.
        model.insert_history(opctx, 3).await; // v1..=v3
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(status.history_limit, LIMIT);
        assert_eq!(
            status.history_pruning_status,
            Ok(status::HistoryPruningStatus::NotPruned { count: 3 })
        );

        // Exactly at the limit: the limit check fires, but the newest `LIMIT`
        // versions are the entire history, so nothing is actually deleted.
        model.insert_history(opctx, 2).await; // v4, v5
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_status,
            Ok(status::HistoryPruningStatus::NotPruned { count: 5 })
        );

        // Over the limit: versions 1..=3 should be pruned from the history,
        // and the sitreps they referenced --- now orphaned --- should be
        // deleted by the orphan sweep in the same activation.
        model.insert_history(opctx, 3).await; // v6..=v8
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_status,
            Ok(status::HistoryPruningStatus::Pruned {
                n_pruned: 3,
                newest_version_pruned: 3,
            })
        );
        assert_eq!(status.orphaned_sitreps_deleted, 3);

        // Exactly at the limit again, but this time the earliest history
        // version is v4, not v1, since we pruned v1 previously. This is the
        // expected steady state of the system, since we try to keep exactly
        // `LIMIT` rows in the history table.
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_status,
            Ok(status::HistoryPruningStatus::NotPruned { count: 5 })
        );
        assert_eq!(status.orphaned_sitreps_deleted, 0);

        // Prune again, now that the minimum history version is no longer 1.
        // This checks that the pruning arithmetic is anchored on the latest
        // version rather than on the row count: history is v4..=v10 (7 rows),
        // so v4 and v5 should go.
        model.insert_history(opctx, 2).await; // v9, v10
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_status,
            Ok(status::HistoryPruningStatus::Pruned {
                n_pruned: 2,
                newest_version_pruned: 5,
            })
        );
        assert_eq!(status.orphaned_sitreps_deleted, 2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Run a GC activation and check both the task's reported status and
    /// the database contents against the model's simulation of what GC
    /// should do with the task's configured `history_limit`.
    async fn run_gc_and_check(
        task: &mut SitrepGc,
        model: &mut SitrepModel,
        opctx: &OpContext,
    ) -> Status {
        let status = dbg!(task.actually_activate(opctx).await);
        let expected = model.simulate_gc(task.history_limit);
        assert_eq!(
            status.history_pruning_status,
            Ok(expected.pruning),
            "the task's reported pruning status should match the model's \
             simulation"
        );
        assert_eq!(
            status.orphaned_sitreps_deleted, expected.orphans_deleted,
            "the task's reported orphan deletions should match the model's \
             simulation"
        );
        assert_eq!(status.errors, Vec::<String>::new());
        model.assert_matches(opctx).await;
        status
    }
}
