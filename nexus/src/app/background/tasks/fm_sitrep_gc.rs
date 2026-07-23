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
use nexus_db_queries::db::datastore::fm::HistoryPruningParams;
use nexus_db_queries::db::datastore::fm::HistoryPruningResult;
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
        // want to try to GC rows that were already orphaned.
        let mut history_pruning_status =
            status::HistoryPruningStatus::default();
        let history_pruning_outcome =
            self.prune_history(opctx, &mut history_pruning_status).await;

        let mut status = Status {
            history_limit: self.history_limit.get(),
            history_pruning_status,
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

    async fn prune_history(
        &self,
        opctx: &OpContext,
        status: &mut status::HistoryPruningStatus,
    ) -> status::HistoryPruningOutcome {
        let params = HistoryPruningParams {
            limit: self.history_limit,
            batch_size: self.batch_size,
        };
        // Each call to `fm_sitrep_history_prune` deletes at most BATCH_SIZE of
        // the oldest history table entries, so we must keep calling it until it
        // tells us that there's nothing left to prune (or until it fails).
        loop {
            match self.datastore.fm_sitrep_history_prune(&opctx, params).await {
                // If we have never pruned any sitreps, and the first call
                // indicates that there are no sitreps pruned, then we haven't
                // done anything and can complete immediately.
                Ok(HistoryPruningResult::NotPruned { count })
                    if status.sitreps_pruned == 0 =>
                {
                    slog::debug!(
                        &opctx.log,
                        "sitrep history depth is within the limit, no records \
                         will be pruned";
                        "sitrep_history_count" => count,
                        "sitrep_history_limit" => self.history_limit.get(),
                    );
                    break status::HistoryPruningOutcome::NotPruned { count };
                }
                // Otherwise, if the call indicates nothing was pruned, that
                // means we have completed pruning the table.
                Ok(HistoryPruningResult::NotPruned { count }) => {
                    slog::info!(
                        &opctx.log,
                        "finished pruning the sitrep history table";
                        "sitrep_history_count" => count,
                        "sitrep_history_limit" => self.history_limit.get(),
                        "total_sitreps_pruned" => status.sitreps_pruned,
                        "versions_pruned" => ?status
                            .versions_pruned.as_ref(),
                        "batches" => status.batches,
                        "batch_size" => self.batch_size.get(),
                    );
                    break status::HistoryPruningOutcome::Pruned { count };
                }
                Ok(HistoryPruningResult::Pruned {
                    n_pruned,
                    oldest_pruned,
                    newest_pruned,
                }) => {
                    slog::debug!(
                        &opctx.log,
                        "pruned a batch of old sitreps from the end of the \
                         history table";
                        "sitrep_history_limit" => self.history_limit.get(),
                        "sitreps_pruned" => n_pruned,
                        "versions_pruned" => ?oldest_pruned..=newest_pruned,
                    );
                    status.batches += 1;
                    status.sitreps_pruned += n_pruned;
                    // The overall range of versions pruned by this activation
                    // starts wherever the first batch did.
                    let oldest = status
                        .versions_pruned
                        .as_ref()
                        .map_or(oldest_pruned, |r| *r.start());
                    status.versions_pruned = Some(oldest..=newest_pruned);
                }
                Err(e) => {
                    let error = InlineErrorChain::new(&e);
                    slog::error!(
                        &opctx.log,
                        "pruning the sitrep history table failed";
                        "sitrep_history_limit" => self.history_limit.get(),
                        "total_sitreps_pruned" => status.sitreps_pruned,
                        "versions_pruned" => ?status
                            .versions_pruned.as_ref(),
                        "batches" => status.batches,
                        "batch_size" => self.batch_size.get(),
                        &error
                    );
                    break status::HistoryPruningOutcome::Error(
                        error.to_string(),
                    );
                }
            }
        }
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
            status.history_pruning_outcome,
            status::HistoryPruningOutcome::NotPruned { count: 2 }
        );
        assert_eq!(status.history_pruning_status.sitreps_pruned, 0);

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
        assert_eq!(
            status.history_pruning_outcome,
            status::HistoryPruningOutcome::NotPruned { count: 3 }
        );

        // Exactly at the limit: the limit check fires, but the newest `LIMIT`
        // versions are the entire history, so nothing is actually deleted.
        model.insert_history(opctx, 2).await; // v4, v5
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_outcome,
            status::HistoryPruningOutcome::NotPruned { count: 5 }
        );

        // Over the limit: versions 1..=3 should be pruned from the history,
        // and the sitreps they referenced --- now orphaned --- should be
        // deleted by the orphan sweep in the same activation.
        model.insert_history(opctx, 3).await; // v6..=v8
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_status,
            status::HistoryPruningStatus {
                batches: 1,
                sitreps_pruned: 3,
                versions_pruned: Some(1..=3),
            }
        );
        assert_eq!(
            status.history_pruning_outcome,
            status::HistoryPruningOutcome::Pruned { count: 5 }
        );
        assert_eq!(status.orphaned_sitreps_deleted, 3);

        // Exactly at the limit again, but this time the earliest history
        // version is v4, not v1, since we pruned v1 previously. This is the
        // expected steady state of the system, since we try to keep exactly
        // `LIMIT` rows in the history table.
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(
            status.history_pruning_outcome,
            status::HistoryPruningOutcome::NotPruned { count: 5 }
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
            status::HistoryPruningStatus {
                batches: 1,
                sitreps_pruned: 2,
                versions_pruned: Some(4..=5),
            }
        );
        assert_eq!(
            status.history_pruning_outcome,
            status::HistoryPruningOutcome::Pruned { count: 5 }
        );
        assert_eq!(status.orphaned_sitreps_deleted, 2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests that history pruning always converges on the same eventual
    /// outcome regardless of the batch size, even when the batch size forces
    /// the pruning loop to run many times per activation.
    #[tokio::test]
    async fn test_history_pruning_batched() {
        let logctx = dev::test_setup_log("test_history_pruning_batched");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const LIMIT: u32 = 5;
        // How far over the limit the history table goes before each GC
        // activation.
        const EXCESS_HISTORY: u32 = 7;
        let mut task = SitrepGc::new(datastore.clone());
        task.history_limit = NonZeroU32::new(LIMIT).unwrap();

        let mut model = SitrepModel::new(datastore.clone());

        // Run the same scenario with a variety of batch sizes: one row at a
        // time, a batch size that divides the excess records evenly, unevenly,
        // exactly one batch, and one much larger than everything batch (which
        // also happens to be the SQL_BATCH_SIZE). The eventual outcome must
        // always be the same; only the number of batches it takes to get there
        // may differ.
        for (batch_size, expected_batches) in
            [(1, 7), (2, 4), (3, 3), (7, 1), (1000, 1)]
        {
            eprintln!("--- batch_size: {batch_size} ---");
            task.batch_size = NonZeroU32::new(batch_size).unwrap();

            // Insert new sitreps until the history is `EXCESS_HISTORY` entries over
            // the limit.
            let count = u32::try_from(model.history_count()).unwrap();
            let needed = (LIMIT + EXCESS_HISTORY) - count;
            model.insert_history(opctx, needed as usize).await;

            // `run_gc_and_check` asserts that the pruning status, outcome,
            // and the state of the database all match the model.
            let status = run_gc_and_check(&mut task, &mut model, opctx).await;
            assert_eq!(
                status.history_pruning_status.sitreps_pruned,
                EXCESS_HISTORY as usize
            );
            assert_eq!(
                status.history_pruning_outcome,
                status::HistoryPruningOutcome::Pruned { count: LIMIT.into() }
            );
            // ...and did we actually report that we did that many batches?
            assert_eq!(status.history_pruning_status.batches, expected_batches);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Run a GC activation and check both the task's reported status and
    /// the database contents against the model's simulation of what GC
    /// should do with the task's configured pruning parameters.
    async fn run_gc_and_check(
        task: &mut SitrepGc,
        model: &mut SitrepModel,
        opctx: &OpContext,
    ) -> Status {
        let status = dbg!(task.actually_activate(opctx).await);
        let expected = model.simulate_gc(task.history_limit);
        assert_eq!(
            status.history_pruning_status.sitreps_pruned,
            expected.sitreps_pruned,
            "the number of history entries pruned should match the model's \
             simulation"
        );
        assert_eq!(
            status.history_pruning_status.versions_pruned,
            expected.versions_pruned,
            "the range of versions pruned should match the model's simulation"
        );
        assert_eq!(
            status.history_pruning_outcome, expected.outcome,
            "the task's reported pruning outcome should match the model's \
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
