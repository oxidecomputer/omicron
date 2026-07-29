// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pruning the fault management sitrep history.
//!
//! This task enforces an upper bound on the number of entries in the
//! `fm_sitrep_history` table by deleting the entries with the oldest version
//! numbers when the table exceeds the configured limit. Deleting a history
//! entry makes the sitrep it references an orphan, so whenever this task prunes
//! something, it activates the [`fm_sitrep_gc`](super::fm_sitrep_gc) background
//! task, which actually deletes the contents of the newly-orphaned sitreps.
//!
//! History pruning and orphan GC are deliberately separate tasks: pruning runs
//! in a loop until the history table is back within the limit, and if new
//! sitreps are being committed quickly, that loop may run for multiple batches.
//! Orphaned sitreps are produced both by pruning the history *and* when two
//! Nexii race to commit a new sitrep. Keeping these two related processes
//! separate ensures that one cannot prevent the other from running if it keeps
//! having more work to do.

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::datastore::fm::HistoryPruningParams;
use nexus_db_queries::db::datastore::fm::HistoryPruningResult;
use nexus_types::fm::FmConfigView;
use nexus_types::internal_api::background::SitrepHistoryPrunerStatus as Status;
use nexus_types::internal_api::background::fm_sitrep_history_pruner as status;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

pub struct SitrepHistoryPruner {
    datastore: Arc<DataStore>,
    /// Activator for the `fm_sitrep_gc` background task, which we poke
    /// whenever pruning has orphaned some sitreps for it to delete.
    sitrep_gc: Activator,
    cfg: watch::Receiver<Option<FmConfigView>>,
    /// Maximum batch size for deletion queries. This is currently hard-coded to
    /// [`SQL_BATCH_SIZE`] but is a field so that it can be overriden to a
    /// smaller value in tests.
    batch_size: NonZeroU32,
}

impl BackgroundTask for SitrepHistoryPruner {
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

impl SitrepHistoryPruner {
    pub fn new(
        datastore: Arc<DataStore>,
        sitrep_gc: Activator,
        cfg: watch::Receiver<Option<FmConfigView>>,
    ) -> Self {
        Self {
            datastore,
            sitrep_gc,
            cfg,
            batch_size: datastore::SQL_BATCH_SIZE,
        }
    }

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        let cfg = self.cfg.borrow().as_ref().map(|view| view.config);
        let Some(cfg) = cfg else {
            return Status::WaitingForConfig;
        };
        let thresh = cfg.history_pruning_threshold;
        let mut pruned = status::SitrepsPruned::default();
        let params =
            HistoryPruningParams { limit: thresh, batch_size: self.batch_size };

        // Each call to `fm_sitrep_history_prune` deletes at most BATCH_SIZE of
        // the oldest history table entries, so we must keep calling it until it
        // tells us that there's nothing left to prune (or until it fails).
        let outcome = loop {
            match self.datastore.fm_sitrep_history_prune(&opctx, params).await {
                // If we have never pruned any sitreps, and the first call
                // indicates that there are no sitreps pruned, then we haven't
                // done anything and can complete immediately.
                Ok(HistoryPruningResult::NotPruned { count })
                    if pruned.total == 0 =>
                {
                    slog::debug!(
                        &opctx.log,
                        "sitrep history depth is within the limit, no records \
                         will be pruned";
                        "sitrep_history_count" => count,
                        "history_pruning_threshold" => thresh.get(),
                    );
                    break status::Outcome::NotPruned { count };
                }
                // Otherwise, if the call indicates nothing was pruned, that
                // means we have completed pruning the table.
                Ok(HistoryPruningResult::NotPruned { count }) => {
                    slog::info!(
                        &opctx.log,
                        "finished pruning the sitrep history table";
                        "sitrep_history_count" => count,
                        "history_pruning_threshold" => thresh.get(),
                        "total_sitreps_pruned" => pruned.total,
                        "versions_pruned" => ?pruned
                            .versions.as_ref(),
                        "batches" => pruned.batches,
                        "batch_size" => self.batch_size.get(),
                    );
                    // Note that although the *query* reported that nothing
                    // was pruned, this arm is only reachable when a previous
                    // iteration pruned something, so the activation as a
                    // whole is a `Pruned` outcome.
                    break status::Outcome::Pruned { count };
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
                         "history_pruning_threshold" => thresh.get(),
                        "sitreps_pruned" => n_pruned,
                        "versions_pruned" => ?oldest_pruned..=newest_pruned,
                    );
                    pruned.batches += 1;
                    pruned.total += n_pruned;
                    // The overall range of versions pruned by this activation
                    // starts wherever the first batch did.
                    let oldest = pruned
                        .versions
                        .as_ref()
                        .map_or(oldest_pruned, |r| *r.start());
                    pruned.versions = Some(oldest..=newest_pruned);
                    // We've just orphaned a batch of sitreps, so poke the GC
                    // task to come clean them up. Doing this per-batch, rather
                    // than once when the loop completes, lets the GC start
                    // reclaiming space concurrently while we keep pruning a
                    // long backlog.
                    self.sitrep_gc.activate();
                }
                Err(e) => {
                    let error = InlineErrorChain::new(&e);
                    slog::error!(
                        &opctx.log,
                        "pruning the sitrep history table failed";
                        "history_pruning_threshold" => thresh.get(),
                        "total_sitreps_pruned" => pruned.total,
                        "versions_pruned" => ?pruned
                            .versions.as_ref(),
                        "batches" => pruned.batches,
                        "batch_size" => self.batch_size.get(),
                        &error
                    );
                    break status::Outcome::Error(error.to_string());
                }
            }
        };

        Status::Activated {
            cfg,
            batch_size: self.batch_size.get(),
            outcome,
            pruned,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::fm::SitrepModel;
    use omicron_test_utils::dev;

    /// Returns a config watch receiver carrying an FM config with the given
    /// history pruning threshold, as though the config loader task had
    /// published it.
    ///
    /// The config is constructed directly rather than validated through
    /// `FmConfigParam`, so tests may use small thresholds. The sitrep limit
    /// is not used by this task; it is set to one greater than the
    /// threshold, as the real config validation would require.
    fn config_rx(
        history_pruning_threshold: u32,
    ) -> watch::Receiver<Option<FmConfigView>> {
        let config = nexus_types::fm::FmConfig {
            analysis_enabled: true,
            history_pruning_threshold: NonZeroU32::new(
                history_pruning_threshold,
            )
            .expect("test pruning thresholds must be nonzero"),
            sitrep_limit: NonZeroU32::new(history_pruning_threshold + 1)
                .unwrap(),
        };
        let view = FmConfigView { config, source: Default::default() };
        // The sender is dropped here; watch receivers continue to yield the
        // last-sent value after the channel closes.
        watch::channel(Some(view)).1
    }

    /// Tests that the task prunes the sitrep history table down to (at most)
    /// `history_limit` entries, deleting oldest versions first, and activates
    /// the GC task if (and only if) it actually deleted something from the
    /// history.
    #[tokio::test]
    async fn test_history_pruning() {
        let logctx = dev::test_setup_log("test_history_pruning");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const LIMIT: u32 = 5;
        let gc = Activator::new();
        gc.mark_wired_up().unwrap();
        let mut task = SitrepHistoryPruner::new(
            datastore.clone(),
            gc.clone(),
            config_rx(LIMIT),
        );

        let mut model = SitrepModel::new(datastore.clone());

        // Below the limit: nothing should be pruned.
        model.insert_history(opctx, 3).await; // v1..=v3
        let (_pruned, outcome) =
            run_prune_and_check(&mut task, &gc, &mut model, opctx).await;
        assert_eq!(outcome, status::Outcome::NotPruned { count: 3 });

        // Exactly at the limit: the limit check fires, but the newest `LIMIT`
        // versions are the entire history, so nothing is actually deleted.
        model.insert_history(opctx, 2).await; // v4, v5
        let (_pruned, outcome) =
            run_prune_and_check(&mut task, &gc, &mut model, opctx).await;
        assert_eq!(outcome, status::Outcome::NotPruned { count: 5 });

        // Over the limit: versions 1..=3 should be pruned from the history,
        // orphaning the sitreps they referenced, and the GC task should be
        // activated to clean them up.
        model.insert_history(opctx, 3).await; // v6..=v8
        let (pruned, outcome) =
            run_prune_and_check(&mut task, &gc, &mut model, opctx).await;
        assert_eq!(pruned.total, 3);
        assert_eq!(pruned.versions, Some(1..=3));
        assert_eq!(outcome, status::Outcome::Pruned { count: 5 });

        // Exactly at the limit again, but this time the earliest history
        // version is v4, not v1, since we pruned v1 previously. This is the
        // expected steady state of the system, since we try to keep exactly
        // `LIMIT` rows in the history table.
        let (_pruned, outcome) =
            run_prune_and_check(&mut task, &gc, &mut model, opctx).await;
        assert_eq!(outcome, status::Outcome::NotPruned { count: 5 });

        // Prune again, now that the minimum history version is no longer 1.
        // This checks that the pruning arithmetic is anchored on the latest
        // version rather than on the row count: history is v4..=v10 (7 rows),
        // so v4 and v5 should go.
        model.insert_history(opctx, 2).await; // v9, v10
        let (pruned, _outcome) =
            run_prune_and_check(&mut task, &gc, &mut model, opctx).await;
        assert_eq!(pruned.total, 2);
        assert_eq!(pruned.versions, Some(4..=5));

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
        // How far over the limit the history table goes before each pruner
        // activation.
        const EXCESS_HISTORY: u32 = 7;
        let gc = Activator::new();
        gc.mark_wired_up().unwrap();
        let mut task = SitrepHistoryPruner::new(
            datastore.clone(),
            gc.clone(),
            config_rx(LIMIT),
        );

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

            // Insert new sitreps until the history is `EXCESS_HISTORY`
            // entries over the limit.
            let count = u32::try_from(model.history_count()).unwrap();
            let needed = (LIMIT + EXCESS_HISTORY) - count;
            model.insert_history(opctx, needed as usize).await;

            let (pruned, outcome) =
                run_prune_and_check(&mut task, &gc, &mut model, opctx).await;
            assert_eq!(pruned.total, EXCESS_HISTORY as usize);
            assert_eq!(
                outcome,
                status::Outcome::Pruned { count: LIMIT.into() }
            );
            // ...and did we actually report that we did that many batches?
            assert_eq!(pruned.batches, expected_batches);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Activate the sitrep history pruner task and check the task's reported
    /// status, the database contents, and the GC task's activation against the
    /// model's simulation of what pruning should do with the task's configured
    /// pruning threshold.
    ///
    /// If pruning orphaned any sitreps, this asserts that the pruner activated
    /// the GC task, and then performs the orphan sweep the GC task *would* do
    /// (via the model, which also checks its effects), so that the database is
    /// updated to reflect the state as though the activated GC pass had run.
    /// When a subsequent activation runs, it will see a database that appears
    /// to have been GCed. This lets the pruner be tested in isolation, without
    /// running the actual GC task.
    async fn run_prune_and_check(
        task: &mut SitrepHistoryPruner,
        gc: &Activator,
        model: &mut SitrepModel,
        opctx: &OpContext,
    ) -> (status::SitrepsPruned, status::Outcome) {
        let (cfg, pruned, outcome) =
            match dbg!(task.actually_activate(opctx).await) {
                Status::Activated { cfg, pruned, outcome, .. } => {
                    (cfg, pruned, outcome)
                }
                status => panic!(
                    "the pruner should have activated with a loaded config, \
                     got: {status:?}"
                ),
            };
        // The status must echo the config values the task was actually given.
        assert_eq!(
            Some(cfg),
            task.cfg.borrow().as_ref().map(|view| view.config),
            "the status should report the config used for pruning",
        );
        let expected =
            model.simulate_history_pruning(cfg.history_pruning_threshold);
        assert_eq!(
            pruned.total, expected.sitreps_pruned,
            "the number of history entries pruned should match the model's \
             simulation"
        );
        assert_eq!(
            pruned.versions, expected.versions_pruned,
            "the range of versions pruned should match the model's simulation"
        );
        assert_eq!(
            outcome, expected.outcome,
            "the task's reported pruning outcome should match the model's \
             simulation"
        );
        // The database should now match the model: pruned history entries
        // are gone, but the newly-orphaned sitreps still exist, since the GC
        // task hasn't actually run.
        model.assert_matches(opctx).await;

        if pruned.total > 0 {
            gc.assert_activated(
                "the GC task should be activated when sitreps were pruned",
            );
            // Apply the effects of the orphan sweep that the GC task would
            // perform once activated, so that the next scenario starts clean.
            model.gc_orphans().await;
            model.assert_matches(opctx).await;
        } else {
            gc.assert_not_activated(
                "the GC task should not be activated when nothing was pruned",
            );
        }
        (pruned, outcome)
    }
}
