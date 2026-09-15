// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for fault management sitrep garbage collection.
//!
//! This task deletes sitreps which exist in the database but whose IDs do not
//! exist in the `fm_sitrep_history` table. Such sitreps may exist for one of two reasons:
//!
//! 1. Multiple Nexii raced to insert a new sitrep, and the sitrep was inserted
//!    with a parent sitrep ID which was no longer the current sitrep. When this
//!    occurs, the sitrep is not made current, as the state has already advanced
//!    past it. Such a sitrep is said to be *orphaned*.
//!
//! 2. A sitrep was commited to the history at some point in time, but its entry
//!    in the `fm_sitrep_history` table has been deleted. This is done
//!    periodically by the [`fm_sitrep_history_pruner`] background task in order
//!    to ensure that the history does not exceed a configured limit.
//!
//!    When that task prunes sitrep versions from the end of the history, it
//!    activates this task to clean up those newly-abandoned sitreps.
//!
//! [`fm_sitrep_history_pruner`]: super::fm_sitrep_history_pruner

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
        Self { datastore, batch_size: datastore::SQL_BATCH_SIZE }
    }

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        let mut status = Status {
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
        // deleted.
        assert_eq!(status.orphaned_sitreps_deleted, 7);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests that the GC task deletes sitreps orphaned by history pruning.
    ///
    /// This test performs the pruning half via the model (which updates the
    /// database as though the history pruning task had just run), so that the
    /// GC task can be tested in isolation from the pruner task.
    #[tokio::test]
    async fn test_gc_deletes_sitreps_pruned_from_history() {
        let logctx =
            dev::test_setup_log("test_gc_deletes_sitreps_pruned_from_history");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        const LIMIT: u32 = 5;
        let mut task = SitrepGc::new(datastore.clone());
        let mut model = SitrepModel::new(datastore.clone());

        // 8 sitreps, and a history limit of 5: pruning should orphan the
        // sitreps at v1..=v3.
        model.insert_history(opctx, 8).await;
        model.prune_history(NonZeroU32::new(LIMIT).unwrap()).await;
        // The pruned sitreps are now orphans: still present in `fm_sitrep`,
        // but no longer referenced by the history table.
        model.assert_matches(opctx).await;

        // Now, the GC task should clean them up.
        let status = run_gc_and_check(&mut task, &mut model, opctx).await;
        assert_eq!(status.orphaned_sitreps_deleted, 3);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Run a GC activation and check both the task's reported status and
    /// the database contents against the model's simulation of what the
    /// orphan sweep should do.
    async fn run_gc_and_check(
        task: &mut SitrepGc,
        model: &mut SitrepModel,
        opctx: &OpContext,
    ) -> Status {
        let status = dbg!(task.actually_activate(opctx).await);
        let expected_orphans_deleted = model.simulate_orphan_gc();
        assert_eq!(
            status.orphaned_sitreps_deleted, expected_orphans_deleted,
            "the task's reported orphan deletions should match the model's \
             simulation"
        );
        assert_eq!(status.errors, Vec::<String>::new());
        model.assert_matches(opctx).await;
        status
    }
}
