// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault-management-specific datastore test helpers.

use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::IsLimitReached;
use crate::db::datastore::fm;
use crate::db::datastore::fm::InsertSitrepError;
use chrono::Utc;
use nexus_types::fm::Sitrep;
use nexus_types::fm::SitrepMetadata;
use nexus_types::internal_api::background::fm_sitrep_gc::HistoryPruningStatus;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;

/// An in-memory model of the expected contents of the sitrep tables.
///
/// Tests that exercise sitrep insertion, history pruning, and orphan collection
/// generally perform a sequence of setup operations and then check the database
/// against what those operations should have produced. This struct is the
/// corresponding model of the expected state of the records in the database.
/// Operations are methods on the model, which update the model and the database
/// together, and [`Self::assert_matches`] checks the database against the
/// model.
///
/// The model tracks:
///
/// - [`Self::history`]: every sitrep ever made current, in version order.
///   `history[i]` is the sitrep at version `i + 1`. Entries are not
///   removed when the history is pruned. Instead, `earliest_live_version`
///   records the version number of the oldest sitrep expected to exist in the
///   database. Versions < `earliest_live_version` are still tracked in the
///   model's history, but should no longer exist in the `fm_sitrep_history`
///   table.
///
/// - [`Self::live_orphans`]: the IDs of sitreps present in `fm_sitrep` but
///   never made current. This models what is left behind when a Nexus loses the
///   race to commit a new sitrep.
///
/// - [`Self::gced_orphans`]: the IDs of sitreps that the GC is expected to have
///   deleted.
///
/// Expected effects on the database that happen *outside* the model's own
/// insert methods are recorded explicitly.
///
/// - When a GC activation runs, call [`Self::simulate_gc`] with the task's
///   configured history limit. The model computes what the GC should do
///   given that limit, applies those effects to itself, and returns them so
///   that the test can compare the task's reported status against the
///   model's prediction.
/// - When a sitrep is made current by the code under test, rather than the
///   model, call [`Self::record_committed`] with the ID of the newly
///   committed sitrep.
pub struct SitrepModel {
    datastore: Arc<DataStore>,
    /// Every sitrep ever made current, in version order: `history[i]` is
    /// the sitrep at version `i + 1`, including entries that have since
    /// been pruned (see `earliest_live_version`).
    pub history: Vec<SitrepUuid>,
    /// Orphaned sitreps expected to be present in `fm_sitrep`.
    pub live_orphans: BTreeSet<SitrepUuid>,
    /// Orphaned sitreps expected to have been deleted by the GC.
    pub gced_orphans: BTreeSet<SitrepUuid>,
    /// The earliest version expected to remain in `fm_sitrep_history`.
    /// Versions older than this have been pruned.
    earliest_live_version: u32,
}

impl SitrepModel {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self {
            datastore,
            history: Vec::new(),
            live_orphans: BTreeSet::new(),
            gced_orphans: BTreeSet::new(),
            earliest_live_version: 1,
        }
    }

    /// The number of rows expected in the `fm_sitrep` table: live
    /// history entries, plus orphans the GC has not yet deleted.
    pub fn sitrep_count(&self) -> u64 {
        (self.history_count() + self.live_orphans.len()) as u64
    }

    /// The number of rows expected in the `fm_sitrep_history` table.
    pub fn history_count(&self) -> usize {
        (self.history.len() + 1) - self.earliest_live_version as usize
    }

    pub fn latest_version(&self) -> u32 {
        u32::try_from(self.history.len()).expect(
            "total number of sitreps in test exceeds a u32, that's wacky",
        )
    }

    /// Insert `n` empty sitreps through the real insert path
    /// ([`DataStore::fm_sitrep_insert`]), each the child of the previous
    /// current sitrep, appending them to [`Self::history`].
    ///
    /// # Panics
    ///
    /// Panics if any insert fails, including with `ParentNotCurrent`; this
    /// can occur if a sitrep was made current without being recorded in the
    /// model via [`Self::record_committed`].
    pub async fn insert_history(&mut self, opctx: &OpContext, n: usize) {
        for _ in 0..n {
            let version = self.history.len() + 1;
            let id = SitrepUuid::new_v4();
            let parent = self.history.last().copied();
            let sitrep =
                test_sitrep(id, parent, format!("test sitrep v{version}"));
            self.datastore
                .fm_sitrep_insert(opctx, sitrep, None)
                .await
                .unwrap_or_else(|e| {
                    panic!("inserting sitrep v{version} should succeed: {e}")
                });
            self.history.push(id);
            eprintln!(
                "model: inserted sitrep {id} at v{version} (parent: {parent:?})"
            );
        }
    }

    /// Insert an empty sitrep that is *not* made current, leaving it
    /// orphaned, and record it in [`Self::live_orphans`]. Returns the
    /// orphan's ID.
    ///
    /// The sitrep is inserted with the given (stale) `parent_sitrep_id` through
    /// the real insert path, which writes its rows to `fm_sitrep` and then
    /// fails with [`InsertSitrepError::ParentNotCurrent`], leaving the rows
    /// behind for the GC task to delete. This is exactly what happens when a
    /// Nexus loses the race to commit a new sitrep.
    ///
    /// The orphan is inserted with no cases or ereports. Tests that care
    /// about an orphan's child rows being cleaned up alongside it are in
    /// the `nexus_db_queries::db::datastore::fm` module (see
    /// `test_sitrep_delete_deletes_cases`), so callers here may trust that
    /// deleting the orphan deletes its children.
    ///
    /// # Panics
    ///
    /// - If the insert successfully committed the sitrep (i.e.,
    ///   `parent_sitrep_id` was actually current (or was `None` while the
    ///   sitrep history was empty)
    /// - If the insert fails with an error other than `ParentNotCurrent`
    ///   (i.e. a database error)
    pub async fn insert_orphan(
        &mut self,
        opctx: &OpContext,
        parent_sitrep_id: Option<SitrepUuid>,
    ) -> SitrepUuid {
        let id = SitrepUuid::new_v4();
        let sitrep =
            test_sitrep(id, parent_sitrep_id, "orphaned test sitrep".into());
        match self.datastore.fm_sitrep_insert(opctx, sitrep, None).await {
            Ok(()) => panic!(
                "inserting sitrep {id} with parent {parent_sitrep_id:?} \
                should fail with ParentNotCurrent, but it was made current \
                unexpectedly. this is *probably* a test bug!"
            ),
            Err(InsertSitrepError::ParentNotCurrent(orphan_id)) => {
                assert_eq!(
                    orphan_id, id,
                    "ParentNotCurrent should carry the ID of the sitrep we \
                     just tried to insert"
                );
                self.live_orphans.insert(id);
                eprintln!(
                    "model: inserted sitrep {id}, orphaned \
                     (parent: {parent_sitrep_id:?})"
                );
                id
            }
            Err(InsertSitrepError::Other(e)) => panic!(
                "expected inserting sitrep {id} to fail with \
                 ParentNotCurrent, but it failed with an unexpected error: {e}"
            ),
        }
    }

    /// Record a sitrep that was made current by something other than this
    /// model (e.g. committed by the FM analysis task), appending it to
    /// [`Self::history`].
    pub fn record_committed(&mut self, id: SitrepUuid) {
        self.history.push(id);
        eprintln!("model: recorded sitrep {id} at v{}", self.history.len());
    }

    /// Simulate a sitrep GC activation with the given `history_limit`,
    /// applying its expected effects to the model and returning them as an
    /// [`ExpectedGc`], so that the test can compare the task's reported
    /// status against the model's prediction.
    ///
    /// This simulates what the GC task *should* do:
    ///
    /// - If the history table holds fewer than `history_limit` rows, nothing
    ///   is pruned.
    /// - Otherwise, the newest `history_limit` versions are kept, and every
    ///   version at or below `latest_version - history_limit` is pruned.
    /// - All orphaned sitreps, including those newly orphaned by pruning
    ///   the history, are deleted by the orphan sweep.
    pub fn simulate_gc(&mut self, history_limit: NonZeroU32) -> ExpectedGc {
        let latest_version = self.latest_version();
        let history_count = u32::try_from(self.history_count())
            .expect("test current history count exceeds u32");
        let history_limit = history_limit.get();
        eprintln!(
            "model: simulating GC with history_limit={history_limit}, \
             latest_version=v{latest_version}, history_count={history_count}"
        );
        let mut history_pruned = 0;
        let pruning = if history_count < history_limit {
            HistoryPruningStatus::BelowLimit { count: history_count.into() }
        } else {
            let newest_version_pruned = latest_version - history_limit;
            // The versions pruned are
            // `earliest_live_version..=newest_version_pruned`.
            // Note that the indices in `history` are 0-indexed, while versions
            // start at 1, so we must subtract to find the starting index. Since
            // `newest_version_pruned` is 1 more than the index of the newest
            // live version, the end index for this slice is already exclusive.
            let earliest_pruned_idx = self.earliest_live_version as usize - 1;
            let to_prune = &self.history
                [earliest_pruned_idx..newest_version_pruned as usize];
            for &id in to_prune {
                let v = self.earliest_live_version + history_pruned;
                eprintln!("  - pruned v{v} (sitrep {id}) from history");
                self.live_orphans.insert(id);
                history_pruned += 1;
            }
            self.earliest_live_version = newest_version_pruned + 1;
            HistoryPruningStatus::Pruned {
                n_pruned: history_pruned as usize,
                newest_version_pruned,
            }
        };
        let gced = std::mem::take(&mut self.live_orphans);
        let orphans_deleted = gced.len();
        eprintln!(
            "model: simulated GC with history limit {history_limit}:\n  \
             pruning: {pruning:?}, \
             earliest live version now v{}, \
             expecting {orphans_deleted} orphaned sitreps deleted",
            self.earliest_live_version,
        );
        for orphan in &gced {
            eprintln!("  - GC'd: {orphan}")
        }
        self.gced_orphans.extend(gced);
        ExpectedGc { pruning, orphans_deleted }
    }

    /// Assert that the database contents match this model:
    ///
    /// - `fm_sitrep_history` contains exactly the live (unpruned) versions,
    ///   mapped to the expected sitrep IDs, and the current-sitrep query
    ///   agrees;
    /// - sitreps for pruned history versions and collected orphans have
    ///   been deleted, while live history sitreps and uncollected orphans
    ///   exist;
    /// - no child tables for deleted sitreps exist in the database;
    /// - the total `fm_sitrep` row count matches [`Self::sitrep_count`]
    ///   (catching stray rows that the per-ID checks above cannot, since a
    ///   sitrep this model never learned about has an unknown ID).
    pub async fn assert_matches(&self, opctx: &OpContext) {
        let Self {
            ref datastore,
            ref history,
            live_orphans: ref orphans,
            gced_orphans: ref collected_orphans,
            earliest_live_version: first_live_version,
        } = *self;
        let first_live_idx = first_live_version as usize - 1;

        eprintln!(
            "model: checking database against model:\n  \
              history v{first_live_version}..=v{} ({} pruned),\n  \
              {} live orphans, {} GC'd orphans,\n  \
              {} total fm_sitrep rows expected",
            history.len(),
            first_live_idx,
            orphans.len(),
            collected_orphans.len(),
            self.sitrep_count(),
        );

        // The history table contains exactly the live versions...
        let versions = datastore
            .fm_sitrep_version_list(opctx, &DataPageParams::max_page())
            .await
            .expect("listing sitrep versions should succeed");
        let actual: Vec<(u32, SitrepUuid)> =
            versions.iter().map(|v| (v.version, v.id)).collect();
        let expected: Vec<(u32, SitrepUuid)> = (first_live_version..)
            .zip(history[first_live_idx..].iter().copied())
            .collect();
        assert_eq!(
            actual,
            expected,
            "fm_sitrep_history should contain exactly versions {}..={}",
            first_live_version,
            history.len(),
        );

        // ...and the current-sitrep query agrees with the model.
        let current = datastore
            .fm_current_sitrep_version(opctx)
            .await
            .expect("reading the current sitrep version should succeed");
        match (current, history.last()) {
            (Some(current), Some(&id)) => {
                assert_eq!(
                    (current.version, current.id),
                    (history.len() as u32, id),
                    "the current sitrep should be the latest history entry"
                );
            }
            (None, None) => {}
            (current, expected) => panic!(
                "current sitrep mismatch: datastore has {current:?}, \
                 model expects {expected:?}"
            ),
        }

        // Pruned history sitreps are deleted; live ones exist.
        for &id in &history[..first_live_idx] {
            assert_sitrep_deleted(datastore, opctx, id).await;
        }
        for &id in &history[first_live_idx..] {
            assert_sitrep_exists(datastore, opctx, id).await;
        }

        // Uncollected orphans exist; collected ones are deleted.
        for &id in orphans {
            assert_sitrep_exists(datastore, opctx, id).await;
        }
        for &id in collected_orphans {
            assert_sitrep_deleted(datastore, opctx, id).await;
        }

        // Finally, the total row count matches, so there are no stray
        // sitreps unknown to the model.
        let count = self.sitrep_count();
        let result = datastore
            .fm_sitrep_check_limit_reached(opctx, count + 1)
            .await
            .expect("counting fm_sitrep rows should succeed");
        assert_eq!(
            result,
            IsLimitReached::No { count },
            "fm_sitrep should contain exactly {count} rows"
        );

        eprintln!("model: database matches model");
    }
}

/// The expected outcome of a sitrep GC activation, as computed by
/// [`SitrepModel::simulate_gc`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedGc {
    /// The pruning status the GC task should report.
    pub pruning: HistoryPruningStatus,
    /// The number of orphaned sitreps the task's orphan sweep should delete,
    /// including sitreps newly orphaned by pruning the history.
    pub orphans_deleted: usize,
}

/// Returns an empty test [`Sitrep`] with the given ID, parent, and comment.
fn test_sitrep(
    id: SitrepUuid,
    parent_sitrep_id: Option<SitrepUuid>,
    comment: String,
) -> Sitrep {
    Sitrep {
        metadata: SitrepMetadata {
            id,
            parent_sitrep_id,
            inv_collection_id: CollectionUuid::new_v4(),
            next_inv_min_time_started: Utc::now(),
            creator_id: OmicronZoneUuid::new_v4(),
            comment,
            time_created: Utc::now(),
            alert_generation: Generation::new(),
            support_bundle_generation: Generation::new(),
        },
        cases: Default::default(),
        ereports_by_id: Default::default(),
    }
}

async fn assert_sitrep_exists(
    datastore: &DataStore,
    opctx: &OpContext,
    id: SitrepUuid,
) {
    match datastore.fm_sitrep_metadata_read(opctx, id).await {
        Ok(_) => {}
        Err(Error::NotFound { .. }) => {
            panic!("sitrep {id} should exist, but it has been deleted")
        }
        Err(e) => panic!("unexpected error reading sitrep {id}: {e}"),
    }
}

async fn assert_sitrep_deleted(
    datastore: &DataStore,
    opctx: &OpContext,
    id: SitrepUuid,
) {
    match datastore.fm_sitrep_metadata_read(opctx, id).await {
        Ok(_) => {
            panic!(
                "sitrep {id} should have been deleted, but its metadata row \
                 still exists"
            )
        }
        Err(Error::NotFound { .. }) => {}
        Err(e) => {
            panic!("unexpected error reading sitrep metadata for {id}: {e}")
        }
    }

    fm::test_utils::ensure_sitrep_children_fully_deleted(datastore, id).await;
}
