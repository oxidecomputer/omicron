// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for fault management sitrep garbage collection.

use crate::app::background::BackgroundTask;
use crate::db::model::SqlU32;
use crate::db::pagination::Paginator;
use dropshot::PaginationOrder;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_types::internal_api::background::OrphanedSitreps;
use nexus_types::internal_api::background::SitrepGcStatus as Status;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

pub struct SitrepGc {
    datastore: Arc<DataStore>,
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
        Self { datastore }
    }

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        let mut status = Status::default();
        let mut total_found = 0;
        let mut total_deleted = 0;
        let mut paginator =
            Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Descending);

        while let Some(p) = paginator.next() {
            let versions = match self
                .datastore
                .fm_sitrep_version_list(opctx, &p.current_pagparams())
                .await
            {
                Ok(versions) => versions,
                Err(err) => {
                    let err = InlineErrorChain::new(&err);
                    slog::error!(
                        &opctx.log,
                        "failed to list committed sitrep versions";
                        &err,
                    );
                    status.errors.push(format!(
                        "failed to list sitrep versions: {err}",
                    ));
                    break;
                }
            };

            paginator = p.found_batch(&versions, &|v| SqlU32::from(v.version));

            for v in versions {
                let version = v.version;
                let orphans = match self
                    .datastore
                    .fm_sitrep_list_orphaned(opctx, &v)
                    .await
                {
                    Ok(orphans) if orphans.is_empty() => {
                        slog::trace!(
                            &opctx.log,
                            "no orphaned sitreps found at v{version}";
                            "version" => version,
                        );
                        continue;
                    }
                    Ok(orphans) => orphans,
                    Err(err) => {
                        const MSG: &str =
                            "failed to list orphaned sitreps at v";
                        let err = InlineErrorChain::new(&err);
                        slog::error!(
                            &opctx.log,
                            "{MSG}{version}";
                            "version" => version,
                            &err,
                        );
                        status.errors.push(format!("{MSG}{version}: {err}"));
                        continue;
                    }
                };

                status.versions_scanned += 1;
                let found = orphans.len();
                total_found += found;
                slog::trace!(
                    &opctx.log,
                    "found {found} orphaned sitreps at v{version}";
                    "version" => version,
                );

                let deleted = match self
                    .datastore
                    .fm_sitrep_delete_all(
                        &opctx,
                        orphans.into_iter().map(|m| m.id),
                    )
                    .await
                {
                    Ok(deleted) => deleted,
                    Err(err) => {
                        let err = InlineErrorChain::new(&err);
                        const MSG: &str =
                            "failed to delete orphaned sitreps at v";
                        slog::error!(
                            &opctx.log,
                            "{MSG}{version}";
                            "version" => v.version,
                            &err,
                        );
                        status.errors.push(format!("{MSG}{version}: {err}"));
                        continue;
                    }
                };

                total_deleted += deleted;
                status
                    .orphaned_sitreps
                    .insert(version, OrphanedSitreps { found, deleted });

                if deleted > 0 {
                    slog::debug!(
                        &opctx.log,
                        "deleted {deleted} of {found} orphaned sitreps at \
                         v{version}";
                        "version" => v.version,
                    );
                } else {
                    slog::trace!(
                        &opctx.log,
                        "all {found} orphaned sitreps at v{version} have \
                         already been deleted";
                        "version" => v.version,
                    );
                }
            }
        }

        slog::info!(
            &opctx.log,
            "sitrep garbage collection scanned {} versions, \
             found {total_found} orphaned sitreps, deleted {total_deleted}",
            status.versions_scanned,
        );

        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use nexus_db_queries::db::datastore::fm::InsertSitrepError;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_types::fm;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn test_orphaned_sitrep_gc() {
        let logctx = dev::test_setup_log("test_orphaned_sitrep_gc");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut task = SitrepGc::new(datastore.clone());

        // First, insert an initial sitrep. This should succeed.
        let sitrep1 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep v1".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
        };
        datastore
            .fm_sitrep_insert(&opctx, &sitrep1)
            .await
            .expect("inserting initial sitrep should succeed");

        // Now, create some orphaned sitreps which also have no parent.
        let mut orphans = BTreeSet::new();
        for i in 1..5 {
            insert_orphan(&datastore, &opctx, &mut orphans, None, 1, i).await;
        }

        // Next, create a new sitrep which descends from sitrep 1.
        let sitrep2 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep v2".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: Some(sitrep1.metadata.id),
            },
        };
        datastore
            .fm_sitrep_insert(&opctx, &sitrep2)
            .await
            .expect("inserting child sitrep should succeed");

        // Now, create some orphaned sitreps which also descend from sitrep 1.
        for i in 1..4 {
            insert_orphan(
                &datastore,
                &opctx,
                &mut orphans,
                Some(sitrep1.metadata.id),
                2,
                i,
            )
            .await;
        }

        // Make sure the orphans exist.
        for &id in &orphans {
            match datastore.fm_sitrep_metadata_read(&opctx, id).await {
                Ok(_) => {}
                Err(Error::NotFound { .. }) => {
                    panic!("orphaned sitrep {id} should exist");
                }
                Err(e) => {
                    panic!(
                        "unexpected error reading orphaned sitrep {id}: {e}"
                    );
                }
            }
        }

        // Activate the background task.
        let status = dbg!(task.actually_activate(opctx).await);

        // Now, the orphans should all be gone.
        for &id in &orphans {
            match datastore.fm_sitrep_metadata_read(&opctx, id).await {
                Ok(_) => {
                    panic!(
                        "orphaned sitrep {id} should have been deleted, \
                         but it appears to still exist!"
                    )
                }
                Err(Error::NotFound { .. }) => {
                    // Okay, it's gone.
                }
                Err(e) => {
                    panic!(
                        "unexpected error reading orphaned sitrep {id}: {e}"
                    );
                }
            }
        }
        // But the non-orphaned sitreps should still be there!
        datastore
            .fm_sitrep_metadata_read(&opctx, sitrep1.id())
            .await
            .expect("sitrep 1 should still exist");
        datastore
            .fm_sitrep_metadata_read(&opctx, sitrep2.id())
            .await
            .expect("sitrep 2 should still exist");

        assert_eq!(status.errors, Vec::<String>::new());
        assert_eq!(status.versions_scanned, 2);
        assert_eq!(
            status.orphaned_sitreps.get(&1),
            Some(&OrphanedSitreps { found: 4, deleted: 4 })
        );
        assert_eq!(
            status.orphaned_sitreps.get(&2),
            Some(&OrphanedSitreps { found: 3, deleted: 3 })
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn insert_orphan(
        datastore: &DataStore,
        opctx: &OpContext,
        orphans: &mut BTreeSet<SitrepUuid>,
        parent_sitrep_id: Option<SitrepUuid>,
        v: usize,
        i: usize,
    ) {
        let sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: format!("test sitrep v{i}; orphan {i}"),
                time_created: Utc::now(),
                parent_sitrep_id,
            },
        };
        match datastore.fm_sitrep_insert(&opctx, &sitrep).await {
            Ok(_) => {
                panic!("inserting sitrep v{v} orphan {i} should not succeed")
            }
            Err(InsertSitrepError::ParentNotCurrent(id)) => {
                orphans.insert(id);
            }
            Err(InsertSitrepError::Other(e)) => {
                panic!(
                    "expected inserting sitrep v{v} orphan {i} to fail because \
                     its parent is out of date, but saw an unexpected error: {e}"
                );
            }
        }
    }
}
