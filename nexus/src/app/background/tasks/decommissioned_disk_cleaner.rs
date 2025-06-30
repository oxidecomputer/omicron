// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Cleans up old database state from decommissioned disks.
//!
//! This cannot happen at decommissioning time, because it depends on region
//! (and snapshot) replacement, which happens in the background.
//!
//! Cleanup involves deleting database records for disks (datasets, zpools)
//! that are no longer viable after the physical disk has been decommissioned.

use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_model::Zpool;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::identity::Asset;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, ZpoolUuid};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that cleans decommissioned disk DB records.
pub struct DecommissionedDiskCleaner {
    datastore: Arc<DataStore>,
    disable: bool,
}

#[derive(Debug, Default)]
struct ActivationResults {
    found: usize,
    not_ready_to_be_deleted: usize,
    deleted: usize,
    error_count: usize,
}

const MAX_BATCH: NonZeroU32 = NonZeroU32::new(100).unwrap();

impl DecommissionedDiskCleaner {
    pub fn new(datastore: Arc<DataStore>, disable: bool) -> Self {
        Self { datastore, disable }
    }

    async fn clean_all(
        &mut self,
        results: &mut ActivationResults,
        opctx: &OpContext,
    ) -> Result<(), anyhow::Error> {
        slog::info!(opctx.log, "Decommissioned disk cleaner running");

        let mut paginator =
            Paginator::new(MAX_BATCH, dropshot::PaginationOrder::Ascending);
        let mut last_err = Ok(());
        while let Some(p) = paginator.next() {
            let zpools = self
                .datastore
                .zpool_on_decommissioned_disk_list(
                    opctx,
                    &p.current_pagparams(),
                )
                .await
                .context("failed to list zpools on decommissioned disks")?;
            paginator = p.found_batch(&zpools, &|zpool| zpool.id());
            self.clean_batch(results, &mut last_err, opctx, &zpools).await;
        }

        last_err
    }

    async fn clean_batch(
        &mut self,
        results: &mut ActivationResults,
        last_err: &mut Result<(), anyhow::Error>,
        opctx: &OpContext,
        zpools: &[Zpool],
    ) {
        results.found += zpools.len();
        slog::debug!(opctx.log, "Found zpools on decommissioned disks"; "count" => zpools.len());

        for zpool in zpools {
            let zpool_id = ZpoolUuid::from_untyped_uuid(zpool.id());
            slog::trace!(opctx.log, "Deleting Zpool"; "zpool" => %zpool_id);

            match self
                .datastore
                .zpool_delete_self_and_all_datasets(opctx, zpool_id)
                .await
            {
                Ok(_) => {
                    slog::info!(
                        opctx.log,
                        "Deleted zpool and datasets within";
                        "zpool" => %zpool_id,
                    );
                    results.deleted += 1;
                }
                Err(Error::ServiceUnavailable { internal_message }) => {
                    slog::trace!(
                        opctx.log,
                        "Zpool on decommissioned disk not ready for deletion";
                        "zpool" => %zpool_id,
                        "error" => internal_message,
                    );
                    results.not_ready_to_be_deleted += 1;
                }
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "Failed to zpool on decommissioned disk";
                        "zpool" => %zpool_id,
                        "error" => %e,
                    );
                    results.error_count += 1;
                    *last_err = Err(e).with_context(|| {
                        format!("failed to delete zpool record {zpool_id}")
                    });
                }
            }
        }
    }
}

impl BackgroundTask for DecommissionedDiskCleaner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let mut results = ActivationResults::default();

            let error = if !self.disable {
                match self.clean_all(&mut results, opctx).await {
                    Ok(_) => {
                        slog::info!(opctx.log, "Cleaned decommissioned zpools";
                            "found" => results.found,
                            "not_ready_to_be_deleted" => results.not_ready_to_be_deleted,
                            "deleted" => results.deleted,
                            "error_count" => results.error_count,
                        );
                        None
                    }
                    Err(err) => {
                        slog::error!(opctx.log, "Failed to clean decommissioned zpools";
                            "error" => %err,
                            "found" => results.found,
                            "not_ready_to_be_deleted" => results.not_ready_to_be_deleted,
                            "deleted" => results.deleted,
                            "error_count" => results.error_count,
                        );
                        Some(err.to_string())
                    }
                }
            } else {
                slog::info!(opctx.log, "Decommissioned Disk Cleaner disabled");
                None
            };
            serde_json::json!({
                "found": results.found,
                "not_ready_to_be_deleted": results.not_ready_to_be_deleted,
                "deleted": results.deleted,
                "error_count": results.error_count,
                "error": error,
            })
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use nexus_db_model::CrucibleDataset;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::Region;
    use nexus_test_utils::SLED_AGENT_UUID;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_uuid_kinds::{
        DatasetUuid, PhysicalDiskUuid, RegionUuid, SledUuid, VolumeUuid,
    };
    use std::str::FromStr;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    async fn make_disk_in_db(
        datastore: &DataStore,
        opctx: &OpContext,
        i: usize,
        sled_id: SledUuid,
    ) -> PhysicalDiskUuid {
        let id = PhysicalDiskUuid::new_v4();
        let physical_disk = PhysicalDisk::new(
            id,
            "v".into(),
            format!("s-{i})"),
            "m".into(),
            PhysicalDiskKind::U2,
            sled_id.into_untyped_uuid(),
        );
        datastore
            .physical_disk_insert(&opctx, physical_disk.clone())
            .await
            .unwrap();
        id
    }

    async fn add_zpool_dataset_and_region(
        datastore: &DataStore,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
        sled_id: SledUuid,
    ) -> (ZpoolUuid, DatasetUuid, RegionUuid) {
        let zpool = datastore
            .zpool_insert(
                opctx,
                Zpool::new(
                    Uuid::new_v4(),
                    sled_id.into_untyped_uuid(),
                    id,
                    ByteCount::from(0).into(),
                ),
            )
            .await
            .unwrap();

        let dataset = datastore
            .crucible_dataset_upsert(CrucibleDataset::new(
                DatasetUuid::new_v4(),
                zpool.id(),
                std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::LOCALHOST,
                    0,
                    0,
                    0,
                ),
            ))
            .await
            .unwrap();

        // There isn't a great API to insert regions (we normally allocate!)
        // so insert the record manually here.
        let region = {
            let volume_id = VolumeUuid::new_v4();
            Region::new(
                dataset.id(),
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
                1,
                false,
            )
        };
        let region_id = region.id();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::region::dsl;
        diesel::insert_into(dsl::region)
            .values(region)
            .execute_async(&*conn)
            .await
            .unwrap();

        (
            ZpoolUuid::from_untyped_uuid(zpool.id()),
            dataset.id(),
            RegionUuid::from_untyped_uuid(region_id),
        )
    }

    struct TestFixture {
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        region_id: RegionUuid,
        disk_id: PhysicalDiskUuid,
    }

    impl TestFixture {
        async fn setup(datastore: &Arc<DataStore>, opctx: &OpContext) -> Self {
            let sled_id = SledUuid::from_untyped_uuid(
                Uuid::from_str(&SLED_AGENT_UUID).unwrap(),
            );

            let disk_id = make_disk_in_db(datastore, opctx, 0, sled_id).await;
            let (zpool_id, dataset_id, region_id) =
                add_zpool_dataset_and_region(
                    &datastore, &opctx, disk_id, sled_id,
                )
                .await;
            datastore
                .physical_disk_update_policy(
                    &opctx,
                    disk_id,
                    PhysicalDiskPolicy::Expunged,
                )
                .await
                .unwrap();

            Self { zpool_id, dataset_id, region_id, disk_id }
        }

        async fn delete_region(&self, datastore: &DataStore) {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            use nexus_db_schema::schema::region::dsl;

            diesel::delete(
                dsl::region
                    .filter(dsl::id.eq(self.region_id.into_untyped_uuid())),
            )
            .execute_async(&*conn)
            .await
            .unwrap();
        }

        async fn has_been_cleaned(&self, datastore: &DataStore) -> bool {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::{
                ExpressionMethods, OptionalExtension, QueryDsl,
                SelectableHelper,
            };
            use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let fetched_zpool = zpool_dsl::zpool
                .filter(zpool_dsl::id.eq(self.zpool_id.into_untyped_uuid()))
                .filter(zpool_dsl::time_deleted.is_null())
                .select(Zpool::as_select())
                .first_async(&*conn)
                .await
                .optional()
                .expect("Zpool query should succeed");

            use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
            let fetched_dataset = dataset_dsl::crucible_dataset
                .filter(dataset_dsl::id.eq(self.dataset_id.into_untyped_uuid()))
                .filter(dataset_dsl::time_deleted.is_null())
                .select(CrucibleDataset::as_select())
                .first_async(&*conn)
                .await
                .optional()
                .expect("Dataset query should succeed");

            match (fetched_zpool, fetched_dataset) {
                (Some(_), Some(_)) => false,
                (None, None) => true,
                _ => panic!(
                    "If zpool and dataset were cleaned, they should be cleaned together"
                ),
            }
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_disk_cleanup_ignores_active_disks(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let fixture = TestFixture::setup(datastore, &opctx).await;

        let mut task = DecommissionedDiskCleaner::new(datastore.clone(), false);

        // Setup: Disk is expunged, not decommissioned.
        // Expectation: We ignore it.

        let mut results = ActivationResults::default();
        dbg!(task.clean_all(&mut results, &opctx).await)
            .expect("activation completes successfully");
        dbg!(&results);
        assert_eq!(results.found, 0);
        assert_eq!(results.not_ready_to_be_deleted, 0);
        assert_eq!(results.deleted, 0);
        assert_eq!(results.error_count, 0);

        assert!(!fixture.has_been_cleaned(&datastore).await);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_disk_cleanup_does_not_clean_disks_with_regions(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let fixture = TestFixture::setup(datastore, &opctx).await;

        let mut task = DecommissionedDiskCleaner::new(datastore.clone(), false);

        datastore
            .physical_disk_decommission(&opctx, fixture.disk_id)
            .await
            .unwrap();

        // Setup: Disk is decommissioned, but has a region.
        // Expectation: We ignore it.

        let mut results = ActivationResults::default();
        dbg!(task.clean_all(&mut results, &opctx).await)
            .expect("activation completes successfully");
        dbg!(&results);
        assert_eq!(results.found, 1);
        assert_eq!(results.not_ready_to_be_deleted, 1);
        assert_eq!(results.deleted, 0);
        assert_eq!(results.error_count, 0);

        assert!(!fixture.has_been_cleaned(&datastore).await);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_disk_cleanup_cleans_disks_with_no_regions(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let fixture = TestFixture::setup(datastore, &opctx).await;

        let mut task = DecommissionedDiskCleaner::new(datastore.clone(), false);

        datastore
            .physical_disk_decommission(&opctx, fixture.disk_id)
            .await
            .unwrap();

        fixture.delete_region(&datastore).await;

        // Setup: Disk is decommissioned and has no regions.
        // Expectation: We clean it.

        let mut results = ActivationResults::default();
        dbg!(task.clean_all(&mut results, &opctx).await)
            .expect("activation completes successfully");
        dbg!(&results);
        assert_eq!(results.found, 1);
        assert_eq!(results.not_ready_to_be_deleted, 0);
        assert_eq!(results.deleted, 1);
        assert_eq!(results.error_count, 0);

        assert!(fixture.has_been_cleaned(&datastore).await);
    }
}
