// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RegionSnapshot`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::RegionSnapshot;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::OptionalExtension;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;

impl DataStore {
    pub async fn region_snapshot_create(
        &self,
        region_snapshot: RegionSnapshot,
    ) -> CreateResult<()> {
        use db::schema::region_snapshot::dsl;

        diesel::insert_into(dsl::region_snapshot)
            .values(region_snapshot.clone())
            .on_conflict_do_nothing()
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn region_snapshot_get(
        &self,
        dataset_id: Uuid,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> LookupResult<Option<RegionSnapshot>> {
        use db::schema::region_snapshot::dsl;

        dsl::region_snapshot
            .filter(dsl::dataset_id.eq(dataset_id))
            .filter(dsl::region_id.eq(region_id))
            .filter(dsl::snapshot_id.eq(snapshot_id))
            .select(RegionSnapshot::as_select())
            .first_async::<RegionSnapshot>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn region_snapshot_remove(
        &self,
        dataset_id: Uuid,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> DeleteResult {
        use db::schema::region_snapshot::dsl;

        diesel::delete(dsl::region_snapshot)
            .filter(dsl::dataset_id.eq(dataset_id))
            .filter(dsl::region_id.eq(region_id))
            .filter(dsl::snapshot_id.eq(snapshot_id))
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Find region snapshots on expunged disks
    pub async fn find_region_snapshots_on_expunged_physical_disks(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Vec<RegionSnapshot>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::physical_disk::dsl as physical_disk_dsl;
        use db::schema::region_snapshot::dsl as region_snapshot_dsl;
        use db::schema::zpool::dsl as zpool_dsl;

        region_snapshot_dsl::region_snapshot
            .filter(region_snapshot_dsl::dataset_id.eq_any(
                dataset_dsl::dataset
                    .filter(dataset_dsl::time_deleted.is_null())
                    .filter(dataset_dsl::pool_id.eq_any(
                        zpool_dsl::zpool
                            .filter(zpool_dsl::time_deleted.is_null())
                            .filter(zpool_dsl::physical_disk_id.eq_any(
                                physical_disk_dsl::physical_disk
                                    .filter(physical_disk_dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
                                    .select(physical_disk_dsl::id)
                            ))
                            .select(zpool_dsl::id)
                    ))
                    .select(dataset_dsl::id)
            ))
            .select(RegionSnapshot::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
