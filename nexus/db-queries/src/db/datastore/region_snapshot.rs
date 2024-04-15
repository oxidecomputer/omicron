// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RegionSnapshot`]s.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::RegionSnapshot;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::OptionalExtension;
use nexus_db_model::to_db_typed_uuid;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::OmicronZoneUuid;
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
        dataset_id: OmicronZoneUuid,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> LookupResult<Option<RegionSnapshot>> {
        use db::schema::region_snapshot::dsl;

        dsl::region_snapshot
            .filter(dsl::dataset_id.eq(to_db_typed_uuid(dataset_id)))
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
        dataset_id: OmicronZoneUuid,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> DeleteResult {
        use db::schema::region_snapshot::dsl;

        diesel::delete(dsl::region_snapshot)
            .filter(dsl::dataset_id.eq(to_db_typed_uuid(dataset_id)))
            .filter(dsl::region_id.eq(region_id))
            .filter(dsl::snapshot_id.eq(snapshot_id))
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
