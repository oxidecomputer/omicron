// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`PhysicalDisk`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::PhysicalDisk;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new physical disk in the database.
    // TODO: Test idempotency?
    pub async fn physical_disk_upsert(
        &self,
        disk: PhysicalDisk,
    ) -> CreateResult<PhysicalDisk> {
        use db::schema::physical_disk::dsl;

        let sled_id = disk.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::physical_disk)
                .values(disk.clone())
                .on_conflict((dsl::vendor, dsl::serial, dsl::model))
                .do_update()
                .set((dsl::sled_id.eq(excluded(dsl::sled_id)),)),
        )
        .insert_and_get_result_async(self.pool())
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            }
        })
    }

    pub async fn sled_list_physical_disks(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<PhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;
        paginated(dsl::physical_disk, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::sled_id.eq(sled_id))
            .select(PhysicalDisk::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Deletes a disk from the database.
    // TODO: Test idempotency?
    pub async fn physical_disk_delete(
        &self,
        vendor: String,
        serial: String,
        model: String,
    ) -> DeleteResult {
        let now = Utc::now();

        use db::schema::physical_disk::dsl;
        diesel::update(dsl::physical_disk)
            .filter(dsl::vendor.eq(vendor))
            .filter(dsl::serial.eq(serial))
            .filter(dsl::model.eq(model))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool())
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}
