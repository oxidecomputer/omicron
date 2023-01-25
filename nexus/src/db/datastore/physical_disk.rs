// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`PhysicalDisk`]s.

use super::DataStore;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::PhysicalDisk;
use crate::db::model::Sled;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl DataStore {
    /// Stores a new physical disk in the database.
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
}
