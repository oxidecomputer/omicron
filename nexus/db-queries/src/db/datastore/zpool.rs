// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Zpool`]s.

use super::DataStore;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl DataStore {
    /// Stores a new zpool in the database.
    pub async fn zpool_upsert(&self, zpool: Zpool) -> CreateResult<Zpool> {
        use db::schema::zpool::dsl;

        let sled_id = zpool.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::zpool)
                .values(zpool.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::total_size.eq(excluded(dsl::total_size)),
                )),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_unauthorized().await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::Zpool,
                    &zpool.id().to_string(),
                ),
            ),
        })
    }
}
