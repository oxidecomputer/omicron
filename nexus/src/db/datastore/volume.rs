// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Volume`]s.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Volume;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    pub async fn volume_create(&self, volume: Volume) -> CreateResult<Volume> {
        use db::schema::volume::dsl;

        diesel::insert_into(dsl::volume)
            .values(volume.clone())
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(Volume::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Volume,
                        volume.id().to_string().as_str(),
                    ),
                )
            })
    }

    pub async fn volume_delete(&self, volume_id: Uuid) -> DeleteResult {
        use db::schema::volume::dsl;

        let now = Utc::now();
        diesel::update(dsl::volume)
            .filter(dsl::id.eq(volume_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<Volume>(volume_id)
            .execute_and_check(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Volume,
                        LookupType::ById(volume_id),
                    ),
                )
            })?;
        Ok(())
    }

    pub async fn volume_get(&self, volume_id: Uuid) -> LookupResult<Volume> {
        use db::schema::volume::dsl;

        dsl::volume
            .filter(dsl::id.eq(volume_id))
            .select(Volume::as_select())
            .get_result_async(self.pool())
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}
