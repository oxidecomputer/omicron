// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Migration`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Migration;
use crate::db::schema::migration::dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    /// Insert a database record for a migration.
    pub async fn migration_insert(
        &self,
        opctx: &OpContext,
        migration: Migration,
    ) -> CreateResult<Migration> {
        diesel::insert_into(dsl::migration)
            .values(migration)
            .on_conflict(dsl::id)
            .do_update()
            // I don't know what this does but it somehow
            .set(dsl::time_created.eq(dsl::time_created))
            .returning(Migration::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn migration_mark_deleted(
        &self,
        opctx: &OpContext,
        migration_id: Uuid,
    ) -> UpdateResult<bool> {
        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration_id))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<Migration>(migration_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
