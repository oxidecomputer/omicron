// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Migration`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::{Migration, MigrationState};
use crate::db::schema::migration::dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
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
            .set(dsl::time_created.eq(dsl::time_created))
            .returning(Migration::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Marks a migration record as deleted if and only if both sides of the
    /// migration are in a terminal state.
    pub async fn migration_terminate(
        &self,
        opctx: &OpContext,
        migration_id: Uuid,
    ) -> UpdateResult<bool> {
        const TERMINAL_STATES: &[MigrationState] = &[
            MigrationState(nexus::MigrationState::Completed),
            MigrationState(nexus::MigrationState::Failed),
        ];

        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration_id))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::source_state.eq_any(TERMINAL_STATES))
            .filter(dsl::target_state.eq_any(TERMINAL_STATES))
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

    /// Unconditionally mark a migration record as deleted.
    ///
    /// This is distinct from [`DataStore::migration_terminate`], as it will
    /// mark a migration as deleted regardless of the states of the source and
    /// target VMMs.
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
