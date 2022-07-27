// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::UpdateAvailableArtifact;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::InternalContext;

impl DataStore {
    pub async fn update_available_artifact_upsert(
        &self,
        opctx: &OpContext,
        artifact: UpdateAvailableArtifact,
    ) -> CreateResult<UpdateAvailableArtifact> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use db::schema::update_available_artifact::dsl;
        diesel::insert_into(dsl::update_available_artifact)
            .values(artifact.clone())
            .on_conflict((dsl::name, dsl::version, dsl::kind))
            .do_update()
            .set(artifact.clone())
            .returning(UpdateAvailableArtifact::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn update_available_artifact_hard_delete_outdated(
        &self,
        opctx: &OpContext,
        current_targets_role_version: i64,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // We use the `targets_role_version` column in the table to delete any
        // old rows, keeping the table in sync with the current copy of
        // artifacts.json.
        use db::schema::update_available_artifact::dsl;
        diesel::delete(dsl::update_available_artifact)
            .filter(dsl::targets_role_version.lt(current_targets_role_version))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
            .internal_context("deleting outdated available artifacts")
    }
}
