// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SshKey`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::model::Name;
use crate::db::model::SshKey;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use crate::db::update_and_check::UpdateAndCheck;

impl DataStore {
    pub async fn ssh_keys_list(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        page_params: &DataPageParams<'_, Name>,
    ) -> ListResultVec<SshKey> {
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        use db::schema::ssh_key::dsl;
        paginated(dsl::ssh_key, dsl::name, page_params)
            .filter(dsl::silo_user_id.eq(authz_user.id()))
            .filter(dsl::time_deleted.is_null())
            .select(SshKey::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Create a new SSH public key for a user.
    pub async fn ssh_key_create(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        ssh_key: SshKey,
    ) -> CreateResult<SshKey> {
        assert_eq!(authz_user.id(), ssh_key.silo_user_id);
        opctx.authorize(authz::Action::CreateChild, authz_user).await?;
        let name = ssh_key.name().to_string();

        use db::schema::ssh_key::dsl;
        diesel::insert_into(dsl::ssh_key)
            .values(ssh_key)
            .returning(SshKey::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::SshKey, &name),
                )
            })
    }

    /// Delete an existing SSH public key.
    pub async fn ssh_key_delete(
        &self,
        opctx: &OpContext,
        authz_ssh_key: &authz::SshKey,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_ssh_key).await?;

        use db::schema::ssh_key::dsl;
        diesel::update(dsl::ssh_key)
            .filter(dsl::id.eq(authz_ssh_key.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<SshKey>(authz_ssh_key.id())
            .execute_and_check(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_ssh_key),
                )
            })?;
        Ok(())
    }
}
