// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloGroup`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::lookup::LookupPath;
use crate::db::model::SiloGroup;
use crate::db::model::SiloGroupMembership;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;
use async_bb8_diesel::OptionalExtension;

impl DataStore {
    pub async fn silo_group_create(
        &self,
        opctx: &OpContext,
        silo_group: SiloGroup,
    ) -> CreateResult<SiloGroup> {
        use db::schema::silo_group::dsl;

        diesel::insert_into(dsl::silo_group)
            .values(silo_group)
            .returning(SiloGroup::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_group_optional_lookup(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        external_id: String,
    ) -> LookupResult<Option<db::model::SiloGroup>> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use db::schema::silo_group::dsl;

        dsl::silo_group
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::external_id.eq(external_id))
            .select(db::model::SiloGroup::as_select())
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_group_membership_create(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group_membership: SiloGroupMembership,
    ) -> CreateResult<SiloGroupMembership> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        let existing_group_memberships = self
            .silo_group_membership_for_user(
                opctx,
                authz_silo,
                silo_group_membership.silo_user_id,
            )
            .await?;

        if existing_group_memberships
            .into_iter()
            .map(|x| x.silo_group_id)
            .any(|x| x == silo_group_membership.silo_group_id)
        {
            return Ok(silo_group_membership);
        }

        let (_authz_silo_group, ..) = LookupPath::new(opctx, &self)
            .silo_group_id(silo_group_membership.silo_group_id)
            .fetch_for(authz::Action::Modify)
            .await?;

        use db::schema::silo_group_membership::dsl;
        diesel::insert_into(dsl::silo_group_membership)
            .values(silo_group_membership)
            .returning(SiloGroupMembership::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_group_membership_for_user(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_user_id: Uuid,
    ) -> ListResultVec<SiloGroupMembership> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use db::schema::silo_group_membership::dsl;
        dsl::silo_group_membership
            .filter(dsl::silo_user_id.eq(silo_user_id))
            .select(SiloGroupMembership::as_returning())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    // For use in [`load_roles_for_resource`], which cannot perform authz
    // lookup, because that would cause an infinite loop and overload the stack
    pub async fn silo_group_membership_for_user_no_authz(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> ListResultVec<SiloGroupMembership> {
        use db::schema::silo_group_membership::dsl;
        dsl::silo_group_membership
            .filter(dsl::silo_user_id.eq(silo_user_id))
            .select(SiloGroupMembership::as_returning())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_group_membership_delete(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_user_id: Uuid,
        silo_group_id: Uuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        let (_authz_silo_group, ..) = LookupPath::new(opctx, &self)
            .silo_group_id(silo_group_id)
            .fetch_for(authz::Action::Modify)
            .await?;

        use db::schema::silo_group_membership::dsl;
        diesel::delete(dsl::silo_group_membership)
            .filter(dsl::silo_user_id.eq(silo_user_id))
            .filter(dsl::silo_group_id.eq(silo_group_id))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting group membership of group {} for user {}: {:?}",
                    silo_group_id,
                    silo_user_id,
                    e,
                ))
            })?;
        Ok(())
    }

    pub async fn silo_group_delete(
        &self,
        opctx: &OpContext,
        authz_silo_group: &authz::SiloGroup,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_silo_group).await?;

        use db::schema::silo_group::dsl;
        diesel::update(dsl::silo_group)
            .filter(dsl::id.eq(authz_silo_group.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<SiloGroup>(authz_silo_group.id())
            .execute_and_check(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo_group),
                )
            })?;
        Ok(())
    }
}
