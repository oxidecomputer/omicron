// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloGroup`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::RunnableQuery;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::lookup::LookupPath;
use crate::db::model::SiloGroup;
use crate::db::model::SiloGroupMembership;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::{AsyncConnection, OptionalExtension};
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    pub async fn silo_group_create_query(
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group: SiloGroup,
    ) -> Result<impl RunnableQuery<SiloGroup>, Error> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        use db::schema::silo_group::dsl;
        Ok(diesel::insert_into(dsl::silo_group)
            .values(silo_group)
            .returning(SiloGroup::as_returning()))
    }

    pub async fn silo_group_create(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group: SiloGroup,
    ) -> CreateResult<SiloGroup> {
        DataStore::silo_group_create_query(opctx, authz_silo, silo_group)
            .await?
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
            .filter(dsl::time_deleted.is_null())
            .select(db::model::SiloGroup::as_select())
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .optional()
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

    /// Update a silo user's group membership:
    ///
    /// - add the user to groups they are supposed to be a member of, and
    /// - remove the user from groups if they no longer have membership
    ///
    /// Do this as one transaction that deletes all current memberships for a
    /// user, then adds back the ones they are in. This avoids the scenario
    /// where a crash half way through causes the resulting group memberships to
    /// be incorrect.
    pub async fn silo_group_membership_replace_for_user(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
        silo_group_ids: Vec<Uuid>,
    ) -> UpdateResult<()> {
        let (_authz_silo_user, ..) = LookupPath::new(opctx, &self)
            .silo_user_id(silo_user_id)
            .fetch_for(authz::Action::Modify)
            .await?;

        self.pool_authorized(opctx)
            .await?
            .transaction(move |conn| {
                use db::schema::silo_group_membership::dsl;

                // Delete existing memberships for user
                diesel::delete(dsl::silo_group_membership)
                    .filter(dsl::silo_user_id.eq(silo_user_id))
                    .execute(conn)?;

                // Create new memberships for user
                let silo_group_memberships: Vec<
                    db::model::SiloGroupMembership,
                > = silo_group_ids
                    .iter()
                    .map(|group_id| db::model::SiloGroupMembership {
                        silo_group_id: *group_id,
                        silo_user_id: silo_user_id,
                    })
                    .collect();

                diesel::insert_into(dsl::silo_group_membership)
                    .values(silo_group_memberships)
                    .execute(conn)?;

                Ok(())
            })
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_group_delete(
        &self,
        opctx: &OpContext,
        authz_silo_group: &authz::SiloGroup,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_silo_group).await?;

        #[derive(Debug, thiserror::Error)]
        enum SiloDeleteError {
            #[error("group {0} still has memberships")]
            GroupStillHasMemberships(Uuid),
        }
        type TxnError = TransactionError<SiloDeleteError>;

        let group_id = authz_silo_group.id();

        self.pool_authorized(opctx)
            .await?
            .transaction(move |conn| {
                use db::schema::silo_group_membership;

                // Don't delete groups that still have memberships
                let group_memberships =
                    silo_group_membership::dsl::silo_group_membership
                        .filter(
                            silo_group_membership::dsl::silo_group_id
                                .eq(group_id),
                        )
                        .select(SiloGroupMembership::as_returning())
                        .load(conn)?;

                if !group_memberships.is_empty() {
                    return Err(TxnError::CustomError(
                        SiloDeleteError::GroupStillHasMemberships(group_id),
                    ));
                }

                // Delete silo group
                use db::schema::silo_group::dsl;
                diesel::update(dsl::silo_group)
                    .filter(dsl::id.eq(group_id))
                    .filter(dsl::time_deleted.is_null())
                    .set(dsl::time_deleted.eq(Utc::now()))
                    .execute(conn)?;

                Ok(())
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(
                    SiloDeleteError::GroupStillHasMemberships(id),
                ) => Error::invalid_request(&format!(
                    "group {0} still has memberships",
                    id
                )),

                TxnError::Pool(pool_error) => public_error_from_diesel_pool(
                    pool_error,
                    ErrorHandler::Server,
                ),
            })
    }
}
