// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloGroup`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::RunnableQueryNoReturn;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::SiloGroup;
use crate::db::model::SiloGroupMembership;
use crate::db::pagination::paginated;
use crate::db::IncompleteOnConflictExt;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::OptionalExtension;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    pub(super) async fn silo_group_ensure_query(
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group: SiloGroup,
    ) -> Result<impl RunnableQueryNoReturn, Error> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        use db::schema::silo_group::dsl;
        Ok(diesel::insert_into(dsl::silo_group)
            .values(silo_group)
            .on_conflict((dsl::silo_id, dsl::external_id))
            .as_partial_index()
            .do_nothing())
    }

    pub async fn silo_group_ensure(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group: SiloGroup,
    ) -> CreateResult<SiloGroup> {
        let external_id = silo_group.external_id.clone();

        DataStore::silo_group_ensure_query(opctx, authz_silo, silo_group)
            .await?
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(self
            .silo_group_optional_lookup(opctx, authz_silo, external_id)
            .await?
            .unwrap())
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
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn silo_groups_for_self(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloGroup> {
        // Similar to session_hard_delete (see comment there), we do not do a
        // typical authz check, instead effectively encoding the policy here
        // that any user is allowed to fetch their own group memberships
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("fetching current user's group memberships")?;

        use db::schema::{silo_group as sg, silo_group_membership as sgm};
        paginated(sg::dsl::silo_group, sg::id, pagparams)
            .inner_join(sgm::table.on(sgm::silo_group_id.eq(sg::id)))
            .filter(sgm::silo_user_id.eq(actor.actor_id()))
            .filter(sg::time_deleted.is_null())
            .select(SiloGroup::as_returning())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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
        authz_silo_user: &authz::SiloUser,
        silo_group_ids: Vec<Uuid>,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, authz_silo_user).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("silo_group_membership_replace_for_user")
            .transaction(&conn, |conn| {
                let silo_group_ids = silo_group_ids.clone();
                async move {
                    use db::schema::silo_group_membership::dsl;

                    // Delete existing memberships for user
                    let silo_user_id = authz_silo_user.id();
                    diesel::delete(dsl::silo_group_membership)
                        .filter(dsl::silo_user_id.eq(silo_user_id))
                        .execute_async(&conn)
                        .await?;

                    // Create new memberships for user
                    let silo_group_memberships: Vec<
                        db::model::SiloGroupMembership,
                    > = silo_group_ids
                        .iter()
                        .map(|group_id| db::model::SiloGroupMembership {
                            silo_group_id: *group_id,
                            silo_user_id,
                        })
                        .collect();

                    diesel::insert_into(dsl::silo_group_membership)
                        .values(silo_group_memberships)
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::silo_group_membership;

                // Don't delete groups that still have memberships
                let group_memberships =
                    silo_group_membership::dsl::silo_group_membership
                        .filter(
                            silo_group_membership::dsl::silo_group_id
                                .eq(group_id),
                        )
                        .select(SiloGroupMembership::as_returning())
                        .limit(1)
                        .load_async(&conn)
                        .await?;

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
                    .execute_async(&conn)
                    .await?;

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
                TxnError::Database(error) => {
                    public_error_from_diesel(error, ErrorHandler::Server)
                }
            })
    }

    pub async fn silo_groups_list_by_id(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloGroup> {
        use db::schema::silo_group::dsl;

        opctx.authorize(authz::Action::Read, authz_silo).await?;
        paginated(dsl::silo_group, dsl::id, pagparams)
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::time_deleted.is_null())
            .select(SiloGroup::as_select())
            .load_async::<SiloGroup>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
