// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloGroup`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::IncompleteOnConflictExt;
use crate::db::datastore::RunnableQueryNoReturn;
use crate::db::model;
use crate::db::model::Silo;
use crate::db::model::SiloGroupMembership;
use crate::db::model::UserProvisionType;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloGroupUuid;
use omicron_uuid_kinds::SiloUserUuid;
use uuid::Uuid;

/// The datastore crate's SiloGroup is intended to provide type safety above the
/// database model, as the same database model is used to store semantically
/// different group types. Higher level parts of Nexus should use this type
/// instead.
#[derive(Debug, Clone)]
pub enum SiloGroup {
    /// A Group created via the API
    ApiOnly(SiloGroupApiOnly),

    /// A Group created during an authenticated SAML login
    Jit(SiloGroupJit),
}

impl SiloGroup {
    pub fn id(&self) -> SiloGroupUuid {
        match &self {
            SiloGroup::ApiOnly(u) => u.id,
            SiloGroup::Jit(u) => u.id,
        }
    }

    pub fn silo_id(&self) -> Uuid {
        match &self {
            SiloGroup::ApiOnly(u) => u.silo_id,
            SiloGroup::Jit(u) => u.silo_id,
        }
    }

    pub fn external_id(&self) -> &str {
        match &self {
            SiloGroup::ApiOnly(u) => &u.external_id,
            SiloGroup::Jit(u) => &u.external_id,
        }
    }
}

impl From<model::SiloGroup> for SiloGroup {
    fn from(record: model::SiloGroup) -> SiloGroup {
        match record.user_provision_type {
            UserProvisionType::ApiOnly => {
                SiloGroup::ApiOnly(SiloGroupApiOnly {
                    id: record.id(),
                    time_created: record.time_created(),
                    time_modified: record.time_modified(),
                    time_deleted: record.time_deleted,
                    silo_id: record.silo_id,
                    // SAFETY: there is a database constraint that prevents a group
                    // with provision type 'api_only' from having a null external id
                    external_id: record
                        .external_id
                        .expect("database constraint exists"),
                })
            }

            UserProvisionType::Jit => SiloGroup::Jit(SiloGroupJit {
                id: record.id(),
                time_created: record.time_created(),
                time_modified: record.time_modified(),
                time_deleted: record.time_deleted,
                silo_id: record.silo_id,
                // SAFETY: there is a database constraint that prevents a group
                // with provision type 'jit' from having a null external id
                external_id: record
                    .external_id
                    .expect("database constraint exists"),
            }),
        }
    }
}

impl From<SiloGroup> for model::SiloGroup {
    fn from(u: SiloGroup) -> model::SiloGroup {
        match u {
            SiloGroup::ApiOnly(u) => u.into(),
            SiloGroup::Jit(u) => u.into(),
        }
    }
}

impl From<SiloGroup> for views::Group {
    fn from(u: SiloGroup) -> views::Group {
        match u {
            SiloGroup::ApiOnly(u) => u.into(),
            SiloGroup::Jit(u) => u.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SiloGroupApiOnly {
    pub id: SiloGroupUuid,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_modified: chrono::DateTime<chrono::Utc>,
    pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
    pub silo_id: Uuid,

    /// The identity provider's ID for this group.
    pub external_id: String,
}

impl SiloGroupApiOnly {
    pub fn new(silo_id: Uuid, id: SiloGroupUuid, external_id: String) -> Self {
        Self {
            id,
            time_created: chrono::Utc::now(),
            time_modified: chrono::Utc::now(),
            time_deleted: None,
            silo_id,
            external_id,
        }
    }
}

impl From<SiloGroupApiOnly> for model::SiloGroup {
    fn from(u: SiloGroupApiOnly) -> model::SiloGroup {
        model::SiloGroup {
            identity: model::SiloGroupIdentity {
                id: u.id.into(),
                time_created: u.time_created,
                time_modified: u.time_modified,
            },
            time_deleted: u.time_deleted,
            silo_id: u.silo_id,
            external_id: Some(u.external_id),
            user_provision_type: UserProvisionType::ApiOnly,
        }
    }
}

impl From<SiloGroupApiOnly> for SiloGroup {
    fn from(u: SiloGroupApiOnly) -> SiloGroup {
        SiloGroup::ApiOnly(u)
    }
}

impl From<SiloGroupApiOnly> for views::Group {
    fn from(u: SiloGroupApiOnly) -> views::Group {
        views::Group {
            id: u.id,
            // TODO the use of external_id as display_name is temporary
            display_name: u.external_id,
            silo_id: u.silo_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SiloGroupJit {
    pub id: SiloGroupUuid,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_modified: chrono::DateTime<chrono::Utc>,
    pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
    pub silo_id: Uuid,

    /// The identity provider's ID for this user.
    pub external_id: String,
}

impl SiloGroupJit {
    pub fn new(silo_id: Uuid, id: SiloGroupUuid, external_id: String) -> Self {
        Self {
            id,
            time_created: chrono::Utc::now(),
            time_modified: chrono::Utc::now(),
            time_deleted: None,
            silo_id,
            external_id,
        }
    }
}

impl From<SiloGroupJit> for model::SiloGroup {
    fn from(u: SiloGroupJit) -> model::SiloGroup {
        model::SiloGroup {
            identity: model::SiloGroupIdentity {
                id: u.id.into(),
                time_created: u.time_created,
                time_modified: u.time_modified,
            },
            time_deleted: u.time_deleted,
            silo_id: u.silo_id,
            external_id: Some(u.external_id),
            user_provision_type: UserProvisionType::Jit,
        }
    }
}

impl From<SiloGroupJit> for SiloGroup {
    fn from(u: SiloGroupJit) -> SiloGroup {
        SiloGroup::Jit(u)
    }
}

impl From<SiloGroupJit> for views::Group {
    fn from(u: SiloGroupJit) -> views::Group {
        views::Group {
            id: u.id,
            // TODO the use of external_id as display_name is temporary
            display_name: u.external_id,
            silo_id: u.silo_id,
        }
    }
}

/// Different types of group have different fields that are considered unique,
/// and therefore lookup needs to be typed as well.
#[derive(Debug)]
pub enum SiloGroupLookup<'a> {
    ApiOnly { external_id: &'a str },

    Jit { external_id: &'a str },
}

impl<'a> SiloGroupLookup<'a> {
    pub fn user_provision_type(&self) -> UserProvisionType {
        match &self {
            SiloGroupLookup::ApiOnly { .. } => UserProvisionType::ApiOnly,
            SiloGroupLookup::Jit { .. } => UserProvisionType::Jit,
        }
    }
}

impl DataStore {
    pub(super) async fn silo_group_ensure_query(
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group: model::SiloGroup,
    ) -> Result<impl RunnableQueryNoReturn, Error> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        use nexus_db_schema::schema::silo_group::dsl;
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
        let conn = self.pool_connection_authorized(opctx).await?;

        let group_id = silo_group.id();
        let model: model::SiloGroup = silo_group.into();

        // Check the user's provision type matches the silo

        let silo = {
            use nexus_db_schema::schema::silo::dsl;
            dsl::silo
                .filter(dsl::id.eq(authz_silo.id()))
                .select(model::Silo::as_select())
                .get_result_async::<model::Silo>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(authz_silo),
                    )
                })?
        };

        if silo.user_provision_type != model.user_provision_type {
            error!(
                self.log,
                "silo {} provision type {:?} does not match silo group {} \
                provision type {:?}",
                authz_silo.id(),
                silo.user_provision_type,
                model.id(),
                model.user_provision_type,
            );

            return Err(Error::invalid_request(
                "user provision type of silo does not match silo group",
            ));
        }

        DataStore::silo_group_ensure_query(opctx, authz_silo, model)
            .await?
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let silo_group = {
            use nexus_db_schema::schema::silo_group::dsl;

            let maybe_silo_group = dsl::silo_group
                .filter(dsl::silo_id.eq(authz_silo.id()))
                .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
                .select(model::SiloGroup::as_select())
                .first_async(&*conn)
                .await
                .optional()
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            // This function is _not_ transactional, meaning a delete could race
            // between the ensure query and the lookup.

            let Some(silo_group) = maybe_silo_group else {
                return Err(Error::not_found_by_id(
                    ResourceType::SiloGroup,
                    group_id.as_untyped_uuid(),
                ));
            };

            silo_group
        };

        Ok(silo_group.into())
    }

    pub async fn silo_group_optional_lookup<'a>(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group_lookup: &'a SiloGroupLookup<'a>,
    ) -> LookupResult<Option<SiloGroup>> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        // Check the silo's provision type matches the lookup.

        let conn = self.pool_connection_authorized(opctx).await?;

        let silo = {
            use nexus_db_schema::schema::silo::dsl;
            dsl::silo
                .filter(dsl::id.eq(authz_silo.id()))
                .select(Silo::as_select())
                .get_result_async::<Silo>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(authz_silo),
                    )
                })?
        };

        let lookup_user_provision_type =
            silo_group_lookup.user_provision_type();

        if silo.user_provision_type != lookup_user_provision_type {
            return Err(Error::invalid_request(format!(
                "silo provision type {:?} does not match lookup {:?}",
                silo.user_provision_type, lookup_user_provision_type,
            )));
        }

        use nexus_db_schema::schema::silo_group::dsl;

        let maybe_silo_group = match silo_group_lookup {
            SiloGroupLookup::ApiOnly { external_id }
            | SiloGroupLookup::Jit { external_id } => dsl::silo_group
                .filter(dsl::silo_id.eq(authz_silo.id()))
                .filter(dsl::user_provision_type.eq(lookup_user_provision_type))
                .filter(dsl::external_id.eq(external_id.to_string()))
                .filter(dsl::time_deleted.is_null())
                .select(model::SiloGroup::as_select())
                .first_async(&*conn)
                .await
                .optional()
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?,
        };

        Ok(maybe_silo_group.map(|group| group.into()))
    }

    pub async fn silo_group_membership_for_user(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_user_id: SiloUserUuid,
    ) -> ListResultVec<SiloGroupMembership> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use nexus_db_schema::schema::silo_group_membership::dsl;
        dsl::silo_group_membership
            .filter(dsl::silo_user_id.eq(to_db_typed_uuid(silo_user_id)))
            .select(SiloGroupMembership::as_returning())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn silo_group_membership_for_group(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_group_id: SiloGroupUuid,
    ) -> ListResultVec<SiloGroupMembership> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use nexus_db_schema::schema::silo_group_membership::dsl;
        dsl::silo_group_membership
            .filter(dsl::silo_group_id.eq(to_db_typed_uuid(silo_group_id)))
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

        let silo_user_id = match actor.silo_user_id() {
            Some(silo_user_id) => silo_user_id,
            None => {
                return Err(Error::non_resourcetype_not_found(
                    "could not find silo user",
                ))?;
            }
        };

        use nexus_db_schema::schema::{
            silo_group as sg, silo_group_membership as sgm,
        };

        let page = paginated(sg::dsl::silo_group, sg::id, pagparams)
            .inner_join(sgm::table.on(sgm::silo_group_id.eq(sg::id)))
            .filter(sgm::silo_user_id.eq(to_db_typed_uuid(silo_user_id)))
            .filter(sg::time_deleted.is_null())
            .select(model::SiloGroup::as_returning())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(|group: model::SiloGroup| group.into())
            .collect::<Vec<SiloGroup>>();

        Ok(page)
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
        silo_group_ids: Vec<SiloGroupUuid>,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, authz_silo_user).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("silo_group_membership_replace_for_user")
            .transaction(&conn, |conn| {
                let silo_group_ids = silo_group_ids.clone();
                async move {
                    use nexus_db_schema::schema::silo_group_membership::dsl;

                    // Delete existing memberships for user
                    let silo_user_id = authz_silo_user.id();
                    diesel::delete(dsl::silo_group_membership)
                        .filter(
                            dsl::silo_user_id
                                .eq(to_db_typed_uuid(silo_user_id)),
                        )
                        .execute_async(&conn)
                        .await?;

                    // Create new memberships for user
                    let silo_group_memberships: Vec<
                        model::SiloGroupMembership,
                    > = silo_group_ids
                        .iter()
                        .map(|group_id| model::SiloGroupMembership {
                            silo_group_id: to_db_typed_uuid(*group_id),
                            silo_user_id: to_db_typed_uuid(silo_user_id),
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
            GroupStillHasMemberships(SiloGroupUuid),
        }
        type TxnError = TransactionError<SiloDeleteError>;

        let group_id = authz_silo_group.id();
        let conn = self.pool_connection_authorized(opctx).await?;

        // Prefer to use "transaction_retry_wrapper"
        self.transaction_non_retry_wrapper("silo_group_delete")
            .transaction(&conn, |conn| async move {
                use nexus_db_schema::schema::silo_group_membership;

                // Don't delete groups that still have memberships
                let group_memberships =
                    silo_group_membership::dsl::silo_group_membership
                        .filter(
                            silo_group_membership::dsl::silo_group_id
                                .eq(to_db_typed_uuid(group_id)),
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
                use nexus_db_schema::schema::silo_group::dsl;
                diesel::update(dsl::silo_group)
                    .filter(dsl::id.eq(to_db_typed_uuid(group_id)))
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
        use nexus_db_schema::schema::silo_group::dsl;

        opctx.authorize(authz::Action::Read, authz_silo).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        let silo = {
            use nexus_db_schema::schema::silo::dsl;
            dsl::silo
                .filter(dsl::id.eq(authz_silo.id()))
                .select(model::Silo::as_select())
                .get_result_async::<model::Silo>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(authz_silo),
                    )
                })?
        };

        let page = paginated(dsl::silo_group, dsl::id, pagparams)
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::user_provision_type.eq(silo.user_provision_type))
            .select(model::SiloGroup::as_select())
            .load_async::<model::SiloGroup>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(|group: model::SiloGroup| group.into())
            .collect::<Vec<SiloGroup>>();

        Ok(page)
    }
}
