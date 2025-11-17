// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloUser`]s.

use super::DataStore;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::IdentityMetadataCreateParams;
use crate::db::model;
use crate::db::model::Name;
use crate::db::model::Silo;
use crate::db::model::SiloUserPasswordHash;
use crate::db::model::SiloUserPasswordUpdate;
use crate::db::model::UserBuiltin;
use crate::db::model::UserProvisionType;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use uuid::Uuid;

/// The datastore crate's SiloUser is intended to provide type safety above the
/// database model, as the same database model is used to store semantically
/// different user types. Higher level parts of Nexus should use this type
/// instead.
#[derive(Debug, Clone)]
pub enum SiloUser {
    /// A User created via the API
    ApiOnly(SiloUserApiOnly),

    /// A User created during an authenticated SAML login
    Jit(SiloUserJit),

    /// A User created by a SCIM provisioning client
    Scim(SiloUserScim),
}

impl SiloUser {
    pub fn id(&self) -> SiloUserUuid {
        match &self {
            SiloUser::ApiOnly(u) => u.id,
            SiloUser::Jit(u) => u.id,
            SiloUser::Scim(u) => u.id,
        }
    }

    pub fn silo_id(&self) -> Uuid {
        match &self {
            SiloUser::ApiOnly(u) => u.silo_id,
            SiloUser::Jit(u) => u.silo_id,
            SiloUser::Scim(u) => u.silo_id,
        }
    }

    /// Return what field is guaranteed to be unique for this type
    pub fn conflict_field(&self) -> &str {
        match &self {
            SiloUser::ApiOnly(u) => &u.external_id,
            SiloUser::Jit(u) => &u.external_id,
            SiloUser::Scim(u) => &u.user_name,
        }
    }

    pub fn user_provision_type(&self) -> UserProvisionType {
        match &self {
            SiloUser::ApiOnly(_) => UserProvisionType::ApiOnly,
            SiloUser::Jit(_) => UserProvisionType::Jit,
            SiloUser::Scim(_) => UserProvisionType::Scim,
        }
    }

    /// Return if a user should be considered active, where inactive users
    /// should not have sessions created for them.
    pub fn is_active(&self) -> bool {
        match &self {
            SiloUser::ApiOnly(_) => true,

            SiloUser::Jit(_) => true,

            SiloUser::Scim(u) => {
                // If a SCIM provisioning client has informed Nexus of the
                // active field's value, use that. Otherwise, consider users
                // created by clients that do _not_ use the "active" field at
                // all to be active: otherwise every user created by a client
                // does not use the "active" field would be considered inactive.
                u.active.unwrap_or(true)
            }
        }
    }
}

impl From<model::SiloUser> for SiloUser {
    fn from(record: model::SiloUser) -> SiloUser {
        match record.user_provision_type {
            UserProvisionType::ApiOnly => SiloUser::ApiOnly(SiloUserApiOnly {
                id: record.id(),
                time_created: record.time_created(),
                time_modified: record.time_modified(),
                time_deleted: record.time_deleted,
                silo_id: record.silo_id,
                external_id: record.external_id.expect(
                    "the constraint `external_id_consistency` prevents a user \
                    with provision type 'api_only' from having a null \
                    external_id",
                ),
            }),

            UserProvisionType::Jit => SiloUser::Jit(SiloUserJit {
                id: record.id(),
                time_created: record.time_created(),
                time_modified: record.time_modified(),
                time_deleted: record.time_deleted,
                silo_id: record.silo_id,
                external_id: record.external_id.expect(
                    "the constraint `external_id_consistency` prevents a user \
                    with provision type 'jit' from having a null external_id",
                ),
            }),

            UserProvisionType::Scim => SiloUser::Scim(SiloUserScim {
                id: record.id(),
                time_created: record.time_created(),
                time_modified: record.time_modified(),
                time_deleted: record.time_deleted,
                silo_id: record.silo_id,
                user_name: record.user_name.expect(
                    "the constraint `user_name_consistency` prevents a user \
                    with provision type 'scim' from having null user_name",
                ),
                active: record.active,
                external_id: record.external_id,
            }),
        }
    }
}

impl From<SiloUser> for model::SiloUser {
    fn from(u: SiloUser) -> model::SiloUser {
        match u {
            SiloUser::ApiOnly(u) => u.into(),
            SiloUser::Jit(u) => u.into(),
            SiloUser::Scim(u) => u.into(),
        }
    }
}

impl From<SiloUser> for views::User {
    fn from(u: SiloUser) -> views::User {
        match u {
            SiloUser::ApiOnly(u) => u.into(),
            SiloUser::Jit(u) => u.into(),
            SiloUser::Scim(u) => u.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SiloUserApiOnly {
    pub id: SiloUserUuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub silo_id: Uuid,

    /// The identity provider's ID for this user.
    pub external_id: String,
}

impl SiloUserApiOnly {
    pub fn new(silo_id: Uuid, id: SiloUserUuid, external_id: String) -> Self {
        Self {
            id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
            silo_id,
            external_id,
        }
    }
}

impl From<SiloUserApiOnly> for model::SiloUser {
    fn from(u: SiloUserApiOnly) -> model::SiloUser {
        model::SiloUser {
            identity: model::SiloUserIdentity {
                id: u.id.into(),
                time_created: u.time_created,
                time_modified: u.time_modified,
            },
            time_deleted: u.time_deleted,
            silo_id: u.silo_id,
            external_id: Some(u.external_id),
            user_provision_type: UserProvisionType::ApiOnly,
            user_name: None,
            active: None,
        }
    }
}

impl From<SiloUserApiOnly> for SiloUser {
    fn from(u: SiloUserApiOnly) -> SiloUser {
        SiloUser::ApiOnly(u)
    }
}

impl From<SiloUserApiOnly> for views::User {
    fn from(u: SiloUserApiOnly) -> views::User {
        views::User {
            id: u.id,
            // TODO the use of external_id as display_name is temporary
            display_name: u.external_id,
            silo_id: u.silo_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SiloUserJit {
    pub id: SiloUserUuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub silo_id: Uuid,

    /// The identity provider's ID for this user.
    pub external_id: String,
}

impl SiloUserJit {
    pub fn new(silo_id: Uuid, id: SiloUserUuid, external_id: String) -> Self {
        Self {
            id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
            silo_id,
            external_id,
        }
    }
}

impl From<SiloUserJit> for model::SiloUser {
    fn from(u: SiloUserJit) -> model::SiloUser {
        model::SiloUser {
            identity: model::SiloUserIdentity {
                id: u.id.into(),
                time_created: u.time_created,
                time_modified: u.time_modified,
            },
            time_deleted: u.time_deleted,
            silo_id: u.silo_id,
            external_id: Some(u.external_id),
            user_provision_type: UserProvisionType::Jit,
            user_name: None,
            active: None,
        }
    }
}

impl From<SiloUserJit> for SiloUser {
    fn from(u: SiloUserJit) -> SiloUser {
        SiloUser::Jit(u)
    }
}

impl From<SiloUserJit> for views::User {
    fn from(u: SiloUserJit) -> views::User {
        views::User {
            id: u.id,
            // TODO the use of external_id as display_name is temporary
            display_name: u.external_id,
            silo_id: u.silo_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SiloUserScim {
    pub id: SiloUserUuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub silo_id: Uuid,

    /// The identity provider's ID for this user.
    pub user_name: String,

    pub active: Option<bool>,
    pub external_id: Option<String>,
}

impl SiloUserScim {
    pub fn new(
        silo_id: Uuid,
        id: SiloUserUuid,
        user_name: String,
        active: Option<bool>,
        external_id: Option<String>,
    ) -> Self {
        Self {
            id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
            silo_id,
            user_name,
            active,
            external_id,
        }
    }
}

impl From<SiloUserScim> for model::SiloUser {
    fn from(u: SiloUserScim) -> model::SiloUser {
        model::SiloUser {
            identity: model::SiloUserIdentity {
                id: u.id.into(),
                time_created: u.time_created,
                time_modified: u.time_modified,
            },
            time_deleted: u.time_deleted,
            silo_id: u.silo_id,
            external_id: u.external_id,
            user_provision_type: UserProvisionType::Scim,
            user_name: Some(u.user_name),
            active: u.active,
        }
    }
}

impl From<SiloUserScim> for SiloUser {
    fn from(u: SiloUserScim) -> SiloUser {
        SiloUser::Scim(u)
    }
}

impl From<SiloUserScim> for views::User {
    fn from(u: SiloUserScim) -> views::User {
        views::User {
            id: u.id,
            // TODO the use of user_name as display_name is temporary
            display_name: u.user_name,
            silo_id: u.silo_id,
        }
    }
}

/// Different types of user have different fields that are considered unique,
/// and therefore lookup needs to be typed as well.
#[derive(Debug)]
pub enum SiloUserLookup<'a> {
    ApiOnly { external_id: &'a str },

    Jit { external_id: &'a str },

    Scim { user_name: &'a str },
}

impl<'a> SiloUserLookup<'a> {
    pub fn user_provision_type(&self) -> UserProvisionType {
        match &self {
            SiloUserLookup::ApiOnly { .. } => UserProvisionType::ApiOnly,
            SiloUserLookup::Jit { .. } => UserProvisionType::Jit,
            SiloUserLookup::Scim { .. } => UserProvisionType::Scim,
        }
    }
}

impl DataStore {
    /// Create a silo user
    // TODO-security This function should take an OpContext and do an authz
    // check.
    pub async fn silo_user_create(
        &self,
        authz_silo: &authz::Silo,
        silo_user: SiloUser,
    ) -> CreateResult<(authz::SiloUser, SiloUser)> {
        // TODO-security This needs an authz check.

        let silo_user_conflict_field = silo_user.conflict_field().to_string();
        let model: model::SiloUser = silo_user.into();

        let conn = self.pool_connection_unauthorized().await?;

        // Check the user's provision type matches the silo

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

        if silo.user_provision_type != model.user_provision_type {
            error!(
                self.log,
                "silo {} provision type {:?} does not match silo user {} \
                provision type {:?}",
                authz_silo.id(),
                silo.user_provision_type,
                model.id(),
                model.user_provision_type,
            );

            return Err(Error::internal_error(&format!(
                "user provision type of silo ({:?}) does not match silo \
                user's ({:?})",
                silo.user_provision_type, model.user_provision_type,
            )));
        }

        use nexus_db_schema::schema::silo_user::dsl;
        diesel::insert_into(dsl::silo_user)
            .values(model)
            .returning(model::SiloUser::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloUser,
                        &silo_user_conflict_field,
                    ),
                )
            })
            .map(|db_silo_user: model::SiloUser| {
                let silo_user_id = db_silo_user.id();
                let authz_silo_user = authz::SiloUser::new(
                    authz_silo.clone(),
                    silo_user_id,
                    LookupType::by_id(silo_user_id),
                );
                (authz_silo_user, db_silo_user.into())
            })
    }

    /// Delete a Silo User
    pub async fn silo_user_delete(
        &self,
        opctx: &OpContext,
        authz_silo_user: &authz::SiloUser,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_silo_user).await?;

        // Delete the user and everything associated with them.
        // TODO-scalability Many of these should be done in batches.
        // TODO-robustness We might consider the RFD 192 "rcgen" pattern as well
        // so that people can't, say, login while we do this.
        let authz_silo_user_id = authz_silo_user.id();

        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("silo_user_delete")
            .transaction(&conn, |conn| async move {
                // Delete the user record.
                {
                    use nexus_db_schema::schema::silo_user::dsl;
                    diesel::update(dsl::silo_user)
                        .filter(
                            dsl::id.eq(to_db_typed_uuid(authz_silo_user_id)),
                        )
                        .filter(dsl::time_deleted.is_null())
                        .set(dsl::time_deleted.eq(Utc::now()))
                        .check_if_exists::<model::SiloUser>(
                            authz_silo_user_id.into_untyped_uuid(),
                        )
                        .execute_and_check(&conn)
                        .await?;
                }

                // Delete console sessions.
                {
                    use nexus_db_schema::schema::console_session::dsl;
                    diesel::delete(dsl::console_session)
                        .filter(
                            dsl::silo_user_id
                                .eq(to_db_typed_uuid(authz_silo_user_id)),
                        )
                        .execute_async(&conn)
                        .await?;
                }

                // Delete device authentication tokens.
                {
                    use nexus_db_schema::schema::device_access_token::dsl;
                    diesel::delete(dsl::device_access_token)
                        .filter(
                            dsl::silo_user_id
                                .eq(to_db_typed_uuid(authz_silo_user_id)),
                        )
                        .execute_async(&conn)
                        .await?;
                }

                // Delete group memberships.
                {
                    use nexus_db_schema::schema::silo_group_membership::dsl;
                    diesel::delete(dsl::silo_group_membership)
                        .filter(
                            dsl::silo_user_id
                                .eq(to_db_typed_uuid(authz_silo_user_id)),
                        )
                        .execute_async(&conn)
                        .await?;
                }

                // Delete ssh keys.
                {
                    use nexus_db_schema::schema::ssh_key::dsl;
                    diesel::update(dsl::ssh_key)
                        .filter(
                            dsl::silo_user_id
                                .eq(to_db_typed_uuid(authz_silo_user_id)),
                        )
                        .filter(dsl::time_deleted.is_null())
                        .set(dsl::time_deleted.eq(Utc::now()))
                        .execute_async(&conn)
                        .await?;
                }

                Ok(())
            })
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo_user),
                )
                .internal_context("deleting silo user")
            })
    }

    /// Given a silo user lookup, return
    /// - Ok(Some((authz::SiloUser, SiloUser))) if that lookup refers to an
    ///   existing silo user
    /// - Ok(None) if it does not
    /// - Err(...) if there was an error doing this lookup.
    pub async fn silo_user_fetch<'a>(
        &'a self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_user_lookup: &'a SiloUserLookup<'a>,
    ) -> Result<Option<(authz::SiloUser, SiloUser)>, Error> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use nexus_db_schema::schema::silo_user::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        // Check the silo's provision type matches the lookup.

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

        let lookup_user_provision_type = silo_user_lookup.user_provision_type();

        if silo.user_provision_type != lookup_user_provision_type {
            error!(
                self.log,
                "silo {} provision type {:?} does not match lookup provision \
                type {:?}",
                authz_silo.id(),
                silo.user_provision_type,
                lookup_user_provision_type,
            );

            return Err(Error::internal_error(&format!(
                "silo provision type {:?} does not match lookup {:?}",
                silo.user_provision_type, lookup_user_provision_type,
            )));
        }

        match silo_user_lookup {
            SiloUserLookup::ApiOnly { external_id }
            | SiloUserLookup::Jit { external_id } => {
                let maybe_db_silo_user = dsl::silo_user
                    .filter(dsl::silo_id.eq(authz_silo.id()))
                    .filter(
                        dsl::user_provision_type.eq(lookup_user_provision_type),
                    )
                    .filter(dsl::external_id.eq(external_id.to_string()))
                    .filter(dsl::time_deleted.is_null())
                    .select(model::SiloUser::as_select())
                    .get_result_async::<model::SiloUser>(&*conn)
                    .await
                    .optional()
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

                let Some(db_silo_user) = maybe_db_silo_user else {
                    return Ok(None);
                };

                let authz_silo_user = authz::SiloUser::new(
                    authz_silo.clone(),
                    db_silo_user.id(),
                    LookupType::ByName(external_id.to_string()),
                );

                Ok(Some((authz_silo_user, db_silo_user.into())))
            }

            SiloUserLookup::Scim { user_name } => {
                let maybe_db_silo_user = dsl::silo_user
                    .filter(dsl::silo_id.eq(authz_silo.id()))
                    .filter(
                        dsl::user_provision_type.eq(lookup_user_provision_type),
                    )
                    .filter(dsl::user_name.eq(user_name.to_string()))
                    .filter(dsl::time_deleted.is_null())
                    .select(model::SiloUser::as_select())
                    .get_result_async::<model::SiloUser>(&*conn)
                    .await
                    .optional()
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

                let Some(db_silo_user) = maybe_db_silo_user else {
                    return Ok(None);
                };

                let authz_silo_user = authz::SiloUser::new(
                    authz_silo.clone(),
                    db_silo_user.id(),
                    LookupType::ByName(user_name.to_string()),
                );

                Ok(Some((authz_silo_user, db_silo_user.into())))
            }
        }
    }

    pub async fn silo_users_list(
        &self,
        opctx: &OpContext,
        authz_silo_user_list: &authz::SiloUserList,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloUser> {
        use nexus_db_schema::schema::silo_user::dsl::*;

        opctx
            .authorize(authz::Action::ListChildren, authz_silo_user_list)
            .await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        let db_silo = {
            use nexus_db_schema::schema::silo::dsl;
            dsl::silo
                .filter(dsl::id.eq(authz_silo_user_list.silo().id()))
                .select(Silo::as_select())
                .get_result_async::<Silo>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(
                            authz_silo_user_list.silo(),
                        ),
                    )
                })?
        };

        paginated(silo_user, id, pagparams)
            .filter(silo_id.eq(authz_silo_user_list.silo().id()))
            .filter(user_provision_type.eq(db_silo.user_provision_type))
            .filter(time_deleted.is_null())
            .select(model::SiloUser::as_select())
            .load_async::<model::SiloUser>(&*conn)
            .await
            .map(|list| list.into_iter().map(|user| user.into()).collect())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn silo_group_users_list(
        &self,
        opctx: &OpContext,
        authz_silo_user_list: &authz::SiloUserList,
        pagparams: &DataPageParams<'_, Uuid>,
        // TODO: this should be an authz::SiloGroup probably
        authz_silo_group: &authz::SiloGroup,
    ) -> ListResultVec<SiloUser> {
        use nexus_db_schema::schema::silo_group_membership as user_to_group;
        use nexus_db_schema::schema::silo_user as user;

        opctx
            .authorize(authz::Action::ListChildren, authz_silo_user_list)
            .await?;

        paginated(user::table, user::id, pagparams)
            .filter(user::silo_id.eq(authz_silo_user_list.silo().id()))
            .filter(user::time_deleted.is_null())
            .inner_join(
                user_to_group::table.on(user_to_group::silo_user_id
                    .eq(user::id)
                    .and(
                        user_to_group::silo_group_id
                            .eq(to_db_typed_uuid(authz_silo_group.id())),
                    )),
            )
            .select(model::SiloUser::as_select())
            .load_async::<model::SiloUser>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map(|list| list.into_iter().map(|user| user.into()).collect())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Updates or deletes the password hash for a given Silo user
    ///
    /// If `password_hash` is `Some(...)`, the provided value is stored as the
    /// user's password hash.  Otherwise, any existing hash is deleted so that
    /// it cannot be used for authentication any more.
    pub async fn silo_user_password_hash_set(
        &self,
        opctx: &OpContext,
        db_silo: &Silo,
        authz_silo_user: &authz::SiloUser,
        silo_user: &SiloUserApiOnly,
        db_silo_user_password_hash: Option<SiloUserPasswordHash>,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, authz_silo_user).await?;

        // Verify that the various objects we're given actually match up.
        // Failures here reflect bugs, not bad input.
        bail_unless!(db_silo.id() == silo_user.silo_id);
        bail_unless!(silo_user.id == authz_silo_user.id());
        if let Some(db_silo_user_password_hash) = &db_silo_user_password_hash {
            bail_unless!(
                db_silo_user_password_hash.silo_user_id() == silo_user.id
            );
        }

        // Verify that this Silo supports setting local passwords on users.
        // The caller is supposed to have verified this already.
        bail_unless!(db_silo.user_provision_type == UserProvisionType::ApiOnly);

        use nexus_db_schema::schema::silo_user_password_hash::dsl;

        if let Some(db_silo_user_password_hash) = db_silo_user_password_hash {
            let hash_for_update = db_silo_user_password_hash.hash.clone();
            diesel::insert_into(dsl::silo_user_password_hash)
                .values(db_silo_user_password_hash)
                .on_conflict(dsl::silo_user_id)
                .do_update()
                .set(SiloUserPasswordUpdate::new(hash_for_update))
                .execute_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        } else {
            diesel::delete(dsl::silo_user_password_hash)
                .filter(
                    dsl::silo_user_id
                        .eq(to_db_typed_uuid(authz_silo_user.id())),
                )
                .execute_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        Ok(())
    }

    /// Fetches the stored password hash for a given Silo user
    ///
    /// Returns:
    ///
    /// - `Ok(Some(SiloUserPasswordHash))` if the hash was found
    /// - `Ok(None)` if we successfully queried for it but found none
    /// - `Err(...)` otherwise
    pub async fn silo_user_password_hash_fetch(
        &self,
        opctx: &OpContext,
        authz_silo_user: &authz::SiloUser,
    ) -> LookupResult<Option<SiloUserPasswordHash>> {
        // Although it should be safe enough to leak a password hash, there's no
        // reason to give it out to people who only have "read" access to the
        // user.  So we check for "modify" here.  (Arguably we could consider
        // the password a separate resource nested under the user, so that this
        // would be a check on "read" of the user's password.  This doesn't seem
        // worth the effort right now.)
        opctx.authorize(authz::Action::Modify, authz_silo_user).await?;

        use nexus_db_schema::schema::silo_user_password_hash::dsl;
        Ok(dsl::silo_user_password_hash
            .filter(
                dsl::silo_user_id.eq(to_db_typed_uuid(authz_silo_user.id())),
            )
            .select(SiloUserPasswordHash::as_select())
            .load_async::<SiloUserPasswordHash>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .pop())
    }

    pub async fn users_builtin_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<UserBuiltin> {
        use nexus_db_schema::schema::user_builtin::dsl;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated(dsl::user_builtin, dsl::name, pagparams)
            .select(UserBuiltin::as_select())
            .load_async::<UserBuiltin>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Load built-in users into the database
    pub async fn load_builtin_users(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_builtin::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let builtin_users = [
            // Note: "db_init" is also a builtin user, but that one by necessity
            // is created with the database.
            &authn::USER_SERVICE_BALANCER,
            &authn::USER_INTERNAL_API,
            &authn::USER_INTERNAL_READ,
            &authn::USER_EXTERNAL_AUTHN,
            &authn::USER_SAGA_RECOVERY,
        ]
        .iter()
        .map(|u| {
            UserBuiltin::new(
                u.id,
                params::UserBuiltinCreate {
                    identity: IdentityMetadataCreateParams {
                        name: u.name.clone(),
                        description: String::from(u.description),
                    },
                },
            )
        })
        .collect::<Vec<UserBuiltin>>();

        debug!(opctx.log, "attempting to create built-in users");

        let count = diesel::insert_into(dsl::user_builtin)
            .values(builtin_users)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        info!(opctx.log, "created {} built-in users", count);

        Ok(())
    }

    /// Load the testing users into the database
    pub async fn load_silo_users(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::silo_user::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let users =
            [&*authn::USER_TEST_PRIVILEGED, &*authn::USER_TEST_UNPRIVILEGED];

        debug!(opctx.log, "attempting to create silo users");
        let count = diesel::insert_into(dsl::silo_user)
            .values(users)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        info!(opctx.log, "created {} silo users", count);

        Ok(())
    }

    /// Load role assignments for the test users into the database
    pub async fn load_silo_user_role_assignments(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::role_assignment::dsl;
        debug!(opctx.log, "attempting to create silo user role assignments");
        let count = diesel::insert_into(dsl::role_assignment)
            .values(
                &*nexus_db_fixed_data::silo_user::ROLE_ASSIGNMENTS_PRIVILEGED,
            )
            .on_conflict((
                dsl::identity_type,
                dsl::identity_id,
                dsl::resource_type,
                dsl::resource_id,
                dsl::role_name,
            ))
            .do_nothing()
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        info!(opctx.log, "created {} silo user role assignments", count);

        Ok(())
    }
}
