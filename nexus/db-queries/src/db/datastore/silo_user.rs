// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloUser`]s.

use super::DataStore;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::IdentityMetadataCreateParams;
use crate::db::model::Name;
use crate::db::model::Silo;
use crate::db::model::SiloUser;
use crate::db::model::SiloUserPasswordHash;
use crate::db::model::SiloUserPasswordUpdate;
use crate::db::model::UserBuiltin;
use crate::db::model::UserProvisionType;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_types::external_api::params;
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
use uuid::Uuid;

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
        use nexus_db_schema::schema::silo_user::dsl;

        let silo_user_external_id = silo_user.external_id.clone();
        let conn = self.pool_connection_unauthorized().await?;
        diesel::insert_into(dsl::silo_user)
            .values(silo_user)
            .returning(SiloUser::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloUser,
                        &silo_user_external_id,
                    ),
                )
            })
            .map(|db_silo_user: SiloUser| {
                let silo_user_id = db_silo_user.id();
                let authz_silo_user = authz::SiloUser::new(
                    authz_silo.clone(),
                    silo_user_id,
                    LookupType::by_id(silo_user_id),
                );
                (authz_silo_user, db_silo_user)
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
                        .check_if_exists::<SiloUser>(
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

    /// Given an external ID, return
    /// - Ok(Some((authz::SiloUser, SiloUser))) if that external id refers to an
    ///   existing silo user
    /// - Ok(None) if it does not
    /// - Err(...) if there was an error doing this lookup.
    pub async fn silo_user_fetch_by_external_id(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        external_id: &str,
    ) -> Result<Option<(authz::SiloUser, SiloUser)>, Error> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use nexus_db_schema::schema::silo_user::dsl;

        Ok(dsl::silo_user
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::external_id.eq(external_id.to_string()))
            .filter(dsl::time_deleted.is_null())
            .select(SiloUser::as_select())
            .load_async::<SiloUser>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .pop()
            .map(|db_silo_user| {
                let authz_silo_user = authz::SiloUser::new(
                    authz_silo.clone(),
                    db_silo_user.id(),
                    LookupType::ByName(external_id.to_owned()),
                );
                (authz_silo_user, db_silo_user)
            }))
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

        paginated(silo_user, id, pagparams)
            .filter(silo_id.eq(authz_silo_user_list.silo().id()))
            .filter(time_deleted.is_null())
            .select(SiloUser::as_select())
            .load_async::<SiloUser>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
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
            .select(SiloUser::as_select())
            .load_async::<SiloUser>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
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
        db_silo_user: &SiloUser,
        db_silo_user_password_hash: Option<SiloUserPasswordHash>,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, authz_silo_user).await?;

        // Verify that the various objects we're given actually match up.
        // Failures here reflect bugs, not bad input.
        bail_unless!(db_silo.id() == db_silo_user.silo_id);
        bail_unless!(db_silo_user.id() == authz_silo_user.id());
        if let Some(db_silo_user_password_hash) = &db_silo_user_password_hash {
            bail_unless!(
                db_silo_user_password_hash.silo_user_id() == db_silo_user.id()
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
            &authn::USER_EXTERNAL_SCIM,
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
