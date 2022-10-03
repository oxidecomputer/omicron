// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`SiloUser`]s.

use super::DataStore;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::IdentityMetadataCreateParams;
use crate::db::datastore::LookupType;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::Name;
use crate::db::model::SiloUser;
use crate::db::model::UserBuiltin;
use crate::db::pagination::paginated;
use crate::external_api::params;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Create a silo user
    pub async fn silo_user_create(
        &self,
        authz_silo: &authz::Silo,
        silo_user: SiloUser,
    ) -> CreateResult<(authz::SiloUser, SiloUser)> {
        // TODO-security This needs an authz check.
        use db::schema::silo_user::dsl;

        let silo_user_external_id = silo_user.external_id.clone();
        diesel::insert_into(dsl::silo_user)
            .values(silo_user)
            .returning(SiloUser::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
                    LookupType::ById(silo_user_id),
                );
                (authz_silo_user, db_silo_user)
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

        use db::schema::silo_user::dsl;

        Ok(dsl::silo_user
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::external_id.eq(external_id.to_string()))
            .filter(dsl::time_deleted.is_null())
            .select(SiloUser::as_select())
            .load_async::<SiloUser>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?
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

    pub async fn silo_users_list_by_id(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloUser> {
        use db::schema::silo_user::dsl;

        opctx.authorize(authz::Action::Read, authz_silo).await?;
        paginated(dsl::silo_user, dsl::id, pagparams)
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::time_deleted.is_null())
            .select(SiloUser::as_select())
            .load_async::<SiloUser>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn users_builtin_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<UserBuiltin> {
        use db::schema::user_builtin::dsl;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated(dsl::user_builtin, dsl::name, pagparams)
            .select(UserBuiltin::as_select())
            .load_async::<UserBuiltin>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Load built-in users into the database
    pub async fn load_builtin_users(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use db::schema::user_builtin::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let builtin_users = [
            // Note: "db_init" is also a builtin user, but that one by necessity
            // is created with the database.
            &*authn::USER_SERVICE_BALANCER,
            &*authn::USER_INTERNAL_API,
            &*authn::USER_INTERNAL_READ,
            &*authn::USER_EXTERNAL_AUTHN,
            &*authn::USER_SAGA_RECOVERY,
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
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in users", count);

        Ok(())
    }

    /// Load the testing users into the database
    pub async fn load_silo_users(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use db::schema::silo_user::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let users =
            [&*authn::USER_TEST_PRIVILEGED, &*authn::USER_TEST_UNPRIVILEGED];

        debug!(opctx.log, "attempting to create silo users");
        let count = diesel::insert_into(dsl::silo_user)
            .values(users)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} silo users", count);

        Ok(())
    }

    /// Load role assignments for the test users into the database
    pub async fn load_silo_user_role_assignments(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use db::schema::role_assignment::dsl;
        debug!(opctx.log, "attempting to create silo user role assignments");
        let count = diesel::insert_into(dsl::role_assignment)
            .values(&*db::fixed_data::silo_user::ROLE_ASSIGNMENTS_PRIVILEGED)
            .on_conflict((
                dsl::identity_type,
                dsl::identity_id,
                dsl::resource_type,
                dsl::resource_id,
                dsl::role_name,
            ))
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} silo user role assignments", count);

        Ok(())
    }
}
