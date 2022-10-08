// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`Silo`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::RunnableQuery;
use crate::db::error::diesel_pool_result_optional;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::fixed_data::silo::DEFAULT_SILO;
use crate::db::identity::Resource;
use crate::db::model::CollectionType;
use crate::db::model::Name;
use crate::db::model::ResourceUsage;
use crate::db::model::Silo;
use crate::db::pagination::paginated;
use crate::external_api::params;
use crate::external_api::shared;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use uuid::Uuid;

impl DataStore {
    /// Load built-in silos into the database
    pub async fn load_builtin_silos(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in silo");

        use db::schema::silo::dsl;
        let count = diesel::insert_into(dsl::silo)
            .values(&*DEFAULT_SILO)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in silos", count);

        self.resource_usage_create(
            opctx,
            ResourceUsage::new(DEFAULT_SILO.id(), CollectionType::Silo),
        )
        .await?;

        Ok(())
    }

    async fn silo_create_query(
        opctx: &OpContext,
        silo: Silo,
    ) -> Result<impl RunnableQuery<Silo>, Error> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        Ok(diesel::insert_into(dsl::silo)
            .values(silo)
            .returning(Silo::as_returning()))
    }

    pub async fn silo_create(
        &self,
        opctx: &OpContext,
        group_opctx: &OpContext,
        new_silo_params: params::SiloCreate,
    ) -> CreateResult<Silo> {
        let silo_id = Uuid::new_v4();
        let silo_group_id = Uuid::new_v4();

        let silo_create_query = Self::silo_create_query(
            opctx,
            db::model::Silo::new_with_id(
                silo_id,
                new_silo_params.clone(),
                *db::fixed_data::FLEET_ID,
            ),
        )
        .await?;

        let authz_silo =
            authz::Silo::new(authz::FLEET, silo_id, LookupType::ById(silo_id));

        let silo_admin_group_ensure_query = if let Some(ref admin_group_name) =
            new_silo_params.admin_group_name
        {
            let silo_admin_group_ensure_query =
                DataStore::silo_group_ensure_query(
                    &group_opctx,
                    &authz_silo,
                    db::model::SiloGroup::new(
                        silo_group_id,
                        silo_id,
                        admin_group_name.clone(),
                    ),
                )
                .await?;

            Some(silo_admin_group_ensure_query)
        } else {
            None
        };

        let silo_admin_group_role_assignment_queries =
            if new_silo_params.admin_group_name.is_some() {
                // Grant silo admin role for members of the admin group.
                let policy = shared::Policy {
                    role_assignments: vec![shared::RoleAssignment {
                        identity_type: shared::IdentityType::SiloGroup,
                        identity_id: silo_group_id,
                        role_name: authz::SiloRole::Admin,
                    }],
                };

                let silo_admin_group_role_assignment_queries =
                    DataStore::role_assignment_replace_visible_queries(
                        opctx,
                        &authz_silo,
                        &policy.role_assignments,
                    )
                    .await?;

                Some(silo_admin_group_role_assignment_queries)
            } else {
                None
            };

        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let silo = silo_create_query.get_result_async(&conn).await?;
                use db::schema::resource_usage::dsl;
                diesel::insert_into(dsl::resource_usage)
                    .values(ResourceUsage::new(silo.id(), CollectionType::Silo))
                    .execute_async(&conn)
                    .await?;

                self.resource_usage_create_on_connection(
                    &conn,
                    ResourceUsage::new(DEFAULT_SILO.id(), CollectionType::Silo),
                )
                .await?;

                if let Some(query) = silo_admin_group_ensure_query {
                    query.get_result_async(&conn).await?;
                }

                if let Some(queries) = silo_admin_group_role_assignment_queries
                {
                    let (delete_old_query, insert_new_query) = queries;
                    delete_old_query.execute_async(&conn).await?;
                    insert_new_query.execute_async(&conn).await?;
                }

                Ok(silo)
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn silos_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        paginated(dsl::silo, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silos_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        paginated(dsl::silo, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_delete(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        db_silo: &db::model::Silo,
    ) -> DeleteResult {
        assert_eq!(authz_silo.id(), db_silo.id());
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use db::schema::organization;
        use db::schema::silo;
        use db::schema::silo_group;
        use db::schema::silo_group_membership;
        use db::schema::silo_user;

        // Make sure there are no organizations present within this silo.
        let id = authz_silo.id();
        let rcgen = db_silo.rcgen;
        let org_found = diesel_pool_result_optional(
            organization::dsl::organization
                .filter(organization::dsl::silo_id.eq(id))
                .filter(organization::dsl::time_deleted.is_null())
                .select(organization::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        if org_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "silo to be deleted contains an organization"
                    .to_string(),
            });
        }

        let now = Utc::now();
        let updated_rows = diesel::update(silo::dsl::silo)
            .filter(silo::dsl::time_deleted.is_null())
            .filter(silo::dsl::id.eq(id))
            .filter(silo::dsl::rcgen.eq(rcgen))
            .set(silo::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "silo deletion failed due to concurrent modification"
                    .to_string(),
            });
        }

        info!(opctx.log, "deleted silo {}", id);

        // If silo deletion succeeded, delete all silo users
        // TODO-correctness This needs to happen in a saga or some other
        // mechanism that ensures it happens even if we crash at this point.
        // TODO-scalability This needs to happen in batches
        let updated_rows = diesel::update(silo_user::dsl::silo_user)
            .filter(silo_user::dsl::silo_id.eq(id))
            .filter(silo_user::dsl::time_deleted.is_null())
            .set(silo_user::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        debug!(
            opctx.log,
            "deleted {} silo users for silo {}", updated_rows, id
        );

        // delete all silo group memberships
        let updated_rows =
            diesel::delete(silo_group_membership::dsl::silo_group_membership)
                .filter(
                    silo_group_membership::dsl::silo_group_id.eq_any(
                        silo_group::dsl::silo_group
                            .filter(silo_group::dsl::silo_id.eq(id))
                            .filter(silo_group::dsl::time_deleted.is_null())
                            .select(silo_group::dsl::id),
                    ),
                )
                .execute_async(self.pool_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                })?;

        debug!(
            opctx.log,
            "deleted {} silo group memberships for silo {}", updated_rows, id
        );

        // delete all silo groups
        let updated_rows = diesel::update(silo_group::dsl::silo_group)
            .filter(silo_group::dsl::silo_id.eq(id))
            .filter(silo_group::dsl::time_deleted.is_null())
            .set(silo_group::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        debug!(
            opctx.log,
            "deleted {} silo groups for silo {}", updated_rows, id
        );

        // delete all silo identity providers
        use db::schema::identity_provider::dsl as idp_dsl;

        let updated_rows = diesel::update(idp_dsl::identity_provider)
            .filter(idp_dsl::silo_id.eq(id))
            .filter(idp_dsl::time_deleted.is_null())
            .set(idp_dsl::time_deleted.eq(Utc::now()))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        debug!(opctx.log, "deleted {} silo IdPs for silo {}", updated_rows, id);

        use db::schema::saml_identity_provider::dsl as saml_idp_dsl;

        let updated_rows = diesel::update(saml_idp_dsl::saml_identity_provider)
            .filter(saml_idp_dsl::silo_id.eq(id))
            .filter(saml_idp_dsl::time_deleted.is_null())
            .set(saml_idp_dsl::time_deleted.eq(Utc::now()))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        debug!(
            opctx.log,
            "deleted {} silo saml IdPs for silo {}", updated_rows, id
        );

        Ok(())
    }
}
