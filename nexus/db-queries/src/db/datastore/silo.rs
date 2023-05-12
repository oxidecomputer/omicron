// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`Silo`]s.

use super::dns::DnsVersionUpdateBuilder;
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
use crate::db::model::CollectionTypeProvisioned;
use crate::db::model::Name;
use crate::db::model::Silo;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use ref_cast::RefCast;
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

        self.virtual_provisioning_collection_create(
            opctx,
            VirtualProvisioningCollection::new(
                DEFAULT_SILO.id(),
                CollectionTypeProvisioned::Silo,
            ),
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

    /// Create a Silo
    ///
    /// This function accepts two different OpContexts.  Different authz checks
    /// are used for different OpContexts:
    ///
    /// * `nexus_opctx`: used to authorize operations that are part of Silo
    ///   creation that we expect end users (even Fleet Administrators) may not
    ///   have.  This includes creating a group within the new Silo and reading/
    ///   modifying the fleet-wide external DNS configuration.  This OpContext
    ///   generally represents an internal Nexus identity operating _on behalf_
    ///   of the user in `opctx` to do these things _in this specific
    ///   (controlled) context_.
    ///
    /// * `opctx`: used for everything else, where we actually want to check
    ///   whether the end user creating this Silo should be allowed to do so
    pub async fn silo_create(
        &self,
        opctx: &OpContext,
        nexus_opctx: &OpContext,
        new_silo_params: params::SiloCreate,
        dns_update: DnsVersionUpdateBuilder,
    ) -> CreateResult<Silo> {
        let conn = self.pool_authorized(opctx).await?;
        self.silo_create_conn(
            conn,
            opctx,
            nexus_opctx,
            new_silo_params,
            dns_update,
        )
        .await
    }

    pub async fn silo_create_conn<ConnErr, CalleeConnErr>(
        &self,
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection, ConnErr>
              + Sync),
        opctx: &OpContext,
        nexus_opctx: &OpContext,
        new_silo_params: params::SiloCreate,
        dns_update: DnsVersionUpdateBuilder,
    ) -> CreateResult<Silo>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
        TransactionError<Error>: From<ConnErr>,

        CalleeConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<CalleeConnErr>,
        TransactionError<Error>: From<CalleeConnErr>,
        async_bb8_diesel::Connection<DbConnection>:
            AsyncConnection<DbConnection, CalleeConnErr>,
    {
        let silo_id = Uuid::new_v4();
        let silo_group_id = Uuid::new_v4();

        let silo_create_query = Self::silo_create_query(
            opctx,
            db::model::Silo::new_with_id(silo_id, new_silo_params.clone()),
        )
        .await?;

        let authz_silo =
            authz::Silo::new(authz::FLEET, silo_id, LookupType::ById(silo_id));

        let silo_admin_group_ensure_query = if let Some(ref admin_group_name) =
            new_silo_params.admin_group_name
        {
            let silo_admin_group_ensure_query =
                DataStore::silo_group_ensure_query(
                    &nexus_opctx,
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

        conn.transaction_async(|conn| async move {
            let silo = silo_create_query
                .get_result_async(&conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Conflict(
                            ResourceType::Silo,
                            new_silo_params.identity.name.as_str(),
                        ),
                    )
                })?;
            self.virtual_provisioning_collection_create_on_connection(
                &conn,
                VirtualProvisioningCollection::new(
                    silo.id(),
                    CollectionTypeProvisioned::Silo,
                ),
            )
            .await?;

            if let Some(query) = silo_admin_group_ensure_query {
                query.get_result_async(&conn).await?;
            }

            if let Some(queries) = silo_admin_group_role_assignment_queries {
                let (delete_old_query, insert_new_query) = queries;
                delete_old_query.execute_async(&conn).await?;
                insert_new_query.execute_async(&conn).await?;
            }

            self.dns_update(nexus_opctx, &conn, dns_update).await?;

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

    pub async fn silos_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        match pagparams {
            PaginatedBy::Id(params) => paginated(dsl::silo, dsl::id, &params),
            PaginatedBy::Name(params) => paginated(
                dsl::silo,
                dsl::name,
                &params.map_name(|n| Name::ref_cast(n)),
            ),
        }
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
        dns_opctx: &OpContext,
        dns_update: DnsVersionUpdateBuilder,
    ) -> DeleteResult {
        assert_eq!(authz_silo.id(), db_silo.id());
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use db::schema::project;
        use db::schema::silo;
        use db::schema::silo_group;
        use db::schema::silo_group_membership;
        use db::schema::silo_user;
        use db::schema::silo_user_password_hash;

        // Make sure there are no projects present within this silo.
        let id = authz_silo.id();
        let rcgen = db_silo.rcgen;
        let project_found = diesel_pool_result_optional(
            project::dsl::project
                .filter(project::dsl::silo_id.eq(id))
                .filter(project::dsl::time_deleted.is_null())
                .select(project::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        if project_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "silo to be deleted contains a project".to_string(),
            });
        }

        let now = Utc::now();

        type TxnError = TransactionError<Error>;
        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let updated_rows = diesel::update(silo::dsl::silo)
                    .filter(silo::dsl::time_deleted.is_null())
                    .filter(silo::dsl::id.eq(id))
                    .filter(silo::dsl::rcgen.eq(rcgen))
                    .set(silo::dsl::time_deleted.eq(now))
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            PoolError::from(e),
                            ErrorHandler::NotFoundByResource(authz_silo),
                        )
                    })?;

                if updated_rows == 0 {
                    return Err(TxnError::CustomError(Error::InvalidRequest {
                        message: "silo deletion failed due to concurrent modification"
                            .to_string(),
                    }));
                }

                self.virtual_provisioning_collection_delete_on_connection(
                    &conn,
                    id,
                ).await?;

                self.dns_update(dns_opctx, &conn, dns_update).await?;

                info!(opctx.log, "deleted silo {}", id);

                Ok(())
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(e) => e,
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })?;

        // TODO-correctness This needs to happen in a saga or some other
        // mechanism that ensures it happens even if we crash at this point.
        // TODO-scalability This needs to happen in batches
        // If silo deletion succeeded, delete all silo users and password hashes
        let updated_rows = diesel::delete(
            silo_user_password_hash::dsl::silo_user_password_hash,
        )
        .filter(
            silo_user_password_hash::dsl::silo_user_id.eq_any(
                silo_user::dsl::silo_user
                    .filter(silo_user::dsl::silo_id.eq(id))
                    .filter(silo_user::dsl::time_deleted.is_null())
                    .select(silo_user::dsl::id),
            ),
        )
        .execute_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "deleted {} password hashes for silo {}", updated_rows, id
        );

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

        // delete certificates
        use db::schema::certificate::dsl as cert_dsl;

        let updated_rows = diesel::update(cert_dsl::certificate)
            .filter(cert_dsl::silo_id.eq(id))
            .filter(cert_dsl::time_deleted.is_null())
            .set(cert_dsl::time_deleted.eq(Utc::now()))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        debug!(opctx.log, "deleted {} silo IdPs for silo {}", updated_rows, id);

        Ok(())
    }
}
