// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`Silo`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use super::dns::DnsVersionUpdateBuilder;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::RunnableQuery;
use crate::db::identity::Resource;
use crate::db::model::CollectionTypeProvisioned;
use crate::db::model::IpPoolResourceType;
use crate::db::model::Name;
use crate::db::model::Silo;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_errors::retryable;
use nexus_db_fixed_data::silo::{DEFAULT_SILO, INTERNAL_SILO};
use nexus_db_lookup::DbConnection;
use nexus_db_model::Certificate;
use nexus_db_model::ServiceKind;
use nexus_db_model::SiloQuotas;
use nexus_db_model::SiloSettings;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::SiloRole;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;
use uuid::Uuid;

/// Filter a "silo_list" query based on silos' discoverability
#[derive(Debug, Clone, Copy)]
pub enum Discoverability {
    /// Show all Silos
    All,
    /// Show only discoverable Silos
    DiscoverableOnly,
}

impl DataStore {
    /// Load built-in silos into the database
    pub async fn load_builtin_silos(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in silos");

        use nexus_db_schema::schema::silo;
        use nexus_db_schema::schema::silo_quotas;
        use nexus_db_schema::schema::silo_settings;
        let conn = self.pool_connection_authorized(opctx).await?;

        let count = self
            .transaction_retry_wrapper("load_builtin_silos")
            .transaction(&conn, |conn| async move {
                diesel::insert_into(silo_quotas::table)
                    .values(SiloQuotas::arbitrarily_high_default(
                        DEFAULT_SILO.id(),
                    ))
                    .on_conflict(silo_quotas::silo_id)
                    .do_nothing()
                    .execute_async(&conn)
                    .await?;
                diesel::insert_into(silo_settings::table)
                    .values(SiloSettings::new(DEFAULT_SILO.id()))
                    .on_conflict(silo_settings::silo_id)
                    .do_nothing()
                    .execute_async(&conn)
                    .await?;
                let count = diesel::insert_into(silo::table)
                    .values([&*DEFAULT_SILO, &*INTERNAL_SILO])
                    .on_conflict(silo::id)
                    .do_nothing()
                    .execute_async(&conn)
                    .await?;
                Ok(count)
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

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

        // If the new Silo has configuration mapping its roles to Fleet-level
        // roles, that's effectively trying to modify the Fleet-level policy.
        if !silo.mapped_fleet_roles()?.is_empty() {
            opctx.authorize(authz::Action::ModifyPolicy, &authz::FLEET).await?;
        }

        use nexus_db_schema::schema::silo::dsl;
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
        new_silo_dns_names: &[String],
        dns_update: DnsVersionUpdateBuilder,
    ) -> CreateResult<Silo> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let silo = self
            .silo_create_conn(
                &conn,
                opctx,
                nexus_opctx,
                new_silo_params,
                new_silo_dns_names,
                dns_update,
            )
            .await?;
        Ok(silo)
    }

    pub async fn silo_create_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        nexus_opctx: &OpContext,
        new_silo_params: params::SiloCreate,
        new_silo_dns_names: &[String],
        dns_update: DnsVersionUpdateBuilder,
    ) -> Result<Silo, TransactionError<Error>> {
        let silo_id = Uuid::new_v4();
        let silo_group_id = Uuid::new_v4();

        let silo_create_query = Self::silo_create_query(
            opctx,
            db::model::Silo::new_with_id(silo_id, new_silo_params.clone())?,
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
                        role_name: SiloRole::Admin,
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

        // This method uses nested transactions, which are not supported
        // with retryable transactions.
        let silo = self
            .transaction_non_retry_wrapper("silo_create")
            .transaction(&conn, |conn| async move {
                let silo = silo_create_query
                    .get_result_async(&conn)
                    .await
                    .map_err(|e| {
                        if retryable(&e) {
                            return TransactionError::Database(e);
                        }
                        TransactionError::CustomError(public_error_from_diesel(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::Silo,
                                new_silo_params.identity.name.as_str(),
                            ),
                        ))
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
                    query.execute_async(&conn).await?;
                }

                if let Some(queries) = silo_admin_group_role_assignment_queries
                {
                    let (delete_old_query, insert_new_query) = queries;
                    delete_old_query.execute_async(&conn).await?;
                    insert_new_query.execute_async(&conn).await?;
                }

                let certificates = new_silo_params
                    .tls_certificates
                    .into_iter()
                    .map(|c| {
                        Certificate::new(
                            silo.id(),
                            Uuid::new_v4(),
                            ServiceKind::Nexus,
                            c,
                            new_silo_dns_names,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Error::from)?;
                {
                    use nexus_db_schema::schema::certificate::dsl;
                    diesel::insert_into(dsl::certificate)
                        .values(certificates)
                        .execute_async(&conn)
                        .await?;
                }

                self.dns_update_incremental(nexus_opctx, &conn, dns_update)
                    .await?;

                self.silo_quotas_create(
                    &conn,
                    &authz_silo,
                    SiloQuotas::new(
                        authz_silo.id(),
                        new_silo_params.quotas.cpus,
                        new_silo_params.quotas.memory.into(),
                        new_silo_params.quotas.storage.into(),
                    ),
                )
                .await?;
                self.silo_settings_create(
                    &conn,
                    &authz_silo,
                    SiloSettings::new(authz_silo.id()),
                )
                .await?;

                Ok::<Silo, TransactionError<Error>>(silo)
            })
            .await?;
        Ok(silo)
    }

    pub async fn silos_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use nexus_db_schema::schema::silo::dsl;
        paginated(dsl::silo, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn silos_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
        discoverability: Discoverability,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use nexus_db_schema::schema::silo::dsl;
        let mut query = match pagparams {
            PaginatedBy::Id(params) => paginated(dsl::silo, dsl::id, &params),
            PaginatedBy::Name(params) => paginated(
                dsl::silo,
                dsl::name,
                &params.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::id.ne(INTERNAL_SILO.id()))
        .filter(dsl::time_deleted.is_null());

        if let Discoverability::DiscoverableOnly = discoverability {
            query = query.filter(dsl::discoverable.eq(true));
        }

        query
            .select(Silo::as_select())
            .load_async::<Silo>(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all Silos, making as many queries as needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn silo_list_all_batched(
        &self,
        opctx: &OpContext,
        discoverability: Discoverability,
    ) -> ListResultVec<Silo> {
        opctx.check_complex_operations_allowed()?;
        let mut all_silos = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .silos_list(
                    opctx,
                    &PaginatedBy::Id(p.current_pagparams()),
                    discoverability,
                )
                .await?;
            paginator =
                p.found_batch(&batch, &|s: &nexus_db_model::Silo| s.id());
            all_silos.extend(batch);
        }
        Ok(all_silos)
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

        use nexus_db_schema::schema::project;
        use nexus_db_schema::schema::silo;
        use nexus_db_schema::schema::silo_group;
        use nexus_db_schema::schema::silo_group_membership;
        use nexus_db_schema::schema::silo_user;
        use nexus_db_schema::schema::silo_user_password_hash;

        let conn = self.pool_connection_authorized(opctx).await?;

        // Make sure there are no projects present within this silo.
        let id = authz_silo.id();
        let rcgen = db_silo.rcgen;
        let project_found = project::dsl::project
            .filter(project::dsl::silo_id.eq(id))
            .filter(project::dsl::time_deleted.is_null())
            .select(project::dsl::id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if project_found.is_some() {
            return Err(Error::invalid_request(
                "silo to be deleted contains a project",
            ));
        }

        let now = Utc::now();

        type TxnError = TransactionError<Error>;

        // This method uses nested transactions, which are not supported
        // with retryable transactions.
        self.transaction_non_retry_wrapper("silo_delete")
            .transaction(&conn, |conn| async move {
                let updated_rows = diesel::update(silo::dsl::silo)
                    .filter(silo::dsl::time_deleted.is_null())
                    .filter(silo::dsl::id.eq(id))
                    .filter(silo::dsl::rcgen.eq(rcgen))
                    .set(silo::dsl::time_deleted.eq(now))
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(
                            e,
                            ErrorHandler::NotFoundByResource(authz_silo),
                        )
                    })?;

                if updated_rows == 0 {
                    return Err(TxnError::CustomError(Error::invalid_request(
                        "silo deletion failed due to concurrent modification",
                    )));
                }

                self.silo_quotas_delete(opctx, &conn, &authz_silo).await?;
                self.silo_settings_delete(opctx, &conn, &authz_silo).await?;

                self.virtual_provisioning_collection_delete_on_connection(
                    &opctx.log, &conn, id,
                )
                .await?;

                self.dns_update_incremental(dns_opctx, &conn, dns_update)
                    .await?;

                info!(opctx.log, "deleted silo {}", id);

                Ok(())
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(e) => e,
                TxnError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
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
        .execute_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "deleted {} password hashes for silo {}", updated_rows, id
        );

        let updated_rows = diesel::update(silo_user::dsl::silo_user)
            .filter(silo_user::dsl::silo_id.eq(id))
            .filter(silo_user::dsl::time_deleted.is_null())
            .set(silo_user::dsl::time_deleted.eq(now))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

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
                .execute_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
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
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "deleted {} silo groups for silo {}", updated_rows, id
        );

        // delete all silo identity providers
        use nexus_db_schema::schema::identity_provider::dsl as idp_dsl;

        let updated_rows = diesel::update(idp_dsl::identity_provider)
            .filter(idp_dsl::silo_id.eq(id))
            .filter(idp_dsl::time_deleted.is_null())
            .set(idp_dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(opctx.log, "deleted {} silo IdPs for silo {}", updated_rows, id);

        use nexus_db_schema::schema::saml_identity_provider::dsl as saml_idp_dsl;

        let updated_rows = diesel::update(saml_idp_dsl::saml_identity_provider)
            .filter(saml_idp_dsl::silo_id.eq(id))
            .filter(saml_idp_dsl::time_deleted.is_null())
            .set(saml_idp_dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "deleted {} silo saml IdPs for silo {}", updated_rows, id
        );

        // delete certificates
        use nexus_db_schema::schema::certificate::dsl as cert_dsl;

        let updated_rows = diesel::update(cert_dsl::certificate)
            .filter(cert_dsl::silo_id.eq(id))
            .filter(cert_dsl::time_deleted.is_null())
            .set(cert_dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(opctx.log, "deleted {} silo IdPs for silo {}", updated_rows, id);

        // delete IP pool links (not IP pools, just the links)
        use nexus_db_schema::schema::ip_pool_resource;

        let updated_rows = diesel::delete(ip_pool_resource::table)
            .filter(ip_pool_resource::resource_id.eq(id))
            .filter(
                ip_pool_resource::resource_type.eq(IpPoolResourceType::Silo),
            )
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "deleted {} IP pool links for silo {}", updated_rows, id
        );

        Ok(())
    }
}
