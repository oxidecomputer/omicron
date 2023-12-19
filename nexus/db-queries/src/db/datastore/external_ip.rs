// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`ExternalIp`]s.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::retryable;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::lookup::LookupPath;
use crate::db::model::ExternalIp;
use crate::db::model::FloatingIp;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::IpKind;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::queries::external_ip::NextExternalIp;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use std::net::IpAddr;
use uuid::Uuid;

impl DataStore {
    /// Create an external IP address for source NAT for an instance.
    pub async fn allocate_instance_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: Uuid,
        pool_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let data = IncompleteExternalIp::for_instance_source_nat(
            ip_id,
            instance_id,
            pool_id,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Create an Ephemeral IP address for an instance.
    pub async fn allocate_instance_ephemeral_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: Uuid,
        pool_name: Option<Name>,
    ) -> CreateResult<ExternalIp> {
        let pool = match pool_name {
            Some(name) => {
                let (.., authz_pool, pool) = LookupPath::new(opctx, &self)
                    .ip_pool_name(&name)
                    // any authenticated user can CreateChild on an IP pool. this is
                    // meant to represent allocating an IP
                    .fetch_for(authz::Action::CreateChild)
                    .await?;

                // If the named pool conflicts with user's current scope, i.e.,
                // if it has a silo and it's different from the current silo,
                // then as far as IP allocation is concerned, that pool doesn't
                // exist. If the pool has no silo, it's fleet-scoped and can
                // always be used.
                let authz_silo_id = opctx.authn.silo_required()?.id();
                if let Some(pool_silo_id) = pool.silo_id {
                    if pool_silo_id != authz_silo_id {
                        return Err(authz_pool.not_found());
                    }
                }

                pool
            }
            // If no name given, use the default logic
            None => self.ip_pools_fetch_default(&opctx).await?,
        };

        let pool_id = pool.identity.id;
        let data =
            IncompleteExternalIp::for_ephemeral(ip_id, instance_id, pool_id);
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates an IP address for internal service usage.
    pub async fn allocate_service_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        name: &Name,
        description: &str,
        service_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;

        let data = IncompleteExternalIp::for_service(
            ip_id,
            name,
            description,
            service_id,
            pool.id(),
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates an SNAT IP address for internal service usage.
    pub async fn allocate_service_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        service_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;

        let data = IncompleteExternalIp::for_service_snat(
            ip_id,
            service_id,
            pool.id(),
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates a floating IP address for instance usage.
    pub async fn allocate_floating_ip(
        &self,
        opctx: &OpContext,
        project_id: Uuid,
        params: params::FloatingIpCreate,
    ) -> CreateResult<ExternalIp> {
        let ip_id = Uuid::new_v4();

        // See `allocate_instance_ephemeral_ip`: we're replicating
        // its strucutre to prevent cross-silo pool access.
        let pool_id = if let Some(name_or_id) = params.pool {
            let (.., authz_pool, pool) = match name_or_id {
                NameOrId::Name(name) => {
                    LookupPath::new(opctx, self)
                        .ip_pool_name(&Name(name))
                        .fetch_for(authz::Action::CreateChild)
                        .await?
                }
                NameOrId::Id(id) => {
                    LookupPath::new(opctx, self)
                        .ip_pool_id(id)
                        .fetch_for(authz::Action::CreateChild)
                        .await?
                }
            };

            let authz_silo_id = opctx.authn.silo_required()?.id();
            if let Some(pool_silo_id) = pool.silo_id {
                if pool_silo_id != authz_silo_id {
                    return Err(authz_pool.not_found());
                }
            }

            pool
        } else {
            self.ip_pools_fetch_default(opctx).await?
        }
        .id();

        let data = if let Some(ip) = params.address {
            IncompleteExternalIp::for_floating_explicit(
                ip_id,
                &Name(params.identity.name),
                &params.identity.description,
                project_id,
                ip,
                pool_id,
            )
        } else {
            IncompleteExternalIp::for_floating(
                ip_id,
                &Name(params.identity.name),
                &params.identity.description,
                project_id,
                pool_id,
            )
        };

        self.allocate_external_ip(opctx, data).await
    }

    async fn allocate_external_ip(
        &self,
        opctx: &OpContext,
        data: IncompleteExternalIp,
    ) -> CreateResult<ExternalIp> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let ip = Self::allocate_external_ip_on_connection(&conn, data).await?;
        Ok(ip)
    }

    /// Variant of [Self::allocate_external_ip] which may be called from a
    /// transaction context.
    pub(crate) async fn allocate_external_ip_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        data: IncompleteExternalIp,
    ) -> Result<ExternalIp, TransactionError<Error>> {
        use diesel::result::DatabaseErrorKind::UniqueViolation;
        // Name needs to be cloned out here (if present) to give users a
        // sensible error message on name collision.
        let name = data.name().clone();
        let explicit_ip = data.explicit_ip().is_some();
        NextExternalIp::new(data).get_result_async(conn).await.map_err(|e| {
            use diesel::result::Error::DatabaseError;
            use diesel::result::Error::NotFound;
            match e {
                NotFound => {
                    if explicit_ip {
                        TransactionError::CustomError(Error::invalid_request(
                            "Requested external IP address not available",
                        ))
                    } else {
                        TransactionError::CustomError(
                            Error::insufficient_capacity(
                                "No external IP addresses available",
                                "NextExternalIp::new returned NotFound",
                            ),
                        )
                    }
                }
                DatabaseError(UniqueViolation, ..) if name.is_some() => {
                    TransactionError::CustomError(public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::FloatingIp,
                            name.as_ref()
                                .map(|m| m.as_str())
                                .unwrap_or_default(),
                        ),
                    ))
                }
                _ => {
                    if retryable(&e) {
                        return TransactionError::Database(e);
                    }
                    TransactionError::CustomError(
                        crate::db::queries::external_ip::from_diesel(e),
                    )
                }
            }
        })
    }

    /// Allocates an explicit Floating IP address for an internal service.
    ///
    /// Unlike the other IP allocation requests, this does not search for an
    /// available IP address, it asks for one explicitly.
    pub async fn allocate_explicit_service_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        name: &Name,
        description: &str,
        service_id: Uuid,
        ip: IpAddr,
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;
        let data = IncompleteExternalIp::for_service_explicit(
            ip_id,
            name,
            description,
            service_id,
            pool.id(),
            ip,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Allocates an explicit SNAT IP address for an internal service.
    ///
    /// Unlike the other IP allocation requests, this does not search for an
    /// available IP address, it asks for one explicitly.
    pub async fn allocate_explicit_service_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        service_id: Uuid,
        ip: IpAddr,
        port_range: (u16, u16),
    ) -> CreateResult<ExternalIp> {
        let (.., pool) = self.ip_pools_service_lookup(opctx).await?;
        let data = IncompleteExternalIp::for_service_explicit_snat(
            ip_id,
            service_id,
            pool.id(),
            ip,
            port_range,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Deallocate the external IP address with the provided ID.
    ///
    /// To support idempotency, such as in saga operations, this method returns
    /// an extra boolean, rather than the usual `DeleteResult`. The meaning of
    /// return values are:
    /// - `Ok(true)`: The record was deleted during this call
    /// - `Ok(false)`: The record was already deleted, such as by a previous
    /// call
    /// - `Err(_)`: Any other condition, including a non-existent record.
    pub async fn deallocate_external_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
    ) -> Result<bool, Error> {
        use db::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(ip_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<ExternalIp>(ip_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete all external IP addresses associated with the provided instance
    /// ID.
    ///
    /// This method returns the number of records deleted, rather than the usual
    /// `DeleteResult`. That's mostly useful for tests, but could be important
    /// if callers have some invariants they'd like to check.
    pub async fn deallocate_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        use db::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_service.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::kind.ne(IpKind::Floating))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Detach an individual Floating IP address from its parent instance.
    ///
    /// As in `deallocate_external_ip_by_instance_id`, this method returns the
    /// number of records altered, rather than an `UpdateResult`.
    pub async fn detach_floating_ips_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        use db::schema::external_ip::dsl;
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_service.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::kind.eq(IpKind::Floating))
            .set(dsl::parent_id.eq(Option::<Uuid>::None))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch all external IP addresses of any kind for the provided instance
    pub async fn instance_lookup_external_ips(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> LookupResult<Vec<ExternalIp>> {
        use db::schema::external_ip::dsl;
        dsl::external_ip
            .filter(dsl::is_service.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch all Floating IP addresses for the provided project.
    pub async fn floating_ips_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<FloatingIp> {
        use db::schema::floating_ip::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::floating_ip, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::floating_ip,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::project_id.eq(authz_project.id()))
        .filter(dsl::time_deleted.is_null())
        .select(FloatingIp::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete a Floating IP, verifying first that it is not in use.
    pub async fn floating_ip_delete(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
    ) -> DeleteResult {
        use db::schema::external_ip::dsl;

        opctx.authorize(authz::Action::Delete, authz_fip).await?;

        let now = Utc::now();
        let result = diesel::update(dsl::external_ip)
            .filter(dsl::id.eq(authz_fip.id()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.is_null())
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<ExternalIp>(authz_fip.id())
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_fip),
                )
            })?;

        match result.status {
            // Verify this FIP is not attached to any instances/services.
            UpdateStatus::NotUpdatedButExists if result.found.parent_id.is_some() => Err(Error::invalid_request(
                "Floating IP cannot be deleted while attached to an instance",
            )),
            // Only remaining cause of `NotUpdated` is earlier soft-deletion.
            // Return success in this case to maintain idempotency.
            UpdateStatus::Updated | UpdateStatus::NotUpdatedButExists => Ok(()),
        }
    }

    /// Attaches a Floating IP address to an instance.
    pub async fn floating_ip_attach(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
        instance_id: Uuid,
    ) -> UpdateResult<FloatingIp> {
        use db::schema::external_ip::dsl;

        let (.., authz_instance, _db_instance) = LookupPath::new(&opctx, self)
            .instance_id(instance_id)
            .fetch_for(authz::Action::Modify)
            .await?;

        opctx.authorize(authz::Action::Modify, authz_fip).await?;
        opctx.authorize(authz::Action::Modify, &authz_instance).await?;

        let fip_id = authz_fip.id();

        let out = diesel::update(dsl::external_ip)
            .filter(dsl::id.eq(fip_id))
            .filter(dsl::kind.eq(IpKind::Floating))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.is_null())
            .set((
                dsl::parent_id.eq(Some(instance_id)),
                dsl::time_modified.eq(Utc::now()),
            ))
            .check_if_exists::<ExternalIp>(fip_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_fip),
                )
            })?;

        match (out.status, out.found.parent_id) {
            (UpdateStatus::NotUpdatedButExists, Some(_)) => Err(Error::invalid_request(
                "Floating IP cannot be attached to one instance while still attached to another",
            )),
            (UpdateStatus::Updated, _) => Ok(out.found.try_into().map_err(|e| Error::internal_error(&format!("{e}")))?),
            _ => unreachable!(),
        }
    }

    /// Detaches a Floating IP address from an instance.
    pub async fn floating_ip_detach(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
        instance_id: Uuid,
    ) -> UpdateResult<FloatingIp> {
        use db::schema::external_ip::dsl;

        let (.., authz_instance) = LookupPath::new(&opctx, self)
            .instance_id(instance_id)
            .lookup_for(authz::Action::Modify)
            .await?;

        opctx.authorize(authz::Action::Modify, authz_fip).await?;
        opctx.authorize(authz::Action::Modify, &authz_instance).await?;

        let fip_id = authz_fip.id();

        let out = diesel::update(dsl::external_ip)
            .filter(dsl::id.eq(fip_id))
            .filter(dsl::kind.eq(IpKind::Floating))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(instance_id))
            .set((
                dsl::parent_id.eq(Option::<Uuid>::None),
                dsl::time_modified.eq(Utc::now()),
            ))
            .check_if_exists::<ExternalIp>(fip_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_fip),
                )
            })?;

        match (out.status, out.found.parent_id) {
            (UpdateStatus::NotUpdatedButExists, Some(id))
                if id != instance_id =>
            {
                Err(Error::invalid_request(
                    "Floating IP is not attached to the target instance",
                ))
            }
            (UpdateStatus::NotUpdatedButExists, None) => {
                Err(Error::invalid_request(
                    "Floating IP is not attached to an instance",
                ))
            }
            (UpdateStatus::Updated, _) => Ok(out
                .found
                .try_into()
                .map_err(|e| Error::internal_error(&format!("{e}")))?),
            _ => unreachable!(),
        }
    }
}
