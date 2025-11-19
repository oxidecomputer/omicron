// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`ExternalIp`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_attach::AttachError;
use crate::db::collection_attach::DatastoreAttachTarget;
use crate::db::collection_detach::DatastoreDetachTarget;
use crate::db::collection_detach::DetachError;
use crate::db::model::ExternalIp;
use crate::db::model::FloatingIp;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::IpKind;
use crate::db::model::IpPool;
use crate::db::model::IpPoolType;
use crate::db::model::Name;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::external_ip::MAX_EXTERNAL_IPS_PER_INSTANCE;
use crate::db::queries::external_ip::NextExternalIp;
use crate::db::queries::external_ip::SAFE_TO_ATTACH_INSTANCE_STATES;
use crate::db::queries::external_ip::SAFE_TO_ATTACH_INSTANCE_STATES_CREATING;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_errors::retryable;
use nexus_db_lookup::DbConnection;
use nexus_db_lookup::LookupPath;
use nexus_db_model::FloatingIpUpdate;
use nexus_db_model::Instance;
use nexus_db_model::IpAttachState;
use nexus_db_model::IpVersion;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::OmicronZoneExternalIp;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use ref_cast::RefCast;
use std::net::IpAddr;
use uuid::Uuid;

const MAX_EXTERNAL_IPS_PLUS_SNAT: u32 = MAX_EXTERNAL_IPS_PER_INSTANCE + 1;

impl DataStore {
    /// Create an external IP address for source NAT for an instance.
    pub async fn allocate_instance_snat_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: InstanceUuid,
        pool_id: Uuid,
    ) -> CreateResult<ExternalIp> {
        let data = IncompleteExternalIp::for_instance_source_nat(
            ip_id,
            instance_id.into_untyped_uuid(),
            pool_id,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Create an Ephemeral IP address for a probe.
    pub async fn allocate_probe_ephemeral_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        probe_id: Uuid,
        pool: Option<authz::IpPool>,
    ) -> CreateResult<ExternalIp> {
        let authz_pool = self
            .resolve_pool_for_allocation(opctx, pool, IpPoolType::Unicast)
            .await?;
        let data = IncompleteExternalIp::for_ephemeral_probe(
            ip_id,
            probe_id,
            authz_pool.id(),
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Create an Ephemeral IP address for an instance.
    ///
    /// For consistency between instance create and External IP attach/detach
    /// operations, this IP will be created in the `Attaching` state to block
    /// concurrent access.
    /// Callers must call `external_ip_complete_op` on saga completion to move
    /// the IP to `Attached`.
    ///
    /// To better handle idempotent attachment, this method returns an
    /// additional bool:
    /// - true: EIP was detached or attaching. proceed with saga.
    /// - false: EIP was attached. No-op for remainder of saga.
    pub async fn allocate_instance_ephemeral_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: InstanceUuid,
        pool: Option<authz::IpPool>,
        creating_instance: bool,
    ) -> CreateResult<(ExternalIp, bool)> {
        // This is slightly hacky: we need to create an unbound ephemeral IP, and
        // then attempt to bind it to respect two separate constraints:
        // - At most one Ephemeral IP per instance
        // - At most MAX external IPs per instance
        // Naturally, we now *need* to destroy the ephemeral IP if the newly alloc'd
        // IP was not attached, including on idempotent success.

        let authz_pool = self
            .resolve_pool_for_allocation(opctx, pool, IpPoolType::Unicast)
            .await?;
        let data = IncompleteExternalIp::for_ephemeral(ip_id, authz_pool.id());

        // We might not be able to acquire a new IP, but in the event of an
        // idempotent or double attach this failure is allowed.
        let temp_ip = self.allocate_external_ip(opctx, data).await;
        if let Err(e) = temp_ip {
            let eip = self
                .instance_lookup_ephemeral_ip(opctx, instance_id)
                .await?
                .ok_or(e)?;

            return Ok((eip, false));
        }
        let temp_ip = temp_ip?;

        match self
            .begin_attach_ip(
                opctx,
                temp_ip.id,
                instance_id,
                IpKind::Ephemeral,
                creating_instance,
            )
            .await
        {
            Err(e) => {
                self.deallocate_external_ip(opctx, temp_ip.id).await?;
                Err(e)
            }
            // Idempotent case: attach failed due to a caught UniqueViolation.
            Ok(None) => {
                self.deallocate_external_ip(opctx, temp_ip.id).await?;
                let eip = self
                    .instance_lookup_ephemeral_ip(opctx, instance_id)
                    .await?
                    .ok_or_else(|| Error::internal_error(
                        "failed to lookup current ephemeral IP for idempotent attach"
                    ))?;
                let do_saga = eip.state != IpAttachState::Attached;
                Ok((eip, do_saga))
            }
            Ok(Some(v)) => Ok(v),
        }
    }

    /// Fetch all external IP addresses of any kind for the provided service.
    pub async fn external_ip_list_service_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        service_id: Uuid,
    ) -> LookupResult<Vec<ExternalIp>> {
        use nexus_db_schema::schema::external_ip::dsl;
        dsl::external_ip
            .filter(dsl::is_service.eq(true))
            .filter(dsl::parent_id.eq(service_id))
            .filter(dsl::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .get_results_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Allocates a floating IP address for instance usage.
    pub async fn allocate_floating_ip(
        &self,
        opctx: &OpContext,
        project_id: Uuid,
        identity: IdentityMetadataCreateParams,
        ip: Option<IpAddr>,
        pool: Option<authz::IpPool>,
    ) -> CreateResult<ExternalIp> {
        let ip_id = Uuid::new_v4();

        let authz_pool = self
            .resolve_pool_for_allocation(opctx, pool, IpPoolType::Unicast)
            .await?;

        let data = if let Some(ip) = ip {
            IncompleteExternalIp::for_floating_explicit(
                ip_id,
                &Name(identity.name),
                &identity.description,
                project_id,
                ip,
                authz_pool.id(),
            )
        } else {
            IncompleteExternalIp::for_floating(
                ip_id,
                &Name(identity.name),
                &identity.description,
                project_id,
                authz_pool.id(),
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
        // Name needs to be cloned out here (if present) to give users a
        // sensible error message on name collision.
        let name = data.name().clone();
        let explicit_ip = data.explicit_ip().is_some();
        NextExternalIp::new(data).get_result_async(conn).await.map_err(|e| {
            use diesel::result::DatabaseErrorKind::NotNullViolation;
            use diesel::result::DatabaseErrorKind::UniqueViolation;
            use diesel::result::Error::DatabaseError;
            use diesel::result::Error::NotFound;
            let emit_err_msg = |explicit_ip: bool,
                                msg: &str|
             -> TransactionError<Error> {
                if explicit_ip {
                    TransactionError::CustomError(Error::invalid_request(
                        "Requested external IP address not available",
                    ))
                } else {
                    TransactionError::CustomError(Error::insufficient_capacity(
                        "No external IP addresses available",
                        msg,
                    ))
                }
            };
            match e {
                DatabaseError(NotNullViolation, ref info)
                    if info.message().contains("in column \"ip\"") =>
                {
                    emit_err_msg(
                        explicit_ip,
                        "NextExternalIp::new tried to insert NULL ip",
                    )
                }
                NotFound => emit_err_msg(
                    explicit_ip,
                    "NextExternalIp::new returned NotFound",
                ),
                DatabaseError(UniqueViolation, ref info) => {
                    // Attempt to re-use same IP address.
                    if info.constraint_name() == Some("external_ip_unique") {
                        TransactionError::CustomError(Error::invalid_request(
                            "Requested external IP address not available",
                        ))
                    // Floating IP: name conflict
                    } else if info
                        .constraint_name()
                        .map(|name| name.starts_with("lookup_floating_"))
                        .unwrap_or(false)
                    {
                        TransactionError::CustomError(public_error_from_diesel(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::FloatingIp,
                                name.as_ref()
                                    .map(|m| m.as_str())
                                    .unwrap_or_default(),
                            ),
                        ))
                    } else {
                        TransactionError::CustomError(
                            crate::db::queries::external_ip::from_diesel(e),
                        )
                    }
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

    /// Allocates an explicit IP address for an Omicron zone.
    pub async fn external_ip_allocate_omicron_zone(
        &self,
        opctx: &OpContext,
        zone_id: OmicronZoneUuid,
        zone_kind: ZoneKind,
        external_ip: OmicronZoneExternalIp,
    ) -> CreateResult<ExternalIp> {
        let version = IpVersion::from(external_ip.ip_version());
        let (authz_pool, pool) =
            self.ip_pools_service_lookup(opctx, version).await?;
        opctx.authorize(authz::Action::CreateChild, &authz_pool).await?;
        let data = IncompleteExternalIp::for_omicron_zone(
            pool.id(),
            external_ip,
            zone_id,
            zone_kind,
        );
        self.allocate_external_ip(opctx, data).await
    }

    /// Variant of [Self::external_ip_allocate_omicron_zone] which may be called
    /// from a transaction context.
    pub(crate) async fn external_ip_allocate_omicron_zone_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        service_pool: &IpPool,
        zone_id: OmicronZoneUuid,
        zone_kind: ZoneKind,
        external_ip: OmicronZoneExternalIp,
    ) -> Result<ExternalIp, TransactionError<Error>> {
        let data = IncompleteExternalIp::for_omicron_zone(
            service_pool.id(),
            external_ip,
            zone_id,
            zone_kind,
        );
        Self::allocate_external_ip_on_connection(conn, data).await
    }

    /// List one page of all external IPs allocated to internal services
    pub async fn external_ip_list_service_all(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ExternalIp> {
        use nexus_db_schema::schema::external_ip::dsl;

        // Note the IP version used here isn't important. It's just for the
        // authz check to list children, and not used for the actual database
        // query below, which filters on is_service to get external IPs from
        // either pool.
        let (authz_pool, _pool) =
            self.ip_pools_service_lookup(opctx, IpVersion::V4).await?;
        opctx.authorize(authz::Action::ListChildren, &authz_pool).await?;

        paginated(dsl::external_ip, dsl::id, pagparams)
            .filter(dsl::is_service)
            .filter(dsl::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all external IPs allocated to internal services, making as many
    /// queries as needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn external_ip_list_service_all_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<ExternalIp> {
        opctx.check_complex_operations_allowed()?;

        let mut all_ips = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .external_ip_list_service_all(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|ip: &ExternalIp| ip.id);
            all_ips.extend(batch);
        }
        Ok(all_ips)
    }

    /// Attempt to move a target external IP from detached to attaching,
    /// checking that its parent instance does not have too many addresses
    /// and is in a valid state.
    ///
    /// Returns the `ExternalIp` which was modified, where possible. This
    /// is only nullable when trying to double-attach ephemeral IPs.
    /// To better handle idempotent attachment, this method returns an
    /// additional bool:
    /// - true: EIP was detached or attaching. proceed with saga.
    /// - false: EIP was attached. No-op for remainder of saga.
    async fn begin_attach_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: InstanceUuid,
        kind: IpKind,
        creating_instance: bool,
    ) -> Result<Option<(ExternalIp, bool)>, Error> {
        use diesel::result::DatabaseErrorKind::UniqueViolation;
        use diesel::result::Error::DatabaseError;
        use nexus_db_schema::schema::external_ip::dsl;
        use nexus_db_schema::schema::external_ip::table;
        use nexus_db_schema::schema::instance::dsl as inst_dsl;
        use nexus_db_schema::schema::instance::table as inst_table;

        let safe_states = if creating_instance {
            &SAFE_TO_ATTACH_INSTANCE_STATES_CREATING[..]
        } else {
            &SAFE_TO_ATTACH_INSTANCE_STATES[..]
        };

        let query = Instance::attach_resource(
            instance_id.into_untyped_uuid(),
            ip_id,
            inst_table
                .into_boxed()
                .filter(inst_dsl::state.eq_any(safe_states))
                .filter(inst_dsl::migration_id.is_null()),
            table
                .into_boxed()
                .filter(dsl::state.eq(IpAttachState::Detached))
                .filter(dsl::kind.eq(kind))
                .filter(dsl::parent_id.is_null()),
            MAX_EXTERNAL_IPS_PLUS_SNAT,
            diesel::update(dsl::external_ip).set((
                dsl::parent_id.eq(Some(instance_id.into_untyped_uuid())),
                dsl::time_modified.eq(Utc::now()),
                dsl::state.eq(IpAttachState::Attaching),
            )),
        );

        let mut do_saga = true;
        query.attach_and_get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map(|(_, resource)| Some(resource))
        .or_else(|e: AttachError<ExternalIp, _, _>| match e {
            AttachError::CollectionNotFound => {
                Err(Error::not_found_by_id(
                    ResourceType::Instance,
                    &instance_id.into_untyped_uuid(),
                ))
            },
            AttachError::ResourceNotFound => {
                Err(if kind == IpKind::Ephemeral {
                    Error::internal_error("call-scoped ephemeral IP was lost")
                } else {
                    Error::not_found_by_id(
                        ResourceType::FloatingIp,
                        &ip_id,
                    )
                })
            },
            AttachError::NoUpdate { attached_count, resource, collection } => {
                match resource.state {
                    // Idempotent errors: is in progress or complete for same resource pair -- this is fine.
                    IpAttachState::Attaching if resource.parent_id == Some(instance_id.into_untyped_uuid()) =>
                        return Ok(Some(resource)),
                    IpAttachState::Attached if resource.parent_id == Some(instance_id.into_untyped_uuid()) => {
                        do_saga = false;
                        return Ok(Some(resource))
                    },
                    IpAttachState::Attached =>
                        return Err(Error::invalid_request(&format!(
                        "{kind} IP cannot be attached to one \
                         instance while still attached to another"
                    ))),
                    // User can reattempt depending on how the current saga unfolds.
                    // NB; only floating IP can return this case, eph will return
                    // a UniqueViolation.
                    IpAttachState::Attaching | IpAttachState::Detaching
                        => return Err(Error::unavail(&format!(
                        "tried to attach {kind} IP mid-attach/detach: \
                         attach will be safe to retry once operation on \
                         same IP resource completes"
                    ))),

                    IpAttachState::Detached => {},
                }

                if collection.runtime_state.migration_id.is_some() {
                    return Err(Error::unavail(&format!(
                        "tried to attach {kind} IP while instance was migrating: \
                         detach will be safe to retry once migrate completes"
                    )))
                }

                Err(match &collection.runtime_state.nexus_state {
                    state if SAFE_TO_ATTACH_INSTANCE_STATES.contains(&state) => {
                        if attached_count >= i64::from(MAX_EXTERNAL_IPS_PLUS_SNAT) {
                            Error::invalid_request(&format!(
                                "an instance may not have more than \
                                {MAX_EXTERNAL_IPS_PER_INSTANCE} external IP addresses",
                            ))
                        } else {
                            Error::internal_error(&format!("failed to attach {kind} IP"))
                        }
                    },
                    state => Error::invalid_request(&format!(
                        "cannot attach {kind} IP to instance in {state} state"
                    )),
                })
            },
            // This case occurs for both currently attaching and attached ephemeral IPs:
            AttachError::DatabaseError(DatabaseError(UniqueViolation, ..))
                if kind == IpKind::Ephemeral => {
                Ok(None)
            },
            AttachError::DatabaseError(e) => {
                Err(public_error_from_diesel(e, ErrorHandler::Server))
            },
        })
        .map(|eip| eip.map(|v| (v, do_saga)))
    }

    /// Attempt to move a target external IP from attached to detaching,
    /// checking that its parent instance is in a valid state.
    ///
    /// Returns the `ExternalIp` which was modified, where possible. This
    /// is only nullable when trying to double-detach ephemeral IPs.
    /// To better handle idempotent attachment, this method returns an
    /// additional bool:
    /// - true: EIP was detached or attaching. proceed with saga.
    /// - false: EIP was attached. No-op for remainder of saga.
    async fn begin_detach_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: InstanceUuid,
        kind: IpKind,
        creating_instance: bool,
    ) -> UpdateResult<Option<(ExternalIp, bool)>> {
        use nexus_db_schema::schema::external_ip::dsl;
        use nexus_db_schema::schema::external_ip::table;
        use nexus_db_schema::schema::instance::dsl as inst_dsl;
        use nexus_db_schema::schema::instance::table as inst_table;

        let safe_states = if creating_instance {
            &SAFE_TO_ATTACH_INSTANCE_STATES_CREATING[..]
        } else {
            &SAFE_TO_ATTACH_INSTANCE_STATES[..]
        };

        let query = Instance::detach_resource(
            instance_id.into_untyped_uuid(),
            ip_id,
            inst_table
                .into_boxed()
                .filter(inst_dsl::state.eq_any(safe_states))
                .filter(inst_dsl::migration_id.is_null()),
            table
                .into_boxed()
                .filter(dsl::state.eq(IpAttachState::Attached))
                .filter(dsl::kind.eq(kind)),
            diesel::update(dsl::external_ip).set((
                dsl::time_modified.eq(Utc::now()),
                dsl::state.eq(IpAttachState::Detaching),
            )),
        );

        let mut do_saga = true;
        query.detach_and_get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map(Some)
        .or_else(|e: DetachError<ExternalIp, _, _>| Err(match e {
            DetachError::CollectionNotFound => {
                Error::not_found_by_id(
                    ResourceType::Instance,
                    &instance_id.into_untyped_uuid(),
                )
            },
            DetachError::ResourceNotFound => {
                if kind == IpKind::Ephemeral {
                    return Ok(None);
                } else {
                    Error::not_found_by_id(
                        ResourceType::FloatingIp,
                        &ip_id,
                    )
                }
            },
            DetachError::NoUpdate { resource, collection } => {
                let parent_match = resource.parent_id == Some(instance_id.into_untyped_uuid());
                match resource.state {
                    // Idempotent cases: already detached OR detaching from same instance.
                    IpAttachState::Detached => {
                        do_saga = false;
                        return Ok(Some(resource))
                    },
                    IpAttachState::Detaching if parent_match => return Ok(Some(resource)),
                    IpAttachState::Attached if !parent_match
                        => return Err(Error::invalid_request(&format!(
                        "{kind} IP is not attached to the target instance",
                    ))),
                    // User can reattempt depending on how the current saga unfolds.
                    IpAttachState::Attaching
                        | IpAttachState::Detaching => return Err(Error::unavail(&format!(
                        "tried to detach {kind} IP mid-attach/detach: \
                         detach will be safe to retry once operation on \
                         same IP resource completes"
                    ))),
                    IpAttachState::Attached => {},
                }

                if collection.runtime_state.migration_id.is_some() {
                    return Err(Error::unavail(&format!(
                        "tried to detach {kind} IP while instance was migrating: \
                         detach will be safe to retry once migrate completes"
                    )))
                }

                match collection.runtime_state.nexus_state {
                    state if SAFE_TO_ATTACH_INSTANCE_STATES.contains(&state) => {
                        Error::internal_error(&format!("failed to detach {kind} IP"))
                    },
                    state => Error::invalid_request(&format!(
                        "cannot detach {kind} IP from instance in {state} state"
                    )),
                }
            },
            DetachError::DatabaseError(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            },

        }))
        .map(|eip| eip.map(|v| (v, do_saga)))
    }

    /// Deallocate the external IP address with the provided ID. This is a complete
    /// removal of the IP entry, in contrast with `begin_deallocate_ephemeral_ip`,
    /// and should only be used for SNAT entries or cleanup of short-lived ephemeral
    /// IPs on failure.
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
        let conn = self.pool_connection_authorized(opctx).await?;
        self.deallocate_external_ip_on_connection(&conn, ip_id).await
    }

    /// Variant of [Self::deallocate_external_ip] which may be called from a
    /// transaction context.
    pub(crate) async fn deallocate_external_ip_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        ip_id: Uuid,
    ) -> Result<bool, Error> {
        use nexus_db_schema::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(ip_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<ExternalIp>(ip_id)
            .execute_and_check(conn)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Moves an instance's ephemeral IP from 'Attached' to 'Detaching'.
    ///
    /// To support idempotency, this method will succeed if the instance
    /// has no ephemeral IP or one is actively being removed. As a result,
    /// information on an actual `ExternalIp` is best-effort.
    pub async fn begin_deallocate_ephemeral_ip(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        instance_id: InstanceUuid,
    ) -> Result<Option<ExternalIp>, Error> {
        let _ = LookupPath::new(opctx, self)
            .instance_id(instance_id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await?;

        self.begin_detach_ip(
            opctx,
            ip_id,
            instance_id,
            IpKind::Ephemeral,
            false,
        )
        .await
        .map(|res| res.map(|(ip, _do_saga)| ip))
    }

    /// Delete all non-floating IP addresses associated with the provided
    /// instance ID.
    ///
    /// This method returns the number of records deleted, rather than the usual
    /// `DeleteResult`. That's mostly useful for tests, but could be important
    /// if callers have some invariants they'd like to check.
    pub async fn deallocate_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        use nexus_db_schema::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_service.eq(false))
            .filter(dsl::is_probe.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::kind.ne(IpKind::Floating))
            .set((
                dsl::time_deleted.eq(now),
                dsl::state.eq(IpAttachState::Detached),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete all external IP addresses associated with the provided probe
    /// ID.
    ///
    /// This method returns the number of records deleted, rather than the usual
    /// `DeleteResult`. That's mostly useful for tests, but could be important
    /// if callers have some invariants they'd like to check.
    pub async fn deallocate_external_ip_by_probe_id(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> Result<usize, Error> {
        use nexus_db_schema::schema::external_ip::dsl;
        let now = Utc::now();
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_probe.eq(true))
            .filter(dsl::parent_id.eq(probe_id))
            .filter(dsl::kind.ne(IpKind::Ephemeral))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Detach an individual Floating IP address from their parent instance.
    ///
    /// As in `deallocate_external_ip_by_instance_id`, this method returns the
    /// number of records altered, rather than an `UpdateResult`.
    ///
    /// This method ignores ongoing state transitions, and is only safely
    /// usable from within the instance_delete saga.
    pub async fn detach_floating_ips_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        use nexus_db_schema::schema::external_ip::dsl;
        diesel::update(dsl::external_ip)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::is_service.eq(false))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::kind.eq(IpKind::Floating))
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::parent_id.eq(Option::<Uuid>::None),
                dsl::state.eq(IpAttachState::Detached),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch all external IP addresses of any kind for the provided instance
    /// in all attachment states.
    pub async fn instance_lookup_external_ips(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> LookupResult<Vec<ExternalIp>> {
        use nexus_db_schema::schema::external_ip::dsl;
        dsl::external_ip
            .filter(dsl::is_service.eq(false))
            .filter(dsl::is_probe.eq(false))
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(ExternalIp::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch the ephmeral IP address assigned to the provided instance, if this
    /// has been configured.
    pub async fn instance_lookup_ephemeral_ip(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> LookupResult<Option<ExternalIp>> {
        Ok(self
            .instance_lookup_external_ips(opctx, instance_id)
            .await?
            .into_iter()
            .find(|v| v.kind == IpKind::Ephemeral))
    }

    /// Fetch all external IP addresses of any kind for the provided probe.
    pub async fn probe_lookup_external_ips(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> LookupResult<Vec<ExternalIp>> {
        use nexus_db_schema::schema::external_ip::dsl;
        dsl::external_ip
            .filter(dsl::is_probe.eq(true))
            .filter(dsl::parent_id.eq(probe_id))
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
        use nexus_db_schema::schema::floating_ip::dsl;

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

    /// Update a Floating IP
    pub async fn floating_ip_update(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
        update: FloatingIpUpdate,
    ) -> UpdateResult<ExternalIp> {
        use nexus_db_schema::schema::external_ip::dsl;

        opctx.authorize(authz::Action::Modify, authz_fip).await?;

        diesel::update(dsl::external_ip)
            .filter(dsl::id.eq(authz_fip.id()))
            .filter(dsl::time_deleted.is_null())
            .set(update)
            .returning(ExternalIp::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_fip),
                )
            })
    }

    /// Delete a Floating IP, verifying first that it is not in use.
    pub async fn floating_ip_delete(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
    ) -> DeleteResult {
        use nexus_db_schema::schema::external_ip::dsl;

        opctx.authorize(authz::Action::Delete, authz_fip).await?;

        let now = Utc::now();
        let result = diesel::update(dsl::external_ip)
            .filter(dsl::id.eq(authz_fip.id()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.is_null())
            .filter(dsl::state.eq(IpAttachState::Detached))
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
            UpdateStatus::NotUpdatedButExists
                if result.found.parent_id.is_some() =>
            {
                Err(Error::invalid_request(
                    "Floating IP cannot be deleted while attached to an instance",
                ))
            }
            // Only remaining cause of `NotUpdated` is earlier soft-deletion.
            // Return success in this case to maintain idempotency.
            UpdateStatus::Updated | UpdateStatus::NotUpdatedButExists => Ok(()),
        }
    }

    /// Attaches a Floating IP address to an instance.
    ///
    /// This moves a floating IP into the 'attaching' state. Callers are
    /// responsible for calling `external_ip_complete_op` to finalise the
    /// IP in 'attached' state at saga completion.
    ///
    /// To better handle idempotent attachment, this method returns an
    /// additional bool:
    /// - true: EIP was detached or attaching. proceed with saga.
    /// - false: EIP was attached. No-op for remainder of saga.
    pub async fn floating_ip_begin_attach(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
        instance_id: InstanceUuid,
        creating_instance: bool,
    ) -> UpdateResult<(ExternalIp, bool)> {
        let (.., authz_instance) = LookupPath::new(opctx, self)
            .instance_id(instance_id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await?;

        opctx.authorize(authz::Action::Modify, authz_fip).await?;
        opctx.authorize(authz::Action::Modify, &authz_instance).await?;

        self.begin_attach_ip(
            opctx,
            authz_fip.id(),
            instance_id,
            IpKind::Floating,
            creating_instance,
        )
        .await
        .and_then(|v| {
            v.ok_or_else(|| {
                Error::internal_error(
                    "floating IP should never return `None` from begin_attach",
                )
            })
        })
    }

    /// Detaches a Floating IP address from an instance.
    ///
    /// This moves a floating IP into the 'detaching' state. Callers are
    /// responsible for calling `external_ip_complete_op` to finalise the
    /// IP in 'detached' state at saga completion.
    ///
    /// To better handle idempotent detachment, this method returns an
    /// additional bool:
    /// - true: EIP was attached or detaching. proceed with saga.
    /// - false: EIP was detached. No-op for remainder of saga.
    pub async fn floating_ip_begin_detach(
        &self,
        opctx: &OpContext,
        authz_fip: &authz::FloatingIp,
        instance_id: InstanceUuid,
        creating_instance: bool,
    ) -> UpdateResult<(ExternalIp, bool)> {
        let (.., authz_instance) = LookupPath::new(opctx, self)
            .instance_id(instance_id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await?;

        opctx.authorize(authz::Action::Modify, authz_fip).await?;
        opctx.authorize(authz::Action::Modify, &authz_instance).await?;

        self.begin_detach_ip(
            opctx,
            authz_fip.id(),
            instance_id,
            IpKind::Floating,
            creating_instance,
        )
        .await
        .and_then(|v| {
            v.ok_or_else(|| {
                Error::internal_error(
                    "floating IP should never return `None` from begin_detach",
                )
            })
        })
    }

    /// Move an external IP from a transitional state (attaching, detaching)
    /// to its intended end state.
    ///
    /// Returns the number of rows modified, this may be zero on:
    ///  - instance delete by another saga
    ///  - saga action rerun
    ///
    /// This is valid in both cases for idempotency.
    pub async fn external_ip_complete_op(
        &self,
        opctx: &OpContext,
        ip_id: Uuid,
        ip_kind: IpKind,
        expected_state: IpAttachState,
        target_state: IpAttachState,
    ) -> Result<usize, Error> {
        use nexus_db_schema::schema::external_ip::dsl;

        if matches!(
            expected_state,
            IpAttachState::Attached | IpAttachState::Detached
        ) {
            return Err(Error::internal_error(&format!(
                "{expected_state:?} is not a valid transition state for attach/detach"
            )));
        }

        let part_out = diesel::update(dsl::external_ip)
            .filter(dsl::id.eq(ip_id))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(expected_state));

        let now = Utc::now();
        let conn = self.pool_connection_authorized(opctx).await?;
        match (ip_kind, expected_state, target_state) {
            (IpKind::SNat, _, _) => {
                return Err(Error::internal_error(
                    "SNAT should not be removed via `external_ip_complete_op`, \
                    use `deallocate_external_ip`",
                ));
            }

            (IpKind::Ephemeral, _, IpAttachState::Detached) => {
                part_out
                    .set((
                        dsl::parent_id.eq(Option::<Uuid>::None),
                        dsl::time_modified.eq(now),
                        dsl::time_deleted.eq(now),
                        dsl::state.eq(target_state),
                    ))
                    .execute_async(&*conn)
                    .await
            }

            (IpKind::Floating, _, IpAttachState::Detached) => {
                part_out
                    .set((
                        dsl::parent_id.eq(Option::<Uuid>::None),
                        dsl::time_modified.eq(now),
                        dsl::state.eq(target_state),
                    ))
                    .execute_async(&*conn)
                    .await
            }

            // Attaching->Attached gets separate logic because we choose to fail
            // and unwind on instance delete. This covers two cases:
            // - External IP is deleted.
            // - Floating IP is suddenly `detached`.
            (_, IpAttachState::Attaching, IpAttachState::Attached) => {
                return part_out
                    .set((
                        dsl::time_modified.eq(Utc::now()),
                        dsl::state.eq(target_state),
                    ))
                    .check_if_exists::<ExternalIp>(ip_id)
                    .execute_and_check(
                        &*self.pool_connection_authorized(opctx).await?,
                    )
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })
                    .and_then(|r| match r.status {
                        UpdateStatus::Updated => Ok(1),
                        UpdateStatus::NotUpdatedButExists
                            if r.found.state == IpAttachState::Detached
                                || r.found.time_deleted.is_some() =>
                        {
                            Err(Error::internal_error(
                                "unwinding due to concurrent instance delete",
                            ))
                        }
                        UpdateStatus::NotUpdatedButExists => Ok(0),
                    });
            }

            // Unwind from failed detach.
            (_, _, IpAttachState::Attached) => {
                part_out
                    .set((
                        dsl::time_modified.eq(Utc::now()),
                        dsl::state.eq(target_state),
                    ))
                    .execute_async(&*conn)
                    .await
            }
            _ => return Err(Error::internal_error("unreachable")),
        }
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use nexus_types::external_api::shared::IpRange;
    use nexus_types::inventory::SourceNatConfig;
    use omicron_common::address::NUM_SOURCE_NAT_PORTS;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ExternalIpUuid;
    use std::collections::BTreeSet;
    use std::net::Ipv4Addr;

    async fn read_all_service_ips(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> Vec<ExternalIp> {
        let all_batched = datastore
            .external_ip_list_service_all_batched(opctx)
            .await
            .expect("failed to fetch all service IPs batched");
        let all_paginated = datastore
            .external_ip_list_service_all(opctx, &DataPageParams::max_page())
            .await
            .expect("failed to fetch all service IPs paginated");
        assert_eq!(all_batched, all_paginated);
        all_batched
    }

    #[tokio::test]
    async fn test_service_ip_list() {
        let logctx = dev::test_setup_log("test_service_ip_list");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // No IPs, to start
        let ips = read_all_service_ips(&datastore, opctx).await;
        assert_eq!(ips, vec![]);

        // Set up service IP pool range
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 10),
        ))
        .unwrap();
        let (service_ip_pool, db_pool) = datastore
            .ip_pools_service_lookup(opctx, IpVersion::V4)
            .await
            .expect("lookup service ip pool");
        datastore
            .ip_pool_add_range(opctx, &service_ip_pool, &db_pool, &ip_range)
            .await
            .expect("add range to service ip pool");

        // Allocate a bunch of fake service IPs.
        let mut external_ips = Vec::new();
        let mut allocate_snat = false; // flip-flop between regular and snat
        for ip in ip_range.iter() {
            let external_ip = if allocate_snat {
                OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
                    id: ExternalIpUuid::new_v4(),
                    snat_cfg: SourceNatConfig::new(
                        ip,
                        0,
                        NUM_SOURCE_NAT_PORTS - 1,
                    )
                    .unwrap(),
                })
            } else {
                OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                    id: ExternalIpUuid::new_v4(),
                    ip,
                })
            };
            let external_ip = datastore
                .external_ip_allocate_omicron_zone(
                    opctx,
                    OmicronZoneUuid::new_v4(),
                    ZoneKind::Nexus,
                    external_ip,
                )
                .await
                .expect("failed to allocate service IP");
            external_ips.push(external_ip);
            allocate_snat = !allocate_snat;
        }
        external_ips.sort_by_key(|ip| ip.id);

        // Ensure we see them all.
        let ips = read_all_service_ips(&datastore, opctx).await;
        assert_eq!(ips, external_ips);

        // Deallocate a few, and ensure we don't see them anymore.
        let mut removed_ip_ids = BTreeSet::new();
        for (i, external_ip) in external_ips.iter().enumerate() {
            if i % 3 == 0 {
                let id = external_ip.id;
                datastore
                    .deallocate_external_ip(opctx, id)
                    .await
                    .expect("failed to deallocate IP");
                removed_ip_ids.insert(id);
            }
        }

        // Check that we removed at least one, then prune them from our list of
        // expected IPs.
        assert!(!removed_ip_ids.is_empty());
        external_ips.retain(|ip| !removed_ip_ids.contains(&ip.id));

        // Ensure we see them all remaining IPs.
        let ips = read_all_service_ips(&datastore, opctx).await;
        assert_eq!(ips, external_ips);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
