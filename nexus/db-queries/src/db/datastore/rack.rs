// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Rack`]s.

use super::DataStore;
use super::SERVICE_IPV4_POOL_NAME;
use super::SERVICE_IPV6_POOL_NAME;
use super::dns::DnsVersionUpdateBuilder;
use super::ip_pool::ServiceIpPools;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::Asset;
use crate::db::model::CrucibleDataset;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::PhysicalDisk;
use crate::db::model::Rack;
use crate::db::model::UserProvisionType;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use diesel::upsert::excluded;
use ipnetwork::IpNetwork;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::MaybeRetryable::*;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_errors::retryable;
use nexus_db_fixed_data::vpc_subnet::DNS_VPC_SUBNET;
use nexus_db_fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
use nexus_db_fixed_data::vpc_subnet::NTP_VPC_SUBNET;
use nexus_db_lookup::DbConnection;
use nexus_db_lookup::LookupPath;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_model::InitialDnsGroup;
use nexus_db_model::IpConfig;
use nexus_db_model::IpVersion;
use nexus_db_model::PasswordHashString;
use nexus_db_model::SiloUser;
use nexus_db_model::SiloUserPasswordHash;
use nexus_db_model::SledState;
use nexus_db_model::SledUnderlaySubnetAllocation;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::OmicronZoneExternalIp;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::external_api::params as external_params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::identity::Resource;
use nexus_types::inventory::NetworkInterface;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::UserId;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog_error_chain::InlineErrorChain;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

/// Groups arguments related to rack initialization
#[derive(Clone)]
pub struct RackInit {
    pub rack_id: Uuid,
    pub rack_subnet: IpNetwork,
    pub blueprint: Blueprint,
    pub blueprint_execution_enabled: bool,
    pub physical_disks: Vec<PhysicalDisk>,
    pub zpools: Vec<Zpool>,
    pub datasets: Vec<CrucibleDataset>,
    pub service_ip_pool_ranges: Vec<IpRange>,
    pub internal_dns: InitialDnsGroup,
    pub external_dns: InitialDnsGroup,
    pub recovery_silo: external_params::SiloCreate,
    pub recovery_silo_fq_dns_name: String,
    pub recovery_user_id: UserId,
    pub recovery_user_password_hash: omicron_passwords::PasswordHashString,
    pub dns_update: DnsVersionUpdateBuilder,
    pub allowed_source_ips: AllowedSourceIps,
}

/// Possible errors while trying to initialize rack
#[derive(Debug)]
enum RackInitError {
    AddingIp(Error),
    AddingNic(Error),
    BlueprintInsert(Error),
    BlueprintTargetSet(Error),
    NexusDatabaseAccessRecordsInsert(Error),
    DatasetInsert { err: AsyncInsertError, zpool_id: ZpoolUuid },
    PhysicalDiskInsert(Error),
    ZpoolInsert(Error),
    RackUpdate { err: DieselError, rack_id: Uuid },
    DnsSerialization(Error),
    Silo(Error),
    RoleAssignment(Error),
    // Retryable database error
    Retryable(DieselError),
    // Other non-retryable database error
    Database(DieselError),
    // Error adding initial allowed source IP list
    AllowedSourceIpError(Error),
}

// Catch-all for Diesel error conversion into RackInitError, which
// can also label errors as retryable.
impl From<DieselError> for RackInitError {
    fn from(e: DieselError) -> Self {
        if retryable(&e) { Self::Retryable(e) } else { Self::Database(e) }
    }
}

impl From<RackInitError> for Error {
    fn from(e: RackInitError) -> Self {
        match e {
            RackInitError::AddingIp(err) => err,
            RackInitError::AddingNic(err) => err,
            RackInitError::DatasetInsert { err, zpool_id } => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Zpool,
                    lookup_type: LookupType::by_id(zpool_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            },
            RackInitError::PhysicalDiskInsert(err) => {
                err.internal_context("failed to insert physical disk")
            }
            RackInitError::ZpoolInsert(err) => {
                err.internal_context("failed to insert zpool")
            }
            RackInitError::BlueprintInsert(err) => {
                err.internal_context("failed to insert Blueprint")
            }
            RackInitError::BlueprintTargetSet(err) => {
                err.internal_context("failed to set target Blueprint")
            }
            RackInitError::NexusDatabaseAccessRecordsInsert(err) => err
                .internal_context(
                    "failed to insert nexus database access records",
                ),
            RackInitError::RackUpdate { err, rack_id } => {
                public_error_from_diesel(
                    err,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Rack,
                        LookupType::ById(rack_id),
                    ),
                )
            }
            RackInitError::DnsSerialization(err) => {
                err.internal_context("failed to serialize initial DNS records")
            }
            RackInitError::Silo(err) => {
                err.internal_context("failed to create recovery Silo")
            }
            RackInitError::RoleAssignment(err) => {
                err.internal_context("failed to assign role to initial user")
            }
            RackInitError::Retryable(err) => Error::unavail(&format!(
                "failed operation due to database contention: {:#}",
                err
            )),
            RackInitError::Database(err) => Error::internal_error(&format!(
                "failed operation due to database error: {:#}",
                err
            )),
            RackInitError::AllowedSourceIpError(err) => err,
        }
    }
}

/// Possible results of attempting a new sled underlay allocation
#[derive(Debug, Clone)]
pub enum SledUnderlayAllocationResult {
    /// A new allocation was created
    New(SledUnderlaySubnetAllocation),
    /// A prior allocation associated with a commissioned sled was found
    CommissionedSled(SledUnderlaySubnetAllocation),
}

impl DataStore {
    pub async fn rack_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Rack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::rack::dsl;
        paginated(dsl::rack, dsl::id, pagparams)
            .select(Rack::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn rack_list_initialized(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Rack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::rack::dsl;
        paginated(dsl::rack, dsl::id, pagparams)
            .select(Rack::as_select())
            .filter(dsl::initialized.eq(true))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Stores a new rack in the database.
    ///
    /// This function is a no-op if the rack already exists.
    pub async fn rack_insert(
        &self,
        opctx: &OpContext,
        rack: &Rack,
    ) -> Result<Rack, Error> {
        use nexus_db_schema::schema::rack::dsl;

        diesel::insert_into(dsl::rack)
            .values(rack.clone())
            .on_conflict(dsl::id)
            .do_update()
            // This is a no-op, since we conflicted on the ID.
            .set(dsl::id.eq(excluded(dsl::id)))
            .returning(Rack::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Rack,
                        &rack.id().to_string(),
                    ),
                )
            })
    }

    pub async fn update_rack_subnet(
        &self,
        opctx: &OpContext,
        rack: &Rack,
    ) -> Result<(), Error> {
        debug!(
            opctx.log,
            "updating rack subnet for rack {} to {:#?}",
            rack.id(),
            rack.rack_subnet
        );
        use nexus_db_schema::schema::rack::dsl;
        diesel::update(dsl::rack)
            .filter(dsl::id.eq(rack.id()))
            .set(dsl::rack_subnet.eq(rack.rack_subnet))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    // Return the subnet for the rack
    pub async fn rack_subnet(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<IpNetwork, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        use nexus_db_schema::schema::rack::dsl;
        // It's safe to unwrap the returned `rack_subnet` because
        // we filter on `rack_subnet.is_not_null()`
        let subnet = dsl::rack
            .filter(dsl::id.eq(rack_id))
            .filter(dsl::rack_subnet.is_not_null())
            .select(dsl::rack_subnet)
            .first_async::<Option<IpNetwork>>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        match subnet {
            Some(subnet) => Ok(subnet),
            None => Err(Error::internal_error(
                "DB Error(bug): returned a null subnet for {rack_id}",
            )),
        }
    }

    /// Allocate a rack subnet octet to a given sled
    ///
    /// 1. Find the existing allocations
    /// 2. Calculate the new allocation
    /// 3. Save the new allocation, if there isn't one for the given
    ///    `hw_baseboard_id`
    /// 4. Return the new allocation
    ///
    // TODO: This could all actually be done in SQL using a `next_item` query.
    // See https://github.com/oxidecomputer/omicron/issues/4544
    pub async fn allocate_sled_underlay_subnet_octets(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        hw_baseboard_id: Uuid,
    ) -> Result<SledUnderlayAllocationResult, Error> {
        // Fetch all the existing allocations via self.rack_id
        let allocations = self.rack_subnet_allocations(opctx, rack_id).await?;

        // Calculate the allocation for the new sled by choosing the minimum
        // octet. The returned allocations are ordered by octet, so we will know
        // when we have a free one. However, if we already have an allocation
        // for the given sled then reuse that one.
        const MIN_SUBNET_OCTET: i16 = 33;
        let mut new_allocation = SledUnderlaySubnetAllocation {
            rack_id,
            sled_id: SledUuid::new_v4().into(),
            subnet_octet: MIN_SUBNET_OCTET,
            hw_baseboard_id,
        };
        for allocation in allocations {
            if allocation.hw_baseboard_id == new_allocation.hw_baseboard_id {
                // We already have an allocation for this sled, but we need to
                // check whether this allocation matches a sled that has been
                // decommissioned. (The same physical sled, tracked by
                // `hw_baseboard_id`, can be logically removed from the control
                // plane via decommissioning, then added back again later, which
                // requires allocating a new subnet.)
                match LookupPath::new(opctx, self)
                    .sled_id(allocation.sled_id.into())
                    .optional_fetch_for(authz::Action::Read)
                    .await?
                    .map(|(_, sled)| sled.state())
                {
                    Some(SledState::Active) => {
                        // This allocation is for an active sled; return the
                        // existing allocation.
                        return Ok(
                            SledUnderlayAllocationResult::CommissionedSled(
                                allocation,
                            ),
                        );
                    }
                    Some(SledState::Decommissioned) => {
                        // This allocation was for a now-decommissioned sled;
                        // ignore it and keep searching.
                    }
                    None => {
                        // This allocation is still "new" in the sense that it
                        // is assigned to a sled that has not yet upserted
                        // itself to join the control plane. We must return
                        // `::New(_)` here to ensure idempotence of allocation
                        // (e.g., if we allocate a sled, but its sled-agent
                        // crashes before it can upsert itself, we need to be
                        // able to get the same allocation back again).
                        return Ok(SledUnderlayAllocationResult::New(
                            allocation,
                        ));
                    }
                }
            }
            if allocation.subnet_octet == new_allocation.subnet_octet {
                bail_unless!(
                    new_allocation.subnet_octet < 255,
                    "Too many sled subnets allocated"
                );
                new_allocation.subnet_octet += 1;
            }
        }

        // Write the new allocation row to CRDB. The UNIQUE constraint
        // on `subnet_octet` will prevent dueling administrators reusing
        // allocations when sleds are being added. We will need another
        // mechanism ala generation numbers when we must interleave additions
        // and removals of sleds.
        self.sled_subnet_allocation_insert(opctx, &new_allocation).await?;
        Ok(SledUnderlayAllocationResult::New(new_allocation))
    }

    /// Return all current underlay allocations for the rack.
    ///
    /// Order allocations by `subnet_octet`
    pub async fn rack_subnet_allocations(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<Vec<SledUnderlaySubnetAllocation>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::sled_underlay_subnet_allocation::dsl as subnet_dsl;
        subnet_dsl::sled_underlay_subnet_allocation
            .filter(subnet_dsl::rack_id.eq(rack_id))
            .select(SledUnderlaySubnetAllocation::as_select())
            .order_by(subnet_dsl::subnet_octet.asc())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Store a new sled subnet allocation in the database
    pub async fn sled_subnet_allocation_insert(
        &self,
        opctx: &OpContext,
        allocation: &SledUnderlaySubnetAllocation,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::sled_underlay_subnet_allocation::dsl;
        diesel::insert_into(dsl::sled_underlay_subnet_allocation)
            .values(allocation.clone())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn rack_create_recovery_silo(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        log: &slog::Logger,
        recovery_silo: external_params::SiloCreate,
        recovery_silo_fq_dns_name: String,
        recovery_user_id: UserId,
        recovery_user_password_hash: omicron_passwords::PasswordHashString,
        dns_update: DnsVersionUpdateBuilder,
    ) -> Result<(), RackInitError> {
        if !matches!(
            &recovery_silo.identity_mode,
            shared::SiloIdentityMode::LocalOnly
        ) {
            return Err(RackInitError::Silo(Error::invalid_request(
                "recovery silo should only use identity mode LocalOnly",
            )));
        }

        let db_silo = self
            .silo_create_conn(
                conn,
                opctx,
                opctx,
                recovery_silo,
                &[recovery_silo_fq_dns_name],
                dns_update,
            )
            .await
            .map_err(|err| match err.retryable() {
                NotRetryable(err) => RackInitError::Silo(err.into()),
                Retryable(err) => RackInitError::Retryable(err),
            })?;
        info!(log, "Created recovery silo");

        // Create the first user in the initial Recovery Silo
        let silo_user_id = SiloUserUuid::new_v4();

        let silo_user = match &db_silo.user_provision_type {
            UserProvisionType::ApiOnly => SiloUser::new_api_only(
                db_silo.id(),
                silo_user_id,
                recovery_user_id.as_ref().to_owned(),
            ),

            UserProvisionType::Jit | UserProvisionType::Scim => {
                unreachable!("match at start of function should prevent this");
            }
        };

        {
            use nexus_db_schema::schema::silo_user::dsl;
            diesel::insert_into(dsl::silo_user)
                .values(silo_user)
                .execute_async(conn)
                .await?;
        }
        info!(log, "Created recovery user");

        // Set that user's password.
        let hash = SiloUserPasswordHash::new(
            silo_user_id,
            PasswordHashString::from(recovery_user_password_hash),
        );
        {
            use nexus_db_schema::schema::silo_user_password_hash::dsl;
            diesel::insert_into(dsl::silo_user_password_hash)
                .values(hash)
                .execute_async(conn)
                .await?;
        }
        info!(log, "Created recovery user's password");

        // Grant that user Admin privileges on the Recovery Silo.

        // This is very subtle: we must generate both of these queries before we
        // execute either of them, and we must not attempt to do any authz
        // checks after this in the same transaction because they may deadlock
        // with our query.
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            db_silo.id(),
            LookupType::ById(db_silo.id()),
        );
        let (q1, q2) = Self::role_assignment_replace_visible_queries(
            opctx,
            &authz_silo,
            &[shared::RoleAssignment::for_silo_user(
                silo_user_id,
                SiloRole::Admin,
            )],
        )
        .await
        .map_err(RackInitError::RoleAssignment)?;
        debug!(log, "Generated role assignment queries");

        q1.execute_async(conn).await?;
        q2.execute_async(conn).await?;
        info!(log, "Granted Silo privileges");

        Ok(())
    }

    async fn rack_populate_service_networking_records(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        log: &slog::Logger,
        service_pools: &ServiceIpPools,
        zone_config: &BlueprintZoneConfig,
    ) -> Result<(), RackInitError> {
        // For services with external connectivity, we record their
        // explicit IP allocation and create a service NIC as well.
        let zone_type = &zone_config.zone_type;
        let zone_report_str = zone_type.kind().report_str();

        // Extract an IPv4 address from the `shared::NetworkInterface` object.
        //
        // TODO-completeness: Handle IPv6 interface addresses. See
        // https://github.com/oxidecomputer/omicron/issues/9246.
        let extract_ipv4 = |nic: &NetworkInterface| -> Result<Ipv4Addr, Error> {
            let IpAddr::V4(ipv4) = nic.ip else {
                return Err(Error::invalid_request(
                    "IPv6 addresses are not yet supported",
                ));
            };
            Ok(ipv4)
        };

        let service_ip_nic = match zone_type {
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { nic, dns_address, .. },
            ) => {
                let external_ip =
                    OmicronZoneExternalIp::Floating(dns_address.into_ip());
                let ip = extract_ipv4(nic).map_err(RackInitError::AddingNic)?;
                let db_nic = IncompleteNetworkInterface::new_service(
                    nic.id,
                    zone_config.id.into_untyped_uuid(),
                    DNS_VPC_SUBNET.clone(),
                    IdentityMetadataCreateParams {
                        name: nic.name.clone(),
                        description: format!(
                            "{} service vNIC",
                            zone_report_str
                        ),
                    },
                    IpConfig::from_ipv4(ip),
                    nic.mac,
                    nic.slot,
                )
                .map_err(|e| RackInitError::AddingNic(e))?;
                Some((external_ip, db_nic))
            }
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nic,
                external_ip,
                ..
            }) => {
                let external_ip = OmicronZoneExternalIp::Floating(*external_ip);
                let ip = extract_ipv4(nic).map_err(RackInitError::AddingNic)?;
                let db_nic = IncompleteNetworkInterface::new_service(
                    nic.id,
                    zone_config.id.into_untyped_uuid(),
                    NEXUS_VPC_SUBNET.clone(),
                    IdentityMetadataCreateParams {
                        name: nic.name.clone(),
                        description: format!(
                            "{} service vNIC",
                            zone_report_str
                        ),
                    },
                    IpConfig::from_ipv4(ip),
                    nic.mac,
                    nic.slot,
                )
                .map_err(|e| RackInitError::AddingNic(e))?;
                Some((external_ip, db_nic))
            }
            BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp { external_ip, nic, .. },
            ) => {
                let external_ip = OmicronZoneExternalIp::Snat(*external_ip);
                let ip = extract_ipv4(nic).map_err(RackInitError::AddingNic)?;
                let db_nic = IncompleteNetworkInterface::new_service(
                    nic.id,
                    zone_config.id.into_untyped_uuid(),
                    NTP_VPC_SUBNET.clone(),
                    IdentityMetadataCreateParams {
                        name: nic.name.clone(),
                        description: format!(
                            "{} service vNIC",
                            zone_report_str
                        ),
                    },
                    IpConfig::from_ipv4(ip),
                    nic.mac,
                    nic.slot,
                )
                .map_err(|e| RackInitError::AddingNic(e))?;
                Some((external_ip, db_nic))
            }
            BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Clickhouse(_)
            | BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::ClickhouseServer(_)
            | BlueprintZoneType::CockroachDb(_)
            | BlueprintZoneType::Crucible(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalDns(_)
            | BlueprintZoneType::Oximeter(_) => None,
        };
        let Some((external_ip, db_nic)) = service_ip_nic else {
            info!(
                log,
                "No networking records needed for {} service", zone_report_str,
            );
            return Ok(());
        };
        let service_pool =
            service_pools.pool_for_version(external_ip.ip_version().into());
        let db_ip = IncompleteExternalIp::for_omicron_zone(
            service_pool.id(),
            external_ip,
            zone_config.id,
            zone_config.zone_type.kind(),
        );
        Self::allocate_external_ip_on_connection(conn, db_ip).await.map_err(
            |err| {
                error!(
                    log,
                    "Initializing Rack: Failed to allocate \
                     IP address for {}",
                     zone_report_str;
                    "err" => %err,
                );
                match err.retryable() {
                    Retryable(e) => RackInitError::Retryable(e),
                    NotRetryable(e) => RackInitError::AddingIp(e.into()),
                }
            },
        )?;

        self.create_network_interface_raw_conn(conn, db_nic)
            .await
            .map(|_| ())
            .or_else(|e| {
                use db::queries::network_interface::InsertError;
                match e {
                    InsertError::InterfaceAlreadyExists(
                        _,
                        db::model::NetworkInterfaceKind::Service,
                    ) => Ok(()),
                    InsertError::Retryable(err) => {
                        Err(RackInitError::Retryable(err))
                    }
                    _ => Err(RackInitError::AddingNic(e.into_external())),
                }
            })?;
        info!(
            log,
            "Inserted networking records for {} service",
            zone_type.kind().report_str(),
        );

        Ok(())
    }

    /// Update a rack to mark that it has been initialized
    pub async fn rack_set_initialized(
        &self,
        opctx: &OpContext,
        rack_init: RackInit,
    ) -> UpdateResult<Rack> {
        use nexus_db_schema::schema::rack::dsl as rack_dsl;

        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        // The `RackInit` request will eventually be modified to include the
        // full details of the IP Pool(s) delegated to Oxide at RSS time. For
        // now, we still rely on the pre-populated IP Pools. There's one for
        // IPv4 and one for IPv6.
        //
        // See https://github.com/oxidecomputer/omicron/issues/8946.
        let service_ip_pools =
            self.ip_pools_service_lookup_both_versions(&opctx).await?;

        // NOTE: This operation could likely be optimized with a CTE, but given
        // the low-frequency of calls, this optimization has been deferred.
        let log = opctx.log.clone();
        let err = Arc::new(OnceLock::new());

        // This method uses nested transactions, which are not supported
        // with retryable transactions.
        let conn = self.pool_connection_authorized(opctx).await?;
        let rack = self.transaction_non_retry_wrapper("rack_set_initialized")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let log = log.clone();
                let rack_init = rack_init.clone();
                async move {
                    let rack_id = rack_init.rack_id;
                    let blueprint = rack_init.blueprint;
                    let physical_disks = rack_init.physical_disks;
                    let zpools = rack_init.zpools;
                    let datasets = rack_init.datasets;
                    let service_ip_pool_ranges =
                        rack_init.service_ip_pool_ranges;
                    let internal_dns = rack_init.internal_dns;
                    let external_dns = rack_init.external_dns;
                    let blueprint_execution_enabled =
                        rack_init.blueprint_execution_enabled;

                    // Early exit if the rack has already been initialized.
                    let rack = rack_dsl::rack
                        .filter(rack_dsl::id.eq(rack_id))
                        .select(Rack::as_select())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            error!(
                                log,
                                "Initializing Rack: Rack UUID not found";
                                InlineErrorChain::new(&e),
                            );
                            err.set(RackInitError::RackUpdate {
                                err: e,
                                rack_id,
                            })
                            .unwrap();
                            DieselError::RollbackTransaction
                        })?;
                    if rack.initialized {
                        info!(log, "Early exit: Rack already initialized");
                        return Ok::<_, DieselError>(rack);
                    }

                    // Otherwise, insert:
                    // - Services
                    // - PhysicalDisks
                    // - Zpools
                    // - Datasets
                    // - A blueprint
                    // - Nexus database access records
                    //
                    // Which RSS has already allocated during bootstrapping.

                    // Set up the IP pool for internal services.
                    for range in service_ip_pool_ranges {
                        let service_pool = service_ip_pools.pool_for_range(&range);
                        Self::ip_pool_add_range_on_connection(
                            &conn,
                            opctx,
                            &service_pool.authz_pool,
                            &service_pool.db_pool,
                            &range,
                        )
                        .await
                        .map_err(|e| {
                            error!(
                                log,
                                "Initializing Rack: Failed to add \
                                 IP pool range";
                                &e,
                            );
                            err.set(RackInitError::AddingIp(e)).unwrap();
                            DieselError::RollbackTransaction
                        })?;
                    }

                    // Insert the RSS-generated blueprint.
                    self.blueprint_insert_on_connection(
                        &conn, opctx, &blueprint,
                    )
                    .await
                    .map_err(|e| {
                        error!(
                            log,
                            "Initializing Rack: Failed to insert blueprint";
                            &e,
                        );
                        err.set(RackInitError::BlueprintInsert(e)).unwrap();
                        DieselError::RollbackTransaction
                    })?;

                    // Make that initial blueprint the target.
                    Self::blueprint_target_set_current_on_connection(
                        &conn,
                        opctx,
                        BlueprintTarget {
                            target_id: blueprint.id,
                            enabled: blueprint_execution_enabled,
                            time_made_target: Utc::now(),
                        },
                    )
                    .await
                    .map_err(|e| {
                        error!(
                            log,
                            "Initializing Rack: Failed to set blueprint \
                             as target";
                            &e,
                        );
                        err.set(RackInitError::BlueprintTargetSet(e)).unwrap();
                        DieselError::RollbackTransaction
                    })?;

                    // Insert Nexus database access records
                    self.initialize_nexus_access_from_blueprint_on_connection(
                        &conn,
                        blueprint.all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                            .filter_map(|(_sled, zone_cfg)| {
                                if zone_cfg.zone_type.is_nexus() {
                                    Some(zone_cfg.id)
                                } else {
                                    None
                                }
                            }).collect(),
                    ).await.map_err(|e| {
                        err.set(RackInitError::NexusDatabaseAccessRecordsInsert(e)).unwrap();
                        DieselError::RollbackTransaction
                    })?;

                    // Allocate networking records for all services.
                    for (_, zone_config) in blueprint.all_omicron_zones(BlueprintZoneDisposition::is_in_service) {
                        self.rack_populate_service_networking_records(
                            &conn,
                            &log,
                            &service_ip_pools,
                            zone_config,
                        )
                        .await
                        .map_err(|e| {
                            error!(log, "Failed to upsert physical disk"; "err" => ?e);
                            err.set(e).unwrap();
                            DieselError::RollbackTransaction
                        })?;
                    }
                    info!(log, "Inserted service networking records");

                    for physical_disk in physical_disks {
                        info!(log, "physical disk upsert in handoff: {physical_disk:#?}");
                        if let Err(e) = Self::physical_disk_insert_on_connection(&conn, &opctx, physical_disk)
                            .await {
                            if !matches!(e, TransactionError::CustomError(Error::ObjectAlreadyExists { .. })) {
                                error!(log, "Failed to upsert physical disk"; "err" => #%e);
                                err.set(RackInitError::PhysicalDiskInsert(e.into()))
                                    .unwrap();
                                return Err(DieselError::RollbackTransaction);
                            }
                        }
                    }

                    info!(log, "Inserted physical disks");

                    for zpool in zpools {
                        if let Err(e) = Self::zpool_insert_on_connection(&conn, &opctx, zpool).await {
                            if !matches!(e, TransactionError::CustomError(Error::ObjectAlreadyExists { .. })) {
                                error!(log, "Failed to upsert zpool"; "err" => #%e);
                                err.set(RackInitError::ZpoolInsert(e.into())).unwrap();
                                return Err(DieselError::RollbackTransaction);
                            }
                        }
                    }

                    info!(log, "Inserted zpools");

                    for dataset in datasets {
                        use nexus_db_schema::schema::crucible_dataset::dsl;
                        let zpool_id = dataset.pool_id();
                        let _: CrucibleDataset = Zpool::insert_resource(
                            zpool_id.into(),
                            diesel::insert_into(dsl::crucible_dataset)
                                .values(dataset.clone())
                                .on_conflict(dsl::id)
                                .do_update()
                                .set((
                                    dsl::time_modified.eq(Utc::now()),
                                    dsl::pool_id.eq(excluded(dsl::pool_id)),
                                    dsl::ip.eq(excluded(dsl::ip)),
                                    dsl::port.eq(excluded(dsl::port)),
                                )),
                        )
                        .insert_and_get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.set(RackInitError::DatasetInsert {
                                err: e,
                                zpool_id,
                            })
                            .unwrap();
                            DieselError::RollbackTransaction
                        })?;
                    }
                    info!(log, "Inserted datasets");

                    // Insert the initial contents of the internal and external DNS
                    // zones.
                    Self::load_dns_data(&conn, internal_dns).await.map_err(
                        |e| {
                            err.set(RackInitError::DnsSerialization(e))
                                .unwrap();
                            DieselError::RollbackTransaction
                        },
                    )?;
                    info!(log, "Populated DNS tables for internal DNS");

                    Self::load_dns_data(&conn, external_dns).await.map_err(
                        |e| {
                            err.set(RackInitError::DnsSerialization(e))
                                .unwrap();
                            DieselError::RollbackTransaction
                        },
                    )?;
                    info!(log, "Populated DNS tables for external DNS");

                    // Create the initial Recovery Silo
                    self.rack_create_recovery_silo(
                        &opctx,
                        &conn,
                        &log,
                        rack_init.recovery_silo,
                        rack_init.recovery_silo_fq_dns_name,
                        rack_init.recovery_user_id,
                        rack_init.recovery_user_password_hash,
                        rack_init.dns_update,
                    )
                    .await
                    .map_err(|e| match e {
                        RackInitError::Retryable(e) => e,
                        _ => {
                            err.set(e).unwrap();
                            DieselError::RollbackTransaction
                        }
                    })?;

                    // Insert the initial source IP allowlist for requests to
                    // user-facing services.
                    Self::allow_list_upsert_on_connection(
                        opctx,
                        &conn,
                        rack_init.allowed_source_ips,
                    ).await.map_err(|e| {
                        err.set(RackInitError::AllowedSourceIpError(e)).unwrap();
                        DieselError::RollbackTransaction
                    })?;

                    let rack = diesel::update(rack_dsl::rack)
                        .filter(rack_dsl::id.eq(rack_id))
                        .set((
                            rack_dsl::initialized.eq(true),
                            rack_dsl::time_modified.eq(Utc::now()),
                        ))
                        .returning(Rack::as_returning())
                        .get_result_async::<Rack>(&conn)
                        .await
                        .map_err(|e| {
                            if retryable(&e) {
                                return e;
                            }
                            err.set(RackInitError::RackUpdate {
                                err: e,
                                rack_id,
                            })
                            .unwrap();
                            DieselError::RollbackTransaction
                        })?;
                    Ok(rack)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = Arc::try_unwrap(err).unwrap().take() {
                    err.into()
                } else {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })?;
        Ok(rack)
    }

    pub async fn load_builtin_rack_data(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        use omicron_common::api::external::Name;

        self.rack_insert(opctx, &db::model::Rack::new(rack_id)).await?;

        // Insert an IP Pool for both IP versions, reserved for Oxide internal
        // use.
        for (version, name) in [
            (IpVersion::V4, SERVICE_IPV4_POOL_NAME),
            (IpVersion::V6, SERVICE_IPV6_POOL_NAME),
        ] {
            let internal_pool = db::model::IpPool::new(
                &IdentityMetadataCreateParams {
                    name: name.parse::<Name>().unwrap(),
                    description: format!(
                        "IP{version} IP Pool for Oxide Services"
                    ),
                },
                version,
                nexus_db_model::IpPoolReservationType::OxideInternal,
            );
            match self.ip_pool_create(opctx, internal_pool).await {
                Ok(_) | Err(Error::ObjectAlreadyExists { .. }) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::Discoverability;
    use crate::db::model::ExternalIp;
    use crate::db::model::IpKind;
    use crate::db::model::IpPoolRange;
    use crate::db::model::Sled;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::SledUpdateBuilder;
    use async_bb8_diesel::AsyncSimpleConnection;
    use id_map::IdMap;
    use internal_dns_types::names::DNS_ZONE;
    use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use nexus_db_model::{DnsGroup, Generation, InitialDnsGroup};
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::system::{
        SledBuilder, SystemDescription,
    };
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use nexus_types::deployment::BlueprintSledConfig;
    use nexus_types::deployment::BlueprintSource;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::{
        BlueprintZoneConfig, OmicronZoneExternalFloatingAddr,
        OmicronZoneExternalFloatingIp,
    };
    use nexus_types::deployment::{
        BlueprintZoneDisposition, BlueprintZoneImageSource,
        OmicronZoneExternalSnatIp, OximeterReadMode,
    };
    use nexus_types::external_api::shared::SiloIdentityMode;
    use nexus_types::external_api::views::SledState;
    use nexus_types::identity::Asset;
    use nexus_types::internal_api::params::DnsRecord;
    use nexus_types::inventory::NetworkInterface;
    use nexus_types::inventory::NetworkInterfaceKind;
    use omicron_common::address::NEXUS_OPTE_IPV6_SUBNET;
    use omicron_common::address::{
        DNS_OPTE_IPV4_SUBNET, NEXUS_OPTE_IPV4_SUBNET, NTP_OPTE_IPV4_SUBNET,
    };
    use omicron_common::api::external::http_pagination::PaginatedBy;
    use omicron_common::api::external::{
        IdentityMetadataCreateParams, MacAddr, Vni,
    };
    use omicron_common::api::internal::shared::SourceNatConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use oxnet::IpNet;
    use std::collections::{BTreeMap, HashMap};
    use std::net::Ipv6Addr;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroU32;

    // Default impl is for tests only, and really just so that tests can more
    // easily specify just the parts that they want.
    impl Default for RackInit {
        fn default() -> Self {
            let blueprint_id = BlueprintUuid::new_v4();
            RackInit {
                rack_id: Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap(),
                rack_subnet: nexus_test_utils::RACK_SUBNET.parse().unwrap(),
                blueprint: Blueprint {
                    id: blueprint_id,
                    sleds: BTreeMap::new(),
                    pending_mgs_updates: PendingMgsUpdates::new(),
                    cockroachdb_setting_preserve_downgrade:
                        CockroachDbPreserveDowngrade::DoNotModify,
                    parent_blueprint_id: None,
                    internal_dns_version: *Generation::new(),
                    external_dns_version: *Generation::new(),
                    target_release_minimum_generation: *Generation::new(),
                    nexus_generation: *Generation::new(),
                    cockroachdb_fingerprint: String::new(),
                    clickhouse_cluster_config: None,
                    oximeter_read_version: *Generation::new(),
                    oximeter_read_mode: OximeterReadMode::SingleNode,
                    time_created: Utc::now(),
                    creator: "test suite".to_string(),
                    comment: "test suite".to_string(),
                    source: BlueprintSource::Test,
                },
                blueprint_execution_enabled: false,
                physical_disks: vec![],
                zpools: vec![],
                datasets: vec![],
                service_ip_pool_ranges: vec![],
                internal_dns: InitialDnsGroup::new(
                    DnsGroup::Internal,
                    DNS_ZONE,
                    "test suite",
                    "test suite",
                    HashMap::new(),
                ),
                external_dns: InitialDnsGroup::new(
                    DnsGroup::External,
                    DNS_ZONE,
                    "test suite",
                    "test suite",
                    HashMap::new(),
                ),
                recovery_silo: external_params::SiloCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "test-silo".parse().unwrap(),
                        description: String::new(),
                    },
                    // Set a default quota of a half rack's worth of resources
                    quotas: external_params::SiloQuotasCreate::arbitrarily_high_default(),
                    discoverable: false,
                    identity_mode: SiloIdentityMode::LocalOnly,
                    admin_group_name: None,
                    tls_certificates: vec![],
                    mapped_fleet_roles: Default::default(),
                },
                recovery_silo_fq_dns_name: format!(
                    "test-silo.sys.{}",
                    DNS_ZONE
                ),
                recovery_user_id: "test-user".parse().unwrap(),
                // Generated via `cargo run --example argon2 -- --input ""`.
                recovery_user_password_hash: "$argon2id$v=19$m=98304,t=23,\
                    p=1$E4DE+f6Yduuy0nSubo5qtg$57JDYGov3SZoEZnLyZZBHOACH95s\
                    8aOpG22zBoWZ2S4"
                    .parse()
                    .unwrap(),
                dns_update: DnsVersionUpdateBuilder::new(
                    DnsGroup::External,
                    "test suite".to_string(),
                    "test suite".to_string(),
                ),
                allowed_source_ips: AllowedSourceIps::Any,
            }
        }
    }

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn rack_set_initialized_empty() {
        let logctx = dev::test_setup_log("rack_set_initialized_empty");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let before = Utc::now();
        let rack_init = RackInit::default();

        // Initializing the rack with no data is odd, but allowed.
        let rack = datastore
            .rack_set_initialized(&opctx, rack_init.clone())
            .await
            .expect("Failed to initialize rack");

        let after = Utc::now();
        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        // Verify the DNS configuration.
        let dns_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_internal.generation), 1);
        assert!(dns_internal.time_created >= before);
        assert!(dns_internal.time_created <= after);
        assert_eq!(dns_internal.zones.len(), 0);

        let dns_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        // The external DNS zone has an extra update due to the initial Silo
        // creation.
        assert_eq!(dns_internal.generation.next(), dns_external.generation);
        assert_eq!(dns_internal.zones, dns_external.zones);

        // Verify the details about the initial Silo.
        let silos = datastore
            .silos_list(
                &opctx,
                &PaginatedBy::Name(DataPageParams {
                    marker: None,
                    limit: NonZeroU32::new(2).unwrap(),
                    direction: dropshot::PaginationOrder::Ascending,
                }),
                Discoverability::DiscoverableOnly,
            )
            .await
            .expect("Failed to list Silos");
        // It should *not* show up in the list because it's not discoverable.
        assert_eq!(silos.len(), 0);
        let (authz_silo, db_silo) = LookupPath::new(&opctx, datastore)
            .silo_name(&nexus_db_model::Name(
                rack_init.recovery_silo.identity.name.clone(),
            ))
            .fetch()
            .await
            .expect("Failed to lookup Silo");
        assert!(!db_silo.discoverable);

        // Verify that the user exists and has the password (hash) that we
        // expect.
        let silo_users = datastore
            .silo_users_list(
                &opctx,
                &authz::SiloUserList::new(authz_silo.clone()),
                &DataPageParams {
                    marker: None,
                    limit: NonZeroU32::new(2).unwrap(),
                    direction: dropshot::PaginationOrder::Ascending,
                },
            )
            .await
            .expect("failed to list users");

        assert_eq!(silo_users.len(), 1);

        let db::datastore::SiloUser::ApiOnly(silo_user) = &silo_users[0] else {
            panic!("wrong user type {:?}", silo_users[0].user_provision_type());
        };

        assert_eq!(silo_user.external_id, rack_init.recovery_user_id.as_ref());

        let authz_silo_user = authz::SiloUser::new(
            authz_silo,
            silo_users[0].id(),
            LookupType::by_id(silo_users[0].id()),
        );

        let hash = datastore
            .silo_user_password_hash_fetch(&opctx, &authz_silo_user)
            .await
            .expect("Failed to lookup password hash")
            .expect("Found no password hash");
        assert_eq!(hash.hash.0, rack_init.recovery_user_password_hash);

        // It should also be idempotent.
        let rack2 = datastore
            .rack_set_initialized(&opctx, rack_init)
            .await
            .expect("Failed to initialize rack");
        assert_eq!(rack.time_modified(), rack2.time_modified());

        let dns_internal2 = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        assert_eq!(dns_internal, dns_internal2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn create_test_sled(db: &DataStore, sled_id: SledUuid) -> Sled {
        let sled_update = SledUpdateBuilder::new()
            .sled_id(sled_id)
            .rack_id(rack_id())
            .build();
        let (sled, _) = db
            .sled_upsert(sled_update)
            .await
            .expect("Could not upsert sled during test prep");
        sled
    }

    // Hacky macro helper to:
    // - Perform a transaction...
    // - ... That queries a particular table for all values...
    // - ... and Selects them as the requested model type.
    macro_rules! fn_to_get_all {
        ($table:ident, $model:ident) => {
            paste::paste! {
                async fn [<get_all_ $table s>](db: &DataStore) -> Vec<$model> {
                    use nexus_db_schema::schema::$table::dsl;
                    use nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL;
                    let conn = db.pool_connection_for_tests()
                        .await
                        .unwrap();

                    db.transaction_retry_wrapper(concat!("fn_to_get_all_", stringify!($table)))
                        .transaction(&conn, |conn| async move {
                            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                                .await
                                .unwrap();
                            Ok(
                                dsl::$table
                                    .select($model::as_select())
                                    .get_results_async(&conn)
                                    .await
                                    .unwrap()
                            )
                        })
                        .await
                        .unwrap()
                }
            }
        };
    }

    fn_to_get_all!(external_ip, ExternalIp);
    fn_to_get_all!(ip_pool_range, IpPoolRange);
    fn_to_get_all!(crucible_dataset, CrucibleDataset);

    fn random_zpool() -> ZpoolName {
        ZpoolName::new_external(ZpoolUuid::new_v4())
    }

    fn random_dataset() -> OmicronZoneDataset {
        OmicronZoneDataset {
            pool_name: illumos_utils::zpool::ZpoolName::new_external(
                ZpoolUuid::new_v4(),
            )
            .to_string()
            .parse()
            .unwrap(),
        }
    }

    fn make_sled_config_only_zones(
        blueprint_zones: BTreeMap<SledUuid, IdMap<BlueprintZoneConfig>>,
    ) -> BTreeMap<SledUuid, BlueprintSledConfig> {
        blueprint_zones
            .into_iter()
            .map(|(sled_id, zones)| {
                (
                    sled_id,
                    BlueprintSledConfig {
                        state: SledState::Active,
                        sled_agent_generation: Generation::new().next(),
                        disks: IdMap::new(),
                        datasets: IdMap::new(),
                        zones,
                        remove_mupdate_override: None,
                        host_phase_2:
                            BlueprintHostPhase2DesiredSlots::current_contents(),
                    },
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn rack_set_initialized_with_services() {
        let test_name = "rack_set_initialized_with_services";
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled1 = create_test_sled(&datastore, SledUuid::new_v4()).await;
        let sled2 = create_test_sled(&datastore, SledUuid::new_v4()).await;
        let sled3 = create_test_sled(&datastore, SledUuid::new_v4()).await;

        let service_ip_pool_ranges = vec![
            IpRange::try_from((
                Ipv4Addr::new(1, 2, 3, 4),
                Ipv4Addr::new(1, 2, 3, 6),
            ))
            .unwrap(),
        ];

        let mut system = SystemDescription::new();
        system
            .service_ip_pool_ranges(service_ip_pool_ranges.clone())
            .sled(SledBuilder::new().id(sled1.id()))
            .expect("failed to add sled1")
            .sled(SledBuilder::new().id(sled2.id()))
            .expect("failed to add sled2")
            .sled(SledBuilder::new().id(sled3.id()))
            .expect("failed to add sled3");

        let external_dns_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let external_dns_pip = DNS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let external_dns_id = OmicronZoneUuid::new_v4();
        let nexus_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 6));
        let nexus_pip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let nexus_id = OmicronZoneUuid::new_v4();
        let ntp1_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 5));
        let ntp1_pip = NTP_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let ntp1_id = OmicronZoneUuid::new_v4();
        let ntp2_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 5));
        let ntp2_pip = NTP_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 2)
            .unwrap();
        let ntp2_id = OmicronZoneUuid::new_v4();
        let ntp3_id = OmicronZoneUuid::new_v4();
        let mut macs = MacAddr::iter_system();

        let mut blueprint_zones = BTreeMap::new();
        let dataset = random_dataset();
        blueprint_zones.insert(
            sled1.id(),
            [
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: external_dns_id,
                    filesystem_pool: dataset.pool_name,
                    zone_type: BlueprintZoneType::ExternalDns(
                        blueprint_zone_type::ExternalDns {
                            dataset,
                            http_address: "[::1]:80".parse().unwrap(),
                            dns_address: OmicronZoneExternalFloatingAddr {
                                id: ExternalIpUuid::new_v4(),
                                addr: SocketAddr::new(external_dns_ip, 53),
                            },
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: external_dns_id.into_untyped_uuid(),
                                },
                                name: "external-dns".parse().unwrap(),
                                ip: external_dns_pip.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*DNS_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: ntp1_id,
                    filesystem_pool: random_zpool(),
                    zone_type: BlueprintZoneType::BoundaryNtp(
                        blueprint_zone_type::BoundaryNtp {
                            address: "[::1]:80".parse().unwrap(),
                            ntp_servers: vec![],
                            dns_servers: vec![],
                            domain: None,
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: ntp1_id.into_untyped_uuid(),
                                },
                                name: "ntp1".parse().unwrap(),
                                ip: ntp1_pip.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*NTP_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                            external_ip: OmicronZoneExternalSnatIp {
                                id: ExternalIpUuid::new_v4(),
                                snat_cfg: SourceNatConfig::new(
                                    ntp1_ip, 16384, 32767,
                                )
                                .unwrap(),
                            },
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
            ]
            .into_iter()
            .collect::<IdMap<_>>(),
        );
        blueprint_zones.insert(
            sled2.id(),
            [
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: nexus_id,
                    filesystem_pool: random_zpool(),
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address: "[::1]:80".parse().unwrap(),
                            lockstep_port:
                                omicron_common::address::NEXUS_LOCKSTEP_PORT,
                            external_ip: OmicronZoneExternalFloatingIp {
                                id: ExternalIpUuid::new_v4(),
                                ip: nexus_ip,
                            },
                            external_tls: false,
                            external_dns_servers: vec![],
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: nexus_id.into_untyped_uuid(),
                                },
                                name: "nexus".parse().unwrap(),
                                ip: nexus_pip.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                            nexus_generation: *Generation::new(),
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: ntp2_id,
                    filesystem_pool: random_zpool(),
                    zone_type: BlueprintZoneType::BoundaryNtp(
                        blueprint_zone_type::BoundaryNtp {
                            address: "[::1]:80".parse().unwrap(),
                            ntp_servers: vec![],
                            dns_servers: vec![],
                            domain: None,
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: ntp2_id.into_untyped_uuid(),
                                },
                                name: "ntp2".parse().unwrap(),
                                ip: ntp2_pip.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*NTP_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                            external_ip: OmicronZoneExternalSnatIp {
                                id: ExternalIpUuid::new_v4(),
                                snat_cfg: SourceNatConfig::new(
                                    ntp2_ip, 0, 16383,
                                )
                                .unwrap(),
                            },
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
            ]
            .into_iter()
            .collect(),
        );
        blueprint_zones.insert(
            sled3.id(),
            [BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: ntp3_id,
                filesystem_pool: random_zpool(),
                zone_type: BlueprintZoneType::InternalNtp(
                    blueprint_zone_type::InternalNtp {
                        address: "[::1]:80".parse().unwrap(),
                    },
                ),
                image_source: BlueprintZoneImageSource::InstallDataset,
            }]
            .into_iter()
            .collect(),
        );
        let blueprint_id = BlueprintUuid::new_v4();
        let blueprint = Blueprint {
            id: blueprint_id,
            sleds: make_sled_config_only_zones(blueprint_zones),
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: None,
            internal_dns_version: *Generation::new(),
            external_dns_version: *Generation::new(),
            target_release_minimum_generation: *Generation::new(),
            nexus_generation: *Generation::new(),
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: *Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
        };

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    blueprint: blueprint.clone(),
                    service_ip_pool_ranges,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to initialize rack");

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        // We should see the blueprint we passed in.
        let (_blueprint_target, observed_blueprint) = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .expect("failed to read blueprint");
        assert_eq!(observed_blueprint, blueprint);

        // We should also see the single external IP allocated for each service
        // save for the non-boundary NTP service.
        let observed_external_ips = get_all_external_ips(&datastore).await;
        assert_eq!(observed_external_ips.len(), 4);
        let dns_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(external_dns_id.into_untyped_uuid()))
            .unwrap();
        let nexus_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(nexus_id.into_untyped_uuid()))
            .unwrap();
        let ntp1_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(ntp1_id.into_untyped_uuid()))
            .unwrap();
        let ntp2_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(ntp2_id.into_untyped_uuid()))
            .unwrap();
        assert!(
            !observed_external_ips
                .iter()
                .any(|e| e.parent_id == Some(ntp3_id.into_untyped_uuid()))
        );

        assert!(dns_external_ip.is_service);
        assert_eq!(dns_external_ip.kind, IpKind::Floating);

        assert!(nexus_external_ip.is_service);
        assert_eq!(nexus_external_ip.kind, IpKind::Floating);

        assert!(ntp1_external_ip.is_service);
        assert_eq!(ntp1_external_ip.kind, IpKind::SNat);
        assert_eq!(ntp1_external_ip.first_port.0, 16384);
        assert_eq!(ntp1_external_ip.last_port.0, 32767);

        assert!(ntp2_external_ip.is_service);
        assert_eq!(ntp2_external_ip.kind, IpKind::SNat);
        assert_eq!(ntp2_external_ip.first_port.0, 0);
        assert_eq!(ntp2_external_ip.last_port.0, 16383);

        // Furthermore, we should be able to see that these IP addresses have
        // been allocated as a part of a service IP pool.
        let (.., svc_pool) = datastore
            .ip_pools_service_lookup(&opctx, IpVersion::V4)
            .await
            .unwrap();
        assert_eq!(svc_pool.name().as_str(), SERVICE_IPV4_POOL_NAME);

        let observed_ip_pool_ranges = get_all_ip_pool_ranges(&datastore).await;
        assert_eq!(observed_ip_pool_ranges.len(), 1);
        assert_eq!(observed_ip_pool_ranges[0].ip_pool_id, svc_pool.id());

        // Verify the allocated external IPs
        assert_eq!(dns_external_ip.ip_pool_id, svc_pool.id());
        assert_eq!(
            dns_external_ip.ip_pool_range_id,
            observed_ip_pool_ranges[0].id
        );
        assert_eq!(dns_external_ip.ip.ip(), external_dns_ip);

        assert_eq!(nexus_external_ip.ip_pool_id, svc_pool.id());
        assert_eq!(
            nexus_external_ip.ip_pool_range_id,
            observed_ip_pool_ranges[0].id
        );
        assert_eq!(nexus_external_ip.ip.ip(), nexus_ip);

        assert_eq!(ntp1_external_ip.ip_pool_id, svc_pool.id());
        assert_eq!(
            ntp1_external_ip.ip_pool_range_id,
            observed_ip_pool_ranges[0].id
        );
        assert_eq!(ntp1_external_ip.ip.ip(), ntp1_ip);

        assert_eq!(ntp2_external_ip.ip_pool_id, svc_pool.id());
        assert_eq!(
            ntp2_external_ip.ip_pool_range_id,
            observed_ip_pool_ranges[0].id
        );
        assert_eq!(ntp2_external_ip.ip.ip(), ntp2_ip);

        let observed_datasets = get_all_crucible_datasets(&datastore).await;
        assert!(observed_datasets.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_with_many_nexus_services() {
        let test_name = "rack_set_initialized_with_many_nexus_services";
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore, SledUuid::new_v4()).await;

        // Ask for two Nexus services, with different external IPs.
        let nexus_ip_start = Ipv4Addr::new(1, 2, 3, 4);
        let nexus_ip_end = Ipv4Addr::new(1, 2, 3, 5);
        let service_ip_pool_ranges = vec![
            IpRange::try_from((nexus_ip_start, nexus_ip_end))
                .expect("Cannot create IP Range"),
        ];

        let mut system = SystemDescription::new();
        system
            .service_ip_pool_ranges(service_ip_pool_ranges.clone())
            .sled(SledBuilder::new().id(sled.id()))
            .expect("failed to add sled");

        let nexus_id1 = OmicronZoneUuid::new_v4();
        let nexus_id2 = OmicronZoneUuid::new_v4();
        let nexus_pip1 = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let nexus_pip2 = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 2)
            .unwrap();
        let mut macs = MacAddr::iter_system();

        let mut blueprint_zones = BTreeMap::new();
        blueprint_zones.insert(
            sled.id(),
            [
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: nexus_id1,
                    filesystem_pool: random_zpool(),
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address: "[::1]:80".parse().unwrap(),
                            lockstep_port:
                                omicron_common::address::NEXUS_LOCKSTEP_PORT,
                            external_ip: OmicronZoneExternalFloatingIp {
                                id: ExternalIpUuid::new_v4(),
                                ip: nexus_ip_start.into(),
                            },
                            external_tls: false,
                            external_dns_servers: vec![],
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: nexus_id1.into_untyped_uuid(),
                                },
                                name: "nexus1".parse().unwrap(),
                                ip: nexus_pip1.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                            nexus_generation: *Generation::new(),
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: nexus_id2,
                    filesystem_pool: random_zpool(),
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address: "[::1]:80".parse().unwrap(),
                            lockstep_port:
                                omicron_common::address::NEXUS_LOCKSTEP_PORT,
                            external_ip: OmicronZoneExternalFloatingIp {
                                id: ExternalIpUuid::new_v4(),
                                ip: nexus_ip_end.into(),
                            },
                            external_tls: false,
                            external_dns_servers: vec![],
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: nexus_id2.into_untyped_uuid(),
                                },
                                name: "nexus2".parse().unwrap(),
                                ip: nexus_pip2.into(),
                                mac: macs.next().unwrap(),
                                subnet: oxnet::IpNet::from(
                                    *NEXUS_OPTE_IPV4_SUBNET,
                                ),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                            nexus_generation: *Generation::new(),
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
            ]
            .into_iter()
            .collect::<IdMap<_>>(),
        );

        let datasets = vec![];

        let internal_records = vec![
            DnsRecord::Aaaa("fe80::1:2:3:4".parse().unwrap()),
            DnsRecord::Aaaa("fe80::1:2:3:5".parse().unwrap()),
        ];
        let internal_dns = InitialDnsGroup::new(
            DnsGroup::Internal,
            DNS_ZONE,
            "test suite",
            "initial test suite internal rev",
            HashMap::from([("nexus".to_string(), internal_records.clone())]),
        );

        let external_records =
            vec![DnsRecord::Aaaa("fe80::5:6:7:8".parse().unwrap())];
        let external_dns = InitialDnsGroup::new(
            DnsGroup::External,
            "test-suite.oxide.test",
            "test suite",
            "initial test suite external rev",
            HashMap::from([("api.sys".to_string(), external_records.clone())]),
        );

        let blueprint_id = BlueprintUuid::new_v4();
        let blueprint = Blueprint {
            id: blueprint_id,
            sleds: make_sled_config_only_zones(blueprint_zones),
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: None,
            internal_dns_version: *Generation::new(),
            external_dns_version: *Generation::new(),
            target_release_minimum_generation: *Generation::new(),
            nexus_generation: *Generation::new(),
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: *Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
        };

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    blueprint: blueprint.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges,
                    internal_dns,
                    external_dns,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to initialize rack");

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        // We should see the blueprint we passed in.
        let (_blueprint_target, observed_blueprint) = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .expect("failed to read blueprint");
        assert_eq!(observed_blueprint, blueprint);

        // We should see both of the Nexus services we provisioned.
        let mut observed_zones: Vec<_> = observed_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .map(|(_, z)| z)
            .collect();
        observed_zones.sort_by_key(|z| z.id);
        assert_eq!(observed_zones.len(), 2);

        // We should see both IPs allocated for these services.
        let observed_external_ips = get_all_external_ips(&datastore).await;
        for external_ip in &observed_external_ips {
            assert!(external_ip.is_service);
            assert!(external_ip.parent_id.is_some());
            assert_eq!(external_ip.kind, IpKind::Floating);
        }
        let observed_external_ips: HashMap<_, _> = observed_external_ips
            .into_iter()
            .map(|ip| (ip.parent_id.unwrap(), ip))
            .collect();
        assert_eq!(observed_external_ips.len(), 2);

        // The address allocated for the service should match the input.
        assert_eq!(
            observed_external_ips[observed_zones[0].id.as_untyped_uuid()]
                .ip
                .ip(),
            if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_ip,
                ..
            }) = &blueprint
                .all_omicron_zones(BlueprintZoneDisposition::any)
                .next()
                .unwrap()
                .1
                .zone_type
            {
                external_ip.ip
            } else {
                panic!("Unexpected zone type")
            }
        );
        assert_eq!(
            observed_external_ips[observed_zones[1].id.as_untyped_uuid()]
                .ip
                .ip(),
            if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_ip,
                ..
            }) = &blueprint
                .all_omicron_zones(BlueprintZoneDisposition::any)
                .nth(1)
                .unwrap()
                .1
                .zone_type
            {
                external_ip.ip
            } else {
                panic!("Unexpected service kind")
            }
        );

        // Furthermore, we should be able to see that this IP addresses have been
        // allocated as a part of a service IP pool.
        let (.., svc_pool) = datastore
            .ip_pools_service_lookup(&opctx, IpVersion::V4)
            .await
            .unwrap();
        assert_eq!(svc_pool.name().as_str(), SERVICE_IPV4_POOL_NAME);

        let observed_ip_pool_ranges = get_all_ip_pool_ranges(&datastore).await;
        assert_eq!(observed_ip_pool_ranges.len(), 1);
        assert_eq!(observed_ip_pool_ranges[0].ip_pool_id, svc_pool.id());

        let observed_datasets = get_all_crucible_datasets(&datastore).await;
        assert!(observed_datasets.is_empty());

        // Verify the internal and external DNS configurations.
        let dns_config_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config_internal.generation), 1);
        assert_eq!(dns_config_internal.zones.len(), 1);
        assert_eq!(dns_config_internal.zones[0].zone_name, DNS_ZONE);
        assert_eq!(
            dns_config_internal.zones[0].records,
            HashMap::from([("nexus".to_string(), internal_records)]),
        );

        let dns_config_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config_external.generation), 2);
        assert_eq!(dns_config_external.zones.len(), 1);
        assert_eq!(
            dns_config_external.zones[0].zone_name,
            "test-suite.oxide.test",
        );
        assert_eq!(
            dns_config_external.zones[0].records.get("api.sys"),
            Some(&external_records)
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_with_ipv6_public_addresses() {
        let test_name = "rack_set_initialized_with_ipv6_public_addresses";
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore, SledUuid::new_v4()).await;

        // Ask for a Nexus service with an IPv6 address.
        let nexus_ip_start =
            Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0, 0, 0, 0, 1);
        let nexus_ip_end =
            Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0, 0, 0, 0, 10);
        let service_ip_pool_ranges = vec![
            IpRange::try_from((nexus_ip_start, nexus_ip_end))
                .expect("Cannot create IP Range"),
        ];

        let mut system = SystemDescription::new();
        system
            .service_ip_pool_ranges(service_ip_pool_ranges.clone())
            .sled(SledBuilder::new().id(sled.id()))
            .expect("failed to add sled");

        let nexus_id = OmicronZoneUuid::new_v4();
        let nexus_pip = NEXUS_OPTE_IPV6_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u128 + 1)
            .unwrap();
        let mut macs = MacAddr::iter_system();

        let mut blueprint_zones = BTreeMap::new();
        blueprint_zones.insert(
            sled.id(),
            [BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: nexus_id,
                filesystem_pool: random_zpool(),
                zone_type: BlueprintZoneType::Nexus(
                    blueprint_zone_type::Nexus {
                        internal_address: "[::1]:80".parse().unwrap(),
                        lockstep_port:
                            omicron_common::address::NEXUS_LOCKSTEP_PORT,
                        external_ip: OmicronZoneExternalFloatingIp {
                            id: ExternalIpUuid::new_v4(),
                            ip: nexus_ip_start.into(),
                        },
                        external_tls: false,
                        external_dns_servers: vec![],
                        nic: NetworkInterface {
                            id: Uuid::new_v4(),
                            kind: NetworkInterfaceKind::Service {
                                id: nexus_id.into_untyped_uuid(),
                            },
                            name: "nexus1".parse().unwrap(),
                            ip: nexus_pip.into(),
                            mac: macs.next().unwrap(),
                            subnet: IpNet::from(*NEXUS_OPTE_IPV6_SUBNET),
                            vni: Vni::SERVICES_VNI,
                            primary: true,
                            slot: 0,
                            transit_ips: vec![],
                        },
                        nexus_generation: *Generation::new(),
                    },
                ),
                image_source: BlueprintZoneImageSource::InstallDataset,
            }]
            .into_iter()
            .collect::<IdMap<_>>(),
        );

        let datasets = vec![];

        let internal_records = vec![
            DnsRecord::Aaaa("fe80::1:2:3:4".parse().unwrap()),
            DnsRecord::Aaaa("fe80::1:2:3:5".parse().unwrap()),
        ];
        let internal_dns = InitialDnsGroup::new(
            DnsGroup::Internal,
            DNS_ZONE,
            "test suite",
            "initial test suite internal rev",
            HashMap::from([("nexus".to_string(), internal_records.clone())]),
        );

        let external_records =
            vec![DnsRecord::Aaaa("fe80::5:6:7:8".parse().unwrap())];
        let external_dns = InitialDnsGroup::new(
            DnsGroup::External,
            "test-suite.oxide.test",
            "test suite",
            "initial test suite external rev",
            HashMap::from([("api.sys".to_string(), external_records.clone())]),
        );

        let blueprint_id = BlueprintUuid::new_v4();
        let blueprint = Blueprint {
            id: blueprint_id,
            sleds: make_sled_config_only_zones(blueprint_zones),
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: None,
            internal_dns_version: *Generation::new(),
            external_dns_version: *Generation::new(),
            target_release_minimum_generation: *Generation::new(),
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: *Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
            nexus_generation: *Generation::new(),
        };

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    blueprint: blueprint.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges,
                    internal_dns,
                    external_dns,
                    ..Default::default()
                },
            )
            .await;

        // IPv6 addresses aren't fully supported right now. See
        // https://github.com/oxidecomputer/omicron/issues/1716. When that is
        // fully-addressed, this will start to fail and we can remove this
        // block to restore the previous test coverage.
        let Err(Error::InvalidRequest { message }) = &rack else {
            panic!(
                "Expected an error initializing a rack with an IPv6 address, \
                until they are fully-supported. Found {rack:#?}"
            );
        };
        assert_eq!(
            message.external_message(),
            "IPv6 addresses are not yet supported"
        );
        let Ok(rack) = rack else {
            db.terminate().await;
            logctx.cleanup_successful();
            return;
        };

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        // We should see the blueprint we passed in.
        let (_blueprint_target, observed_blueprint) = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .expect("failed to read blueprint");
        assert_eq!(observed_blueprint, blueprint);

        // We should see the Nexus service we provisioned.
        let mut observed_zones: Vec<_> = observed_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .map(|(_, z)| z)
            .collect();
        observed_zones.sort_by_key(|z| z.id);
        assert_eq!(observed_zones.len(), 1);

        // We should see the IP allocated for this service.
        let observed_external_ips = get_all_external_ips(&datastore).await;
        for external_ip in &observed_external_ips {
            assert!(external_ip.is_service);
            assert!(external_ip.parent_id.is_some());
            assert_eq!(external_ip.kind, IpKind::Floating);
        }
        let observed_external_ips: HashMap<_, _> = observed_external_ips
            .into_iter()
            .map(|ip| (ip.parent_id.unwrap(), ip))
            .collect();
        assert_eq!(observed_external_ips.len(), 1);

        // The address allocated for the service should match the input.
        let actual_ip = observed_external_ips
            [observed_zones[0].id.as_untyped_uuid()]
        .ip
        .ip();
        assert_eq!(
            actual_ip,
            if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_ip,
                ..
            }) = &blueprint
                .all_omicron_zones(BlueprintZoneDisposition::any)
                .next()
                .unwrap()
                .1
                .zone_type
            {
                external_ip.ip
            } else {
                panic!("Unexpected zone type")
            }
        );
        assert_eq!(actual_ip, nexus_ip_start);

        // Furthermore, we should be able to see that this IP address has been
        // allocated as a part of a service IPv6 IP pool.
        let (.., svc_pool) = datastore
            .ip_pools_service_lookup(&opctx, IpVersion::V6)
            .await
            .unwrap();
        assert_eq!(svc_pool.name().as_str(), SERVICE_IPV6_POOL_NAME);

        let observed_ip_pool_ranges = get_all_ip_pool_ranges(&datastore).await;
        assert_eq!(observed_ip_pool_ranges.len(), 1);
        assert_eq!(observed_ip_pool_ranges[0].ip_pool_id, svc_pool.id());
        assert_eq!(observed_ip_pool_ranges[0].first_address.ip(), actual_ip);

        let observed_datasets = get_all_crucible_datasets(&datastore).await;
        assert!(observed_datasets.is_empty());

        // Verify the internal and external DNS configurations.
        let dns_config_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config_internal.generation), 1);
        assert_eq!(dns_config_internal.zones.len(), 1);
        assert_eq!(dns_config_internal.zones[0].zone_name, DNS_ZONE);
        assert_eq!(
            dns_config_internal.zones[0].records,
            HashMap::from([("nexus".to_string(), internal_records)]),
        );

        let dns_config_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config_external.generation), 2);
        assert_eq!(dns_config_external.zones.len(), 1);
        assert_eq!(
            dns_config_external.zones[0].zone_name,
            "test-suite.oxide.test",
        );
        assert_eq!(
            dns_config_external.zones[0].records.get("api.sys"),
            Some(&external_records)
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_missing_service_pool_ip_throws_error() {
        let test_name =
            "rack_set_initialized_missing_service_pool_ip_throws_error";
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore, SledUuid::new_v4()).await;

        let mut system = SystemDescription::new();
        system
            .sled(SledBuilder::new().id(sled.id()))
            .expect("failed to add sled");

        let nexus_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let nexus_pip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let nexus_id = OmicronZoneUuid::new_v4();
        let mut macs = MacAddr::iter_system();
        let mut blueprint_zones = BTreeMap::new();
        blueprint_zones.insert(
            sled.id(),
            [BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: nexus_id,
                filesystem_pool: random_zpool(),
                zone_type: BlueprintZoneType::Nexus(
                    blueprint_zone_type::Nexus {
                        internal_address: "[::1]:80".parse().unwrap(),
                        lockstep_port:
                            omicron_common::address::NEXUS_LOCKSTEP_PORT,
                        external_ip: OmicronZoneExternalFloatingIp {
                            id: ExternalIpUuid::new_v4(),
                            ip: nexus_ip,
                        },
                        external_tls: false,
                        external_dns_servers: vec![],
                        nic: NetworkInterface {
                            id: Uuid::new_v4(),
                            kind: NetworkInterfaceKind::Service {
                                id: nexus_id.into_untyped_uuid(),
                            },
                            name: "nexus".parse().unwrap(),
                            ip: nexus_pip.into(),
                            mac: macs.next().unwrap(),
                            subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                            vni: Vni::SERVICES_VNI,
                            primary: true,
                            slot: 0,
                            transit_ips: vec![],
                        },
                        nexus_generation: *Generation::new(),
                    },
                ),
                image_source: BlueprintZoneImageSource::InstallDataset,
            }]
            .into_iter()
            .collect::<IdMap<_>>(),
        );
        let blueprint_id = BlueprintUuid::new_v4();
        let blueprint = Blueprint {
            id: blueprint_id,
            sleds: make_sled_config_only_zones(blueprint_zones),
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: None,
            internal_dns_version: *Generation::new(),
            external_dns_version: *Generation::new(),
            target_release_minimum_generation: *Generation::new(),
            nexus_generation: *Generation::new(),
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: *Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
        };

        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit { blueprint: blueprint.clone(), ..Default::default() },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Request: Requested external IP address not available"
        );

        assert!(get_all_crucible_datasets(&datastore).await.is_empty());
        assert!(get_all_external_ips(&datastore).await.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_overlapping_ips_throws_error() {
        let test_name = "rack_set_initialized_overlapping_ips_throws_error";
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore, SledUuid::new_v4()).await;

        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let service_ip_pool_ranges = vec![IpRange::from(ip)];

        let mut system = SystemDescription::new();
        system
            .service_ip_pool_ranges(service_ip_pool_ranges.clone())
            .sled(SledBuilder::new().id(sled.id()))
            .expect("failed to add sled");

        // Request two services which happen to be using the same IP address.
        let external_dns_id = OmicronZoneUuid::new_v4();
        let external_dns_pip = DNS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let nexus_id = OmicronZoneUuid::new_v4();
        let nexus_pip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let mut macs = MacAddr::iter_system();

        let mut blueprint_zones = BTreeMap::new();
        let dataset = random_dataset();
        blueprint_zones.insert(
            sled.id(),
            [
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: external_dns_id,
                    filesystem_pool: dataset.pool_name,
                    zone_type: BlueprintZoneType::ExternalDns(
                        blueprint_zone_type::ExternalDns {
                            dataset,
                            http_address: "[::1]:80".parse().unwrap(),
                            dns_address: OmicronZoneExternalFloatingAddr {
                                id: ExternalIpUuid::new_v4(),
                                addr: SocketAddr::new(ip, 53),
                            },
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: external_dns_id.into_untyped_uuid(),
                                },
                                name: "external-dns".parse().unwrap(),
                                ip: external_dns_pip.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*DNS_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: nexus_id,
                    filesystem_pool: random_zpool(),
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address: "[::1]:80".parse().unwrap(),
                            lockstep_port:
                                omicron_common::address::NEXUS_LOCKSTEP_PORT,
                            external_ip: OmicronZoneExternalFloatingIp {
                                id: ExternalIpUuid::new_v4(),
                                ip,
                            },
                            external_tls: false,
                            external_dns_servers: vec![],
                            nic: NetworkInterface {
                                id: Uuid::new_v4(),
                                kind: NetworkInterfaceKind::Service {
                                    id: nexus_id.into_untyped_uuid(),
                                },
                                name: "nexus".parse().unwrap(),
                                ip: nexus_pip.into(),
                                mac: macs.next().unwrap(),
                                subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                                vni: Vni::SERVICES_VNI,
                                primary: true,
                                slot: 0,
                                transit_ips: vec![],
                            },
                            nexus_generation: *Generation::new(),
                        },
                    ),
                    image_source: BlueprintZoneImageSource::InstallDataset,
                },
            ]
            .into_iter()
            .collect::<IdMap<_>>(),
        );

        let blueprint_id = BlueprintUuid::new_v4();
        let blueprint = Blueprint {
            id: blueprint_id,
            sleds: make_sled_config_only_zones(blueprint_zones),
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: None,
            internal_dns_version: *Generation::new(),
            external_dns_version: *Generation::new(),
            target_release_minimum_generation: *Generation::new(),
            nexus_generation: *Generation::new(),
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: *Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: "test suite".to_string(),
            comment: "test blueprint".to_string(),
            source: BlueprintSource::Test,
        };

        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    blueprint: blueprint.clone(),
                    service_ip_pool_ranges,
                    ..Default::default()
                },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Request: Requested external IP address not available",
        );

        assert!(get_all_crucible_datasets(&datastore).await.is_empty());
        assert!(get_all_external_ips(&datastore).await.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_sled_subnet_allocations() {
        let logctx = dev::test_setup_log("rack_sled_subnet_allocations");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id = Uuid::new_v4();

        // Ensure we get an empty list when there are no allocations
        let allocations =
            datastore.rack_subnet_allocations(&opctx, rack_id).await.unwrap();
        assert!(allocations.is_empty());

        // Add 5 allocations
        for i in 0..5i16 {
            let allocation = SledUnderlaySubnetAllocation {
                rack_id,
                sled_id: SledUuid::new_v4().into(),
                subnet_octet: 33 + i,
                hw_baseboard_id: Uuid::new_v4(),
            };
            datastore
                .sled_subnet_allocation_insert(&opctx, &allocation)
                .await
                .unwrap();
        }

        // List all 5 allocations
        let allocations =
            datastore.rack_subnet_allocations(&opctx, rack_id).await.unwrap();

        assert_eq!(5, allocations.len());

        // Try to add another allocation for the same octet, but with a distinct
        // sled_id. Ensure we get an error due to a unique constraint.
        let mut should_fail_allocation = SledUnderlaySubnetAllocation {
            rack_id,
            sled_id: SledUuid::new_v4().into(),
            subnet_octet: 37,
            hw_baseboard_id: Uuid::new_v4(),
        };
        let _err = datastore
            .sled_subnet_allocation_insert(&opctx, &should_fail_allocation)
            .await
            .unwrap_err();

        // Adding an allocation for the same {rack_id, sled_id} pair fails
        // the second time, even with a distinct subnet_epoch
        let mut allocation = should_fail_allocation.clone();
        allocation.subnet_octet = 38;
        datastore
            .sled_subnet_allocation_insert(&opctx, &allocation)
            .await
            .unwrap();

        should_fail_allocation.subnet_octet = 39;
        should_fail_allocation.hw_baseboard_id = Uuid::new_v4();
        let _err = datastore
            .sled_subnet_allocation_insert(&opctx, &should_fail_allocation)
            .await
            .unwrap_err();

        // Allocations outside our expected range fail
        let mut should_fail_allocation = SledUnderlaySubnetAllocation {
            rack_id,
            sled_id: SledUuid::new_v4().into(),
            subnet_octet: 32,
            hw_baseboard_id: Uuid::new_v4(),
        };
        let _err = datastore
            .sled_subnet_allocation_insert(&opctx, &should_fail_allocation)
            .await
            .unwrap_err();
        should_fail_allocation.subnet_octet = 256;
        let _err = datastore
            .sled_subnet_allocation_insert(&opctx, &should_fail_allocation)
            .await
            .unwrap_err();

        // We should have 6 allocations
        let allocations =
            datastore.rack_subnet_allocations(&opctx, rack_id).await.unwrap();

        assert_eq!(6, allocations.len());
        assert_eq!(
            vec![33, 34, 35, 36, 37, 38],
            allocations.iter().map(|a| a.subnet_octet).collect::<Vec<_>>()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn allocate_sled_underlay_subnet_octets() {
        let logctx = dev::test_setup_log("rack_sled_subnet_allocations");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id = Uuid::new_v4();

        let mut hw_baseboard_ids = vec![];
        let mut allocated_octets = vec![];
        for _ in 0..5 {
            let hw_baseboard_id = Uuid::new_v4();
            hw_baseboard_ids.push(hw_baseboard_id);
            allocated_octets.push(
                match datastore
                    .allocate_sled_underlay_subnet_octets(
                        &opctx,
                        rack_id,
                        hw_baseboard_id,
                    )
                    .await
                    .unwrap()
                {
                    SledUnderlayAllocationResult::New(allocation) => {
                        allocation.subnet_octet
                    }
                    SledUnderlayAllocationResult::CommissionedSled(
                        allocation,
                    ) => {
                        panic!("unexpected allocation {allocation:?}");
                    }
                },
            );
        }

        let expected = vec![33, 34, 35, 36, 37];
        assert_eq!(expected, allocated_octets);

        // We should have 5 allocations in the DB, sorted appropriately
        let allocations =
            datastore.rack_subnet_allocations(&opctx, rack_id).await.unwrap();
        assert_eq!(5, allocations.len());
        assert_eq!(
            expected,
            allocations.iter().map(|a| a.subnet_octet).collect::<Vec<_>>()
        );

        // If we attempt to insert the same baseboards again, we should get the
        // same new allocations back.
        for (&hw_baseboard_id, prev_allocation) in
            hw_baseboard_ids.iter().zip(&allocations)
        {
            match datastore
                .allocate_sled_underlay_subnet_octets(
                    &opctx,
                    rack_id,
                    hw_baseboard_id,
                )
                .await
                .unwrap()
            {
                SledUnderlayAllocationResult::New(allocation) => {
                    assert_eq!(allocation, *prev_allocation);
                }
                SledUnderlayAllocationResult::CommissionedSled(allocation) => {
                    panic!("unexpected allocation {allocation:?}");
                }
            }
        }

        // Pick one of the hw_baseboard_ids and insert a sled record. We should
        // get back the `CommissionedSled` allocation result if we retry
        // allocation of that baseboard.
        create_test_sled(&datastore, allocations[0].sled_id.into()).await;
        match datastore
            .allocate_sled_underlay_subnet_octets(
                &opctx,
                rack_id,
                hw_baseboard_ids[0],
            )
            .await
            .unwrap()
        {
            SledUnderlayAllocationResult::New(allocation) => {
                panic!("unexpected allocation {allocation:?}");
            }
            SledUnderlayAllocationResult::CommissionedSled(allocation) => {
                assert_eq!(allocation, allocations[0]);
            }
        }

        // If we attempt to insert the same baseboard again and that baseboard
        // is only assigned to decommissioned sleds, we should get a new
        // allocation. We'll pick one hw baseboard ID, create a `Sled` for it,
        // decommission that sled, and confirm we get a new octet, five times in
        // a loop (to emulate the same sled being added and decommissioned
        // multiple times).
        let mut next_expected_octet = *expected.last().unwrap() + 1;
        let mut prior_allocation = allocations.last().unwrap().clone();
        let target_hw_baseboard_id = *hw_baseboard_ids.last().unwrap();
        for _ in 0..5 {
            // Commission the sled.
            let sled =
                create_test_sled(&datastore, prior_allocation.sled_id.into())
                    .await;

            // If we attempt this same baseboard again, we get the existing
            // allocation back.
            match datastore
                .allocate_sled_underlay_subnet_octets(
                    &opctx,
                    rack_id,
                    target_hw_baseboard_id,
                )
                .await
                .unwrap()
            {
                SledUnderlayAllocationResult::New(allocation) => {
                    panic!("unexpected allocation {allocation:?}");
                }
                SledUnderlayAllocationResult::CommissionedSled(existing) => {
                    assert_eq!(existing, prior_allocation);
                }
            }

            // Decommission the sled.
            let (authz_sled,) = LookupPath::new(&opctx, datastore)
                .sled_id(sled.id())
                .lookup_for(authz::Action::Modify)
                .await
                .expect("found target sled ID");
            datastore
                .sled_set_policy_to_expunged(&opctx, &authz_sled)
                .await
                .expect("expunged sled");
            datastore
                .sled_set_state_to_decommissioned(&opctx, &authz_sled)
                .await
                .expect("decommissioned sled");

            // Attempt a new allocation for the same hw_baseboard_id.
            let allocation = match datastore
                .allocate_sled_underlay_subnet_octets(
                    &opctx,
                    rack_id,
                    target_hw_baseboard_id,
                )
                .await
                .unwrap()
            {
                SledUnderlayAllocationResult::New(allocation) => allocation,
                SledUnderlayAllocationResult::CommissionedSled(allocation) => {
                    panic!("unexpected existing allocation {allocation:?}");
                }
            };

            // We should get the next octet with a new sled ID.
            assert_eq!(allocation.subnet_octet, next_expected_octet);
            assert_ne!(allocation.sled_id, sled.id().into());
            prior_allocation = allocation;

            // Ensure if we attempt this same baseboard again, we get the
            // same allocation back (the sled hasn't been commissioned yet).
            match datastore
                .allocate_sled_underlay_subnet_octets(
                    &opctx,
                    rack_id,
                    target_hw_baseboard_id,
                )
                .await
                .unwrap()
            {
                SledUnderlayAllocationResult::New(allocation) => {
                    assert_eq!(prior_allocation, allocation);
                }
                SledUnderlayAllocationResult::CommissionedSled(existing) => {
                    panic!("unexpected allocation {existing:?}");
                }
            }

            // Bump our expectations for the next iteration.
            next_expected_octet += 1;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
