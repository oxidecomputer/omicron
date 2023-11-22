// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Rack`]s.

use super::dns::DnsVersionUpdateBuilder;
use super::DataStore;
use super::SERVICE_IP_POOL_NAME;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::fixed_data::silo::INTERNAL_SILO_ID;
use crate::db::fixed_data::vpc_subnet::DNS_VPC_SUBNET;
use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
use crate::db::fixed_data::vpc_subnet::NTP_VPC_SUBNET;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::Rack;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use diesel::upsert::excluded;
use ipnetwork::IpNetwork;
use nexus_db_model::DnsGroup;
use nexus_db_model::DnsZone;
use nexus_db_model::ExternalIp;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_model::InitialDnsGroup;
use nexus_db_model::PasswordHashString;
use nexus_db_model::SiloUser;
use nexus_db_model::SiloUserPasswordHash;
use nexus_types::external_api::params as external_params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::IdentityType;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params as internal_params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use std::net::IpAddr;
use uuid::Uuid;

/// Groups arguments related to rack initialization
#[derive(Clone)]
pub struct RackInit {
    pub rack_id: Uuid,
    pub rack_subnet: IpNetwork,
    pub services: Vec<internal_params::ServicePutRequest>,
    pub datasets: Vec<Dataset>,
    pub service_ip_pool_ranges: Vec<IpRange>,
    pub internal_dns: InitialDnsGroup,
    pub external_dns: InitialDnsGroup,
    pub recovery_silo: external_params::SiloCreate,
    pub recovery_silo_fq_dns_name: String,
    pub recovery_user_id: external_params::UserId,
    pub recovery_user_password_hash: omicron_passwords::PasswordHashString,
    pub dns_update: DnsVersionUpdateBuilder,
}

/// Possible errors while trying to initialize rack
#[derive(Debug)]
enum RackInitError {
    AddingIp(Error),
    AddingNic(Error),
    ServiceInsert(Error),
    DatasetInsert { err: AsyncInsertError, zpool_id: Uuid },
    RackUpdate { err: DieselError, rack_id: Uuid },
    DnsSerialization(Error),
    Silo(Error),
    RoleAssignment(Error),
}
type TxnError = TransactionError<RackInitError>;

impl From<TxnError> for Error {
    fn from(e: TxnError) -> Self {
        match e {
            TxnError::CustomError(RackInitError::AddingIp(err)) => err,
            TxnError::CustomError(RackInitError::AddingNic(err)) => err,
            TxnError::CustomError(RackInitError::DatasetInsert {
                err,
                zpool_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Zpool,
                    lookup_type: LookupType::ById(zpool_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            },
            TxnError::CustomError(RackInitError::ServiceInsert(err)) => {
                Error::internal_error(&format!(
                    "failed to insert Service record: {:#}",
                    err
                ))
            }
            TxnError::CustomError(RackInitError::RackUpdate {
                err,
                rack_id,
            }) => public_error_from_diesel(
                err,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::Rack,
                    LookupType::ById(rack_id),
                ),
            ),
            TxnError::CustomError(RackInitError::DnsSerialization(err)) => {
                Error::internal_error(&format!(
                    "failed to serialize initial DNS records: {:#}",
                    err
                ))
            }
            TxnError::CustomError(RackInitError::Silo(err)) => {
                Error::internal_error(&format!(
                    "failed to create recovery Silo: {:#}",
                    err
                ))
            }
            TxnError::CustomError(RackInitError::RoleAssignment(err)) => {
                Error::internal_error(&format!(
                    "failed to assign role to initial user: {:#}",
                    err
                ))
            }
            TxnError::Database(e) => {
                Error::internal_error(&format!("Transaction error: {}", e))
            }
        }
    }
}

impl DataStore {
    pub async fn rack_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Rack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::rack::dsl;
        paginated(dsl::rack, dsl::id, pagparams)
            .select(Rack::as_select())
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
        use db::schema::rack::dsl;

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
        use db::schema::rack::dsl;
        diesel::update(dsl::rack)
            .filter(dsl::id.eq(rack.id()))
            .set(dsl::rack_subnet.eq(rack.rack_subnet))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    // The following methods which return a `TxnError` take a `conn` parameter
    // which comes from the transaction created in `rack_set_initialized`.

    #[allow(clippy::too_many_arguments)]
    async fn rack_create_recovery_silo(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        log: &slog::Logger,
        recovery_silo: external_params::SiloCreate,
        recovery_silo_fq_dns_name: String,
        recovery_user_id: external_params::UserId,
        recovery_user_password_hash: omicron_passwords::PasswordHashString,
        dns_update: DnsVersionUpdateBuilder,
    ) -> Result<(), TxnError> {
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
            .map_err(RackInitError::Silo)
            .map_err(TxnError::CustomError)?;
        info!(log, "Created recovery silo");

        // Create the first user in the initial Recovery Silo
        let silo_user_id = Uuid::new_v4();
        let silo_user = SiloUser::new(
            db_silo.id(),
            silo_user_id,
            recovery_user_id.as_ref().to_owned(),
        );
        {
            use db::schema::silo_user::dsl;
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
            use db::schema::silo_user_password_hash::dsl;
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
            &[shared::RoleAssignment {
                identity_type: IdentityType::SiloUser,
                identity_id: silo_user_id,
                role_name: SiloRole::Admin,
            }],
        )
        .await
        .map_err(RackInitError::RoleAssignment)
        .map_err(TxnError::CustomError)?;
        debug!(log, "Generated role assignment queries");

        q1.execute_async(conn).await?;
        q2.execute_async(conn).await?;
        info!(log, "Granted Silo privileges");

        Ok(())
    }

    async fn rack_populate_service_records(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        log: &slog::Logger,
        service_pool: &db::model::IpPool,
        service: internal_params::ServicePutRequest,
    ) -> Result<(), TxnError> {
        use internal_params::ServiceKind;

        let service_db = db::model::Service::new(
            service.service_id,
            service.sled_id,
            service.zone_id,
            service.address,
            service.kind.clone().into(),
        );
        self.service_upsert_conn(conn, service_db).await.map_err(|e| {
            TxnError::CustomError(RackInitError::ServiceInsert(e))
        })?;

        // For services with external connectivity, we record their
        // explicit IP allocation and create a service NIC as well.
        let service_ip_nic = match service.kind {
            ServiceKind::ExternalDns { external_address, ref nic }
            | ServiceKind::Nexus { external_address, ref nic } => {
                let db_ip = IncompleteExternalIp::for_service_explicit(
                    Uuid::new_v4(),
                    &db::model::Name(nic.name.clone()),
                    &format!("{}", service.kind),
                    service.service_id,
                    service_pool.id(),
                    external_address,
                );
                let vpc_subnet = match service.kind {
                    ServiceKind::ExternalDns { .. } => DNS_VPC_SUBNET.clone(),
                    ServiceKind::Nexus { .. } => NEXUS_VPC_SUBNET.clone(),
                    _ => unreachable!(),
                };
                let db_nic = IncompleteNetworkInterface::new_service(
                    nic.id,
                    service.service_id,
                    vpc_subnet,
                    IdentityMetadataCreateParams {
                        name: nic.name.clone(),
                        description: format!("{} service vNIC", service.kind),
                    },
                    Some(nic.ip),
                    Some(nic.mac),
                )
                .map_err(|e| {
                    TxnError::CustomError(RackInitError::AddingNic(e))
                })?;
                Some((db_ip, db_nic))
            }
            ServiceKind::BoundaryNtp { snat, ref nic } => {
                let db_ip = IncompleteExternalIp::for_service_explicit_snat(
                    Uuid::new_v4(),
                    service.service_id,
                    service_pool.id(),
                    snat.ip,
                    (snat.first_port, snat.last_port),
                );
                let db_nic = IncompleteNetworkInterface::new_service(
                    nic.id,
                    service.service_id,
                    NTP_VPC_SUBNET.clone(),
                    IdentityMetadataCreateParams {
                        name: nic.name.clone(),
                        description: format!("{} service vNIC", service.kind),
                    },
                    Some(nic.ip),
                    Some(nic.mac),
                )
                .map_err(|e| {
                    TxnError::CustomError(RackInitError::AddingNic(e))
                })?;
                Some((db_ip, db_nic))
            }
            _ => None,
        };
        if let Some((db_ip, db_nic)) = service_ip_nic {
            Self::allocate_external_ip_on_connection(conn, db_ip)
                .await
                .map_err(|err| {
                    warn!(
                        log,
                        "Initializing Rack: Failed to allocate \
                        IP address for {}",
                        service.kind,
                    );
                    TxnError::CustomError(RackInitError::AddingIp(err))
                })?;

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
                        _ => Err(TxnError::CustomError(
                            RackInitError::AddingNic(e.into_external()),
                        )),
                    }
                })?;
        }

        info!(log, "Inserted records for {} service", service.kind);
        Ok(())
    }

    /// Update a rack to mark that it has been initialized
    pub async fn rack_set_initialized(
        &self,
        opctx: &OpContext,
        rack_init: RackInit,
    ) -> UpdateResult<Rack> {
        use db::schema::rack::dsl as rack_dsl;

        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        let rack_id = rack_init.rack_id;
        let services = rack_init.services;
        let datasets = rack_init.datasets;
        let service_ip_pool_ranges = rack_init.service_ip_pool_ranges;
        let internal_dns = rack_init.internal_dns;
        let external_dns = rack_init.external_dns;

        let (authz_service_pool, service_pool) =
            self.ip_pools_service_lookup(&opctx).await?;

        // NOTE: This operation could likely be optimized with a CTE, but given
        // the low-frequency of calls, this optimization has been deferred.
        let log = opctx.log.clone();
        let rack = self
            .pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                // Early exit if the rack has already been initialized.
                let rack = rack_dsl::rack
                    .filter(rack_dsl::id.eq(rack_id))
                    .select(Rack::as_select())
                    .get_result_async(&conn)
                    .await
                    .map_err(|e| {
                        warn!(log, "Initializing Rack: Rack UUID not found");
                        TxnError::CustomError(RackInitError::RackUpdate {
                            err: e,
                            rack_id,
                        })
                    })?;
                if rack.initialized {
                    info!(log, "Early exit: Rack already initialized");
                    return Ok(rack);
                }

                // Otherwise, insert services and datasets.

                // Set up the IP pool for internal services.
                for range in service_ip_pool_ranges {
                    Self::ip_pool_add_range_on_connection(
                        &conn,
                        opctx,
                        &authz_service_pool,
                        &range,
                    )
                    .await
                    .map_err(|err| {
                        warn!(
                            log,
                            "Initializing Rack: Failed to add IP pool range"
                        );
                        TxnError::CustomError(RackInitError::AddingIp(err))
                    })?;
                }

                // Allocate records for all services.
                for service in services {
                    self.rack_populate_service_records(
                        &conn,
                        &log,
                        &service_pool,
                        service,
                    )
                    .await?;
                }
                info!(log, "Inserted services");

                for dataset in datasets {
                    use db::schema::dataset::dsl;
                    let zpool_id = dataset.pool_id;
                    <Zpool as DatastoreCollection<Dataset>>::insert_resource(
                        zpool_id,
                        diesel::insert_into(dsl::dataset)
                            .values(dataset.clone())
                            .on_conflict(dsl::id)
                            .do_update()
                            .set((
                                dsl::time_modified.eq(Utc::now()),
                                dsl::pool_id.eq(excluded(dsl::pool_id)),
                                dsl::ip.eq(excluded(dsl::ip)),
                                dsl::port.eq(excluded(dsl::port)),
                                dsl::kind.eq(excluded(dsl::kind)),
                            )),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|err| {
                        TxnError::CustomError(RackInitError::DatasetInsert {
                            err,
                            zpool_id,
                        })
                    })?;
                }
                info!(log, "Inserted datasets");

                // Insert the initial contents of the internal and external DNS
                // zones.
                Self::load_dns_data(&conn, internal_dns)
                    .await
                    .map_err(RackInitError::DnsSerialization)
                    .map_err(TxnError::CustomError)?;
                info!(log, "Populated DNS tables for internal DNS");

                Self::load_dns_data(&conn, external_dns)
                    .await
                    .map_err(RackInitError::DnsSerialization)
                    .map_err(TxnError::CustomError)?;
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
                .await?;

                let rack = diesel::update(rack_dsl::rack)
                    .filter(rack_dsl::id.eq(rack_id))
                    .set((
                        rack_dsl::initialized.eq(true),
                        rack_dsl::time_modified.eq(Utc::now()),
                    ))
                    .returning(Rack::as_returning())
                    .get_result_async::<Rack>(&conn)
                    .await
                    .map_err(|err| {
                        TxnError::CustomError(RackInitError::RackUpdate {
                            err,
                            rack_id,
                        })
                    })?;
                Ok::<_, TxnError>(rack)
            })
            .await?;
        Ok(rack)
    }

    pub async fn load_builtin_rack_data(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        use omicron_common::api::external::Name;

        self.rack_insert(opctx, &db::model::Rack::new(rack_id)).await?;

        let internal_pool = db::model::IpPool::new(
            &IdentityMetadataCreateParams {
                name: SERVICE_IP_POOL_NAME.parse::<Name>().unwrap(),
                description: String::from("IP Pool for Oxide Services"),
            },
            Some(*INTERNAL_SILO_ID),
            true, // default for internal silo
        );

        self.ip_pool_create(opctx, internal_pool).await.map(|_| ()).or_else(
            |e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            },
        )?;

        let default_pool = db::model::IpPool::new(
            &IdentityMetadataCreateParams {
                name: "default".parse::<Name>().unwrap(),
                description: String::from("default IP pool"),
            },
            None, // no silo ID, fleet scoped
            true, // default for fleet
        );
        self.ip_pool_create(opctx, default_pool).await.map(|_| ()).or_else(
            |e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            },
        )?;

        Ok(())
    }

    pub async fn nexus_external_addresses(
        &self,
        opctx: &OpContext,
    ) -> Result<(Vec<IpAddr>, Vec<DnsZone>), Error> {
        opctx.authorize(authz::Action::Read, &authz::DNS_CONFIG).await?;

        use crate::db::schema::external_ip::dsl as extip_dsl;
        use crate::db::schema::service::dsl as service_dsl;
        type TxnError = TransactionError<Error>;
        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let ips = extip_dsl::external_ip
                    .inner_join(
                        service_dsl::service.on(service_dsl::id
                            .eq(extip_dsl::parent_id.assume_not_null())),
                    )
                    .filter(extip_dsl::parent_id.is_not_null())
                    .filter(extip_dsl::time_deleted.is_null())
                    .filter(extip_dsl::is_service)
                    .filter(service_dsl::kind.eq(db::model::ServiceKind::Nexus))
                    .select(ExternalIp::as_select())
                    .get_results_async(&conn)
                    .await?
                    .into_iter()
                    .map(|external_ip| external_ip.ip.ip())
                    .collect();

                let dns_zones = self
                    .dns_zones_list_all_on_connection(
                        opctx,
                        &conn,
                        DnsGroup::External,
                    )
                    .await?;

                Ok((ips, dns_zones))
            })
            .await
            .map_err(|error: TxnError| match error {
                TransactionError::CustomError(err) => err,
                TransactionError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::datastore_test;
    use crate::db::datastore::test::{
        sled_baseboard_for_test, sled_system_hardware_for_test,
    };
    use crate::db::datastore::Discoverability;
    use crate::db::lookup::LookupPath;
    use crate::db::model::ExternalIp;
    use crate::db::model::IpKind;
    use crate::db::model::IpPoolRange;
    use crate::db::model::Service;
    use crate::db::model::ServiceKind;
    use crate::db::model::Sled;
    use async_bb8_diesel::AsyncSimpleConnection;
    use internal_params::DnsRecord;
    use nexus_db_model::{DnsGroup, InitialDnsGroup, SledUpdate};
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::shared::SiloIdentityMode;
    use nexus_types::identity::Asset;
    use nexus_types::internal_api::params::ServiceNic;
    use omicron_common::address::{
        DNS_OPTE_IPV4_SUBNET, NEXUS_OPTE_IPV4_SUBNET, NTP_OPTE_IPV4_SUBNET,
    };
    use omicron_common::api::external::http_pagination::PaginatedBy;
    use omicron_common::api::external::{
        IdentityMetadataCreateParams, MacAddr,
    };
    use omicron_common::api::internal::shared::SourceNatConfig;
    use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
    use std::num::NonZeroU32;

    // Default impl is for tests only, and really just so that tests can more
    // easily specify just the parts that they want.
    impl Default for RackInit {
        fn default() -> Self {
            RackInit {
                rack_id: Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap(),
                rack_subnet: nexus_test_utils::RACK_SUBNET.parse().unwrap(),
                services: vec![],
                datasets: vec![],
                service_ip_pool_ranges: vec![],
                internal_dns: InitialDnsGroup::new(
                    DnsGroup::Internal,
                    internal_dns::DNS_ZONE,
                    "test suite",
                    "test suite",
                    HashMap::new(),
                ),
                external_dns: InitialDnsGroup::new(
                    DnsGroup::External,
                    internal_dns::DNS_ZONE,
                    "test suite",
                    "test suite",
                    HashMap::new(),
                ),
                recovery_silo: external_params::SiloCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "test-silo".parse().unwrap(),
                        description: String::new(),
                    },
                    discoverable: false,
                    identity_mode: SiloIdentityMode::LocalOnly,
                    admin_group_name: None,
                    tls_certificates: vec![],
                    mapped_fleet_roles: Default::default(),
                },
                recovery_silo_fq_dns_name: format!(
                    "test-silo.sys.{}",
                    internal_dns::DNS_ZONE
                ),
                recovery_user_id: "test-user".parse().unwrap(),
                // empty string password
                recovery_user_password_hash: "$argon2id$v=19$m=98304,t=13,\
                p=1$d2t2UHhOdWt3NkYyY1l3cA$pIvmXrcTk/\
                nsUzWvBQIeuMJk96ijye/oIXHCj15xg+M"
                    .parse()
                    .unwrap(),
                dns_update: DnsVersionUpdateBuilder::new(
                    DnsGroup::External,
                    "test suite".to_string(),
                    "test suite".to_string(),
                ),
            }
        }
    }

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn rack_set_initialized_empty() {
        let logctx = dev::test_setup_log("rack_set_initialized_empty");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
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
        assert_eq!(dns_internal.generation, 1);
        assert!(dns_internal.time_created >= before);
        assert!(dns_internal.time_created <= after);
        assert_eq!(dns_internal.zones.len(), 0);

        let dns_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        // The external DNS zone has an extra update due to the initial Silo
        // creation.
        assert_eq!(dns_internal.generation + 1, dns_external.generation);
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
        let (authz_silo, db_silo) = LookupPath::new(&opctx, &datastore)
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
        assert_eq!(
            silo_users[0].external_id,
            rack_init.recovery_user_id.as_ref()
        );
        let authz_silo_user = authz::SiloUser::new(
            authz_silo,
            silo_users[0].id(),
            LookupType::ById(silo_users[0].id()),
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

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn create_test_sled(db: &DataStore) -> Sled {
        let sled_id = Uuid::new_v4();
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
        let sled_update = SledUpdate::new(
            sled_id,
            addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id(),
        );
        db.sled_upsert(sled_update)
            .await
            .expect("Could not upsert sled during test prep")
    }

    // Hacky macro helper to:
    // - Perform a transaction...
    // - ... That queries a particular table for all values...
    // - ... and Selects them as the requested model type.
    macro_rules! fn_to_get_all {
        ($table:ident, $model:ident) => {
            paste::paste! {
                async fn [<get_all_ $table s>](db: &DataStore) -> Vec<$model> {
                    use crate::db::schema::$table::dsl;
                    use nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL;
                    db.pool_connection_for_tests()
                        .await
                        .unwrap()
                        .transaction_async(|conn| async move {
                            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                                .await
                                .unwrap();
                            Ok::<_, crate::db::TransactionError<()>>(
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

    fn_to_get_all!(service, Service);
    fn_to_get_all!(external_ip, ExternalIp);
    fn_to_get_all!(ip_pool_range, IpPoolRange);
    fn_to_get_all!(dataset, Dataset);

    #[tokio::test]
    async fn rack_set_initialized_with_services() {
        let logctx = dev::test_setup_log("rack_set_initialized_with_services");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled1 = create_test_sled(&datastore).await;
        let sled2 = create_test_sled(&datastore).await;
        let sled3 = create_test_sled(&datastore).await;

        let service_ip_pool_ranges = vec![IpRange::try_from((
            Ipv4Addr::new(1, 2, 3, 4),
            Ipv4Addr::new(1, 2, 3, 6),
        ))
        .unwrap()];

        let external_dns_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let external_dns_pip = DNS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let external_dns_id = Uuid::new_v4();
        let nexus_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 6));
        let nexus_pip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let nexus_id = Uuid::new_v4();
        let ntp1_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 5));
        let ntp1_pip = NTP_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let ntp1_id = Uuid::new_v4();
        let ntp2_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 5));
        let ntp2_pip = NTP_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 2)
            .unwrap();
        let ntp2_id = Uuid::new_v4();
        let ntp3_id = Uuid::new_v4();
        let mut macs = MacAddr::iter_system();
        let services = vec![
            internal_params::ServicePutRequest {
                service_id: external_dns_id,
                sled_id: sled1.id(),
                zone_id: Some(external_dns_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::ExternalDns {
                    external_address: external_dns_ip,
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "external-dns".parse().unwrap(),
                        ip: external_dns_pip.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
            internal_params::ServicePutRequest {
                service_id: ntp1_id,
                sled_id: sled1.id(),
                zone_id: Some(ntp1_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9090, 0, 0),
                kind: internal_params::ServiceKind::BoundaryNtp {
                    snat: SourceNatConfig {
                        ip: ntp1_ip,
                        first_port: 16384,
                        last_port: 32767,
                    },
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "ntp1".parse().unwrap(),
                        ip: ntp1_pip.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
            internal_params::ServicePutRequest {
                service_id: nexus_id,
                sled_id: sled2.id(),
                zone_id: Some(nexus_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 456, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: nexus_ip,
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "nexus".parse().unwrap(),
                        ip: nexus_pip.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
            internal_params::ServicePutRequest {
                service_id: ntp2_id,
                sled_id: sled2.id(),
                zone_id: Some(ntp2_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9090, 0, 0),
                kind: internal_params::ServiceKind::BoundaryNtp {
                    snat: SourceNatConfig {
                        ip: ntp2_ip,
                        first_port: 0,
                        last_port: 16383,
                    },
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "ntp2".parse().unwrap(),
                        ip: ntp2_pip.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
            internal_params::ServicePutRequest {
                service_id: ntp3_id,
                sled_id: sled3.id(),
                zone_id: Some(ntp3_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9090, 0, 0),
                kind: internal_params::ServiceKind::InternalNtp,
            },
        ];

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    services: services.clone(),
                    service_ip_pool_ranges,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to initialize rack");

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        let observed_services = get_all_services(&datastore).await;
        let observed_datasets = get_all_datasets(&datastore).await;

        // We should see all the services we initialized
        assert_eq!(observed_services.len(), 5);
        let dns_service = observed_services
            .iter()
            .find(|s| s.id() == external_dns_id)
            .unwrap();
        let nexus_service =
            observed_services.iter().find(|s| s.id() == nexus_id).unwrap();
        let ntp1_service =
            observed_services.iter().find(|s| s.id() == ntp1_id).unwrap();
        let ntp2_service =
            observed_services.iter().find(|s| s.id() == ntp2_id).unwrap();
        let ntp3_service =
            observed_services.iter().find(|s| s.id() == ntp3_id).unwrap();

        assert_eq!(dns_service.sled_id, sled1.id());
        assert_eq!(dns_service.kind, ServiceKind::ExternalDns);
        assert_eq!(*dns_service.ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*dns_service.port, 123);

        assert_eq!(nexus_service.sled_id, sled2.id());
        assert_eq!(nexus_service.kind, ServiceKind::Nexus);
        assert_eq!(*nexus_service.ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*nexus_service.port, 456);

        assert_eq!(ntp1_service.sled_id, sled1.id());
        assert_eq!(ntp1_service.kind, ServiceKind::Ntp);
        assert_eq!(*ntp1_service.ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*ntp1_service.port, 9090);

        assert_eq!(ntp2_service.sled_id, sled2.id());
        assert_eq!(ntp2_service.kind, ServiceKind::Ntp);
        assert_eq!(*ntp2_service.ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*ntp2_service.port, 9090);

        assert_eq!(ntp3_service.sled_id, sled3.id());
        assert_eq!(ntp3_service.kind, ServiceKind::Ntp);
        assert_eq!(*ntp3_service.ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*ntp3_service.port, 9090);

        // We should also see the single external IP allocated for each service
        // save for the non-boundary NTP service.
        let observed_external_ips = get_all_external_ips(&datastore).await;
        assert_eq!(observed_external_ips.len(), 4);
        let dns_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(external_dns_id))
            .unwrap();
        let nexus_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(nexus_id))
            .unwrap();
        let ntp1_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(ntp1_id))
            .unwrap();
        let ntp2_external_ip = observed_external_ips
            .iter()
            .find(|e| e.parent_id == Some(ntp2_id))
            .unwrap();
        assert!(!observed_external_ips
            .iter()
            .any(|e| e.parent_id == Some(ntp3_id)));

        assert_eq!(dns_external_ip.parent_id, Some(dns_service.id()));
        assert!(dns_external_ip.is_service);
        assert_eq!(dns_external_ip.kind, IpKind::Floating);

        assert_eq!(nexus_external_ip.parent_id, Some(nexus_service.id()));
        assert!(nexus_external_ip.is_service);
        assert_eq!(nexus_external_ip.kind, IpKind::Floating);

        assert_eq!(ntp1_external_ip.parent_id, Some(ntp1_service.id()));
        assert!(ntp1_external_ip.is_service);
        assert_eq!(ntp1_external_ip.kind, IpKind::SNat);
        assert_eq!(ntp1_external_ip.first_port.0, 16384);
        assert_eq!(ntp1_external_ip.last_port.0, 32767);

        assert_eq!(ntp2_external_ip.parent_id, Some(ntp2_service.id()));
        assert!(ntp2_external_ip.is_service);
        assert_eq!(ntp2_external_ip.kind, IpKind::SNat);
        assert_eq!(ntp2_external_ip.first_port.0, 0);
        assert_eq!(ntp2_external_ip.last_port.0, 16383);

        // Furthermore, we should be able to see that these IP addresses have
        // been allocated as a part of the service IP pool.
        let (.., svc_pool) =
            datastore.ip_pools_service_lookup(&opctx).await.unwrap();
        assert_eq!(svc_pool.silo_id, Some(*INTERNAL_SILO_ID));

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

        assert!(observed_datasets.is_empty());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_with_many_nexus_services() {
        let logctx = dev::test_setup_log(
            "rack_set_initialized_with_many_nexus_services",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        // Ask for two Nexus services, with different external IPs.
        let nexus_ip_start = Ipv4Addr::new(1, 2, 3, 4);
        let nexus_ip_end = Ipv4Addr::new(1, 2, 3, 5);
        let nexus_id1 = Uuid::new_v4();
        let nexus_id2 = Uuid::new_v4();
        let nexus_pip1 = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let nexus_pip2 = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 2)
            .unwrap();
        let mut macs = MacAddr::iter_system();
        let mut services = vec![
            internal_params::ServicePutRequest {
                service_id: nexus_id1,
                sled_id: sled.id(),
                zone_id: Some(nexus_id1),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: IpAddr::V4(nexus_ip_start),
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "nexus1".parse().unwrap(),
                        ip: nexus_pip1.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
            internal_params::ServicePutRequest {
                service_id: nexus_id2,
                sled_id: sled.id(),
                zone_id: Some(nexus_id2),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 456, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: IpAddr::V4(nexus_ip_end),
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "nexus2".parse().unwrap(),
                        ip: nexus_pip2.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
        ];
        services
            .sort_by(|a, b| a.service_id.partial_cmp(&b.service_id).unwrap());

        let datasets = vec![];
        let service_ip_pool_ranges =
            vec![IpRange::try_from((nexus_ip_start, nexus_ip_end))
                .expect("Cannot create IP Range")];

        let internal_records = vec![
            DnsRecord::Aaaa("fe80::1:2:3:4".parse().unwrap()),
            DnsRecord::Aaaa("fe80::1:2:3:5".parse().unwrap()),
        ];
        let internal_dns = InitialDnsGroup::new(
            DnsGroup::Internal,
            internal_dns::DNS_ZONE,
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

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    services: services.clone(),
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

        let mut observed_services = get_all_services(&datastore).await;
        let observed_datasets = get_all_datasets(&datastore).await;

        // We should see both of the Nexus services we provisioned.
        assert_eq!(observed_services.len(), 2);
        observed_services.sort_by(|a, b| a.id().partial_cmp(&b.id()).unwrap());

        assert_eq!(observed_services[0].sled_id, sled.id());
        assert_eq!(observed_services[1].sled_id, sled.id());
        assert_eq!(observed_services[0].kind, ServiceKind::Nexus);
        assert_eq!(observed_services[1].kind, ServiceKind::Nexus);
        assert_eq!(*observed_services[0].ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*observed_services[1].ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*observed_services[0].port, services[0].address.port());
        assert_eq!(*observed_services[1].port, services[1].address.port());

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
            observed_external_ips[&observed_services[0].id()].ip.ip(),
            if let internal_params::ServiceKind::Nexus {
                external_address,
                ..
            } = services[0].kind
            {
                external_address
            } else {
                panic!("Unexpected service kind")
            }
        );
        assert_eq!(
            observed_external_ips[&observed_services[1].id()].ip.ip(),
            if let internal_params::ServiceKind::Nexus {
                external_address,
                ..
            } = services[1].kind
            {
                external_address
            } else {
                panic!("Unexpected service kind")
            }
        );

        // Furthermore, we should be able to see that this IP addresses have been
        // allocated as a part of the service IP pool.
        let (.., svc_pool) =
            datastore.ip_pools_service_lookup(&opctx).await.unwrap();
        assert_eq!(svc_pool.silo_id, Some(*INTERNAL_SILO_ID));

        let observed_ip_pool_ranges = get_all_ip_pool_ranges(&datastore).await;
        assert_eq!(observed_ip_pool_ranges.len(), 1);
        assert_eq!(observed_ip_pool_ranges[0].ip_pool_id, svc_pool.id());

        assert!(observed_datasets.is_empty());

        // Verify the internal and external DNS configurations.
        let dns_config_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        assert_eq!(dns_config_internal.generation, 1);
        assert_eq!(dns_config_internal.zones.len(), 1);
        assert_eq!(
            dns_config_internal.zones[0].zone_name,
            internal_dns::DNS_ZONE
        );
        assert_eq!(
            dns_config_internal.zones[0].records,
            HashMap::from([("nexus".to_string(), internal_records)]),
        );

        let dns_config_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(dns_config_external.generation, 2);
        assert_eq!(dns_config_external.zones.len(), 1);
        assert_eq!(
            dns_config_external.zones[0].zone_name,
            "test-suite.oxide.test",
        );
        assert_eq!(
            dns_config_external.zones[0].records.get("api.sys"),
            Some(&external_records)
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_missing_service_pool_ip_throws_error() {
        let logctx = dev::test_setup_log(
            "rack_set_initialized_missing_service_pool_ip_throws_error",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        let nexus_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let nexus_pip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let nexus_id = Uuid::new_v4();
        let mut macs = MacAddr::iter_system();
        let services = vec![internal_params::ServicePutRequest {
            service_id: nexus_id,
            sled_id: sled.id(),
            zone_id: Some(nexus_id),
            address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
            kind: internal_params::ServiceKind::Nexus {
                external_address: nexus_ip,
                nic: ServiceNic {
                    id: Uuid::new_v4(),
                    name: "nexus".parse().unwrap(),
                    ip: nexus_pip.into(),
                    mac: macs.next().unwrap(),
                },
            },
        }];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit { services: services.clone(), ..Default::default() },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Request: Requested external IP address not available"
        );

        assert!(get_all_services(&datastore).await.is_empty());
        assert!(get_all_datasets(&datastore).await.is_empty());
        assert!(get_all_external_ips(&datastore).await.is_empty());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_overlapping_ips_throws_error() {
        let logctx = dev::test_setup_log(
            "rack_set_initialized_overlapping_ips_throws_error",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        // Request two services which happen to be using the same IP address.
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let external_dns_id = Uuid::new_v4();
        let external_dns_pip = DNS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let nexus_id = Uuid::new_v4();
        let nexus_pip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
            .unwrap();
        let mut macs = MacAddr::iter_system();

        let services = vec![
            internal_params::ServicePutRequest {
                service_id: external_dns_id,
                sled_id: sled.id(),
                zone_id: Some(external_dns_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::ExternalDns {
                    external_address: ip,
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "external-dns".parse().unwrap(),
                        ip: external_dns_pip.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
            internal_params::ServicePutRequest {
                service_id: nexus_id,
                sled_id: sled.id(),
                zone_id: Some(nexus_id),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: ip,
                    nic: ServiceNic {
                        id: Uuid::new_v4(),
                        name: "nexus".parse().unwrap(),
                        ip: nexus_pip.into(),
                        mac: macs.next().unwrap(),
                    },
                },
            },
        ];
        let service_ip_pool_ranges = vec![IpRange::from(ip)];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    services: services.clone(),
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

        assert!(get_all_services(&datastore).await.is_empty());
        assert!(get_all_datasets(&datastore).await.is_empty());
        assert!(get_all_external_ips(&datastore).await.is_empty());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
