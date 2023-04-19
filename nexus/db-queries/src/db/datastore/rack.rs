// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Rack`]s.

use super::DataStore;
use super::SERVICE_IP_POOL_NAME;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::model::Certificate;
use crate::db::model::Dataset;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::NexusService;
use crate::db::model::Rack;
use crate::db::model::Service;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use async_bb8_diesel::PoolError;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_model::ExternalIp;
use nexus_db_model::InitialDnsGroup;
use nexus_types::external_api::shared::IpRange;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params as internal_params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use std::net::IpAddr;
use uuid::Uuid;

/// Groups arguments related to rack initialization
pub struct RackInit {
    pub rack_id: Uuid,
    pub services: Vec<internal_params::ServicePutRequest>,
    pub datasets: Vec<Dataset>,
    pub service_ip_pool_ranges: Vec<IpRange>,
    pub certificates: Vec<Certificate>,
    pub internal_dns: InitialDnsGroup,
    pub external_dns: InitialDnsGroup,
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
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Rack,
                        &rack.id().to_string(),
                    ),
                )
            })
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
        let certificates = rack_init.certificates;
        let internal_dns = rack_init.internal_dns;
        let external_dns = rack_init.external_dns;

        #[derive(Debug)]
        enum RackInitError {
            AddingIp(Error),
            ServiceInsert { err: AsyncInsertError, sled_id: Uuid, svc_id: Uuid },
            DatasetInsert { err: AsyncInsertError, zpool_id: Uuid },
            RackUpdate(PoolError),
            DnsSerialization(Error),
        }
        type TxnError = TransactionError<RackInitError>;

        let (authz_service_pool, service_pool) =
            self.ip_pools_service_lookup(&opctx).await?;

        // NOTE: This operation could likely be optimized with a CTE, but given
        // the low-frequency of calls, this optimization has been deferred.
        let log = opctx.log.clone();
        self.pool_authorized(opctx)
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
                        TxnError::CustomError(RackInitError::RackUpdate(
                            PoolError::from(e),
                        ))
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
                    ).await.map_err(|err| {
                        warn!(log, "Initializing Rack: Failed to add IP pool range");
                        TxnError::CustomError(RackInitError::AddingIp(err))
                    })?;
                }

                // Allocate records for all services.
                for service in services {
                    let service_db = db::model::Service::new(
                        service.service_id,
                        service.sled_id,
                        service.address,
                        service.kind.into(),
                    );

                    use db::schema::service::dsl;
                    let sled_id = service.sled_id;
                    <Sled as DatastoreCollection<Service>>::insert_resource(
                        sled_id,
                        diesel::insert_into(dsl::service)
                            .values(service_db.clone())
                            .on_conflict(dsl::id)
                            .do_update()
                            .set((
                                dsl::time_modified.eq(Utc::now()),
                                dsl::sled_id.eq(excluded(dsl::sled_id)),
                                dsl::ip.eq(excluded(dsl::ip)),
                                dsl::kind.eq(excluded(dsl::kind)),
                            )),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|err| {
                        warn!(log, "Initializing Rack: Failed to insert service");
                        TxnError::CustomError(RackInitError::ServiceInsert {
                            err,
                            sled_id,
                            svc_id: service.service_id,
                        })
                    })?;

                    if let internal_params::ServiceKind::Nexus { external_address } = service.kind {
                        // Allocate the explicit IP address that is currently
                        // in-use by this Nexus service.
                        let ip_id = Uuid::new_v4();
                        let data = IncompleteExternalIp::for_service_explicit(
                            ip_id,
                            service_pool.id(),
                            external_address
                        );
                        let allocated_ip = Self::allocate_external_ip_on_connection(
                            &conn,
                            data
                        ).await.map_err(|err| {
                            warn!(log, "Initializing Rack: Failed to allocate IP address");
                            TxnError::CustomError(RackInitError::AddingIp(err))
                        })?;
                        assert_eq!(allocated_ip.ip.ip(), external_address);

                        // Add a service record for Nexus.
                        let nexus_service = NexusService::new(service.service_id, allocated_ip.id);
                        use db::schema::nexus_service::dsl;
                        diesel::insert_into(dsl::nexus_service)
                            .values(nexus_service)
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                warn!(log, "Initializing Rack: Failed to insert Nexus Service record");
                                e
                            })?;
                    }
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

                {
                    use db::schema::certificate::dsl;
                    diesel::insert_into(dsl::certificate)
                        .values(certificates)
                        .on_conflict(dsl::id)
                        .do_nothing()
                        .execute_async(&conn)
                        .await?;
                }
                info!(log, "Inserted certificates");

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
                        TxnError::CustomError(RackInitError::RackUpdate(
                            PoolError::from(e),
                        ))
                    })?;
                Ok(rack)
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(RackInitError::AddingIp(err)) => err,
                TxnError::CustomError(RackInitError::DatasetInsert {
                    err,
                    zpool_id,
                }) => match err {
                    AsyncInsertError::CollectionNotFound => {
                        Error::ObjectNotFound {
                            type_name: ResourceType::Zpool,
                            lookup_type: LookupType::ById(zpool_id),
                        }
                    }
                    AsyncInsertError::DatabaseError(e) => {
                        public_error_from_diesel_pool(e, ErrorHandler::Server)
                    }
                },
                TxnError::CustomError(RackInitError::ServiceInsert {
                    err,
                    sled_id,
                    svc_id,
                }) => match err {
                    AsyncInsertError::CollectionNotFound => {
                        Error::ObjectNotFound {
                            type_name: ResourceType::Sled,
                            lookup_type: LookupType::ById(sled_id),
                        }
                    }
                    AsyncInsertError::DatabaseError(e) => {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::Service,
                                &svc_id.to_string(),
                            ),
                        )
                    }
                },
                TxnError::CustomError(RackInitError::RackUpdate(err)) => {
                    public_error_from_diesel_pool(
                        err,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::Rack,
                            LookupType::ById(rack_id),
                        ),
                    )
                }
                TxnError::CustomError(RackInitError::DnsSerialization(err)) => {
                    Error::internal_error(&format!(
                        "failed to serialize initial DNS records: {:#}", err
                    ))
                },
                TxnError::Pool(e) => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }

    pub async fn load_builtin_rack_data(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_types::external_api::params;
        use omicron_common::api::external::IdentityMetadataCreateParams;
        use omicron_common::api::external::Name;

        self.rack_insert(opctx, &db::model::Rack::new(rack_id)).await?;

        let params = params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: SERVICE_IP_POOL_NAME.parse::<Name>().unwrap(),
                description: String::from("IP Pool for Oxide Services"),
            },
        };
        self.ip_pool_create(opctx, &params, /*internal=*/ true)
            .await
            .map(|_| ())
            .or_else(|e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            })?;

        let params = params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "default".parse::<Name>().unwrap(),
                description: String::from("default IP pool"),
            },
        };
        self.ip_pool_create(opctx, &params, /*internal=*/ false)
            .await
            .map(|_| ())
            .or_else(|e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            })?;

        Ok(())
    }

    pub async fn nexus_external_addresses(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<IpAddr>, Error> {
        opctx.authorize(authz::Action::Read, &authz::DNS_CONFIG).await?;

        use crate::db::schema::external_ip::dsl as extip_dsl;
        use crate::db::schema::nexus_service::dsl as nexus_dsl;
        type TxnError = TransactionError<()>;
        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                // This is the rare case where we want to allow table scans.  There
                // must not be enough Nexus instances for this to be a problem.
                // XXX-dap copied from ALLOW_FULL_TABLE_SCAN_SQL
                let sql = "set local disallow_full_table_scans = off; \
                    set local large_full_scan_rows = 1000;";
                conn.batch_execute_async(sql).await?;
                Ok(extip_dsl::external_ip
                    .filter(
                        extip_dsl::id.eq_any(
                            nexus_dsl::nexus_service
                                .select(nexus_dsl::external_ip_id),
                        ),
                    )
                    .select(ExternalIp::as_select())
                    .get_results_async(&conn)
                    .await?
                    .into_iter()
                    .map(|external_ip| external_ip.ip.ip())
                    .collect())
            })
            .await
            .map_err(|error: TxnError| match error {
                TransactionError::CustomError(()) => unimplemented!(),
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
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
    use crate::db::model::ExternalIp;
    use crate::db::model::IpKind;
    use crate::db::model::IpPoolRange;
    use crate::db::model::ServiceKind;
    use async_bb8_diesel::AsyncSimpleConnection;
    use internal_params::DnsRecord;
    use nexus_db_model::{DnsGroup, InitialDnsGroup};
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    fn internal_dns_empty() -> InitialDnsGroup {
        InitialDnsGroup::new(
            DnsGroup::Internal,
            internal_dns::DNS_ZONE,
            "test suite",
            "test suite",
            HashMap::new(),
        )
    }

    fn external_dns_empty() -> InitialDnsGroup {
        InitialDnsGroup::new(
            DnsGroup::External,
            "testing.oxide.example",
            "test suite",
            "test suite",
            HashMap::new(),
        )
    }

    #[tokio::test]
    async fn rack_set_initialized_empty() {
        let logctx = dev::test_setup_log("rack_set_initialized_empty");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let before = Utc::now();

        let services = vec![];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![];
        let certificates = vec![];

        // Initializing the rack with no data is odd, but allowed.
        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    services: services.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges: service_ip_pool_ranges.clone(),
                    certificates: certificates.clone(),
                    internal_dns: internal_dns_empty(),
                    external_dns: external_dns_empty(),
                },
            )
            .await
            .expect("Failed to initialize rack");

        let after = Utc::now();
        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

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
        assert_eq!(dns_internal.generation, dns_external.generation);
        assert_eq!(dns_internal.zones, dns_external.zones);

        // It should also be idempotent.
        let rack2 = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    services,
                    datasets,
                    service_ip_pool_ranges,
                    certificates,
                    internal_dns: internal_dns_empty(),
                    external_dns: external_dns_empty(),
                },
            )
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
        let sled = Sled::new(
            sled_id,
            addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id(),
        );
        db.sled_upsert(sled)
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
                    db.pool_for_tests()
                        .await
                        .unwrap()
                        .transaction_async(|conn| async move {
                            conn.batch_execute_async(nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL)
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
        }
    }

    fn_to_get_all!(service, Service);
    fn_to_get_all!(nexus_service, NexusService);
    fn_to_get_all!(external_ip, ExternalIp);
    fn_to_get_all!(ip_pool_range, IpPoolRange);
    fn_to_get_all!(dataset, Dataset);

    #[tokio::test]
    async fn rack_set_initialized_with_nexus_service() {
        let logctx =
            dev::test_setup_log("rack_set_initialized_with_nexus_service");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        let nexus_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let services = vec![internal_params::ServicePutRequest {
            service_id: Uuid::new_v4(),
            sled_id: sled.id(),
            address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
            kind: internal_params::ServiceKind::Nexus {
                external_address: nexus_ip,
            },
        }];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![IpRange::from(nexus_ip)];
        let certificates = vec![];

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    services: services.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges,
                    certificates: certificates.clone(),
                    internal_dns: internal_dns_empty(),
                    external_dns: external_dns_empty(),
                },
            )
            .await
            .expect("Failed to initialize rack");

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        let observed_services = get_all_services(&datastore).await;
        let observed_nexus_services = get_all_nexus_services(&datastore).await;
        let observed_datasets = get_all_datasets(&datastore).await;

        // We should only see the one nexus we inserted earlier
        assert_eq!(observed_services.len(), 1);
        assert_eq!(observed_services[0].sled_id, sled.id());
        assert_eq!(observed_services[0].kind, ServiceKind::Nexus);
        assert_eq!(*observed_services[0].ip, Ipv6Addr::LOCALHOST);
        assert_eq!(*observed_services[0].port, 123);

        // It should have a corresponding "Nexus service record"
        assert_eq!(observed_nexus_services.len(), 1);
        assert_eq!(observed_services[0].id(), observed_nexus_services[0].id);

        // We should also see the single external IP allocated for this nexus
        // interface.
        let observed_external_ips = get_all_external_ips(&datastore).await;
        assert_eq!(observed_external_ips.len(), 1);
        assert_eq!(
            observed_external_ips[0].id,
            observed_nexus_services[0].external_ip_id
        );
        assert_eq!(observed_external_ips[0].kind, IpKind::Service);

        // Furthermore, we should be able to see that this IP address has been
        // allocated as a part of the service IP pool.
        let (.., svc_pool) =
            datastore.ip_pools_service_lookup(&opctx).await.unwrap();
        assert!(svc_pool.internal);

        let observed_ip_pool_ranges = get_all_ip_pool_ranges(&datastore).await;
        assert_eq!(observed_ip_pool_ranges.len(), 1);
        assert_eq!(observed_ip_pool_ranges[0].ip_pool_id, svc_pool.id());

        // Verify the allocated external IP
        assert_eq!(observed_external_ips[0].ip_pool_id, svc_pool.id());
        assert_eq!(
            observed_external_ips[0].ip_pool_range_id,
            observed_ip_pool_ranges[0].id
        );
        assert_eq!(observed_external_ips[0].kind, IpKind::Service);
        assert_eq!(observed_external_ips[0].ip.ip(), nexus_ip,);

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
        let mut services = vec![
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: IpAddr::V4(nexus_ip_start),
                },
            },
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 456, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: IpAddr::V4(nexus_ip_end),
                },
            },
        ];
        services
            .sort_by(|a, b| a.service_id.partial_cmp(&b.service_id).unwrap());

        let datasets = vec![];
        let service_ip_pool_ranges =
            vec![IpRange::try_from((nexus_ip_start, nexus_ip_end))
                .expect("Cannot create IP Range")];
        let certificates = vec![];

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
                    rack_id: rack_id(),
                    services: services.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges,
                    certificates: certificates.clone(),
                    internal_dns,
                    external_dns,
                },
            )
            .await
            .expect("Failed to initialize rack");

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        let mut observed_services = get_all_services(&datastore).await;
        let mut observed_nexus_services =
            get_all_nexus_services(&datastore).await;
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

        // It should have a corresponding "Nexus service record"
        assert_eq!(observed_nexus_services.len(), 2);
        observed_nexus_services
            .sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
        assert_eq!(observed_services[0].id(), observed_nexus_services[0].id);
        assert_eq!(observed_services[1].id(), observed_nexus_services[1].id);

        // We should see both IPs allocated for these services.
        let observed_external_ips: HashMap<_, _> =
            get_all_external_ips(&datastore)
                .await
                .into_iter()
                .map(|ip| (ip.id, ip))
                .collect();
        assert_eq!(observed_external_ips.len(), 2);

        // The address referenced by the "NexusService" should match the input.
        assert_eq!(
            observed_external_ips[&observed_nexus_services[0].external_ip_id]
                .ip
                .ip(),
            if let internal_params::ServiceKind::Nexus { external_address } =
                services[0].kind
            {
                external_address
            } else {
                panic!("Unexpected service kind")
            }
        );
        assert_eq!(
            observed_external_ips[&observed_nexus_services[1].external_ip_id]
                .ip
                .ip(),
            if let internal_params::ServiceKind::Nexus { external_address } =
                services[1].kind
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
        assert!(svc_pool.internal);

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
        assert_eq!(dns_config_external.generation, 1);
        assert_eq!(dns_config_external.zones.len(), 1);
        assert_eq!(
            dns_config_external.zones[0].zone_name,
            "test-suite.oxide.test",
        );
        assert_eq!(
            dns_config_external.zones[0].records,
            HashMap::from([("api.sys".to_string(), external_records)]),
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
        let services = vec![internal_params::ServicePutRequest {
            service_id: Uuid::new_v4(),
            sled_id: sled.id(),
            address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
            kind: internal_params::ServiceKind::Nexus {
                external_address: nexus_ip,
            },
        }];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![];
        let certificates = vec![];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    services: services.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges,
                    certificates: certificates.clone(),
                    internal_dns: internal_dns_empty(),
                    external_dns: external_dns_empty(),
                },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Request: Requested external IP address not available"
        );

        assert!(get_all_services(&datastore).await.is_empty());
        assert!(get_all_nexus_services(&datastore).await.is_empty());
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
        let nexus_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));

        let services = vec![
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: nexus_ip,
                },
            },
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0),
                kind: internal_params::ServiceKind::Nexus {
                    external_address: nexus_ip,
                },
            },
        ];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![IpRange::from(nexus_ip)];
        let certificates = vec![];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit {
                    rack_id: rack_id(),
                    services: services.clone(),
                    datasets: datasets.clone(),
                    service_ip_pool_ranges,
                    certificates: certificates.clone(),
                    internal_dns: internal_dns_empty(),
                    external_dns: external_dns_empty(),
                },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Request: Requested external IP address not available",
        );

        assert!(get_all_services(&datastore).await.is_empty());
        assert!(get_all_nexus_services(&datastore).await.is_empty());
        assert!(get_all_datasets(&datastore).await.is_empty());
        assert!(get_all_external_ips(&datastore).await.is_empty());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
