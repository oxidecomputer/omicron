// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Rack`]s.

use super::DataStore;
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
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_types::external_api::shared::IpRange;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use std::net::IpAddr;
use uuid::Uuid;

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
        rack_id: Uuid,
        // Service and corresponding external IP address, if any.
        services: Vec<(Service, Option<IpAddr>)>,
        datasets: Vec<Dataset>,
        service_ip_pool_ranges: Vec<IpRange>,
        certificates: Vec<Certificate>,
    ) -> UpdateResult<Rack> {
        use db::schema::rack::dsl as rack_dsl;

        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        #[derive(Debug)]
        enum RackInitError {
            MissingExternalIp,
            AddingIp(Error),
            ServiceInsert { err: AsyncInsertError, sled_id: Uuid, svc_id: Uuid },
            DatasetInsert { err: AsyncInsertError, zpool_id: Uuid },
            RackUpdate(PoolError),
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

                // Allocate records for all services all services.
                for (svc, external_ip) in services {
                    use db::schema::service::dsl;
                    let sled_id = svc.sled_id;
                    <Sled as DatastoreCollection<Service>>::insert_resource(
                        sled_id,
                        diesel::insert_into(dsl::service)
                            .values(svc.clone())
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
                            svc_id: svc.id(),
                        })
                    })?;

                    if let ServiceKind::Nexus = svc.kind {
                        // Nexus services should come with a corresponding
                        // external IP address.
                        let external_ip = external_ip.ok_or_else(|| {
                            warn!(log, "Initializing Rack: Nexus service missing IP address");
                            TxnError::CustomError(RackInitError::MissingExternalIp)
                        })?;

                        // Allocate the explicit IP address that is currently
                        // in-use by this Nexus service.
                        let ip_id = Uuid::new_v4();
                        let data = IncompleteExternalIp::for_service_explicit(
                            ip_id,
                            service_pool.id(),
                            external_ip
                        );
                        let allocated_ip = Self::allocate_external_ip_on_connection(
                            &conn,
                            data
                        ).await.map_err(|err| {
                            warn!(log, "Initializing Rack: Failed to allocate IP address");
                            TxnError::CustomError(RackInitError::AddingIp(err))
                        })?;
                        assert_eq!(allocated_ip.ip.ip(), external_ip);

                        // Add a service record for Nexus.
                        let nexus_service = NexusService::new(svc.id(), allocated_ip.id);
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
                TxnError::CustomError(RackInitError::MissingExternalIp) => {
                    Error::invalid_request("Missing necessary External IP")
                },
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
        use crate::external_api::params;
        use omicron_common::api::external::IdentityMetadataCreateParams;
        use omicron_common::api::external::Name;

        self.rack_insert(opctx, &db::model::Rack::new(rack_id)).await?;

        let params = params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "oxide-service-pool".parse::<Name>().unwrap(),
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::datastore_test;
    use crate::db::model::ExternalIp;
    use crate::db::model::IpKind;
    use crate::db::model::IpPoolRange;
    use async_bb8_diesel::AsyncSimpleConnection;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn rack_set_initialized_empty() {
        let logctx = dev::test_setup_log("rack_set_initialized_empty");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let services = vec![];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![];
        let certificates = vec![];

        // Initializing the rack with no data is odd, but allowed.
        let rack = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services.clone(),
                datasets.clone(),
                service_ip_pool_ranges.clone(),
                certificates.clone(),
            )
            .await
            .expect("Failed to initialize rack");

        assert_eq!(rack.id(), rack_id());
        assert!(rack.initialized);

        // It should also be idempotent.
        let rack2 = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services,
                datasets,
                service_ip_pool_ranges,
                certificates,
            )
            .await
            .expect("Failed to initialize rack");
        assert_eq!(rack.time_modified(), rack2.time_modified());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn create_test_sled(db: &DataStore) -> Sled {
        let sled_id = Uuid::new_v4();
        let is_scrimlet = false;
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
        let identifier = String::from("identifier");
        let model = String::from("model");
        let revision = 0;
        let sled = Sled::new(
            sled_id,
            addr,
            is_scrimlet,
            identifier,
            model,
            revision,
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
                            conn.batch_execute_async(crate::db::ALLOW_FULL_TABLE_SCAN_SQL)
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
        let services = vec![(
            Service::new(
                Uuid::new_v4(),
                sled.id(),
                Ipv6Addr::LOCALHOST,
                ServiceKind::Nexus,
            ),
            Some(nexus_ip),
        )];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![IpRange::from(nexus_ip)];
        let certificates = vec![];

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services.clone(),
                datasets.clone(),
                service_ip_pool_ranges,
                certificates.clone(),
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
        assert_eq!(
            observed_external_ips[0].ip.ip(),
            *services[0].1.as_ref().unwrap()
        );

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
        let nexus_ip4_first = Ipv4Addr::new(1, 2, 3, 4);
        let nexus_ip4_second = Ipv4Addr::new(5, 6, 7, 8);
        let mut services = vec![
            (
                Service::new(
                    Uuid::new_v4(),
                    sled.id(),
                    Ipv6Addr::LOCALHOST,
                    ServiceKind::Nexus,
                ),
                Some(IpAddr::V4(nexus_ip4_first)),
            ),
            (
                Service::new(
                    Uuid::new_v4(),
                    sled.id(),
                    Ipv6Addr::LOCALHOST,
                    ServiceKind::Nexus,
                ),
                Some(IpAddr::V4(nexus_ip4_second)),
            ),
        ];
        services.sort_by(|a, b| a.0.id().partial_cmp(&b.0.id()).unwrap());

        let datasets = vec![];
        let service_ip_pool_ranges = vec![
            IpRange::try_from((nexus_ip4_first, nexus_ip4_first))
                .expect("Cannot create IP Range"),
            IpRange::try_from((nexus_ip4_second, nexus_ip4_second))
                .expect("Cannot create IP Range"),
        ];

        let certificates = vec![];

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services.clone(),
                datasets.clone(),
                service_ip_pool_ranges,
                certificates.clone(),
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
            services[0].1.unwrap()
        );
        assert_eq!(
            observed_external_ips[&observed_nexus_services[1].external_ip_id]
                .ip
                .ip(),
            services[1].1.unwrap()
        );

        // Furthermore, we should be able to see that this IP addresses have been
        // allocated as a part of the service IP pool.
        let (.., svc_pool) =
            datastore.ip_pools_service_lookup(&opctx).await.unwrap();
        assert!(svc_pool.internal);

        // NOTE: Theoretically, this could be provisioned as part of a single IP
        // pool range in the future.
        let observed_ip_pool_ranges = get_all_ip_pool_ranges(&datastore).await;
        assert_eq!(observed_ip_pool_ranges.len(), 2);
        assert_eq!(observed_ip_pool_ranges[0].ip_pool_id, svc_pool.id());
        assert_eq!(observed_ip_pool_ranges[1].ip_pool_id, svc_pool.id());

        assert!(observed_datasets.is_empty());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_set_initialized_nexus_service_without_ip_throws_error() {
        let logctx = dev::test_setup_log(
            "rack_set_initialized_nexus_service_without_ip_throws_error",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        let services = vec![(
            Service::new(
                Uuid::new_v4(),
                sled.id(),
                Ipv6Addr::LOCALHOST,
                ServiceKind::Nexus,
            ),
            None,
        )];
        let datasets = vec![];
        // Even if we supply an address in the pool, the corresponding nexus
        // doesn't claim to have it - we throw an error.
        let service_ip_pool_ranges =
            vec![IpRange::from(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)))];
        let certificates = vec![];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services.clone(),
                datasets.clone(),
                service_ip_pool_ranges,
                certificates.clone(),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Request: Missing necessary External IP"
        );

        assert!(get_all_services(&datastore).await.is_empty());
        assert!(get_all_nexus_services(&datastore).await.is_empty());
        assert!(get_all_datasets(&datastore).await.is_empty());
        assert!(get_all_external_ips(&datastore).await.is_empty());

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

        let services = vec![(
            Service::new(
                Uuid::new_v4(),
                sled.id(),
                Ipv6Addr::LOCALHOST,
                ServiceKind::Nexus,
            ),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        )];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![];
        let certificates = vec![];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services.clone(),
                datasets.clone(),
                service_ip_pool_ranges,
                certificates.clone(),
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
            (
                Service::new(
                    Uuid::new_v4(),
                    sled.id(),
                    Ipv6Addr::LOCALHOST,
                    ServiceKind::Nexus,
                ),
                Some(nexus_ip),
            ),
            (
                Service::new(
                    Uuid::new_v4(),
                    sled.id(),
                    Ipv6Addr::LOCALHOST,
                    ServiceKind::Nexus,
                ),
                Some(nexus_ip),
            ),
        ];
        let datasets = vec![];
        let service_ip_pool_ranges = vec![IpRange::from(nexus_ip)];
        let certificates = vec![];

        let result = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                services.clone(),
                datasets.clone(),
                service_ip_pool_ranges,
                certificates.clone(),
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
