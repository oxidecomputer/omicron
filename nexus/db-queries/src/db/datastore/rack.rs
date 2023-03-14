// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Rack`]s.

use super::DataStore;
use super::SERVICE_IP_POOL_NAME;
use super::SERVICE_ORG_NAME;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::fixed_data;
use crate::db::identity::Asset;
use crate::db::model::Certificate;
use crate::db::model::Dataset;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::NexusService;
use crate::db::model::Organization;
use crate::db::model::Project;
use crate::db::model::Rack;
use crate::db::model::Service;
use crate::db::model::Sled;
use crate::db::model::Vpc;
use crate::db::model::VpcSubnet;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::queries;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_types::external_api::params as external_params;
use nexus_types::external_api::shared::IpRange;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params as internal_params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

#[derive(Debug)]
enum RackInitError {
    AddingIp(Error),
    ServiceInsert { err: AsyncInsertError, sled_id: Uuid, svc_id: Uuid },
    ProjectInsert { err: AsyncInsertError, org_id: Uuid, proj_id: Uuid },
    VpcCreate(Error),
    VpcInsert { err: AsyncInsertError, proj_id: Uuid, vpc_id: Uuid },
    VpcRouterInsert { err: PoolError, router: String },
    VpcRouteInsert { err: AsyncInsertError, router_id: Uuid, route: String },
    VpcSubnetInsert { err: AsyncInsertError, vpc_id: Uuid, subnet_id: Uuid },
    ServiceNicInsert { err: PoolError, nic: String },
    VpcFwRuleInsert { err: AsyncInsertError, vpc_id: Uuid },
    DatasetInsert { err: AsyncInsertError, zpool_id: Uuid },
    RackUpdate { err: PoolError, rack_id: Uuid },
}

type TxnError = TransactionError<RackInitError>;

impl From<TxnError> for Error {
    fn from(e: TxnError) -> Self {
        match e {
            TxnError::CustomError(RackInitError::AddingIp(err)) => err,
            TxnError::CustomError(RackInitError::DatasetInsert {
                err,
                zpool_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Zpool,
                    lookup_type: LookupType::ById(zpool_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            },
            TxnError::CustomError(RackInitError::ServiceInsert {
                err,
                sled_id,
                svc_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Sled,
                    lookup_type: LookupType::ById(sled_id),
                },
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
            TxnError::CustomError(RackInitError::ProjectInsert {
                err,
                org_id,
                proj_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Organization,
                    lookup_type: LookupType::ById(org_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel_pool(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::Project,
                            &proj_id.to_string(),
                        ),
                    )
                }
            },
            TxnError::CustomError(RackInitError::VpcCreate(err)) => err,
            TxnError::CustomError(RackInitError::VpcInsert {
                err,
                proj_id,
                vpc_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Project,
                    lookup_type: LookupType::ById(proj_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel_pool(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::Vpc,
                            &vpc_id.to_string(),
                        ),
                    )
                }
            },
            TxnError::CustomError(RackInitError::VpcRouterInsert {
                err,
                router,
            }) => public_error_from_diesel_pool(
                err,
                ErrorHandler::Conflict(ResourceType::VpcRouter, &router),
            ),
            TxnError::CustomError(RackInitError::VpcRouteInsert {
                err,
                router_id,
                route,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::VpcRouter,
                    lookup_type: LookupType::ById(router_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel_pool(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::RouterRoute,
                            &route,
                        ),
                    )
                }
            },
            TxnError::CustomError(RackInitError::VpcSubnetInsert {
                err,
                vpc_id,
                subnet_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Vpc,
                    lookup_type: LookupType::ById(vpc_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel_pool(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::VpcSubnet,
                            &subnet_id.to_string(),
                        ),
                    )
                }
            },
            TxnError::CustomError(RackInitError::ServiceNicInsert {
                err,
                nic,
            }) => public_error_from_diesel_pool(
                err,
                ErrorHandler::Conflict(ResourceType::NetworkInterface, &nic),
            ),
            TxnError::CustomError(RackInitError::VpcFwRuleInsert {
                err,
                vpc_id,
            }) => match err {
                AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                    type_name: ResourceType::Vpc,
                    lookup_type: LookupType::ById(vpc_id),
                },
                AsyncInsertError::DatabaseError(e) => {
                    public_error_from_diesel_pool(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::VpcFirewallRule,
                            &vpc_id.to_string(),
                        ),
                    )
                }
            },
            TxnError::CustomError(RackInitError::RackUpdate {
                err,
                rack_id,
            }) => public_error_from_diesel_pool(
                err,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::Rack,
                    LookupType::ById(rack_id),
                ),
            ),
            TxnError::Pool(e) => {
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

    /// Create the database record for the given service.
    async fn create_service_record<ConnErr>(
        service: &internal_params::ServicePutRequest,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        log: &slog::Logger,
    ) -> Result<(), TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::service::dsl;

        let service_db = db::model::Service::new(
            service.service_id,
            service.sled_id,
            service.address,
            service.kind.into(),
        );
        <Sled as DatastoreCollection<Service>>::insert_resource(
            service.sled_id,
            diesel::insert_into(dsl::service)
                .values(service_db)
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|err| {
            warn!(log, "Initializing Rack: Failed to insert service");
            TxnError::CustomError(RackInitError::ServiceInsert {
                err,
                sled_id: service.sled_id,
                svc_id: service.service_id,
            })
        })?;

        Ok(())
    }

    /// Create a project for the given service within the built-in services org.
    async fn create_service_project<ConnErr>(
        log: &slog::Logger,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        service: &internal_params::ServicePutRequest,
    ) -> Result<Project, TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::project::dsl;

        let org_id = *fixed_data::ORGANIZATION_ID;

        let proj_name =
            format!("{}-{}", service.kind, service.service_id).parse().unwrap();
        let project_db = db::model::Project::new(
            org_id,
            external_params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: proj_name,
                    description: format!(
                        "Project for internal service ({}).",
                        service.kind
                    ),
                },
            },
        );
        let proj_id = project_db.id();
        <Organization as DatastoreCollection<Project>>::insert_resource(
            org_id,
            diesel::insert_into(dsl::project)
                .values(project_db)
                .on_conflict(dsl::id)
                .do_nothing(),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|err| {
            warn!(log, "Initializing Rack: Failed to insert service Project");
            TxnError::CustomError(RackInitError::ProjectInsert {
                err,
                org_id,
                proj_id,
            })
        })
    }

    /// Create a VPC in the given project for the given service.
    async fn create_service_vpc<ConnErr>(
        log: &slog::Logger,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        service: &internal_params::ServicePutRequest,
        project: &Project,
    ) -> Result<Vpc, TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::vpc::dsl;
        use queries::vpc::InsertVpcQuery;

        let vpc_id = Uuid::new_v4();
        let system_router_id = Uuid::new_v4();

        let vpc_db = db::model::IncompleteVpc::new(
            vpc_id,
            project.id(),
            system_router_id,
            external_params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: project.name().clone(),
                    description: format!(
                        "VPC for internal service ({}).",
                        service.kind
                    ),
                },
                ipv6_prefix: None,
                dns_name: project.name().clone(), // TODO: should this tie in with internal-dns?
            },
        )
        .map_err(|err| {
            warn!(log, "Initializing Rack: Failed to create service VPC");
            TxnError::CustomError(RackInitError::VpcCreate(err))
        })?;

        let vni = match service.kind {
            // Nexus gets a fixed VNI which must match the VNI used to allocate
            // the OPTE port in sled-agent.
            #[cfg(not(test))]
            internal_params::ServiceKind::Nexus { .. } => {
                Some(db::model::Vni(*nexus_defaults::nexus_service::VNI))
            }
            // For any other service, we'll get a random VNI from the Oxide-reserved range.
            _ => None,
        };

        let vpc = <Project as DatastoreCollection<Vpc>>::insert_resource(
            project.id(),
            diesel::insert_into(dsl::vpc)
                .values(InsertVpcQuery::new_system(vpc_db, vni)),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|err| {
            warn!(log, "Initializing Rack: Failed to insert service VPC");
            TxnError::CustomError(RackInitError::VpcInsert {
                err,
                proj_id: project.id(),
                vpc_id,
            })
        })?;

        // Populate some default VPC resources.
        Self::create_default_vpc_router(log, conn, &vpc).await?;
        let vpc_subnet =
            Self::create_service_vpc_subnet(log, conn, service, &vpc).await?;
        Self::create_service_nic(log, conn, service, &vpc_subnet).await?;

        Ok(vpc)
    }

    /// Create default system router and routes for the given VPC.
    async fn create_default_vpc_router<ConnErr>(
        log: &slog::Logger,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        vpc: &Vpc,
    ) -> Result<(), TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::model::RouterRoute;
        use db::model::VpcRouter;
        use db::schema::router_route;
        use db::schema::vpc_router;
        use omicron_common::api::external::RouteDestination;
        use omicron_common::api::external::RouteTarget;
        use omicron_common::api::external::RouterRouteKind;

        let router_id = vpc.system_router_id;
        let router_db = VpcRouter::new(
            router_id,
            vpc.id(),
            db::model::VpcRouterKind::System,
            external_params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: "system".parse().unwrap(),
                    description: "Routes are automatically added to this \
                        router as vpc subnets are created"
                        .into(),
                },
            },
        );

        diesel::insert_into(vpc_router::dsl::vpc_router)
            .values(router_db)
            .on_conflict(vpc_router::dsl::id)
            .do_nothing()
            .returning(VpcRouter::as_returning())
            .get_result_async(conn)
            .await
            .map_err(|err| {
                warn!(log, "Initializing Rack: Failed to insert service VPC system router");
                TxnError::CustomError(RackInitError::VpcRouterInsert {
                    err: err.into(),
                    router: "system".to_string(),
                })
            })?;

        // Populate the router with a default internet gateway route.
        let route_id = Uuid::new_v4();
        let route_db = RouterRoute::new(
            route_id,
            router_id,
            RouterRouteKind::Default,
            external_params::RouterRouteCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: "The default route of a vpc".to_string(),
                },
                target: RouteTarget::InternetGateway(
                    "outbound".parse().unwrap(),
                ),
                destination: RouteDestination::Vpc(vpc.name().clone()),
            },
        );
        <VpcRouter as DatastoreCollection<RouterRoute>>::insert_resource(
            router_id,
            diesel::insert_into(router_route::dsl::router_route)
                .values(route_db),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|err| {
            warn!(
                log,
                "Initializing Rack: Failed to insert service VPC default route"
            );
            TxnError::CustomError(RackInitError::VpcRouteInsert {
                err,
                router_id,
                route: "default".to_string(),
            })
        })?;

        Ok(())
    }

    /// Create default VPC subnet for the given service.
    async fn create_service_vpc_subnet<ConnErr>(
        log: &slog::Logger,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        service: &internal_params::ServicePutRequest,
        vpc: &Vpc,
    ) -> Result<VpcSubnet, TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::vpc_subnet::dsl;
        use omicron_common::api::external::Ipv6Net;
        use queries::vpc_subnet::FilterConflictingVpcSubnetRangesQuery;

        let subnet_id = Uuid::new_v4();
        let subnet_db = db::model::VpcSubnet::new(
            subnet_id,
            vpc.id(),
            IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: format!(
                    "The default VPC subnet for internal service ({})",
                    service.kind
                ),
            },
            *nexus_defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK,
            Ipv6Net(
                ipnetwork::Ipv6Network::new(vpc.ipv6_prefix.network(), 64)
                    .unwrap(),
            ),
        );
        <Vpc as DatastoreCollection<VpcSubnet>>::insert_resource(
            vpc.id(),
            diesel::insert_into(dsl::vpc_subnet)
                .values(FilterConflictingVpcSubnetRangesQuery::new(subnet_db)),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|err| {
            warn!(log, "Initializing Rack: Failed to insert service default VPC subnet");
            TxnError::CustomError(RackInitError::VpcSubnetInsert {
                err,
                vpc_id: vpc.id(),
                subnet_id,
            })
        })
    }

    /// Create a NIC record for the given service in the given VPC subnet.
    ///
    /// This NIC represents the OPTE port the service zone will be given
    /// that provides externally connectivity.
    async fn create_service_nic<ConnErr>(
        log: &slog::Logger,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        service: &internal_params::ServicePutRequest,
        vpc_subnet: &VpcSubnet,
    ) -> Result<(), TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::model::ServiceNetworkInterface;
        use db::model::ServiceNetworkInterfaceIdentity;
        use db::schema::service_network_interface;

        if !matches!(service.kind, internal_params::ServiceKind::Nexus { .. }) {
            // Nexus is the only service that currently needs external
            // connectivity and thus has an OPTE port.
            return Ok(());
        }

        let nic_id = Uuid::new_v4();
        let nic_db = ServiceNetworkInterface {
            identity: ServiceNetworkInterfaceIdentity::new(
                nic_id,
                IdentityMetadataCreateParams {
                    name: "nexus".parse().unwrap(),
                    description: "Nexus service external NIC".to_string(),
                },
            ),
            service_id: service.service_id,
            vpc_id: vpc_subnet.vpc_id,
            subnet_id: vpc_subnet.id(),
            mac: nexus_defaults::nexus_service::EXTERNAL_NIC_MAC.into(),
            ip: (*nexus_defaults::nexus_service::EXTERNAL_NIC_PRIVATE_IP)
                .into(),
            slot: 0,
        };

        diesel::insert_into(
            service_network_interface::dsl::service_network_interface,
        )
        .values(nic_db)
        .on_conflict(service_network_interface::dsl::id)
        .do_nothing()
        .execute_async(conn)
        .await
        .map_err(|err| {
            warn!(
                log,
                "Initializing Rack: Failed to insert service NIC record"
            );
            TxnError::CustomError(RackInitError::ServiceNicInsert {
                err: err.into(),
                nic: "nexus".to_string(),
            })
        })?;

        Ok(())
    }

    /// Create the default firewall rules for the Nexus service in the given VPC.
    async fn populate_nexus_default_fw_rules<ConnErr>(
        log: &slog::Logger,
        conn: &(impl AsyncConnection<DbConnection, ConnErr> + Sync),
        vpc: &Vpc,
    ) -> Result<(), TxnError>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::model::VpcFirewallRule;
        use db::schema::vpc_firewall_rule::dsl;

        let vpc_id = vpc.id();
        let rules_db = VpcFirewallRule::vec_from_params(
            vpc_id,
            nexus_defaults::nexus_service::firewall_rules(vpc.name().as_str()),
        );
        <Vpc as DatastoreCollection<VpcFirewallRule>>::insert_resource(
            vpc_id,
            diesel::insert_into(dsl::vpc_firewall_rule)
                .values(rules_db),
        )
        .insert_and_get_results_async(conn)
        .await
        .map_err(|err| {
            warn!(log, "Initializing Rack: Failed to insert default firewall rules for Nexus");
            TxnError::CustomError(RackInitError::VpcFwRuleInsert {
                err,
                vpc_id,
            })
        })?;

        Ok(())
    }

    /// Update a rack to mark that it has been initialized
    pub async fn rack_set_initialized(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        services: &[internal_params::ServicePutRequest],
        datasets: Vec<Dataset>,
        service_ip_pool_ranges: Vec<IpRange>,
        certificates: Vec<Certificate>,
    ) -> UpdateResult<Rack> {
        use db::schema::rack::dsl as rack_dsl;

        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        let (authz_service_pool, service_pool) =
            self.ip_pools_service_lookup(&opctx).await?;

        // NOTE: This operation could likely be optimized with a CTE, but given
        // the low-frequency of calls, this optimization has been deferred.
        let log = opctx.log.clone();
        let rack = self.pool_authorized(opctx)
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
                            err: PoolError::from(e),
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
                    ).await.map_err(|err| {
                        warn!(log, "Initializing Rack: Failed to add IP pool range");
                        TxnError::CustomError(RackInitError::AddingIp(err))
                    })?;
                }

                // Allocate resources for all services.
                for service in services {
                    // Create "Service" record in database.
                    Self::create_service_record(&service, &conn, &log).await?;

                    // Create project/vpc for this service within the built-in services org.
                    let project = Self::create_service_project(&log, &conn, &service).await?;
                    let vpc = Self::create_service_vpc(&log, &conn, &service, &project).await?;

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

                        // Populate the Nexus firewall rules.
                        Self::populate_nexus_default_fw_rules(&log, &conn, &vpc).await?;
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
                        TxnError::CustomError(RackInitError::RackUpdate {
                            err: PoolError::from(e),
                            rack_id,
                        })
                    })?;
                Ok::<_, TxnError>(rack)
            })
            .await?;
        Ok(rack)
    }

    async fn load_builtin_service_org(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use omicron_common::api::external::InternalContext;

        let params = external_params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: SERVICE_ORG_NAME.parse().unwrap(),
                description: String::from("Organization for Oxide Services"),
            },
        };

        // Create the service org within the built-in silo.
        let (authz_silo,) = db::lookup::LookupPath::new(&opctx, self)
            .silo_id(*fixed_data::silo::SILO_ID)
            .lookup_for(authz::Action::CreateChild)
            .await
            .internal_context("lookup built-in silo")?;

        let org_id = Some(*fixed_data::ORGANIZATION_ID);
        self.organization_create_in_silo(opctx, org_id, &params, authz_silo)
            .await
            .map(|_| ())
            .or_else(|e| match e {
                Error::ObjectAlreadyExists { .. } => Ok(()),
                _ => Err(e),
            })?;

        Ok(())
    }

    async fn load_builtin_ip_pool(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        let params = external_params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: SERVICE_IP_POOL_NAME.parse().unwrap(),
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

        let params = external_params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
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

    pub async fn load_builtin_rack_data(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        self.rack_insert(opctx, &db::model::Rack::new(rack_id)).await?;

        self.load_builtin_service_org(opctx).await?;
        self.load_builtin_ip_pool(opctx).await?;

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
    use crate::db::model::ServiceKind;
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
                &services,
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
                &services,
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
            address: Ipv6Addr::LOCALHOST,
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
                rack_id(),
                &services,
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
                address: Ipv6Addr::LOCALHOST,
                kind: internal_params::ServiceKind::Nexus {
                    external_address: IpAddr::V4(nexus_ip_start),
                },
            },
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: Ipv6Addr::LOCALHOST,
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

        let rack = datastore
            .rack_set_initialized(
                &opctx,
                rack_id(),
                &services,
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
            address: Ipv6Addr::LOCALHOST,
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
                rack_id(),
                &services,
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
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: Ipv6Addr::LOCALHOST,
                kind: internal_params::ServiceKind::Nexus {
                    external_address: nexus_ip,
                },
            },
            internal_params::ServicePutRequest {
                service_id: Uuid::new_v4(),
                sled_id: sled.id(),
                address: Ipv6Addr::LOCALHOST,
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
                rack_id(),
                &services,
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
