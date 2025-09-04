// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the Nexus, the heart of the control plane

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]
// TODO(#40): Remove this exception once resolved.
#![allow(clippy::unnecessary_wraps)]

pub mod app; // Public for documentation examples
mod cidata;
mod context; // Public for documentation examples
pub mod external_api; // Public for testing
mod internal_api;
mod populate;
mod saga_interface;

pub use app::Nexus;
pub use app::test_interfaces::TestInterfaces;
use context::ApiContext;
use context::ServerContext;
use dropshot::ConfigDropshot;
use external_api::http_entrypoints::external_api;
use internal_api::http_entrypoints::internal_api;
use nexus_config::NexusConfig;
use nexus_db_model::RendezvousDebugDataset;
use nexus_db_queries::db;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::internal_api::params::{
    PhysicalDiskPutRequest, ZpoolPutRequest,
};
use nexus_types::inventory::Collection;
use omicron_common::FileKv;
use omicron_common::address::IpRange;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::{ProducerEndpoint, ProducerKind};
use omicron_common::api::internal::shared::{
    AllowedSourceIps, ExternalPortDiscovery, RackNetworkConfig, SwitchLocation,
};
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::ZpoolUuid;
use oximeter::types::ProducerRegistry;
use oximeter_producer::Server as ProducerServer;
use slog::Logger;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;

#[macro_use]
extern crate slog;

/// A partially-initialized Nexus server, which exposes an internal interface,
/// but is not ready to receive external requests.
pub struct InternalServer {
    /// Shared server state.
    apictx: ApiContext,
    /// dropshot server for internal API
    http_server_internal: dropshot::HttpServer<ApiContext>,
    config: NexusConfig,
    log: Logger,
}

impl InternalServer {
    /// Start a nexus server.
    pub async fn start(
        config: &NexusConfig,
        log: &Logger,
    ) -> Result<InternalServer, String> {
        let log = log.new(o!("name" => config.deployment.id.to_string()));
        info!(log, "setting up nexus server");

        let ctxlog = log.new(o!("component" => "ServerContext"));

        let context = ApiContext::for_internal(
            config.deployment.rack_id,
            ctxlog,
            &config,
        )
        .await?;

        // Launch the internal server.
        let http_server_internal = match dropshot::ServerBuilder::new(
            internal_api(),
            context.clone(),
            log.new(o!("component" => "dropshot_internal")),
        )
        .config(config.deployment.dropshot_internal.clone())
        .start()
        .map_err(|error| format!("initializing internal server: {}", error))
        {
            Ok(server) => server,
            Err(err) => {
                context.context.nexus.datastore().terminate().await;
                return Err(err);
            }
        };

        Ok(Self {
            apictx: context,
            http_server_internal,
            config: config.clone(),
            log,
        })
    }
}

type DropshotServer = dropshot::HttpServer<ApiContext>;

/// Packages up a [`Nexus`], running both external and internal HTTP API servers
/// wired up to Nexus
pub struct Server {
    /// shared state used by API request handlers
    apictx: ApiContext,
}

impl Server {
    async fn start(internal: InternalServer) -> Result<Self, String> {
        let apictx = internal.apictx;
        let http_server_internal = internal.http_server_internal;
        let log = internal.log;
        let config = internal.config;

        // Wait until RSS handoff completes.
        let opctx = apictx.context.nexus.opctx_for_service_balancer();
        apictx.context.nexus.await_rack_initialization(&opctx).await;

        // While we've started our internal server, we need to wait until we've
        // definitely implemented our source IP allowlist for making requests to
        // the external server we're about to start.
        apictx.context.nexus.await_ip_allowlist_plumbing().await;

        // Wait until Nexus has determined if sagas are supposed to be quiesced.
        // This is not strictly necessary.  The goal here is to prevent 503
        // errors to clients that reach this Nexus while it's starting up and
        // before it's figured out that it doesn't need to quiesce.  The risk of
        // doing this is that Nexus gets stuck here, but that should only happen
        // if it's unable to load the current blueprint, in which case
        // something's pretty wrong and it's likely pretty stuck anyway.
        apictx.context.nexus.wait_for_saga_determination().await;

        // Launch the external server.
        let tls_config = apictx
            .context
            .nexus
            .external_tls_config(config.deployment.dropshot_external.tls)
            .await;

        // We launch two dropshot servers providing the external API: one as
        // configured (which is accessible from the customer network), and one
        // that matches the configuration except listens on the same address
        // (but a different port) as the `internal` server. The latter is
        // available for proxied connections via the tech port in the event the
        // rack has lost connectivity (see RFD 431).
        let techport_server_bind_addr = {
            let mut addr = http_server_internal.local_addr();
            addr.set_port(config.deployment.techport_external_server_port);
            addr
        };
        let techport_server_config = ConfigDropshot {
            bind_address: techport_server_bind_addr,
            ..config.deployment.dropshot_external.dropshot.clone()
        };

        let http_server_external = {
            dropshot::ServerBuilder::new(
                external_api(),
                apictx.for_external(),
                log.new(o!("component" => "dropshot_external")),
            )
            .config(config.deployment.dropshot_external.dropshot.clone())
            .tls(tls_config.clone().map(dropshot::ConfigTls::Dynamic))
            .start()
            .map_err(|error| {
                format!("initializing external server: {}", error)
            })?
        };
        let http_server_techport_external = {
            dropshot::ServerBuilder::new(
                external_api(),
                apictx.for_techport(),
                log.new(o!("component" => "dropshot_external_techport")),
            )
            .config(techport_server_config)
            .tls(tls_config.map(dropshot::ConfigTls::Dynamic))
            .start()
            .map_err(|error| {
                format!("initializing external techport server: {}", error)
            })?
        };

        // Start the metric producer server that oximeter uses to fetch our
        // metric data.
        let producer_server = start_producer_server(
            &log,
            &apictx.context.producer_registry,
            http_server_internal.local_addr(),
        )?;

        apictx
            .context
            .nexus
            .set_servers(
                http_server_external,
                http_server_techport_external,
                http_server_internal,
                producer_server,
            )
            .await;
        let server = Server { apictx: apictx.clone() };
        Ok(server)
    }

    pub fn server_context(&self) -> &Arc<ServerContext> {
        &self.apictx.context
    }

    // Wait for the given server to shut down
    //
    // Note that this doesn't initiate a graceful shutdown, so if you call this
    // immediately after calling `start()`, the program will block indefinitely
    // or until something else initiates a graceful shutdown.
    async fn wait_for_finish(self) -> Result<(), String> {
        self.server_context().nexus.wait_for_shutdown().await
    }
}

#[async_trait::async_trait]
impl nexus_test_interface::NexusServer for Server {
    type InternalServer = InternalServer;

    async fn start_internal(
        config: &NexusConfig,
        log: &Logger,
    ) -> Result<(InternalServer, SocketAddr), String> {
        let internal_server = InternalServer::start(config, &log).await?;
        internal_server.apictx.context.nexus.wait_for_populate().await.unwrap();
        let addr = internal_server.http_server_internal.local_addr();
        Ok((internal_server, addr))
    }

    async fn stop_internal(internal_server: InternalServer) {
        internal_server
            .apictx
            .context
            .nexus
            .terminate()
            .await
            .expect("Failed to terminate Nexus");
    }

    async fn start(
        internal_server: InternalServer,
        config: &NexusConfig,
        blueprint: Blueprint,
        physical_disks: Vec<
            nexus_types::internal_api::params::PhysicalDiskPutRequest,
        >,
        zpools: Vec<nexus_types::internal_api::params::ZpoolPutRequest>,
        crucible_datasets: Vec<
            nexus_types::internal_api::params::CrucibleDatasetCreateRequest,
        >,
        internal_dns_zone_config: nexus_types::internal_api::params::DnsConfigParams,
        external_dns_zone_name: &str,
        recovery_silo: nexus_sled_agent_shared::recovery_silo::RecoverySiloConfig,
        certs: Vec<omicron_common::api::internal::nexus::Certificate>,
    ) -> Self {
        // Perform the "handoff from RSS".
        //
        // However, RSS isn't running, so we'll do the handoff ourselves.
        let opctx =
            internal_server.apictx.context.nexus.opctx_for_internal_api();

        // Allocation of initial external IP addresses is a little funny.  In
        // a real system, it'd be allocated by RSS and provided with the rack
        // initialization request (which we're about to simulate).  RSS also
        // provides information about the external IP pool ranges available for
        // system services.  But here, we fake up IP pool ranges based on the
        // external addresses of services that we start or mock.
        let internal_services_ip_pool_ranges = blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_, zc)| match &zc.zone_type {
                BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp { external_ip, .. },
                ) => Some(IpRange::from(external_ip.snat_cfg.ip)),
                BlueprintZoneType::ExternalDns(
                    blueprint_zone_type::ExternalDns { dns_address, .. },
                ) => Some(IpRange::from(dns_address.addr.ip())),
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    external_ip,
                    ..
                }) => Some(IpRange::from(external_ip.ip)),
                _ => None,
            })
            .collect();

        internal_server
            .apictx
            .context
            .nexus
            .rack_initialize(
                &opctx,
                config.deployment.rack_id,
                internal_api::params::RackInitializationRequest {
                    blueprint,
                    physical_disks,
                    zpools,
                    crucible_datasets,
                    internal_services_ip_pool_ranges,
                    certs,
                    internal_dns_zone_config,
                    external_dns_zone_name: external_dns_zone_name.to_owned(),
                    recovery_silo,
                    external_port_count: ExternalPortDiscovery::Static(
                        HashMap::from([(
                            SwitchLocation::Switch0,
                            vec!["qsfp0".parse().unwrap()],
                        )]),
                    ),
                    rack_network_config: RackNetworkConfig {
                        rack_subnet: "fd00:1122:3344:0100::/56"
                            .parse()
                            .unwrap(),
                        infra_ip_first: Ipv4Addr::UNSPECIFIED,
                        infra_ip_last: Ipv4Addr::UNSPECIFIED,
                        ports: Vec::new(),
                        bgp: Vec::new(),
                        bfd: Vec::new(),
                    },
                    allowed_source_ips: AllowedSourceIps::Any,
                },
                false, // blueprint_execution_enabled
            )
            .await
            .expect("Could not initialize rack");

        // Now that we have a blueprint, determine whether sagas should be
        // quiesced.  Wait for that so that tests can assume they can
        // immediately kick off sagas.
        internal_server
            .apictx
            .context
            .nexus
            .wait_for_saga_determination()
            .await;

        // Start the Nexus external API.
        Server::start(internal_server).await.unwrap()
    }

    fn datastore(&self) -> &Arc<db::DataStore> {
        self.apictx.context.nexus.datastore()
    }

    async fn get_http_server_external_address(&self) -> SocketAddr {
        self.apictx.context.nexus.get_external_server_address().await.unwrap()
    }

    async fn get_http_server_techport_address(&self) -> SocketAddr {
        self.apictx.context.nexus.get_techport_server_address().await.unwrap()
    }

    async fn get_http_server_internal_address(&self) -> SocketAddr {
        self.apictx.context.nexus.get_internal_server_address().await.unwrap()
    }

    async fn upsert_test_dataset(
        &self,
        physical_disk: PhysicalDiskPutRequest,
        zpool: ZpoolPutRequest,
        dataset_id: DatasetUuid,
        kind: DatasetKind,
        address: Option<SocketAddrV6>,
    ) {
        let opctx = self.apictx.context.nexus.opctx_for_internal_api();
        self.apictx
            .context
            .nexus
            .insert_test_physical_disk_if_not_exists(&opctx, physical_disk)
            .await
            .unwrap();

        let zpool_id = zpool.id;

        self.apictx.context.nexus.upsert_zpool(&opctx, zpool).await.unwrap();

        match &kind {
            DatasetKind::Crucible => {
                let address =
                    address.expect("crucible datasets have addresses");
                self.apictx
                    .context
                    .nexus
                    .upsert_crucible_dataset(dataset_id, zpool_id, address)
                    .await
                    .unwrap();
            }
            DatasetKind::Debug => {
                self.apictx
                    .context
                    .nexus
                    .datastore()
                    .debug_dataset_insert_if_not_exists(
                        &opctx,
                        RendezvousDebugDataset::new(
                            dataset_id,
                            ZpoolUuid::from_untyped_uuid(zpool_id),
                            BlueprintUuid::new_v4(),
                        ),
                    )
                    .await
                    .unwrap();
            }
            _ => panic!(
                "upsert_test_dataset does not support \
                 datasets of kind {kind:?}"
            ),
        }
    }

    async fn inventory_collect_and_get_latest_collection(
        &self,
    ) -> Result<Option<Collection>, Error> {
        let nexus = &self.apictx.context.nexus;

        nexus.activate_inventory_collection();

        let opctx = nexus.opctx_for_internal_api();
        nexus.datastore().inventory_get_latest_collection(&opctx).await
    }

    async fn close(mut self) {
        self.apictx
            .context
            .nexus
            .terminate()
            .await
            .expect("Failed to terminate Nexus");
    }
}

/// Run an instance of the Nexus server.
pub async fn run_server(config: &NexusConfig) -> Result<(), String> {
    use slog::Drain;
    let (drain, registration) =
        slog_dtrace::with_drain(
            config.pkg.log.to_logger("nexus").map_err(|message| {
                format!("initializing logger: {}", message)
            })?,
        );
    let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(msg);
    } else {
        debug!(log, "registered DTrace probes");
    }
    let internal_server = InternalServer::start(config, &log).await?;
    let server = Server::start(internal_server).await?;
    server.wait_for_finish().await
}

/// Create a new metric producer server.
fn start_producer_server(
    log: &Logger,
    registry: &ProducerRegistry,
    nexus_addr: SocketAddr,
) -> Result<ProducerServer, String> {
    // The producer server should listen on any available port, using the
    // same IP as the main Dropshot server.
    let address = SocketAddr::new(nexus_addr.ip(), 0);

    // Create configuration for the server.
    //
    // Note that because we're registering with _ourselves_, the listening
    // address for the producer server and the registration address use the
    // same IP.
    let config = oximeter_producer::Config {
        server_info: ProducerEndpoint {
            id: registry.producer_id(),
            kind: ProducerKind::Service,
            address,
            interval: std::time::Duration::from_secs(10),
        },
        // Some(_) here prevents DNS resolution, using our own address to
        // register.
        registration_address: Some(nexus_addr),
        default_request_body_max_bytes: 1024 * 1024 * 10,
        log: oximeter_producer::LogConfig::Logger(
            log.new(o!("component" => "nexus-producer-server")),
        ),
    };

    // Start the server, which will run the registration in a task.
    ProducerServer::with_registry(registry.clone(), &config)
        .map_err(|e| e.to_string())
}
