// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the Nexus, the heart of the control plane

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]
// TODO(#40): Remove this exception once resolved.
#![allow(clippy::unnecessary_wraps)]
// Clippy's style lints are useful, but not worth running automatically.
#![allow(clippy::style)]

pub mod app; // Public for documentation examples
mod cidata;
pub mod config; // Public for testing
pub mod context; // Public for documentation examples
pub mod external_api; // Public for testing
pub mod internal_api; // Public for testing
mod populate;
mod saga_interface;
pub mod updates; // public for testing

pub use app::test_interfaces::TestInterfaces;
pub use app::Nexus;
pub use config::Config;
pub use context::ServerContext;
pub use crucible_agent_client;
use external_api::http_entrypoints::external_api;
use internal_api::http_entrypoints::internal_api;
use internal_dns::DnsConfigBuilder;
use nexus_types::internal_api::params::ServiceKind;
use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
use slog::Logger;
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

// These modules used to be within nexus, but have been moved to
// nexus-db-queries. Keeping these around temporarily for migration reasons.
pub use nexus_db_queries::{authn, authz, db};

#[macro_use]
extern crate slog;

/// Run the OpenAPI generator for the external API, which emits the OpenAPI spec
/// to stdout.
pub fn run_openapi_external() -> Result<(), String> {
    external_api()
        .openapi("Oxide Region API", "0.0.1")
        .description("API for interacting with the Oxide control plane")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

pub fn run_openapi_internal() -> Result<(), String> {
    internal_api()
        .openapi("Nexus internal API", "0.0.1")
        .description("Nexus internal API")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

/// A partially-initialized Nexus server, which exposes an internal interface,
/// but is not ready to receive external requests.
pub struct InternalServer {
    /// shared state used by API request handlers
    pub apictx: Arc<ServerContext>,
    /// dropshot server for internal API
    pub http_server_internal: dropshot::HttpServer<Arc<ServerContext>>,

    config: Config,
    log: Logger,
}

impl InternalServer {
    /// Start a nexus server.
    pub async fn start(
        config: &Config,
        log: &Logger,
    ) -> Result<InternalServer, String> {
        let log = log.new(o!("name" => config.deployment.id.to_string()));
        info!(log, "setting up nexus server");

        let ctxlog = log.new(o!("component" => "ServerContext"));

        let apictx =
            ServerContext::new(config.deployment.rack_id, ctxlog, &config)
                .await?;

        // Launch the internal server.
        let server_starter_internal = dropshot::HttpServerStarter::new(
            &config.deployment.dropshot_internal,
            internal_api(),
            Arc::clone(&apictx),
            &log.new(o!("component" => "dropshot_internal")),
        )
        .map_err(|error| format!("initializing internal server: {}", error))?;
        let http_server_internal = server_starter_internal.start();

        Ok(Self { apictx, http_server_internal, config: config.clone(), log })
    }
}

pub type DropshotServer = dropshot::HttpServer<Arc<ServerContext>>;

/// Packages up a [`Nexus`], running both external and internal HTTP API servers
/// wired up to Nexus
pub struct Server {
    /// shared state used by API request handlers
    apictx: Arc<ServerContext>,
}

impl Server {
    async fn start(internal: InternalServer) -> Result<Self, String> {
        let apictx = internal.apictx;
        let http_server_internal = internal.http_server_internal;
        let log = internal.log;
        let config = internal.config;

        // Wait until RSS handoff completes.
        let opctx = apictx.nexus.opctx_for_service_balancer();
        apictx.nexus.await_rack_initialization(&opctx).await;

        // Launch the external server.
        let tls_config = apictx
            .nexus
            .external_tls_config(config.deployment.dropshot_external.tls)
            .await;
        let http_server_external = {
            let server_starter_external =
                dropshot::HttpServerStarter::new_with_tls(
                    &config.deployment.dropshot_external.dropshot,
                    external_api(),
                    Arc::clone(&apictx),
                    &log.new(o!("component" => "dropshot_external")),
                    tls_config.map(dropshot::ConfigTls::Dynamic),
                )
                .map_err(|error| {
                    format!("initializing external server: {}", error)
                })?;
            server_starter_external.start()
        };
        apictx
            .nexus
            .set_servers(http_server_external, http_server_internal)
            .await;
        let server = Server { apictx: apictx.clone() };
        Ok(server)
    }

    pub fn apictx(&self) -> &Arc<ServerContext> {
        &self.apictx
    }

    /// Wait for the given server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(self) -> Result<(), String> {
        self.apictx.nexus.wait_for_shutdown().await
    }

    /// Register the Nexus server as a metric producer with oximeter.
    pub async fn register_as_producer(&self) {
        let nexus = &self.apictx.nexus;

        nexus
            .register_as_producer(
                nexus.get_internal_server_address().await.unwrap(),
            )
            .await;
    }
}

#[async_trait::async_trait]
impl nexus_test_interface::NexusServer for Server {
    type InternalServer = InternalServer;

    async fn start_internal(
        config: &Config,
        log: &Logger,
    ) -> (InternalServer, SocketAddr) {
        let internal_server =
            InternalServer::start(config, &log).await.unwrap();
        internal_server.apictx.nexus.wait_for_populate().await.unwrap();
        let addr = internal_server.http_server_internal.local_addr();
        (internal_server, addr)
    }

    async fn start(
        internal_server: InternalServer,
        config: &Config,
        services: Vec<nexus_types::internal_api::params::ServicePutRequest>,
        external_dns_zone_name: &str,
        recovery_silo: nexus_types::internal_api::params::RecoverySiloConfig,
        certs: Vec<nexus_types::internal_api::params::Certificate>,
    ) -> Self {
        // Perform the "handoff from RSS".
        //
        // However, RSS isn't running, so we'll do the handoff ourselves.
        let opctx = internal_server.apictx.nexus.opctx_for_service_balancer();

        // Allocation of the initial Nexus's external IP is a little funny.  In
        // a real system, it'd be allocated by RSS and provided with the rack
        // initialization request (which we're about to simulate).  RSS also
        // provides information about the external IP pool ranges available for
        // system services.  The Nexus external IP that it picks comes from this
        // range.  During rack initialization, Nexus "allocates" the IP (which
        // was really already allocated) -- recording that allocation like any
        // other one.
        //
        // In this context, the IP was "allocated" by the user.  Most likely,
        // it's 127.0.0.1, having come straight from the stock testing config
        // file.  Whatever it is, we fake up an IP pool range for use by system
        // services that includes solely this IP.
        let internal_services_ip_pool_ranges = services
            .iter()
            .filter_map(|s| match s.kind {
                ServiceKind::Nexus { external_address: IpAddr::V4(addr) } => {
                    Some(IpRange::V4(Ipv4Range::new(addr, addr).unwrap()))
                }
                ServiceKind::Nexus { external_address: IpAddr::V6(addr) } => {
                    Some(IpRange::V6(Ipv6Range::new(addr, addr).unwrap()))
                }
                _ => None,
            })
            .collect();

        internal_server
            .apictx
            .nexus
            .rack_initialize(
                &opctx,
                config.deployment.rack_id,
                internal_api::params::RackInitializationRequest {
                    services,
                    datasets: vec![],
                    internal_services_ip_pool_ranges,
                    certs,
                    internal_dns_zone_config: DnsConfigBuilder::new().build(),
                    external_dns_zone_name: external_dns_zone_name.to_owned(),
                    recovery_silo,
                },
            )
            .await
            .expect("Could not initialize rack");

        // Start the Nexus external API.
        Server::start(internal_server).await.unwrap()
    }

    // XXX-dap allow to return HTTP *or* HTTPS
    async fn get_http_server_external_address(&self) -> Option<SocketAddr> {
        self.apictx.nexus.get_http_external_server_address().await
    }

    // XXX-dap revisit / rip out, only used by test?
    async fn get_https_server_external_address(&self) -> Option<SocketAddr> {
        self.apictx.nexus.get_https_external_server_address().await
    }

    async fn get_http_server_internal_address(&self) -> SocketAddr {
        self.apictx.nexus.get_internal_server_address().await.unwrap()
    }

    async fn set_resolver(&self, resolver: internal_dns::resolver::Resolver) {
        self.apictx.nexus.set_resolver(resolver).await
    }

    async fn upsert_crucible_dataset(
        &self,
        id: Uuid,
        zpool_id: Uuid,
        address: SocketAddrV6,
    ) {
        self.apictx
            .nexus
            .upsert_dataset(
                id,
                zpool_id,
                address,
                crate::db::model::DatasetKind::Crucible,
            )
            .await
            .unwrap();
    }

    async fn close(mut self) {
        self.apictx
            .nexus
            .close_servers()
            .await
            .expect("failed to close servers during test cleanup");
        self.wait_for_finish().await.unwrap()
    }
}

/// Run an instance of the [Server].
pub async fn run_server(config: &Config) -> Result<(), String> {
    use slog::Drain;
    let (drain, registration) =
        slog_dtrace::with_drain(
            config.pkg.log.to_logger("nexus").map_err(|message| {
                format!("initializing logger: {}", message)
            })?,
        );
    let log = slog::Logger::root(drain.fuse(), slog::o!());
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(msg);
    } else {
        debug!(log, "registered DTrace probes");
    }
    let internal_server = InternalServer::start(config, &log).await?;
    let server = Server::start(internal_server).await?;
    server.register_as_producer().await;
    server.wait_for_finish().await
}
