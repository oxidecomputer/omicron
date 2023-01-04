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
pub mod authn; // Public only for testing
pub mod authz; // Public for documentation examples
mod cidata;
pub mod config; // Public for testing
pub mod context; // Public for documentation examples
pub mod db; // Public for documentation examples
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
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

#[macro_use]
extern crate slog;
#[macro_use]
extern crate newtype_derive;
#[cfg(test)]
#[macro_use]
extern crate diesel;

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
pub struct InternalServer<'a> {
    /// shared state used by API request handlers
    pub apictx: Arc<ServerContext>,
    /// dropshot server for internal API
    pub http_server_internal: dropshot::HttpServer<Arc<ServerContext>>,

    config: &'a Config,
    log: Logger,
}

impl<'a> InternalServer<'a> {
    /// Start a nexus server.
    pub async fn start(
        config: &'a Config,
        log: &Logger,
    ) -> Result<InternalServer<'a>, String> {
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

        Ok(Self { apictx, http_server_internal, config, log })
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
    async fn start(internal: InternalServer<'_>) -> Result<Self, String> {
        let apictx = internal.apictx;
        let http_server_internal = internal.http_server_internal;
        let log = internal.log;
        let config = internal.config;

        // Wait until RSS handoff completes.
        let opctx = apictx.nexus.opctx_for_service_balancer();
        apictx.nexus.await_rack_initialization(&opctx).await;

        // Lookup x509 certificates which might be stored in CRDB, specifically
        // for launching the Nexus service.
        let tls_config = apictx
            .nexus
            .get_nexus_tls_config(&opctx)
            .await
            .map_err(|e| e.to_string())?;

        // Launch the external server(s).
        let http_servers_external = config
            .deployment
            .dropshot_external
            .clone()
            .iter_mut()
            .map(|mut cfg| {
                // Populate TLS with data from CRDB
                if cfg.bind_address.port() == 443 {
                    let tls_config = if let Some(tls_config) = &tls_config {
                        tls_config
                    } else {
                        return Err("No x.509 certificates found".to_string());
                    };
                    cfg.tls = Some(tls_config.clone());
                }

                let server_starter_external = dropshot::HttpServerStarter::new(
                    &cfg,
                    external_api(),
                    Arc::clone(&apictx),
                    &log.new(o!("component" => "dropshot_external")),
                )
                .map_err(|error| {
                    format!("initializing external server: {}", error)
                })?;
                Ok(server_starter_external.start())
            })
            .collect::<Result<Vec<dropshot::HttpServer<_>>, String>>()?;

        apictx
            .nexus
            .set_servers(http_servers_external, http_server_internal)
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
        self.apictx.nexus.close_servers().await
    }

    /// Register the Nexus server as a metric producer with `oximeter.
    async fn register_as_producer(&self) {
        let nexus = &self.apictx.nexus;

        nexus
            .register_as_producer(nexus.get_internal_server().await.unwrap())
            .await;
    }
}

#[async_trait::async_trait]
impl nexus_test_interface::NexusServer for Server {
    async fn start_and_populate(config: &Config, log: &Logger) -> Self {
        let internal_server =
            InternalServer::start(config, &log).await.unwrap();
        internal_server.apictx.nexus.wait_for_populate().await.unwrap();

        // If any certificates were provided through the configuration,
        // make them part of the RSS handoff.
        let certs = config
            .deployment
            .dropshot_external
            .iter()
            .filter_map(|external| {
                if let Some(tls) = &external.tls {
                    match tls {
                        dropshot::ConfigTls::AsBytes { certs, key } => {
                            Some(crate::internal_api::params::Certificate {
                                cert: certs.clone(),
                                key: key.clone(),
                            })
                        }
                        _ => panic!("Unsupported"),
                    }
                } else {
                    None
                }
            })
            .collect();

        // Perform the "handoff from RSS".
        //
        // However, RSS isn't running, so we'll do the handoff ourselves.
        let opctx = internal_server.apictx.nexus.opctx_for_service_balancer();
        internal_server
            .apictx
            .nexus
            .rack_initialize(
                &opctx,
                config.deployment.rack_id,
                // NOTE: In the context of this test utility, we arguably do have an
                // instance of CRDB and Nexus running. However, as this info isn't
                // necessary for most tests, we pass no information here.
                internal_api::params::RackInitializationRequest {
                    services: vec![],
                    datasets: vec![],
                    certs,
                },
            )
            .await
            .expect("Could not initialize rack");

        // Start the Nexus external API.
        Server::start(internal_server).await.unwrap()
    }

    async fn get_http_servers_external(&self) -> Vec<SocketAddr> {
        self.apictx.nexus.get_external_servers().await.unwrap()
    }

    async fn get_http_server_internal(&self) -> SocketAddr {
        self.apictx.nexus.get_internal_server().await.unwrap()
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
        self.apictx.nexus.close_servers().await.unwrap();
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
