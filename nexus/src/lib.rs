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
use crate::context::OpContext;
use dropshot::PaginationOrder;
use external_api::http_entrypoints::external_api;
use internal_api::http_entrypoints::internal_api;
use omicron_common::api::external::DataPageParams;
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::Arc;
use tokio::sync::watch;
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

/// Packages up a [`Nexus`], running both external and internal HTTP API servers
/// wired up to Nexus
pub struct ServerInner {
    /// shared state used by API request handlers
    pub apictx: Arc<ServerContext>,
    /// dropshot servers for external API
    pub http_servers_external: Vec<dropshot::HttpServer<Arc<ServerContext>>>,
    /// dropshot server for internal API
    pub http_server_internal: dropshot::HttpServer<Arc<ServerContext>>,
}

pub struct Server {
    inner: Arc<ServerInner>,
}

impl Server {
    pub fn apictx(&self) -> &Arc<ServerContext> {
        &self.inner.apictx
    }

    async fn get_tls_config(nexus: &Nexus, opctx: &OpContext) -> Result<Option<dropshot::ConfigTls>, String> {
        // Lookup x509 certificates which might be stored in CRDB, specifically
        // for launching the Nexus service.
        //
        // We only grab one certificate (see: the "limit" argument) because
        // we're currently fine just using whatever certificate happens to be
        // available (as long as it's for Nexus).
        let certs = nexus
            .datastore()
            .certificate_list_for(
                &opctx,
                db::model::ServiceKind::Nexus,
                &DataPageParams {
                    marker: None,
                    direction: PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(1).unwrap(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        let certificate = if let Some(certificate) = certs.get(0) {
            certificate
        } else {
            return Ok(None)
        };

        Ok(Some(dropshot::ConfigTls::AsBytes {
            certs: certificate.cert.clone(),
            key: certificate.key.clone(),
        }))
    }

    pub async fn start(internal: InternalServer<'_>) -> Result<Self, String> {
        let apictx = internal.apictx;
        let http_server_internal = internal.http_server_internal;
        let log = internal.log;
        let config = internal.config;

        // Wait until RSS handoff completes.
        let opctx = apictx.nexus.opctx_for_service_balancer();
        apictx.nexus.await_rack_initialization(&opctx).await;

        // Lookup x509 certificates which might be stored in CRDB, specifically
        // for launching the Nexus service.
        let tls_config = Self::get_tls_config(&apictx.nexus, &opctx).await?;

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

        let (sender, receiver) = watch::channel(());
        apictx.nexus.set_server_refresh(sender);


        let server = Server {
            inner: Arc::new(ServerInner {
                apictx: apictx.clone(),
                http_servers_external,
                http_server_internal
            })
        };

        Ok(server)
    }

    pub async fn refresh(&self) -> Result<(), String> {
        let opctx = self.inner.apictx.nexus.opctx_for_service_balancer();
        let tls_config = Self::get_tls_config(&self.inner.apictx.nexus, &opctx).await?;
        let tls_config = if let Some(tls_config) = tls_config {
            tls_config
        } else {
            // TODO: Should we be doing this? We ignore the refresh if no certs
            // exist. We *could* actively remove the TLS cert, but that'll
            // require updating the dropshot API.
            return Ok(());
        };

        for server in &self.inner.http_servers_external {
            if server.local_addr().port() == 443 {
                server.refresh_tls(&tls_config).await?;
            }
        }
        Ok(())
    }

    /// Wait for the given server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(self) -> Result<(), String> {
        let mut errors = vec![];
        for server in self.inner.http_servers_external {
            errors.push(server.await.map_err(|e| format!("external: {}", e)));
        }
        errors.push(
            self.inner.http_server_internal
                .await
                .map_err(|e| format!("internal: {}", e)),
        );
        let errors = errors
            .into_iter()
            .filter(Result::is_err)
            .map(|r| r.unwrap_err())
            .collect::<Vec<String>>();

        if errors.len() > 0 {
            let msg = format!("errors shutting down: ({})", errors.join(", "));
            Err(msg)
        } else {
            Ok(())
        }
    }

    /// Register the Nexus server as a metric producer with `oximeter.
    pub async fn register_as_producer(&self) {
        self.inner.apictx
            .nexus
            .register_as_producer(self.inner.http_server_internal.local_addr())
            .await;
    }
}

#[async_trait::async_trait]
impl nexus_test_interface::NexusServer for Server {
    async fn start_and_populate(config: &Config, log: &Logger) -> Self {
        let internal_server =
            InternalServer::start(config, &log).await.unwrap();
        internal_server.apictx.nexus.wait_for_populate().await.unwrap();

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
                    certs: vec![],
                },
            )
            .await
            .expect("Could not initialize rack");

        // Start the Nexus external API.
        Server::start(internal_server).await.unwrap()
    }

    fn get_http_servers_external(&self) -> Vec<SocketAddr> {
        self.inner.http_servers_external
            .iter()
            .map(|server| server.local_addr())
            .collect()
    }

    fn get_http_server_internal(&self) -> SocketAddr {
        self.inner.http_server_internal.local_addr()
    }

    async fn upsert_crucible_dataset(
        &self,
        id: Uuid,
        zpool_id: Uuid,
        address: SocketAddrV6,
    ) {
        self.inner.apictx
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
        for server in self.inner.http_servers_external {
            server.close().await.unwrap();
        }
        self.inner.http_server_internal.close().await.unwrap();
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
