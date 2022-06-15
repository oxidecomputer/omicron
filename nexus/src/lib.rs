// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
#![feature(async_closure)]

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
pub mod defaults; // Public for testing
pub mod external_api; // Public for testing
pub mod internal_api; // Public for testing
mod populate;
mod saga_interface;
pub mod updates; // public for testing

pub use app::test_interfaces::TestInterfaces;
pub use app::Nexus;
pub use config::{Config, PackageConfig};
pub use context::ServerContext;
pub use crucible_agent_client;
use external_api::http_entrypoints::external_api;
use internal_api::http_entrypoints::internal_api;
use slog::Logger;
use std::sync::Arc;

#[macro_use]
extern crate slog;
#[macro_use]
extern crate newtype_derive;
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
    /// Creates a Nexus instance with only the internal API exposed.
    ///
    /// This is often used as an argument when creating a [`Server`],
    /// which also exposes the external API.
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

        let http_server_starter_internal = dropshot::HttpServerStarter::new(
            &config.deployment.dropshot_internal,
            internal_api(),
            Arc::clone(&apictx),
            &log.new(o!("component" => "dropshot_internal")),
        )
        .map_err(|error| format!("initializing internal server: {}", error))?;
        let http_server_internal = http_server_starter_internal.start();

        Ok(Self { apictx, http_server_internal, config, log })
    }
}

/// Packages up a [`Nexus`], running both external and internal HTTP API servers
/// wired up to Nexus
pub struct Server {
    /// shared state used by API request handlers
    pub apictx: Arc<ServerContext>,
    /// dropshot server for external API
    pub http_server_external: dropshot::HttpServer<Arc<ServerContext>>,
    /// dropshot server for internal API
    pub http_server_internal: dropshot::HttpServer<Arc<ServerContext>>,
}

impl Server {
    pub async fn start(internal: InternalServer<'_>) -> Result<Self, String> {
        let apictx = internal.apictx;
        let http_server_internal = internal.http_server_internal;
        let log = internal.log;
        let config = internal.config;

        // Wait until RSS handoff completes.
        let opctx = apictx.nexus.opctx_for_background();
        apictx.nexus.await_rack_initialization(&opctx).await;
        apictx.nexus.start_background_tasks().map_err(|e| e.to_string())?;

        let http_server_starter_external = dropshot::HttpServerStarter::new(
            &config.deployment.dropshot_external,
            external_api(),
            Arc::clone(&apictx),
            &log.new(o!("component" => "dropshot_external")),
        )
        .map_err(|error| format!("initializing external server: {}", error))?;
        let http_server_external = http_server_starter_external.start();

        Ok(Server { apictx, http_server_external, http_server_internal })
    }

    /// Wait for the given server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(self) -> Result<(), String> {
        let errors = vec![
            self.http_server_external
                .await
                .map_err(|e| format!("external: {}", e)),
            self.http_server_internal
                .await
                .map_err(|e| format!("internal: {}", e)),
        ]
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
        self.apictx
            .nexus
            .register_as_producer(self.http_server_internal.local_addr())
            .await;
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
