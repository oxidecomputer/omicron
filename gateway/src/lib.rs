// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod bulk_state_get;
mod config;
mod context;
mod error;
mod serial_console;

pub mod http_entrypoints; // TODO pub only for testing - is this right?

pub use config::Config;
pub use context::ServerContext;

use slog::{debug, error, info, o, warn, Logger};
use std::sync::Arc;
use uuid::Uuid;

/// Run the OpenAPI generator for the API, which emits the OpenAPI spec
/// to stdout.
pub fn run_openapi() -> Result<(), String> {
    http_entrypoints::api()
        .openapi("Oxide Management Gateway Service API", "0.0.1")
        .description("API for interacting with the Oxide control plane's gateway service")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

pub struct Server {
    /// shared state used by API request handlers
    pub apictx: Arc<ServerContext>,
    /// dropshot server for requests from nexus
    pub http_server: dropshot::HttpServer<Arc<ServerContext>>,
}

impl Server {
    /// Start a gateway server.
    pub async fn start(
        config: Config,
        _rack_id: Uuid,
        log: &Logger,
    ) -> Result<Server, String> {
        let log = log.new(o!("name" => config.id.to_string()));
        info!(log, "setting up gateway server");

        match gateway_sp_comms::register_probes() {
            Ok(_) => debug!(log, "successfully registered DTrace USDT probes"),
            Err(err) => {
                warn!(log, "failed to register DTrace USDT probes: {}", err);
            }
        }

        let apictx = ServerContext::new(config.switch, config.timeouts, &log)
            .await
            .map_err(|error| {
                format!("initializing server context: {}", error)
            })?;

        let http_server_starter = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_entrypoints::api(),
            Arc::clone(&apictx),
            &log.new(o!("component" => "dropshot")),
        )
        .map_err(|error| format!("initializing http server: {}", error))?;

        let http_server = http_server_starter.start();

        Ok(Server { apictx, http_server })
    }

    /// Wait for the server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(self) -> Result<(), String> {
        self.http_server.await
    }

    // TODO does MGS register itself with oximeter?
    // Register the Nexus server as a metric producer with `oximeter.
    // pub async fn register_as_producer(&self) {
    // self.apictx
    // .nexus
    // .register_as_producer(self.http_server_internal.local_addr())
    // .await;
    // }
}

/// Run an instance of the [Server].
pub async fn run_server(config: Config) -> Result<(), String> {
    use slog::Drain;
    let (drain, registration) = slog_dtrace::with_drain(
        config
            .log
            .to_logger("gateway")
            .map_err(|message| format!("initializing logger: {}", message))?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!());
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(msg);
    } else {
        debug!(log, "registered DTrace probes");
    }
    let rack_id = Uuid::new_v4();
    let server = Server::start(config, rack_id, &log).await?;
    // server.register_as_producer().await;
    server.wait_for_finish().await
}
