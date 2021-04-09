/*!
 * Library interface to the Nexus, the heart of the control plane
 */

mod client;
mod config;
mod context;
mod datastore;
pub mod db; // Public only for some documentation examples
mod http_entrypoints_external;
mod http_entrypoints_internal;
#[allow(clippy::module_inception)]
mod nexus;
mod saga_interface;
mod sagas;

pub use client::Client;
pub use config::Config;
pub use context::ServerContext;
pub use db::PostgresConfigWithUrl;
pub use nexus::Nexus;
pub use nexus::TestInterfaces;

use http_entrypoints_external::external_api;
use http_entrypoints_internal::internal_api;

use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Run the OpenAPI generator for the external API, which emits the OpenAPI spec
 * to stdout.
 */
pub fn run_openapi_external() -> Result<(), String> {
    external_api()
        .openapi("Oxide Region API", "0.0.1")
        .description("API for interacting with the Oxide control plane")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

/**
 * Packages up a [`Nexus`], running both external and internal HTTP API servers
 * wired up to Nexus
 */
pub struct Server {
    /** shared state used by API request handlers */
    pub apictx: Arc<ServerContext>,
    /** dropshot server for external API */
    pub http_server_external: dropshot::HttpServer<Arc<ServerContext>>,
    /** dropshot server for internal API */
    pub http_server_internal: dropshot::HttpServer<Arc<ServerContext>>,
}

impl Server {
    /**
     * Start a nexus server.
     */
    pub async fn start(
        config: &Config,
        rack_id: &Uuid,
        log: &Logger,
    ) -> Result<Server, String> {
        info!(log, "setting up nexus server");

        let ctxlog = log.new(o!("component" => "ServerContext"));
        let pool = db::Pool::new(&config.database);

        let apictx = ServerContext::new(rack_id, ctxlog, pool);

        let c1 = Arc::clone(&apictx);
        let http_server_starter_external = dropshot::HttpServerStarter::new(
            &config.dropshot_external,
            external_api(),
            c1,
            &log.new(o!("component" => "dropshot_external")),
        )
        .map_err(|error| format!("initializing external server: {}", error))?;

        let c2 = Arc::clone(&apictx);
        let http_server_starter_internal = dropshot::HttpServerStarter::new(
            &config.dropshot_internal,
            internal_api(),
            c2,
            &log.new(o!("component" => "dropshot_internal")),
        )
        .map_err(|error| format!("initializing internal server: {}", error))?;

        let http_server_external = http_server_starter_external.start();
        let http_server_internal = http_server_starter_internal.start();

        Ok(Server { apictx, http_server_external, http_server_internal })
    }

    /**
     * Wait for the given server to shut down
     *
     * Note that this doesn't initiate a graceful shutdown, so if you call this
     * immediately after calling `start()`, the program will block indefinitely
     * or until something else initiates a graceful shutdown.
     */
    pub async fn wait_for_finish(self) -> Result<(), String> {
        let result_external = self.http_server_external.await;
        let result_internal = self.http_server_internal.await;

        match (result_external, result_internal) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error_external), Err(error_internal)) => Err(format!(
                "errors from both external and internal HTTP \
                 servers(external: \"{}\", internal: \"{}\"",
                error_external, error_internal
            )),
            (Err(error_external), Ok(())) => {
                Err(format!("external server: {}", error_external))
            }
            (Ok(()), Err(error_internal)) => {
                Err(format!("internal server: {}", error_internal))
            }
        }
    }
}

/**
 * Run an instance of the [Server].
 */
pub async fn run_server(config: &Config) -> Result<(), String> {
    let log = config
        .log
        .to_logger("nexus")
        .map_err(|message| format!("initializing logger: {}", message))?;
    let rack_id = Uuid::new_v4();
    let server = Server::start(config, &rack_id, &log).await?;
    server.wait_for_finish().await
}
