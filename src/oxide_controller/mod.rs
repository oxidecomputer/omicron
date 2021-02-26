/*!
 * Library interface to the Oxide Controller (OXC)
 */

mod config;
mod context;
mod controller_client;
mod http_entrypoints_external;
mod http_entrypoints_internal;
#[allow(clippy::module_inception)]
mod oxide_controller;
mod saga_interface;
mod sagas;

pub use config::ConfigController;
pub use context::ControllerServerContext;
pub use controller_client::ControllerClient;
pub use oxide_controller::OxideController;
pub use oxide_controller::OxideControllerTestInterfaces;

use http_entrypoints_external::controller_external_api;
use http_entrypoints_internal::controller_internal_api;

use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Run the OpenAPI generator for the external API, which emits the OpenAPI spec
 * to stdout.
 */
pub fn controller_run_openapi_external() -> Result<(), String> {
    controller_external_api()
        .openapi("Oxide Region API", "0.0.1")
        .description("API for interacting with the Oxide control plane")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

/**
 * Packages up an [`OxideController`], running both external and internal HTTP
 * API servers wired up to the controller
 */
pub struct OxideControllerServer {
    /** shared state used by API request handlers */
    pub apictx: Arc<ControllerServerContext>,
    /** dropshot server for external API */
    pub http_server_external:
        dropshot::HttpServer<Arc<ControllerServerContext>>,
    /** dropshot server for internal API */
    pub http_server_internal:
        dropshot::HttpServer<Arc<ControllerServerContext>>,
}

impl OxideControllerServer {
    /**
     * Start an OxideController server.
     */
    pub async fn start(
        config: &ConfigController,
        rack_id: &Uuid,
        log: &Logger,
    ) -> Result<OxideControllerServer, String> {
        info!(log, "setting up controller server");

        let ctxlog = log.new(o!("component" => "ControllerServerContext"));
        let apictx = ControllerServerContext::new(rack_id, ctxlog);

        let c1 = Arc::clone(&apictx);
        let http_server_starter_external = dropshot::HttpServerStarter::new(
            &config.dropshot_external,
            controller_external_api(),
            c1,
            &log.new(o!("component" => "dropshot_external")),
        )
        .map_err(|error| format!("initializing external server: {}", error))?;

        let c2 = Arc::clone(&apictx);
        let http_server_starter_internal = dropshot::HttpServerStarter::new(
            &config.dropshot_internal,
            controller_internal_api(),
            c2,
            &log.new(o!("component" => "dropshot_internal")),
        )
        .map_err(|error| format!("initializing internal server: {}", error))?;

        let http_server_external = http_server_starter_external.start();
        let http_server_internal = http_server_starter_internal.start();

        Ok(OxideControllerServer {
            apictx,
            http_server_external,
            http_server_internal,
        })
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
 * Run an instance of the `OxideControllerServer`.
 */
pub async fn controller_run_server(
    config: &ConfigController,
) -> Result<(), String> {
    let log = config
        .log
        .to_logger("oxide-controller")
        .map_err(|message| format!("initializing logger: {}", message))?;
    let rack_id = Uuid::new_v4();
    let server = OxideControllerServer::start(config, &rack_id, &log).await?;
    server.wait_for_finish().await
}
