/*
 * Library interface to the server controller mechanisms.
 */

mod config;
mod http_entrypoints;
mod server_controller;
mod server_controller_client;

pub use config::ConfigServerController;
pub use config::SimMode;
pub use server_controller_client::ServerControllerClient;
pub use server_controller_client::ServerControllerTestInterfaces;

use crate::api_model::ApiServerStartupInfo;
use crate::ControllerClient;
use http_entrypoints::sc_api;
use server_controller::ServerController;
use slog::Logger;
use std::sync::Arc;
use tokio::task::JoinHandle;

/* TODO-cleanup commonize with OxideControllerServer? */
pub struct ServerControllerServer {
    pub server_controller: Arc<ServerController>,
    pub http_server: dropshot::HttpServer,
    join_handle: JoinHandle<Result<(), hyper::error::Error>>,
}

impl ServerControllerServer {
    pub async fn start(
        config: &ConfigServerController,
        log: &Logger,
    ) -> Result<ServerControllerServer, String> {
        info!(log, "setting up server controller server");

        let client_log = log.new(o!("component" => "ControllerClient"));
        let controller_client = Arc::new(ControllerClient::new(
            config.controller_address.clone(),
            client_log,
        ));

        let sc_log = log.new(o!(
            "component" => "ServerController",
            "server" => config.id.clone().to_string()
        ));
        let server_controller =
            Arc::new(ServerController::new_simulated_with_id(
                &config.id,
                config.sim_mode,
                sc_log,
                Arc::clone(&controller_client),
            ));

        let sc = Arc::clone(&server_controller);
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let mut http_server = dropshot::HttpServer::new(
            &config.dropshot,
            http_entrypoints::sc_api(),
            sc,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?;

        let join_handle = http_server.run();

        /*
         * TODO this should happen continuously until it succeeds.
         */
        controller_client
            .notify_server_online(config.id.clone(), ApiServerStartupInfo {
                sc_address: http_server.local_addr(),
            })
            .await
            .unwrap();

        Ok(ServerControllerServer {
            server_controller,
            http_server,
            join_handle,
        })
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        let server_result = self
            .join_handle
            .await
            .map_err(|error| format!("waiting for server: {}", error))?;
        server_result.map_err(|error| format!("server stopped: {}", error))
    }
}

pub async fn sc_run_server(
    config: &ConfigServerController,
) -> Result<(), String> {
    let log = config
        .log
        .to_logger("server-controller")
        .map_err(|message| format!("initializing logger: {}", message))?;

    let server = ServerControllerServer::start(config, &log).await?;
    server.wait_for_finish().await
}
