/*
 * Library interface to the server controller mechanisms.
 */

mod config;
mod http_entrypoints;
mod server_controller;
mod server_controller_client;

pub use config::ConfigServerController;
pub use config::SimMode;
pub use http_entrypoints::sc_api;
pub use server_controller::ServerController;
pub use server_controller_client::ServerControllerClient;
pub use server_controller_client::ServerControllerTestInterfaces;

use crate::api_model::ApiServerStartupInfo;
use crate::ControllerClient;
use std::sync::Arc;

/* TODO-cleanup commonize with oxide-controller run_server() */
pub async fn run_server_controller_api_server(
    config: ConfigServerController,
) -> Result<(), String> {
    let log = config
        .log
        .to_logger("server-controller")
        .map_err(|message| format!("initializing logger: {}", message))?;
    info!(log, "starting server controller");

    let dropshot_log = log.new(o!("component" => "dropshot"));
    let sc_log = log.new(o!(
        "component" => "server_controller",
        "server" => config.id.clone().to_string()
    ));

    let client_log = log.new(o!("component" => "controller_client"));
    let controller_client = Arc::new(ControllerClient::new(
        config.controller_address.clone(),
        client_log,
    ));

    let sc = Arc::new(ServerController::new_simulated_with_id(
        &config.id,
        config.sim_mode,
        sc_log,
        Arc::clone(&controller_client),
    ));

    let my_address = config.dropshot.bind_address.clone();
    let mut http_server = dropshot::HttpServer::new(
        &config.dropshot,
        http_entrypoints::sc_api(),
        sc,
        &dropshot_log,
    )
    .map_err(|error| format!("initializing server: {}", error))?;

    let server_handle = http_server.run();

    /*
     * TODO this should happen continuously until it succeeds.
     */
    controller_client
        .notify_server_online(config.id.clone(), ApiServerStartupInfo {
            sc_address: my_address,
        })
        .await
        .unwrap();

    let join_handle = server_handle.await;
    let server_result = join_handle
        .map_err(|error| format!("waiting for server: {}", error))?;
    server_result.map_err(|error| format!("server stopped: {}", error))
}
