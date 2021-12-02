// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::config::Config;
use super::http_entrypoints::ba_api as http_api;
use std::sync::Arc;

/// Wraps a [Agent] object, and provides helper methods for exposing it
/// via an HTTP interface.
pub struct Server {
    bootstrap_agent: Arc<Agent>,
    http_server: dropshot::HttpServer<Arc<Agent>>,
}

impl Server {
    pub async fn start(config: &Config) -> Result<Self, String> {
        let log = config
            .log
            .to_logger("bootstrap-agent")
            .map_err(|message| format!("initializing logger: {}", message))?;
        info!(log, "setting up bootstrap agent server");

        let ba_log = log.new(o!(
            "component" => "Agent",
            "server" => config.id.clone().to_string()
        ));
        let bootstrap_agent = Arc::new(
            Agent::new(ba_log, &config.rack_secret_dir)
                .map_err(|e| e.to_string())?,
        );

        let ba = Arc::clone(&bootstrap_agent);
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let http_server = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_api(),
            ba,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        let server = Server { bootstrap_agent, http_server };

        // Initialize the bootstrap agent *after* the server has started.
        // This ordering allows the bootstrap agent to communicate with
        // other bootstrap agents on the rack during the initialization
        // process.
        if let Err(e) = server.bootstrap_agent.initialize().await {
            let _ = server.close().await;
            return Err(e.to_string());
        }

        Ok(server)
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        self.http_server.await
    }

    pub async fn close(self) -> Result<(), String> {
        self.http_server.close().await
    }
}

pub fn run_openapi() -> Result<(), String> {
    http_api()
        .openapi("Oxide Bootstrap Agent API", "0.0.1")
        .description("API for interacting with bootstrapping agents")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}
