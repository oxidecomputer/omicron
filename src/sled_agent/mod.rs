/*!
 * Library interface to the sled agent mechanisms
 */

mod config;
mod http_entrypoints;
mod sled_agent;
mod sled_agent_client;

pub use config::ConfigSledAgent;
pub use config::SimMode;
pub use sled_agent_client::SledAgentClient;
pub use sled_agent_client::SledAgentTestInterfaces;

use crate::api_model::ApiServerStartupInfo;
use crate::ControllerClient;
use sled_agent::SledAgent;
use slog::Logger;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

const SLED_AGENT_NOTIFY_DELAY_MS: u64 = 5000;

pub struct SledAgentServer {
    pub sled_agent: Arc<SledAgent>,
    pub http_server: dropshot::HttpServer,
    join_handle: JoinHandle<Result<(), hyper::error::Error>>,
}

impl SledAgentServer {
    pub async fn start(
        config: &ConfigSledAgent,
        log: &Logger,
    ) -> Result<SledAgentServer, String> {
        info!(log, "setting up sled agent server");

        let client_log = log.new(o!("component" => "ControllerClient"));
        let controller_client = Arc::new(ControllerClient::new(
            config.controller_address.clone(),
            client_log,
        ));

        let sa_log = log.new(o!(
            "component" => "SledAgent",
            "server" => config.id.clone().to_string()
        ));
        let sled_agent = Arc::new(SledAgent::new_simulated_with_id(
            &config.id,
            config.sim_mode,
            sa_log,
            Arc::clone(&controller_client),
        ));

        let sa = Arc::clone(&sled_agent);
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let mut http_server = dropshot::HttpServer::new(
            &config.dropshot,
            http_entrypoints::sa_api(),
            sa,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?;

        let join_handle = http_server.run();

        /*
         * Notify the control plane that we're up, and continue trying this
         * until it succeeds.
         * TODO-robustness if this returns a 400 error, we probably want to
         * stop.
         * TODO-robustness we should probably use randomized, capped exponential
         * backoff here, as the control plane may be overloaded (e.g., cold
         * start of rack).
         */
        loop {
            debug!(log, "contacting server controller");
            let result = controller_client
                .notify_server_online(config.id.clone(), ApiServerStartupInfo {
                    sa_address: http_server.local_addr(),
                })
                .await;
            match result {
                Ok(()) => break,
                Err(error) => {
                    warn!(log, "failed to contact controller (will retry)";
                        "error" => ?error);
                    tokio::time::delay_for(Duration::from_millis(
                        SLED_AGENT_NOTIFY_DELAY_MS,
                    ))
                    .await;
                }
            }
        }

        info!(log, "contacted server controller");

        Ok(SledAgentServer {
            sled_agent: sled_agent,
            http_server,
            join_handle,
        })
    }

    pub async fn wait_for_finish(mut self) -> Result<(), String> {
        self.http_server.wait_for_shutdown(self.join_handle).await
    }
}

pub async fn sa_run_server(config: &ConfigSledAgent) -> Result<(), String> {
    let log = config
        .log
        .to_logger("sled-agent")
        .map_err(|message| format!("initializing logger: {}", message))?;

    let server = SledAgentServer::start(config, &log).await?;
    server.wait_for_finish().await
}
