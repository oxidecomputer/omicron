/*!
 * Library interface to the sled agent
 */

mod config;
mod http_entrypoints;
#[allow(clippy::module_inception)]
mod sled_agent;
mod sled_agent_client;

pub use config::ConfigSledAgent;
pub use config::SimMode;
pub use sled_agent_client::SledAgentClient;
pub use sled_agent_client::SledAgentTestInterfaces;

use crate::api_model::ApiSledAgentStartupInfo;
use crate::ControllerClient;
use sled_agent::SledAgent;
use slog::Logger;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/**
 * Delay time in milliseconds between attempts to notify OXC about a sled agent
 * startup
 */
const SLED_AGENT_NOTIFY_DELAY_MS: u64 = 5000;

/**
 * Packages up a [`SledAgent`], running the sled agent API under a Dropshot
 * server wired up to the sled agent
 */
pub struct SledAgentServer {
    /** underlying sled agent */
    pub sled_agent: Arc<SledAgent>,
    /** dropshot server for the API */
    pub http_server: dropshot::HttpServer,
    /** task handle for the dropshot server */
    join_handle: JoinHandle<Result<(), hyper::error::Error>>,
}

impl SledAgentServer {
    /**
     * Start a SledAgent server
     */
    pub async fn start(
        config: &ConfigSledAgent,
        log: &Logger,
    ) -> Result<SledAgentServer, String> {
        info!(log, "setting up sled agent server");

        let client_log = log.new(o!("component" => "ControllerClient"));
        let controller_client = Arc::new(ControllerClient::new(
            config.controller_address,
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
                .notify_sled_agent_online(config.id, ApiSledAgentStartupInfo {
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
            sled_agent,
            http_server,
            join_handle,
        })
    }

    /**
     * Wait for the given server to shut down
     *
     * Note that this doesn't initiate a graceful shutdown, so if you call this
     * immediately after calling `start()`, the program will block indefinitely
     * or until something else initiates a graceful shutdown.
     */
    pub async fn wait_for_finish(mut self) -> Result<(), String> {
        self.http_server.wait_for_shutdown(self.join_handle).await
    }
}

/**
 * Run an instance of the `SledAgentServer`
 */
pub async fn sa_run_server(config: &ConfigSledAgent) -> Result<(), String> {
    let log = config
        .log
        .to_logger("sled-agent")
        .map_err(|message| format!("initializing logger: {}", message))?;

    let server = SledAgentServer::start(config, &log).await?;
    server.wait_for_finish().await
}
