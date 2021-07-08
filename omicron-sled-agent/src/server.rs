//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use omicron_common::model::ApiSledAgentStartupInfo;
use omicron_common::NexusClient;
use slog::Logger;
use std::sync::Arc;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    /// Dropshot server for the API.
    http_server: dropshot::HttpServer<SledAgent>,
}

impl Server {
    /// Starts a SledAgent server
    pub async fn start(
        config: &Config,
        log: &Logger,
    ) -> Result<Server, String> {
        info!(log, "setting up sled agent server");

        let client_log = log.new(o!("component" => "NexusClient"));
        let nexus_client =
            Arc::new(NexusClient::new(config.nexus_address, client_log));

        let sa_log = log.new(o!(
            "component" => "SledAgent",
            "server" => config.id.clone().to_string()
        ));
        let sled_agent =
            SledAgent::new(&config.id, sa_log, nexus_client.clone())
                .map_err(|e| e.to_string())?;

        let dropshot_log = log.new(o!("component" => "dropshot"));
        let http_server = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_api(),
            sled_agent,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        // Notify the control plane that we're up, and continue trying this
        // until it succeeds. We retry with an randomized, capped exponential
        // backoff.
        //
        // TODO-robustness if this returns a 400 error, we probably want to
        // return a permanent error from the `notify_nexus` closure.
        let sa_address = http_server.local_addr();
        let notify_nexus = || async {
            debug!(log, "contacting server nexus");
            nexus_client
                .notify_sled_agent_online(
                    config.id,
                    ApiSledAgentStartupInfo { sa_address },
                )
                .await
                .map_err(BackoffError::Transient)
        };
        let log_notification_failure = |error, delay| {
            warn!(log, "failed to contact nexus, will retry in {:?}", delay;
                "error" => ?error);
        };
        retry_notify(
            internal_service_policy(),
            notify_nexus,
            log_notification_failure,
        )
        .await
        .expect("Expected an infinite retry loop contacting Nexus");
        Ok(Server { http_server })
    }

    /// Wait for the given server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(self) -> Result<(), String> {
        self.http_server.await
    }
}

/// Run an instance of the `Server`
pub async fn run_server(config: &Config) -> Result<(), String> {
    let log = config
        .log
        .to_logger("sled-agent")
        .map_err(|message| format!("initializing logger: {}", message))?;

    let server = Server::start(config, &log).await?;
    info!(log, "sled agent started successfully");
    server.wait_for_finish().await
}
