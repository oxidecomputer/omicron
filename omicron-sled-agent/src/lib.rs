/*!
* Library interface to the sled agent
 */

/*
 * We only use rustdoc for internal documentation, including private items, so
 * it's expected that we'll have links to private items in the docs.
 */
#![allow(private_intra_doc_links)]
/* Clippy's style lints are useful, but not worth running automatically. */
#![allow(clippy::style)]

mod config;
mod http_entrypoints;
mod sim;
mod sled_agent;

pub use config::Config;
pub use config::SimMode;

use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use omicron_common::model::ApiSledAgentStartupInfo;
use omicron_common::NexusClient;
use sled_agent::SledAgent;
use slog::Logger;
use std::sync::Arc;

#[macro_use]
extern crate slog;

/**
 * Packages up a [`SledAgent`], running the sled agent API under a Dropshot
 * server wired up to the sled agent
 */
pub struct Server {
    /** underlying sled agent */
    pub sled_agent: Arc<SledAgent>,
    /** dropshot server for the API */
    pub http_server: dropshot::HttpServer<Arc<SledAgent>>,
}

impl Server {
    /**
     * Start a SledAgent server
     */
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
        let sled_agent = Arc::new(SledAgent::new_simulated_with_id(
            &config.id,
            config.sim_mode,
            sa_log,
            Arc::clone(&nexus_client),
        ));

        let sa = Arc::clone(&sled_agent);
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let http_server = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_entrypoints::api(),
            sa,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        /*
         * Notify the control plane that we're up, and continue trying this
         * until it succeeds. We retry with an randomized, capped exponential
         * backoff.
         *
         * TODO-robustness if this returns a 400 error, we probably want to
         * return a permanent error from the `notify_nexus` closure.
         */
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
        Ok(Server { sled_agent, http_server })
    }

    /**
     * Wait for the given server to shut down
     *
     * Note that this doesn't initiate a graceful shutdown, so if you call this
     * immediately after calling `start()`, the program will block indefinitely
     * or until something else initiates a graceful shutdown.
     */
    pub async fn wait_for_finish(self) -> Result<(), String> {
        self.http_server.await
    }
}

/**
 * Run an instance of the `Server`
 */
pub async fn run_server(config: &Config) -> Result<(), String> {
    let log = config
        .log
        .to_logger("sled-agent")
        .map_err(|message| format!("initializing logger: {}", message))?;

    let server = Server::start(config, &log).await?;
    server.wait_for_finish().await
}
