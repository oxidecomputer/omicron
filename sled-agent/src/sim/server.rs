// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use crate::nexus::NexusClient;
use crucible_agent_client::types::State as RegionState;

use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use slog::{Drain, Logger};
use std::sync::Arc;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    /// underlying sled agent
    pub sled_agent: Arc<SledAgent>,
    /// dropshot server for the API
    pub http_server: dropshot::HttpServer<Arc<SledAgent>>,
}

impl Server {
    /// Start a SledAgent server
    pub async fn start(
        config: &Config,
        log: &Logger,
    ) -> Result<Server, String> {
        info!(log, "setting up sled agent server");

        let client_log = log.new(o!("component" => "NexusClient"));
        let nexus_client = Arc::new(NexusClient::new(
            &format!("http://{}", config.nexus_address),
            client_log,
        ));

        let sa_log = log.new(o!(
            "component" => "SledAgent",
            "server" => config.id.clone().to_string()
        ));
        let sled_agent = Arc::new(SledAgent::new_simulated_with_id(
            &config,
            sa_log,
            config.nexus_address,
            Arc::clone(&nexus_client),
        ));

        let sa = Arc::clone(&sled_agent);
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let http_server = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_api(),
            sa,
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
            (nexus_client
                .sled_agent_put(
                    &config.id,
                    &nexus_client::types::SledAgentStartupInfo {
                        sa_address: sa_address.to_string(),
                        role: nexus_client::types::SledRole::Gimlet,
                    },
                )
                .await)
                .map_err(BackoffError::transient)
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

        // Create all the Zpools requested by the config, and allocate a single
        // Crucible dataset for each. This emulates the setup we expect to have
        // on the physical rack.
        for zpool in &config.storage.zpools {
            let zpool_id = uuid::Uuid::new_v4();
            sled_agent.create_zpool(zpool_id, zpool.size).await;
            let dataset_id = uuid::Uuid::new_v4();
            sled_agent.create_crucible_dataset(zpool_id, dataset_id).await;

            // Whenever Nexus tries to allocate a region, it should complete
            // immediately. What efficiency!
            let crucible =
                sled_agent.get_crucible_dataset(zpool_id, dataset_id).await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
                .await;
        }

        Ok(Server { sled_agent, http_server })
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
    let (drain, registration) = slog_dtrace::with_drain(
        config
            .log
            .to_logger("sled-agent")
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

    let server = Server::start(config, &log).await?;
    info!(log, "sled agent started successfully");
    server.wait_for_finish().await
}
