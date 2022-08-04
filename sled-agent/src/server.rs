// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use crate::bootstrap::params::SledAgentRequest;
use crate::nexus::LazyNexusClient;
use omicron_common::backoff::{
    internal_service_policy_with_max, retry_notify, BackoffError,
};
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
use uuid::Uuid;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    /// Dropshot server for the API.
    http_server: dropshot::HttpServer<SledAgent>,
    _nexus_notifier_handle: tokio::task::JoinHandle<()>,
}

impl Server {
    pub fn address(&self) -> SocketAddr {
        self.http_server.local_addr()
    }

    pub fn id(&self) -> Uuid {
        self.http_server.app_private().id()
    }

    /// Starts a SledAgent server
    pub async fn start(
        config: &Config,
        log: Logger,
        sled_id: Uuid,
        addr: SocketAddrV6,
        is_scrimlet: bool,
        request: SledAgentRequest,
    ) -> Result<Server, String> {
        info!(log, "setting up sled agent server");

        let client_log = log.new(o!("component" => "NexusClient"));

        let lazy_nexus_client = LazyNexusClient::new(client_log, *addr.ip())
            .map_err(|e| e.to_string())?;

        let sled_agent = SledAgent::new(
            &config,
            log.clone(),
            lazy_nexus_client.clone(),
            sled_id,
            addr,
            request,
        )
        .await
        .map_err(|e| e.to_string())?;

        let mut dropshot_config = dropshot::ConfigDropshot::default();
        dropshot_config.request_body_max_bytes = 1024 * 1024;
        dropshot_config.bind_address = SocketAddr::V6(addr);
        let dropshot_log = log.new(o!("component" => "dropshot (SledAgent)"));
        let http_server = dropshot::HttpServerStarter::new(
            &dropshot_config,
            http_api(),
            sled_agent,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        let sled_address = http_server.local_addr();
        let nexus_notifier_handle = tokio::task::spawn(async move {
            // Notify the control plane that we're up, and continue trying this
            // until it succeeds. We retry with an randomized, capped exponential
            // backoff.
            //
            // TODO-robustness if this returns a 400 error, we probably want to
            // return a permanent error from the `notify_nexus` closure.
            let notify_nexus = || async {
                info!(
                    log,
                    "contacting server nexus, registering sled: {}", sled_id
                );
                let role = if is_scrimlet {
                    nexus_client::types::SledRole::Scrimlet
                } else {
                    nexus_client::types::SledRole::Gimlet
                };

                let nexus_client = lazy_nexus_client
                    .get()
                    .await
                    .map_err(|err| BackoffError::transient(err.to_string()))?;
                nexus_client
                    .sled_agent_put(
                        &sled_id,
                        &nexus_client::types::SledAgentStartupInfo {
                            sa_address: sled_address.to_string(),
                            role,
                        },
                    )
                    .await
                    .map_err(|err| BackoffError::transient(err.to_string()))
            };
            let log_notification_failure = |err, delay| {
                warn!(
                    log,
                    "failed to notify nexus about sled agent: {}, will retry in {:?}", err, delay;
                );
            };
            retry_notify(
                internal_service_policy_with_max(
                    std::time::Duration::from_secs(1),
                ),
                notify_nexus,
                log_notification_failure,
            )
            .await
            .expect("Expected an infinite retry loop contacting Nexus");
        });
        Ok(Server {
            http_server,
            _nexus_notifier_handle: nexus_notifier_handle,
        })
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

/// Runs the OpenAPI generator, emitting the spec to stdout.
pub fn run_openapi() -> Result<(), String> {
    http_api()
        .openapi("Oxide Sled Agent API", "0.0.1")
        .description("API for interacting with individual sleds")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}
