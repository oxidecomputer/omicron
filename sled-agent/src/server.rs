// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use crate::bootstrap::params::StartSledAgentRequest;
use crate::long_running_tasks::LongRunningTaskHandles;
use crate::nexus::NexusClientWithResolver;
use crate::services::ServiceManager;
use internal_dns::resolver::Resolver;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    /// Dropshot server for the API.
    http_server: dropshot::HttpServer<SledAgent>,
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
        request: StartSledAgentRequest,
        long_running_tasks_handles: LongRunningTaskHandles,
        services: ServiceManager,
    ) -> Result<Server, String> {
        info!(log, "setting up sled agent server");

        let sled_address = request.sled_address();
        let resolver = Arc::new(
            Resolver::new_from_ip(
                log.new(o!("component" => "DnsResolver")),
                *sled_address.ip(),
            )
            .map_err(|e| e.to_string())?,
        );

        let nexus_client = NexusClientWithResolver::new(&log, resolver)
            .map_err(|e| e.to_string())?;

        let sled_agent = SledAgent::new(
            &config,
            log.clone(),
            nexus_client,
            request,
            services,
            long_running_tasks_handles,
        )
        .await
        .map_err(|e| e.to_string())?;

        let dropshot_config = dropshot::ConfigDropshot {
            bind_address: SocketAddr::V6(sled_address),
            ..config.dropshot
        };
        let dropshot_log = log.new(o!("component" => "dropshot (SledAgent)"));
        let http_server = dropshot::HttpServerStarter::new(
            &dropshot_config,
            http_api(),
            sled_agent,
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        Ok(Server { http_server })
    }

    pub(crate) fn sled_agent(&self) -> &SledAgent {
        self.http_server.app_private()
    }

    /// Wait for the given server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(&self) -> Result<(), String> {
        self.http_server.wait_for_shutdown().await
    }

    pub async fn close(self) -> Result<(), String> {
        self.http_server.close().await
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
