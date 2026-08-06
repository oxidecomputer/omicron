// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use crate::long_running_tasks::LongRunningTaskHandles;
use crate::nexus::make_nexus_client;
use crate::services::ServiceManager;
use crate::sush::SushHandles;
use internal_dns_resolver::Resolver;
use omicron_uuid_kinds::SledUuid;
use sled_agent_config_reconciler::ConfigReconcilerSpawnToken;
use sled_agent_types::sled::StartSledAgentRequest;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddr;
use std::sync::Arc;
use sush_server::JobManager;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    /// Dropshot server for the API.
    http_server: dropshot::HttpServer<SledAgent>,

    /// The Support Shell's task handles and API server, if it is running.
    sush: Option<(SushHandles, dropshot::HttpServer<Arc<JobManager>>)>,
}

impl Server {
    pub fn address(&self) -> SocketAddr {
        self.http_server.local_addr()
    }

    pub fn id(&self) -> SledUuid {
        self.http_server.app_private().id()
    }

    /// Starts a SledAgent server
    pub async fn start(
        config: &Config,
        log: Logger,
        request: StartSledAgentRequest,
        long_running_tasks_handles: LongRunningTaskHandles,
        config_reconciler_spawn_token: ConfigReconcilerSpawnToken,
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

        let nexus_client = make_nexus_client(&log, resolver);

        let sush_handles = long_running_tasks_handles.sush.clone();
        let sled_agent = SledAgent::new(
            &config,
            log.clone(),
            nexus_client,
            request,
            services,
            long_running_tasks_handles,
            config_reconciler_spawn_token,
        )
        .await
        .map_err(|e| InlineErrorChain::new(&e).to_string())?;

        let dropshot_config = dropshot::ConfigDropshot {
            bind_address: SocketAddr::V6(sled_address),
            ..config.dropshot.clone()
        };
        let dropshot_log = log.new(o!("component" => "dropshot (SledAgent)"));

        let http_server =
            dropshot::ServerBuilder::new(http_api(), sled_agent, dropshot_log)
                .config(dropshot_config)
                .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
                    dropshot::ClientSpecifiesVersionInHeader::new(
                        omicron_common::api::VERSION_HEADER,
                        sled_agent_api::latest_version(),
                    ),
                )))
                .start()
                .map_err(|error| format!("initializing server: {}", error))?;

        // Now that we know our underlay address, sush can serve its API.
        let sush = sush_handles.and_then(|handles| {
            match handles.start_api(*sled_address.ip()) {
                Ok(server) => Some((handles, server)),
                Err(error) => {
                    warn!(log, "failed to start sush server"; "error" => error);
                    None
                }
            }
        });

        Ok(Server { http_server, sush })
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
        let sush = match self.sush {
            Some((handles, server)) => {
                handles.shutdown();
                server.close().await
            }
            None => Ok(()),
        };
        self.http_server.close().await.and(sush)
    }
}
