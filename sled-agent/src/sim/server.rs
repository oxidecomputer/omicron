// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use super::storage::PantryServer;
use crate::nexus::NexusClient;
use crucible_agent_client::types::State as RegionState;
use internal_dns_client::names::{ServiceName, AAAA, SRV};
use nexus_client::types as NexusTypes;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use slog::{info, Drain, Logger};
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::Arc;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    /// underlying sled agent
    pub sled_agent: Arc<SledAgent>,
    /// dropshot server for the API
    pub http_server: dropshot::HttpServer<Arc<SledAgent>>,
    /// simulated pantry server
    pub pantry_server: PantryServer,
    /// real internal dns server storage dir
    pub dns_server_storage_dir: tempfile::TempDir,
    /// real internal dns server
    pub dns_server: internal_dns::dns_server::Server,
    /// real internal dns dropshot server
    pub dns_dropshot_server:
        dropshot::HttpServer<Arc<internal_dns::dropshot_server::Context>>,
}

impl Server {
    /// Start a SledAgent server
    pub async fn start(
        config: &Config,
        log: &Logger,
    ) -> Result<(Server, NexusTypes::RackInitializationRequest), String> {
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
        let sled_agent = SledAgent::new_simulated_with_id(
            &config,
            sa_log,
            config.nexus_address,
            Arc::clone(&nexus_client),
        )
        .await;

        let dropshot_log = log.new(o!("component" => "dropshot"));
        let http_server = dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_api(),
            sled_agent.clone(),
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
            retry_policy_internal_service_aggressive(),
            notify_nexus,
            log_notification_failure,
        )
        .await
        .expect("Expected an infinite retry loop contacting Nexus");

        let mut datasets = vec![];
        // Create all the Zpools requested by the config, and allocate a single
        // Crucible dataset for each. This emulates the setup we expect to have
        // on the physical rack.
        for zpool in &config.storage.zpools {
            let zpool_id = uuid::Uuid::new_v4();
            sled_agent.create_zpool(zpool_id, zpool.size).await;
            let dataset_id = uuid::Uuid::new_v4();
            let address =
                sled_agent.create_crucible_dataset(zpool_id, dataset_id).await;

            datasets.push(NexusTypes::DatasetCreateRequest {
                zpool_id,
                dataset_id,
                request: NexusTypes::DatasetPutRequest {
                    address: address.to_string(),
                    kind: NexusTypes::DatasetKind::Crucible,
                },
            });

            // Whenever Nexus tries to allocate a region, it should complete
            // immediately. What efficiency!
            let crucible =
                sled_agent.get_crucible_dataset(zpool_id, dataset_id).await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
                .await;
        }

        // Create the simulated Pantry
        let pantry_server = PantryServer::new(
            log.new(o!("kind" => "pantry")),
            config.storage.ip,
            sled_agent.clone(),
        )
        .await;

        // Start the internal DNS server, insert the simulated Pantry DNS
        // record
        let dns_server_storage_dir =
            tempfile::tempdir().map_err(|e| e.to_string())?;

        let dns_server_config = internal_dns::Config {
            log: dropshot::ConfigLogging::StderrTerminal {
                level: dropshot::ConfigLoggingLevel::Trace,
            },
            dropshot: dropshot::ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                ..Default::default()
            },
            data: internal_dns::dns_data::Config {
                nmax_messages: 16,
                storage_path: dns_server_storage_dir
                    .path()
                    .to_string_lossy()
                    .to_string(),
            },
        };
        let dns_log = log.new(o!("kind" => "dns"));
        let zone = "control-plane.oxide.internal".to_string();
        let dns_address: SocketAddrV6 = "[::1]:0".parse().unwrap();

        let (dns_server, dns_dropshot_server) = internal_dns::start(
            dns_log,
            dns_server_config,
            zone,
            dns_address.into(),
        )
        .await
        .map_err(|e| e.to_string())?;

        // Insert SRV and AAAA record for Crucible Pantry
        let mut records: HashMap<_, Vec<(_, SocketAddrV6)>> = HashMap::new();
        records
            .entry(SRV::Service(ServiceName::CruciblePantry))
            .or_insert_with(Vec::new)
            .push((
                AAAA::Zone(pantry_server.server.app_private().id),
                match pantry_server.addr() {
                    SocketAddr::V6(v6) => v6,

                    SocketAddr::V4(_) => {
                        panic!("pantry address must be IPv6");
                    }
                },
            ));

        let dns_client =
            internal_dns_client::multiclient::Updater::new_from_addrs(
                vec![dns_dropshot_server.local_addr()],
                log.new(o!("kind" => "dns-client")),
            );

        dns_client
            .insert_dns_records(&records)
            .await
            .map_err(|e| e.to_string())?;

        let rack_init_request = NexusTypes::RackInitializationRequest {
            services: vec![],
            datasets,
        };

        Ok((
            Server {
                sled_agent,
                http_server,
                pantry_server,
                dns_server_storage_dir,
                dns_server,
                dns_dropshot_server,
            },
            rack_init_request,
        ))
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

async fn handoff_to_nexus(
    log: &Logger,
    config: &Config,
    request: &NexusTypes::RackInitializationRequest,
) -> Result<(), String> {
    let nexus_client = NexusClient::new(
        &format!("http://{}", config.nexus_address),
        log.new(o!("component" => "NexusClient")),
    );
    let rack_id = uuid::uuid!("c19a698f-c6f9-4a17-ae30-20d711b8f7dc");

    let notify_nexus = || async {
        nexus_client
            .rack_initialization_complete(&rack_id, &request)
            .await
            .map_err(BackoffError::transient)
    };
    let log_failure = |err, _| {
        info!(log, "Failed to handoff to nexus: {err}");
    };
    retry_notify(
        retry_policy_internal_service_aggressive(),
        notify_nexus,
        log_failure,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
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

    let (server, rack_init_request) = Server::start(config, &log).await?;
    info!(log, "sled agent started successfully");

    handoff_to_nexus(&log, &config, &rack_init_request).await?;
    info!(log, "Handoff to Nexus is complete");

    server.wait_for_finish().await
}
