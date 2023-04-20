// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use super::storage::PantryServer;
use crate::nexus::d2n_params;
use crate::nexus::NexusClient;
use anyhow::Context;
use crucible_agent_client::types::State as RegionState;
use internal_dns::ServiceName;
use nexus_client::types as NexusTypes;
use nexus_client::types::{IpRange, Ipv4Range, Ipv6Range};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use slog::{info, Drain, Logger};
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

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
    pub dns_server: dns_server::dns_server::ServerHandle,
    /// real internal dns dropshot server
    pub dns_dropshot_server:
        dropshot::HttpServer<dns_server::http_server::Context>,
}

impl Server {
    /// Start a SledAgent server
    pub async fn start(
        config: &Config,
        log: &Logger,
        rss_args: &RssArgs,
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
                    &NexusTypes::SledAgentStartupInfo {
                        sa_address: sa_address.to_string(),
                        role: NexusTypes::SledRole::Gimlet,
                        baseboard: NexusTypes::Baseboard {
                            identifier: String::from("Unknown"),
                            model: String::from("Unknown"),
                            revision: 0,
                        },
                        usable_hardware_threads: config
                            .hardware
                            .hardware_threads,
                        usable_physical_ram: NexusTypes::ByteCount::try_from(
                            config.hardware.physical_ram,
                        )
                        .unwrap(),
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
            let vendor = "synthetic-vendor".to_string();
            let serial = format!("synthetic-serial-{zpool_id}");
            let model = "synthetic-model".to_string();
            sled_agent
                .create_external_physical_disk(
                    vendor.clone(),
                    serial.clone(),
                    model.clone(),
                )
                .await;

            sled_agent
                .create_zpool(zpool_id, vendor, serial, model, zpool.size)
                .await;
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

        let dns_log = log.new(o!("kind" => "dns"));

        let store = dns_server::storage::Store::new(
            log.new(o!("component" => "store")),
            &dns_server::storage::Config {
                keep_old_generations: 3,
                storage_path: dns_server_storage_dir
                    .path()
                    .to_string_lossy()
                    .to_string()
                    .into(),
            },
        )
        .context("initializing DNS storage")
        .map_err(|e| e.to_string())?;

        let (dns_server, dns_dropshot_server) = dns_server::start_servers(
            dns_log,
            store,
            &dns_server::dns_server::Config {
                bind_address: "[::1]:0".parse().unwrap(),
            },
            &dropshot::ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;

        // Insert SRV and AAAA record for Crucible Pantry
        let mut dns = internal_dns::DnsConfigBuilder::new();
        let pantry_zone_id = pantry_server.server.app_private().id;
        let pantry_addr = match pantry_server.addr() {
            SocketAddr::V6(v6) => v6,
            SocketAddr::V4(_) => {
                panic!("pantry address must be IPv6");
            }
        };
        let pantry_zone = dns
            .host_zone(pantry_zone_id, *pantry_addr.ip())
            .expect("failed to set up DNS");
        dns.service_backend_zone(
            ServiceName::CruciblePantry,
            &pantry_zone,
            pantry_addr.port(),
        )
        .expect("failed to set up DNS");

        let dns_config = dns.build();
        let dns_config_client = dns_service_client::Client::new(
            &format!("http://{}", dns_dropshot_server.local_addr()),
            log.clone(),
        );
        dns_config_client
            .dns_config_put(&dns_config)
            .await
            .context("initializing DNS")
            .map_err(|e| e.to_string())?;

        // Record the internal DNS server as though RSS had provisioned it so
        // that Nexus knows about it.
        let dns_bound = match dns_server.local_address() {
            SocketAddr::V4(_) => panic!("did not expect v4 address"),
            SocketAddr::V6(a) => *a,
        };
        let http_bound = match dns_dropshot_server.local_addr() {
            SocketAddr::V4(_) => panic!("did not expect v4 address"),
            SocketAddr::V6(a) => a,
        };
        let mut services = vec![
            NexusTypes::ServicePutRequest {
                address: dns_bound.to_string(),
                kind: NexusTypes::ServiceKind::InternalDns,
                service_id: Uuid::new_v4(),
                sled_id: config.id,
            },
            NexusTypes::ServicePutRequest {
                address: http_bound.to_string(),
                kind: NexusTypes::ServiceKind::InternalDnsConfig,
                service_id: Uuid::new_v4(),
                sled_id: config.id,
            },
        ];

        let mut internal_services_ip_pool_ranges = vec![];
        if let Some(nexus_external_addr) = rss_args.nexus_external_addr {
            let ip = nexus_external_addr.ip();

            services.push(NexusTypes::ServicePutRequest {
                address: config.nexus_address.to_string(),
                kind: NexusTypes::ServiceKind::Nexus { external_address: ip },
                service_id: Uuid::new_v4(),
                sled_id: config.id,
            });

            internal_services_ip_pool_ranges.push(match ip {
                IpAddr::V4(addr) => {
                    IpRange::V4(Ipv4Range { first: addr, last: addr })
                }
                IpAddr::V6(addr) => {
                    IpRange::V6(Ipv6Range { first: addr, last: addr })
                }
            });
        }

        if let Some(external_dns_internal_addr) =
            rss_args.external_dns_internal_addr
        {
            services.push(NexusTypes::ServicePutRequest {
                address: external_dns_internal_addr.to_string(),
                kind: NexusTypes::ServiceKind::ExternalDnsConfig,
                service_id: Uuid::new_v4(),
                sled_id: config.id,
            });
        }

        let rack_init_request = NexusTypes::RackInitializationRequest {
            services,
            datasets,
            internal_services_ip_pool_ranges,
            certs: vec![],
            internal_dns_zone_config: d2n_params(&dns_config),
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

/// RSS-related arguments for the simulated sled agent
#[derive(Default)]
pub struct RssArgs {
    /// Specify the external address of Nexus so that we can include it in
    /// external DNS
    pub nexus_external_addr: Option<SocketAddr>,
    /// Specify the (internal) address of an external DNS server so that Nexus
    /// will know about it and keep it up to date
    pub external_dns_internal_addr: Option<SocketAddrV6>,
}

/// Run an instance of the `Server`
pub async fn run_server(
    config: &Config,
    rss_args: &RssArgs,
) -> Result<(), String> {
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

    let (server, rack_init_request) =
        Server::start(config, &log, rss_args).await?;
    info!(log, "sled agent started successfully");

    handoff_to_nexus(&log, &config, &rack_init_request).await?;
    info!(log, "Handoff to Nexus is complete");

    server.wait_for_finish().await
}
