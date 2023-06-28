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
use anyhow::anyhow;
use crucible_agent_client::types::State as RegionState;
use internal_dns::ServiceName;
use nexus_client::types as NexusTypes;
use nexus_client::types::{IpRange, Ipv4Range, Ipv6Range};
use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::api::external::MacAddr;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use slog::{info, Drain, Logger};
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

/// Packages up a [`SledAgent`], running the sled agent API under a Dropshot
/// server wired up to the sled agent
pub struct Server {
    // Configuration used to start server
    config: Config,
    log: Logger,

    /// underlying sled agent
    pub sled_agent: Arc<SledAgent>,
    /// dropshot server for the API
    pub http_server: dropshot::HttpServer<Arc<SledAgent>>,
    /// simulated pantry server
    pub pantry_server: Option<PantryServer>,
}

impl Server {
    pub async fn start(
        config: &Config,
        log: &Logger,
    ) -> Result<Server, anyhow::Error> {
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
        .map_err(|error| anyhow!("initializing server: {}", error))?
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
                .sled_agent_put(
                    &config.id,
                    &NexusTypes::SledAgentStartupInfo {
                        sa_address: sa_address.to_string(),
                        role: NexusTypes::SledRole::Gimlet,
                        baseboard: NexusTypes::Baseboard {
                            serial_number: format!(
                                "Simulated sled {}",
                                config.id
                            ),
                            part_number: String::from("Unknown"),
                            revision: 0,
                        },
                        usable_hardware_threads: config
                            .hardware
                            .hardware_threads,
                        usable_physical_ram: NexusTypes::ByteCount::try_from(
                            config.hardware.physical_ram,
                        )
                        .unwrap(),
                        reservoir_size: NexusTypes::ByteCount::try_from(
                            config.hardware.reservoir_ram,
                        )
                        .unwrap(),
                    },
                )
                .await
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
            let zpool_id = Uuid::new_v4();
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
            let dataset_id = Uuid::new_v4();
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

        Ok(Server {
            config: config.clone(),
            log: log.clone(),
            sled_agent,
            http_server,
            pantry_server: None,
        })
    }

    /// Starts the pantry service
    pub async fn start_pantry(&mut self) -> &PantryServer {
        // Create the simulated Pantry
        let pantry_server = PantryServer::new(
            self.log.new(o!("kind" => "pantry")),
            self.config.storage.ip,
            self.sled_agent.clone(),
        )
        .await;
        self.pantry_server = Some(pantry_server);
        self.pantry_server.as_ref().unwrap()
    }

    /// Wait for the given server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(self) -> Result<(), anyhow::Error> {
        self.http_server.await.map_err(|err| anyhow!(err))
    }
}

async fn handoff_to_nexus(
    log: &Logger,
    config: &Config,
    request: &NexusTypes::RackInitializationRequest,
) -> Result<(), anyhow::Error> {
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
    .await?;
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
    /// Specify the (dns) address of an internal DNS server
    pub internal_dns_dns_addr: Option<SocketAddrV6>,
    /// Specify a certificate and associated private key for the initial Silo's
    /// initial TLS certificates
    pub tls_certificate: Option<NexusTypes::Certificate>,
}

/// Run an instance of the `Server` which is able to handoff to Nexus.
///
/// This starts:
/// - A Sled Agent
/// - An Internal DNS server
/// - A Crucible Pantry
///
/// And performs the following actions, similar to the Rack Setup Service:
/// - Populates the Internal DNS server with records
/// - Performs handoff to Nexus
pub async fn run_standalone_server(
    config: &Config,
    rss_args: &RssArgs,
) -> Result<(), anyhow::Error> {
    let (drain, registration) = slog_dtrace::with_drain(
        config
            .log
            .to_logger("sled-agent")
            .map_err(|message| anyhow!("initializing logger: {}", message))?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!());
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(anyhow!(msg));
    } else {
        debug!(log, "registered DTrace probes");
    }

    // Start the sled agent
    let mut server = Server::start(config, &log).await?;
    info!(log, "sled agent started successfully");

    // Start the Internal DNS server
    let dns = if let Some(addr) = rss_args.internal_dns_dns_addr {
        dns_server::TransientServer::new_with_address(&log, addr.into()).await?
    } else {
        dns_server::TransientServer::new(&log).await?
    };
    let mut dns_config_builder = internal_dns::DnsConfigBuilder::new();

    // Start the Crucible Pantry
    let pantry_server = server.start_pantry().await;

    // Insert SRV and AAAA record for Crucible Pantry
    let pantry_zone_id = pantry_server.server.app_private().id;
    let pantry_addr = match pantry_server.addr() {
        SocketAddr::V6(v6) => v6,
        SocketAddr::V4(_) => {
            panic!("pantry address must be IPv6");
        }
    };
    let pantry_zone = dns_config_builder
        .host_zone(pantry_zone_id, *pantry_addr.ip())
        .expect("failed to set up DNS");
    dns_config_builder
        .service_backend_zone(
            ServiceName::CruciblePantry,
            &pantry_zone,
            pantry_addr.port(),
        )
        .expect("failed to set up DNS");

    // Initialize the internal DNS entries
    let dns_config = dns_config_builder.build();
    dns.initialize_with_config(&log, &dns_config).await?;

    // Record the internal DNS server as though RSS had provisioned it so
    // that Nexus knows about it.
    let http_bound = match dns.dropshot_server.local_addr() {
        SocketAddr::V4(_) => panic!("did not expect v4 address"),
        SocketAddr::V6(a) => a,
    };
    let mut services = vec![NexusTypes::ServicePutRequest {
        address: http_bound.to_string(),
        kind: NexusTypes::ServiceKind::InternalDns,
        service_id: Uuid::new_v4(),
        sled_id: config.id,
        zone_id: Some(Uuid::new_v4()),
    }];

    let mut internal_services_ip_pool_ranges = vec![];
    if let Some(nexus_external_addr) = rss_args.nexus_external_addr {
        let ip = nexus_external_addr.ip();

        services.push(NexusTypes::ServicePutRequest {
            address: config.nexus_address.to_string(),
            kind: NexusTypes::ServiceKind::Nexus {
                external_address: ip,
                nic: NexusTypes::ServiceNic {
                    id: Uuid::new_v4(),
                    name: "nexus".parse().unwrap(),
                    ip: NEXUS_OPTE_IPV4_SUBNET
                        .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
                        .unwrap()
                        .into(),
                    mac: MacAddr::random_system(),
                },
            },
            service_id: Uuid::new_v4(),
            sled_id: config.id,
            zone_id: Some(Uuid::new_v4()),
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
        let ip = *external_dns_internal_addr.ip();
        services.push(NexusTypes::ServicePutRequest {
            address: external_dns_internal_addr.to_string(),
            kind: NexusTypes::ServiceKind::ExternalDns {
                external_address: ip.into(),
                nic: NexusTypes::ServiceNic {
                    id: Uuid::new_v4(),
                    name: "external-dns".parse().unwrap(),
                    ip: DNS_OPTE_IPV4_SUBNET
                        .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
                        .unwrap()
                        .into(),
                    mac: MacAddr::random_system(),
                },
            },
            service_id: Uuid::new_v4(),
            sled_id: config.id,
            zone_id: Some(Uuid::new_v4()),
        });

        internal_services_ip_pool_ranges
            .push(IpRange::V6(Ipv6Range { first: ip, last: ip }));
    }

    let recovery_silo = NexusTypes::RecoverySiloConfig {
        silo_name: "demo-silo".parse().unwrap(),
        user_name: "demo-privileged".parse().unwrap(),
        // The following is a hash for the password "oxide".  This is
        // (obviously) only intended for transient deployments in
        // development with no sensitive data or resources.  You can change
        // this value to any other supported hash.  The only thing that
        // needs to be changed with this hash are the instructions given to
        // individuals running this program who then want to log in as this
        // user.  For more on what's supported, see the API docs for this
        // type and the specific constraints in the nexus-passwords crate.
        user_password_hash: "$argon2id$v=19$m=98304,t=13,p=1$\
        RUlWc0ZxaHo0WFdrN0N6ZQ$S8p52j85GPvMhR/ek3GL0el/oProgTwWpHJZ8lsQQoY"
            .parse()
            .unwrap(),
    };

    let mut datasets = vec![];
    for zpool_id in server.sled_agent.get_zpools().await {
        for (dataset_id, address) in
            server.sled_agent.get_datasets(zpool_id).await
        {
            datasets.push(NexusTypes::DatasetCreateRequest {
                zpool_id,
                dataset_id,
                request: NexusTypes::DatasetPutRequest {
                    address: address.to_string(),
                    kind: NexusTypes::DatasetKind::Crucible,
                },
            });
        }
    }

    let certs = match &rss_args.tls_certificate {
        Some(c) => vec![c.clone()],
        None => vec![],
    };

    let rack_init_request = NexusTypes::RackInitializationRequest {
        services,
        datasets,
        internal_services_ip_pool_ranges,
        certs,
        internal_dns_zone_config: d2n_params(&dns_config),
        external_dns_zone_name: internal_dns::names::DNS_ZONE_EXTERNAL_TESTING
            .to_owned(),
        recovery_silo,
        external_port_count: NexusTypes::ExternalPortDiscovery::Static(
            HashMap::new(),
        ),
        rack_network_config: None,
    };

    handoff_to_nexus(&log, &config, &rack_init_request).await?;
    info!(log, "Handoff to Nexus is complete");

    server.wait_for_finish().await
}
