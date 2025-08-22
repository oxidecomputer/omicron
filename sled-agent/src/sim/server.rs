// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use super::storage::PantryServer;
use crate::nexus::{ConvertInto, NexusClient};
use crate::rack_setup::SledConfig;
use crate::rack_setup::service::build_initial_blueprint_from_sled_configs;
use crate::rack_setup::{
    from_ipaddr_to_external_floating_ip,
    from_sockaddr_to_external_floating_addr,
};
use crate::sim::SimulatedUpstairs;
use anyhow::{Context as _, anyhow};
use crucible_agent_client::types::State as RegionState;
use id_map::IdMap;
use illumos_utils::zpool::ZpoolName;
use internal_dns_types::config::DnsConfigBuilder;
use internal_dns_types::names::DNS_ZONE_EXTERNAL_TESTING;
use internal_dns_types::names::ServiceName;
use nexus_client::types as NexusTypes;
use nexus_client::types::{IpRange, Ipv4Range, Ipv6Range};
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_types::deployment::{
    BlueprintPhysicalDiskConfig, BlueprintPhysicalDiskDisposition,
    BlueprintZoneImageSource, blueprint_zone_type,
};
use nexus_types::deployment::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
};
use nexus_types::inventory::NetworkInterfaceKind;
use omicron_common::FileKv;
use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::backoff::{
    BackoffError, retry_notify, retry_policy_internal_service_aggressive,
};
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use oxnet::Ipv6Net;
use sled_agent_types::rack_init::RecoverySiloConfig;
use slog::{Drain, Logger, info};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
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
    /// address of repo depot server
    pub repo_depot_address: SocketAddr,
}

impl Server {
    /// `sled_index` here is to provide an offset so that Crucible regions have
    /// unique ports in tests that use multiple sled agents.
    pub async fn start(
        config: &Config,
        log: &Logger,
        wait_for_nexus: bool,
        simulated_upstairs: &Arc<SimulatedUpstairs>,
        sled_index: u16,
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
            simulated_upstairs.clone(),
            sled_index,
        )
        .await;

        let dropshot_log = log.new(o!("component" => "dropshot"));
        let http_server = dropshot::ServerBuilder::new(
            http_api(),
            sled_agent.clone(),
            dropshot_log,
        )
        .config(config.dropshot.clone())
        .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
            dropshot::ClientSpecifiesVersionInHeader::new(
                omicron_common::api::VERSION_HEADER,
                sled_agent_api::VERSION_REMOVE_DESTROY_ORPHANED_DATASETS_CHICKEN_SWITCH,
            ),
        )))
        .start()
        .map_err(|error| anyhow!("initializing server: {}", error))?;

        // Notify the control plane that we're up, and continue trying this
        // until it succeeds. We retry with an randomized, capped exponential
        // backoff.
        //
        // TODO-robustness if this returns a 400 error, we probably want to
        // return a permanent error from the `notify_nexus` closure.
        let sa_address = http_server.local_addr();
        let repo_depot_address = sled_agent.repo_depot.local_addr();
        let repo_depot_port = repo_depot_address.port();
        let config_clone = config.clone();
        let log_clone = log.clone();
        let task = tokio::spawn(async move {
            let config = config_clone;
            let log = log_clone;
            let nexus_client = nexus_client.clone();
            let notify_nexus = || async {
                debug!(log, "contacting server nexus");
                nexus_client
                    .sled_agent_put(
                        &config.id,
                        &NexusTypes::SledAgentInfo {
                            sa_address: sa_address.to_string(),
                            repo_depot_port,
                            role: NexusTypes::SledRole::Scrimlet,
                            baseboard: NexusTypes::Baseboard {
                                serial: format!(
                                    "sim-{}",
                                    &config.id.to_string()[0..8]
                                ),
                                part: String::from("Unknown"),
                                revision: 0,
                            },
                            usable_hardware_threads: config
                                .hardware
                                .hardware_threads,
                            usable_physical_ram:
                                NexusTypes::ByteCount::try_from(
                                    config.hardware.physical_ram,
                                )
                                .unwrap(),
                            reservoir_size: NexusTypes::ByteCount::try_from(
                                config.hardware.reservoir_ram,
                            )
                            .unwrap(),
                            cpu_family: config.hardware.cpu_family.convert(),
                            generation: Generation::new(),
                            decommissioned: false,
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
        });

        if wait_for_nexus {
            task.await.unwrap();
        }

        let mut datasets = vec![];
        // Create all the Zpools requested by the config, and allocate a single
        // Crucible dataset for each. This emulates the setup we expect to have
        // on the physical rack.
        for zpool in &config.storage.zpools {
            let physical_disk_id = PhysicalDiskUuid::new_v4();
            let zpool_id = ZpoolUuid::new_v4();
            let vendor = "synthetic-vendor".to_string();
            let serial = format!("synthetic-serial-{zpool_id}");
            let model = "synthetic-model".to_string();
            sled_agent.create_external_physical_disk(
                physical_disk_id,
                DiskIdentity {
                    vendor: vendor.clone(),
                    serial: serial.clone(),
                    model: model.clone(),
                },
            );

            sled_agent.create_zpool(zpool_id, physical_disk_id, zpool.size);
            let dataset_id = DatasetUuid::new_v4();
            let address =
                sled_agent.create_crucible_dataset(zpool_id, dataset_id);

            datasets.push(NexusTypes::CrucibleDatasetCreateRequest {
                zpool_id,
                dataset_id,
                address: address.to_string(),
            });

            // Whenever Nexus tries to allocate a region, it should complete
            // immediately. What efficiency!
            let crucible =
                sled_agent.get_crucible_dataset(zpool_id, dataset_id);
            crucible.set_create_callback(Box::new(|_| RegionState::Created))
        }

        Ok(Server {
            config: config.clone(),
            log: log.clone(),
            sled_agent,
            http_server,
            pantry_server: None,
            repo_depot_address,
        })
    }

    /// Starts the pantry service
    pub async fn start_pantry(&mut self) -> &PantryServer {
        // Create the simulated Pantry
        let pantry_server = PantryServer::new(
            self.log.new(o!("kind" => "pantry")),
            self.config.storage.ip,
            self.sled_agent.simulated_upstairs.clone(),
        );
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
    pub tls_certificate: Option<Certificate>,
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
    logging: &dropshot::ConfigLogging,
    rss_args: &RssArgs,
) -> Result<(), anyhow::Error> {
    let (drain, registration) = slog_dtrace::with_drain(
        logging
            .to_logger("sled-agent")
            .map_err(|message| anyhow!("initializing logger: {}", message))?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(anyhow!(msg));
    } else {
        debug!(log, "registered DTrace probes");
    }

    // Start the sled agent
    let simulated_upstairs = Arc::new(SimulatedUpstairs::new(log.clone()));
    let mut server =
        Server::start(config, &log, true, &simulated_upstairs, 0).await?;
    info!(log, "sled agent started successfully");

    // Start the Internal DNS server
    let dns = if let Some(addr) = rss_args.internal_dns_dns_addr {
        dns_server::TransientServer::new_with_address(&log, addr.into()).await?
    } else {
        dns_server::TransientServer::new(&log).await?
    };
    let mut dns_config_builder = DnsConfigBuilder::new();

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
    let dns_config =
        dns_config_builder.build_full_config_for_initial_generation();
    dns.initialize_with_config(&log, &dns_config).await?;
    let internal_dns_version = dns_config.generation;

    let all_u2_zpools = server.sled_agent.get_zpools();
    let get_random_zpool = || {
        use rand::seq::SliceRandom;
        let pool = all_u2_zpools
            .choose(&mut rand::thread_rng())
            .expect("No external zpools found, but we need one");
        ZpoolName::new_external(ZpoolUuid::from_untyped_uuid(pool.id))
    };

    // Record the internal DNS server as though RSS had provisioned it so
    // that Nexus knows about it.
    let http_bound = match dns.dropshot_server.local_addr() {
        SocketAddr::V4(_) => panic!("did not expect v4 address"),
        SocketAddr::V6(a) => a,
    };
    let pool_name = ZpoolName::new_external(ZpoolUuid::new_v4());
    let mut zones = IdMap::new();
    zones.insert(BlueprintZoneConfig {
        disposition: BlueprintZoneDisposition::InService,
        id: OmicronZoneUuid::new_v4(),
        zone_type: BlueprintZoneType::InternalDns(
            blueprint_zone_type::InternalDns {
                dataset: OmicronZoneDataset { pool_name },
                http_address: http_bound,
                dns_address: match dns.dns_server.local_address() {
                    SocketAddr::V4(_) => panic!("did not expect v4 address"),
                    SocketAddr::V6(a) => a,
                },
                gz_address: Ipv6Addr::LOCALHOST,
                gz_address_index: 0,
            },
        ),
        // Co-locate the filesystem pool with the dataset
        filesystem_pool: pool_name,
        image_source: BlueprintZoneImageSource::InstallDataset,
    });

    let mut internal_services_ip_pool_ranges = vec![];
    let mut macs = MacAddr::iter_system();
    if let Some(nexus_external_addr) = rss_args.nexus_external_addr {
        let ip = nexus_external_addr.ip();
        let id = OmicronZoneUuid::new_v4();

        zones.insert(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            zone_type: BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address: match config.nexus_address {
                    SocketAddr::V4(_) => panic!("did not expect v4 address"),
                    SocketAddr::V6(a) => a,
                },
                external_ip: from_ipaddr_to_external_floating_ip(ip),
                nic: nexus_types::inventory::NetworkInterface {
                    id: Uuid::new_v4(),
                    kind: NetworkInterfaceKind::Service {
                        id: id.into_untyped_uuid(),
                    },
                    name: "nexus".parse().unwrap(),
                    ip: NEXUS_OPTE_IPV4_SUBNET
                        .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
                        .unwrap()
                        .into(),
                    mac: macs.next().unwrap(),
                    subnet: (*NEXUS_OPTE_IPV4_SUBNET).into(),
                    vni: Vni::SERVICES_VNI,
                    primary: true,
                    slot: 0,
                    transit_ips: vec![],
                },
                external_tls: false,
                external_dns_servers: vec![],
            }),
            filesystem_pool: get_random_zpool(),
            image_source: BlueprintZoneImageSource::InstallDataset,
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
        let id = OmicronZoneUuid::new_v4();
        let pool_name = ZpoolName::new_external(ZpoolUuid::new_v4());
        zones.insert(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            zone_type: BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns {
                    dataset: OmicronZoneDataset { pool_name },
                    http_address: external_dns_internal_addr,
                    dns_address: from_sockaddr_to_external_floating_addr(
                        SocketAddr::V6(external_dns_internal_addr),
                    ),
                    nic: nexus_types::inventory::NetworkInterface {
                        id: Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: id.into_untyped_uuid(),
                        },
                        name: "external-dns".parse().unwrap(),
                        ip: DNS_OPTE_IPV4_SUBNET
                            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
                            .unwrap()
                            .into(),
                        mac: macs.next().unwrap(),
                        subnet: (*DNS_OPTE_IPV4_SUBNET).into(),
                        vni: Vni::SERVICES_VNI,
                        primary: true,
                        slot: 0,
                        transit_ips: vec![],
                    },
                },
            ),
            // Co-locate the filesystem pool with the dataset
            filesystem_pool: pool_name,
            image_source: BlueprintZoneImageSource::InstallDataset,
        });

        internal_services_ip_pool_ranges
            .push(IpRange::V6(Ipv6Range { first: ip, last: ip }));
    }

    let recovery_silo = RecoverySiloConfig {
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
        //
        // The hash was generated via:
        // `cargo run --example argon2 -- --input oxide`.
        user_password_hash:
            "$argon2id$v=19$m=98304,t=23,p=1$Effh/p6M2ZKdnpJFeGqtGQ$\
             ZtUwcVODAvUAVK6EQ5FJMv+GMlUCo9PQQsy9cagL+EU"
                .parse()
                .unwrap(),
    };

    let mut crucible_datasets = vec![];
    let physical_disks = server.sled_agent.get_all_physical_disks();
    let zpools = server.sled_agent.get_zpools();
    for zpool in &zpools {
        let zpool_id = ZpoolUuid::from_untyped_uuid(zpool.id);
        for (dataset_id, address) in
            server.sled_agent.get_crucible_datasets(zpool_id)
        {
            crucible_datasets.push(NexusTypes::CrucibleDatasetCreateRequest {
                zpool_id,
                dataset_id,
                address: address.to_string(),
            });
        }
    }

    let certs = match &rss_args.tls_certificate {
        Some(c) => vec![c.clone()],
        None => vec![],
    };

    let omicron_physical_disks_config =
        server.sled_agent.omicron_physical_disks_list()?;
    let mut sled_configs = BTreeMap::new();
    sled_configs.insert(
        config.id,
        SledConfig {
            disks: omicron_physical_disks_config
                .disks
                .into_iter()
                .map(|config| BlueprintPhysicalDiskConfig {
                    disposition: BlueprintPhysicalDiskDisposition::InService,
                    identity: config.identity,
                    id: config.id,
                    pool_id: config.pool_id,
                })
                .collect(),
            datasets: server.sled_agent.datasets_config_list()?.datasets,
            zones,
        },
    );

    let blueprint = build_initial_blueprint_from_sled_configs(
        &sled_configs,
        internal_dns_version,
    )
    .context("could not construct initial blueprint")?;
    let rack_init_request = NexusTypes::RackInitializationRequest {
        blueprint,
        physical_disks,
        zpools,
        crucible_datasets,
        internal_services_ip_pool_ranges,
        certs,
        internal_dns_zone_config: dns_config,
        external_dns_zone_name: DNS_ZONE_EXTERNAL_TESTING.to_owned(),
        recovery_silo,
        external_port_count: NexusTypes::ExternalPortDiscovery::Static(
            HashMap::new(),
        ),
        rack_network_config: NexusTypes::RackNetworkConfigV2 {
            rack_subnet: Ipv6Net::host_net(Ipv6Addr::LOCALHOST),
            infra_ip_first: Ipv4Addr::LOCALHOST,
            infra_ip_last: Ipv4Addr::LOCALHOST,
            ports: Vec::new(),
            bgp: Vec::new(),
            bfd: Vec::new(),
        },
        allowed_source_ips: NexusTypes::AllowedSourceIps::Any,
    };

    handoff_to_nexus(&log, &config, &rack_init_request).await?;
    info!(log, "Handoff to Nexus is complete");

    server.wait_for_finish().await
}
