// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Library interface to the sled agent

use super::config::Config;
use super::http_entrypoints::api as http_api;
use super::sled_agent::SledAgent;
use super::storage::PantryServer;
use crate::nexus::{ConvertInto, NexusClient};
use crate::sim::SimulatedUpstairs;
use anyhow::{Context, anyhow, bail};
use bootstrap_agent_lockstep_types::RecoverySiloConfig;
use bootstrap_agent_lockstep_types::ServiceIpPoolConfig;
use crucible_agent_client::types::State as RegionState;
use iddqd::IdOrdMap;
use illumos_utils::zpool::ZpoolName;
use internal_dns_types::config::DnsConfigBuilder;
use internal_dns_types::config::DnsConfigParams;
use internal_dns_types::names::DNS_ZONE_EXTERNAL_TESTING;
use internal_dns_types::names::ServiceName;
use nexus_client::types as NexusTypes;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_lockstep_client::types::{
    AllowedSourceIps, CrucibleDatasetCreateRequest, RackInitializationRequest,
    RackNetworkConfig,
};
use nexus_types::deployment::{
    BlueprintPhysicalDiskConfig, BlueprintPhysicalDiskDisposition,
    BlueprintZoneImageSource, LastAllocatedSubnetIpOffset, blueprint_zone_type,
};
use nexus_types::deployment::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
};
use omicron_common::FileKv;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv4Range;
use omicron_common::address::Ipv6Range;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::address::RACK_PREFIX_LENGTH;
use omicron_common::address::{DNS_OPTE_IPV4_SUBNET, Ipv6Subnet};
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Name;
use omicron_common::api::external::UserId;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::backoff::{
    BackoffError, retry_notify, retry_policy_internal_service_aggressive,
};
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_generation_kinds::{
    Generation, NexusGeneration, SledConfigGeneration,
};
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use oxnet::Ipv6Net;
use rand::seq::IndexedRandom;
use sled_agent_rack_setup::{
    PlannedSledDescription, ServicePlan, SledConfig,
    from_ipaddr_to_external_floating_ip,
    from_sockaddr_to_external_floating_addr,
};
use sled_agent_types::disk::CompressionAlgorithm;
use sled_agent_types::disk::DatasetConfig;
use sled_agent_types::disk::DiskIdentity;
use sled_agent_types::disk::OmicronPhysicalDiskConfig;
use sled_agent_types::disk::SharedDatasetConfig;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::UplinkPorts;
use sled_agent_types::inventory::HostPhase2DesiredSlots;
use sled_agent_types::inventory::NetworkInterface;
use sled_agent_types::inventory::NetworkInterfaceKind;
use sled_agent_types::inventory::OmicronSledConfig;
use sled_agent_types::inventory::OmicronSledUpdateDisposition;
use sled_agent_types::inventory::OmicronZoneConfig;
use sled_agent_types::inventory::OmicronZoneDataset;
use slog::{Drain, Logger, info, warn};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use transient_dns_server::TransientDnsServer;
use uuid::Uuid;

// Well-known service IP pool names the simulated sled-agent creates, mirroring
// what real rack setup produces.
const SERVICE_POOL_IPV4_NAME: &str = "oxide-service-pool-v4";
const SERVICE_POOL_IPV6_NAME: &str = "oxide-service-pool-v6";

/// How [`Server::start`] handles the sled agent's registration with Nexus.
///
/// A simulated sled agent announces itself to Nexus with `sled_agent_put`,
/// retrying until Nexus accepts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NexusRegistration {
    /// Wait until Nexus has accepted the registration, so that the sled exists
    /// in the db's `sled` table by the time [`Server::start`] returns.
    ///
    /// If registration fails, [`Server::start`] will return an error.
    WaitForCompletion,
    /// Return immediately, retrying registration in the background.
    ///
    /// Since registration becomes asynchronous, if it fails [`Server::start`]
    /// cannot return an error.
    ///
    /// Use this in cases where Nexus might not be reachable (for example, a
    /// sled agent started against a placeholder Nexus address).
    Background,
}

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
        nexus_registration: NexusRegistration,
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
                sled_agent_api::latest_version(),
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
                                serial: config
                                    .hardware
                                    .baseboard
                                    .identifier()
                                    .to_string(),
                                part: config
                                    .hardware
                                    .baseboard
                                    .model()
                                    .to_string(),
                                revision: config.hardware.baseboard.revision(),
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

        match nexus_registration {
            NexusRegistration::WaitForCompletion => {
                task.await.context("registering with Nexus")?;
            }
            NexusRegistration::Background => {}
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

            sled_agent.create_zpool(
                zpool_id,
                physical_disk_id,
                zpool.size,
                zpool.health,
            );
            let dataset_id = DatasetUuid::new_v4();
            let address =
                sled_agent.create_crucible_dataset(zpool_id, dataset_id);

            datasets.push(CrucibleDatasetCreateRequest {
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
    nexus_lockstep_address: SocketAddr,
    request: &RackInitializationRequest,
) -> Result<(), anyhow::Error> {
    let nexus_client = nexus_lockstep_client::Client::new(
        &format!("http://{}", nexus_lockstep_address),
        log.new(o!("component" => "NexusLockstepClient")),
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
    // TODO-RAINCLAUDE: the zone id recorded for Nexus in the initial blueprint; Nexus only opens its external API once it finds its own `deployment.id` in the target blueprint, so this must match that config value.
    pub nexus_id: Option<OmicronZoneUuid>,
    /// Specify the (internal) address of an external DNS server so that Nexus
    /// will know about it and keep it up to date
    pub external_dns_internal_addr: Option<SocketAddrV6>,
    // TODO-RAINCLAUDE: which internal DNS server to populate during rack initialization.
    pub internal_dns: InternalDnsConfig,
    // TODO-RAINCLAUDE: CockroachDB nodes to record in internal DNS and the initial blueprint so Nexus can use `database.type = "from_dns"`.
    pub cockroach_addrs: Vec<SocketAddrV6>,
    // TODO-RAINCLAUDE: rack subnet reported to Nexus; defaults to the /56 containing the sled agent's address.
    pub rack_subnet: Option<Ipv6Net>,
    // TODO-RAINCLAUDE: recovery silo and user names; the password is always "oxide" (see the hash below).
    pub recovery_silo_name: Option<Name>,
    pub recovery_user_name: Option<UserId>,
    /// Specify a certificate and associated private key for the initial Silo's
    /// initial TLS certificates
    pub tls_certificate: Option<Certificate>,
}

// TODO-RAINCLAUDE: how the standalone simulated sled agent obtains the internal DNS server it populates during rack initialization; an external server needs both addresses (Nexus is told the DNS address, the sled agent talks to the HTTP address), so the pair is one variant rather than two independent options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InternalDnsConfig {
    // TODO-RAINCLAUDE: start a DNS server in this process, optionally at a fixed DNS address.
    InProcess { dns_addr: Option<SocketAddrV6> },
    // TODO-RAINCLAUDE: populate an already-running DNS server via its HTTP API.
    External { http_addr: SocketAddrV6, dns_addr: SocketAddrV6 },
}

impl Default for InternalDnsConfig {
    fn default() -> Self {
        InternalDnsConfig::InProcess { dns_addr: None }
    }
}

// TODO-RAINCLAUDE: the standalone server either owns an in-process DNS server or points at an external one whose addresses are only known from the arguments.
enum InternalDns {
    InProcess(TransientDnsServer),
    External { http_addr: SocketAddrV6, dns_addr: SocketAddrV6 },
}

impl InternalDns {
    async fn new(
        log: &Logger,
        config: &InternalDnsConfig,
    ) -> Result<InternalDns, anyhow::Error> {
        match config {
            InternalDnsConfig::External { http_addr, dns_addr } => {
                Ok(InternalDns::External {
                    http_addr: *http_addr,
                    dns_addr: *dns_addr,
                })
            }
            InternalDnsConfig::InProcess { dns_addr: Some(dns_addr) } => {
                Ok(InternalDns::InProcess(
                    TransientDnsServer::new_with_address(
                        log,
                        (*dns_addr).into(),
                    )
                    .await?,
                ))
            }
            InternalDnsConfig::InProcess { dns_addr: None } => {
                Ok(InternalDns::InProcess(TransientDnsServer::new(log).await?))
            }
        }
    }

    fn http_addr(&self) -> Result<SocketAddrV6, anyhow::Error> {
        match self {
            InternalDns::InProcess(dns) => {
                match dns.dropshot_server.local_addr() {
                    SocketAddr::V4(addr) => bail!(
                        "internal DNS HTTP server bound an IPv4 address \
                         ({addr}); internal DNS zones require IPv6"
                    ),
                    SocketAddr::V6(addr) => Ok(addr),
                }
            }
            InternalDns::External { http_addr, .. } => Ok(*http_addr),
        }
    }

    fn dns_addr(&self) -> Result<SocketAddrV6, anyhow::Error> {
        match self {
            InternalDns::InProcess(dns) => {
                match dns.dns_server.local_address() {
                    SocketAddr::V4(addr) => bail!(
                        "internal DNS server bound an IPv4 address ({addr}); \
                         internal DNS zones require IPv6"
                    ),
                    SocketAddr::V6(addr) => Ok(addr),
                }
            }
            InternalDns::External { dns_addr, .. } => Ok(*dns_addr),
        }
    }

    async fn initialize_with_config(
        &self,
        log: &Logger,
        dns_config: &DnsConfigParams,
    ) -> Result<(), anyhow::Error> {
        match self {
            InternalDns::InProcess(dns) => {
                dns.initialize_with_config(log, dns_config).await
            }
            InternalDns::External { http_addr, .. } => {
                let client = dns_service_client::Client::new(
                    &format!("http://{http_addr}"),
                    log.new(o!("component" => "DnsServiceClient")),
                );
                // TODO-RAINCLAUDE: the external DNS server may still be starting, so retry the way the Nexus handoff does.
                let put_config = || async {
                    client
                        .dns_config_put(dns_config)
                        .await
                        .map_err(BackoffError::transient)
                };
                let log_failure = |err, delay| {
                    warn!(
                        log,
                        "failed to initialize internal DNS at {http_addr}, \
                         will retry in {delay:?}";
                        "error" => ?err,
                    );
                };
                retry_notify(
                    retry_policy_internal_service_aggressive(),
                    put_config,
                    log_failure,
                )
                .await
                .with_context(|| {
                    format!("initializing internal DNS at {http_addr}")
                })?;
                Ok(())
            }
        }
    }
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
    nexus_lockstep_port: u16,
    logging: &dropshot::ConfigLogging,
    rss_args: &RssArgs,
) -> Result<(), anyhow::Error> {
    let (drain, registration) = slog_dtrace::with_drain(
        logging.to_logger("sled-agent").context("initializing logger")?,
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
    let mut server = Server::start(
        config,
        &log,
        NexusRegistration::WaitForCompletion,
        &simulated_upstairs,
        0,
    )
    .await?;
    info!(log, "sled agent started successfully");

    let underlay_address = match server.http_server.local_addr() {
        SocketAddr::V4(addr) => {
            bail!("sled agent bound an IPv4 address ({addr}); it must be IPv6")
        }
        SocketAddr::V6(addr) => addr,
    };
    let sled_ip = *underlay_address.ip();

    // Start the Internal DNS server
    let dns = InternalDns::new(&log, &rss_args.internal_dns).await?;
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

    // TODO-RAINCLAUDE: CockroachDB zones go into DNS now and into the blueprint below; the blueprint executor rewrites DNS from the blueprint, so both must agree.
    let cockroach_zones: Vec<(OmicronZoneUuid, SocketAddrV6)> = rss_args
        .cockroach_addrs
        .iter()
        .map(|addr| (OmicronZoneUuid::new_v4(), *addr))
        .collect();
    for (zone_id, addr) in &cockroach_zones {
        dns_config_builder
            .host_zone_with_one_backend(*zone_id, ServiceName::Cockroach, *addr)
            .with_context(|| {
                format!("adding CockroachDB node {addr} to internal DNS")
            })?;
    }

    // TODO-RAINCLAUDE: Nexus discovers DNS servers through `_nameservice._tcp` and its peers through `_nexus._tcp`, and the blueprint executor cannot publish those records until it can find a DNS server, so the initial config must carry them; the zone ids are reused in the blueprint below so the executor's rewrite agrees with them.
    let internal_dns_zone_id = OmicronZoneUuid::new_v4();
    dns_config_builder
        .host_zone_internal_dns(
            internal_dns_zone_id,
            ServiceName::InternalDns,
            dns.http_addr()?,
            dns.dns_addr()?,
        )
        .context("adding the internal DNS server to internal DNS")?;
    let nexus_internal_addr = match config.nexus_address {
        SocketAddr::V4(addr) => {
            bail!("Nexus internal address {addr} must be IPv6")
        }
        SocketAddr::V6(addr) => addr,
    };
    let nexus_zone_id =
        rss_args.nexus_id.unwrap_or_else(OmicronZoneUuid::new_v4);
    if rss_args.nexus_external_addr.is_some() {
        dns_config_builder
            .host_zone_nexus(
                nexus_zone_id,
                nexus_internal_addr,
                nexus_lockstep_port,
            )
            .context("adding Nexus to internal DNS")?;
    }

    // Initialize the internal DNS entries
    let dns_config =
        dns_config_builder.build_full_config_for_initial_generation();
    dns.initialize_with_config(&log, &dns_config).await?;

    let all_u2_zpools = server.sled_agent.get_zpools();
    let get_random_zpool = || {
        let pool = all_u2_zpools
            .choose(&mut rand::rng())
            .expect("No external zpools found, but we need one");
        ZpoolName::new_external(ZpoolUuid::from_untyped_uuid(pool.id))
    };

    // Record the internal DNS server as though RSS had provisioned it so
    // that Nexus knows about it.
    // TODO-RAINCLAUDE: every zone's filesystem pool must be one of the simulated sled's real zpools, because the sled config ledgered below lists exactly those disks and Nexus cross-checks zone pools against them.
    let pool_name = get_random_zpool();
    let mut zones = IdOrdMap::new();
    zones
        .insert_unique(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: internal_dns_zone_id,
            zone_type: BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    dataset: OmicronZoneDataset { pool_name },
                    http_address: dns.http_addr()?,
                    dns_address: dns.dns_addr()?,
                    // TODO-RAINCLAUDE: nothing in the simulation routes through the global zone, so the sled's own address stands in for it.
                    gz_address: sled_ip,
                    gz_address_index: 0,
                },
            ),
            // Co-locate the filesystem pool with the dataset
            filesystem_pool: pool_name,
            image_source: BlueprintZoneImageSource::InstallDataset,
        })
        .expect("freshly generated zone IDs are unique");

    for (zone_id, address) in cockroach_zones {
        let pool_name = get_random_zpool();
        zones
            .insert_unique(BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: zone_id,
                zone_type: BlueprintZoneType::CockroachDb(
                    blueprint_zone_type::CockroachDb {
                        address,
                        dataset: OmicronZoneDataset { pool_name },
                    },
                ),
                filesystem_pool: pool_name,
                image_source: BlueprintZoneImageSource::InstallDataset,
            })
            .expect("freshly generated zone IDs are unique");
    }

    let mut internal_services_ipv4_ranges = vec![];
    let mut internal_services_ipv6_ranges = vec![];
    let mut macs = MacAddr::iter_system();
    if let Some(nexus_external_addr) = rss_args.nexus_external_addr {
        let external_ip = nexus_external_addr.ip();
        let id = nexus_zone_id;
        let private_ip = NEXUS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv4(private_ip, *NEXUS_OPTE_IPV4_SUBNET)
                .context("creating private IP configuration")?;

        zones
            .insert_unique(BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id,
                zone_type: BlueprintZoneType::Nexus(
                    blueprint_zone_type::Nexus {
                        internal_address: nexus_internal_addr,
                        lockstep_port: nexus_lockstep_port,
                        external_ip: from_ipaddr_to_external_floating_ip(
                            external_ip,
                        ),
                        nic: NetworkInterface {
                            id: Uuid::new_v4(),
                            kind: NetworkInterfaceKind::Service {
                                id: id.into_untyped_uuid(),
                            },
                            name: "nexus".parse().unwrap(),
                            ip_config,
                            mac: macs.next().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            primary: true,
                            slot: 0,
                        },
                        external_tls: false,
                        external_dns_servers: vec![],
                        nexus_generation: NexusGeneration::new(),
                    },
                ),
                filesystem_pool: get_random_zpool(),
                image_source: BlueprintZoneImageSource::InstallDataset,
            })
            .expect("freshly generated zone IDs are unique");

        match external_ip {
            IpAddr::V4(addr) => {
                internal_services_ipv4_ranges
                    .push(IpRange::V4(Ipv4Range { first: addr, last: addr }));
            }
            IpAddr::V6(addr) => {
                internal_services_ipv6_ranges
                    .push(IpRange::V6(Ipv6Range { first: addr, last: addr }));
            }
        }
    }

    if let Some(external_dns_internal_addr) =
        rss_args.external_dns_internal_addr
    {
        let ip = *external_dns_internal_addr.ip();
        let id = OmicronZoneUuid::new_v4();
        let private_ip = DNS_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv4(private_ip, *DNS_OPTE_IPV4_SUBNET)
                .context("creating private IP configuration")?;
        let pool_name = get_random_zpool();
        zones
            .insert_unique(BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id,
                zone_type: BlueprintZoneType::ExternalDns(
                    blueprint_zone_type::ExternalDns {
                        dataset: OmicronZoneDataset { pool_name },
                        http_address: external_dns_internal_addr,
                        dns_address: from_sockaddr_to_external_floating_addr(
                            SocketAddr::V6(external_dns_internal_addr),
                        ),
                        nic: NetworkInterface {
                            id: Uuid::new_v4(),
                            kind: NetworkInterfaceKind::Service {
                                id: id.into_untyped_uuid(),
                            },
                            name: "external-dns".parse().unwrap(),
                            ip_config,
                            mac: macs.next().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            primary: true,
                            slot: 0,
                        },
                    },
                ),
                // Co-locate the filesystem pool with the dataset
                filesystem_pool: pool_name,
                image_source: BlueprintZoneImageSource::InstallDataset,
            })
            .expect("freshly generated zone IDs are unique");

        internal_services_ipv6_ranges
            .push(IpRange::V6(Ipv6Range { first: ip, last: ip }));
    }

    let recovery_silo = RecoverySiloConfig {
        silo_name: rss_args
            .recovery_silo_name
            .clone()
            .unwrap_or_else(|| "demo-silo".parse().unwrap()),
        user_name: rss_args
            .recovery_user_name
            .clone()
            .unwrap_or_else(|| "demo-privileged".parse().unwrap()),
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
            crucible_datasets.push(CrucibleDatasetCreateRequest {
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

    // TODO-RAINCLAUDE: RSS ledgers a sled config on every sled before handing off to Nexus, and the blueprint below is derived from what the sled reports as ledgered (see `inventory.ledgered_sled_config`), so the standalone path has to do the same or the handoff cannot be built. Mirrors nexus-test-utils `configure_sled_agents`: one disk per zpool, one transient-zone dataset per zone, generation 2.
    let sled_config_generation = SledConfigGeneration::from_u32(2);
    let mut disk_configs = IdOrdMap::new();
    for zpool in &zpools {
        let disk = physical_disks
            .iter()
            .find(|disk| disk.id == zpool.physical_disk_id)
            .with_context(|| {
                format!(
                    "zpool {} refers to unknown physical disk {}",
                    zpool.id, zpool.physical_disk_id
                )
            })?;
        disk_configs
            .insert_unique(OmicronPhysicalDiskConfig {
                identity: DiskIdentity {
                    vendor: disk.vendor.clone(),
                    model: disk.model.clone(),
                    serial: disk.serial.clone(),
                },
                id: disk.id,
                pool_id: ZpoolUuid::from_untyped_uuid(zpool.id),
            })
            .map_err(|_| {
                anyhow!("physical disk {} backs more than one zpool", disk.id)
            })?;
    }
    let mut dataset_configs = IdOrdMap::new();
    for zone in &zones {
        dataset_configs
            .insert_unique(DatasetConfig {
                id: DatasetUuid::new_v4(),
                name: DatasetName::new(
                    zone.filesystem_pool,
                    DatasetKind::TransientZone {
                        name: illumos_utils::zone::zone_name(
                            zone.zone_type.kind().zone_prefix(),
                            Some(zone.id),
                        ),
                    },
                ),
                inner: SharedDatasetConfig {
                    compression: CompressionAlgorithm::Off,
                    quota: None,
                    reservation: None,
                },
            })
            .expect("freshly generated dataset IDs are unique");
    }
    server
        .sled_agent
        .set_omicron_config(OmicronSledConfig {
            generation: sled_config_generation,
            disks: disk_configs,
            datasets: dataset_configs,
            zones: zones.iter().cloned().map(OmicronZoneConfig::from).collect(),
            remove_mupdate_override: None,
            host_phase_2: HostPhase2DesiredSlots::current_contents(),
            measurements: BTreeSet::new(),
            update_disposition: OmicronSledUpdateDisposition::Available,
        })
        .map_err(|error| {
            anyhow!("ledgering the simulated sled config: {error}")
        })?;

    let blueprint = {
        let sled_config =
            server.sled_agent.omicron_sled_config().unwrap_or_default();

        let subnet = Ipv6Subnet::new(sled_ip);
        let last_allocated_ip_subnet_offset = LastAllocatedSubnetIpOffset::new(
            zones
                .iter()
                .map(|zone| zone.zone_type.underlay_ip())
                .filter(|ip| subnet.net().contains(*ip))
                .map(|ip| ip.segments()[7])
                .max()
                .expect("no zones are included in the plan"),
        );

        let inventory = server.sled_agent.inventory(underlay_address.into())?;
        let mut all_sleds = IdOrdMap::new();
        all_sleds.insert_overwrite(PlannedSledDescription {
            underlay_address,
            sled_id: config.id,
            subnet,
            last_allocated_ip_subnet_offset,
            config: SledConfig {
                disks: sled_config
                    .disks
                    .into_iter()
                    .map(|config| BlueprintPhysicalDiskConfig {
                        disposition:
                            BlueprintPhysicalDiskDisposition::InService,
                        identity: config.identity,
                        id: config.id,
                        pool_id: config.pool_id,
                    })
                    .collect(),
                datasets: sled_config
                    .datasets
                    .into_iter()
                    .map(|config| (config.id, config))
                    .collect(),
                zones,
            },
        });

        let plan = ServicePlan { all_sleds, dns_config: dns_config.clone() };
        let generation = inventory
            .ledgered_sled_config
            .context(
                "simulated inventory does not have a ledgered sled config",
            )?
            .generation;
        plan.to_blueprint(generation)
            .context("could not construct initial blueprint")?
    };

    let mut service_ip_pools = IdOrdMap::new();
    if let Ok(service_ipv4_pool) = ServiceIpPoolConfig::new(
        SERVICE_POOL_IPV4_NAME.parse().unwrap(),
        String::from("IPv4 IP Pool for Oxide Services"),
        internal_services_ipv4_ranges,
    ) {
        if service_ip_pools.insert_unique(service_ipv4_pool).is_err() {
            anyhow::bail!(
                "duplicate IPv4 service pool name: '{SERVICE_POOL_IPV4_NAME}'"
            );
        }
    }
    if let Ok(service_ipv6_pool) = ServiceIpPoolConfig::new(
        SERVICE_POOL_IPV6_NAME.parse().unwrap(),
        String::from("IPv6 IP Pool for Oxide Services"),
        internal_services_ipv6_ranges,
    ) {
        if service_ip_pools.insert_unique(service_ipv6_pool).is_err() {
            anyhow::bail!(
                "duplicate IPv6 service pool name: '{SERVICE_POOL_IPV6_NAME}'"
            );
        }
    }

    let rack_init_request = RackInitializationRequest {
        blueprint,
        physical_disks,
        zpools,
        crucible_datasets,
        service_ip_pools,
        certs,
        internal_dns_zone_config: dns_config,
        external_dns_zone_name: DNS_ZONE_EXTERNAL_TESTING.to_owned(),
        recovery_silo,
        rack_network_config: RackNetworkConfig {
            // TODO-RAINCLAUDE: default to the /56 around the sled so that non-loopback deployments (containers) get a subnet that contains their zones.
            rack_subnet: rss_args.rack_subnet.unwrap_or_else(|| {
                Ipv6Subnet::<RACK_PREFIX_LENGTH>::new(sled_ip).net()
            }),
            infra_ip_first: IpAddr::V4(Ipv4Addr::LOCALHOST),
            infra_ip_last: IpAddr::V4(Ipv4Addr::LOCALHOST),
            // `UplinkPorts` must be non-empty; the simulated rack doesn't
            // exercise uplinks, so use a single placeholder port.
            ports: UplinkPorts::new(vec![PortConfig::empty_for_tests("qsfp0")])
                .expect("placeholder port list is non-empty"),
            bgp: Vec::new(),
            bfd: Vec::new(),
        },
        allowed_source_ips: AllowedSourceIps::Any,
        initial_trust_quorum_configuration: None,
        external_jumbo_frames_opt_in_enabled: false,
    };

    let mut nexus_lockstep_address = config.nexus_address;
    nexus_lockstep_address.set_port(nexus_lockstep_port);
    handoff_to_nexus(&log, nexus_lockstep_address, &rack_init_request).await?;
    info!(log, "Handoff to Nexus is complete");

    server.wait_for_finish().await
}
