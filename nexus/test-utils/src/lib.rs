// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use chrono::Utc;
use dns_service_client::types::DnsConfigParams;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HandlerTaskMode;
use futures::future::BoxFuture;
use futures::FutureExt;
use gateway_test_utils::setup::GatewayTestContext;
use nexus_config::Database;
use nexus_config::DpdConfig;
use nexus_config::InternalDns;
use nexus_config::MgdConfig;
use nexus_config::NexusConfig;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_test_interface::NexusServer;
use nexus_types::deployment::Blueprint;
use nexus_types::external_api::params::UserId;
use nexus_types::internal_api::params::Certificate;
use nexus_types::internal_api::params::DatasetCreateRequest;
use nexus_types::internal_api::params::DatasetKind;
use nexus_types::internal_api::params::DatasetPutRequest;
use nexus_types::internal_api::params::RecoverySiloConfig;
use nexus_types::internal_api::params::ServiceKind;
use nexus_types::internal_api::params::ServiceNic;
use nexus_types::internal_api::params::ServicePutRequest;
use nexus_types::inventory::OmicronZoneConfig;
use nexus_types::inventory::OmicronZoneDataset;
use nexus_types::inventory::OmicronZoneType;
use nexus_types::inventory::OmicronZonesConfig;
use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Vni;
use omicron_common::api::external::{IdentityMetadata, Name};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use oximeter_collector::Oximeter;
use oximeter_producer::LogConfig;
use oximeter_producer::Server as ProducerServer;
use slog::{debug, error, o, Logger};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;
use trust_dns_resolver::config::NameServerConfig;
use trust_dns_resolver::config::Protocol;
use trust_dns_resolver::config::ResolverConfig;
use trust_dns_resolver::config::ResolverOpts;
use trust_dns_resolver::TokioAsyncResolver;
use uuid::Uuid;

pub use sim::TEST_HARDWARE_THREADS;
pub use sim::TEST_RESERVOIR_RAM;

pub mod db;
pub mod http_testing;
pub mod resource_helpers;

pub const SLED_AGENT_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
pub const SLED_AGENT2_UUID: &str = "039be560-54cc-49e3-88df-1a29dadbf913";
pub const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";
pub const SWITCH_UUID: &str = "dae4e1f1-410e-4314-bff1-fec0504be07e";
pub const OXIMETER_UUID: &str = "39e6175b-4df2-4730-b11d-cbc1e60a2e78";
pub const PRODUCER_UUID: &str = "a6458b7d-87c3-4483-be96-854d814c20de";
pub const RACK_SUBNET: &str = "fd00:1122:3344:0100::/56";

/// Password for the user created by the test suite
///
/// This is only used by the test suite and `omicron-dev run-all` (the latter of
/// which uses the test suite setup code for most of its operation).   These are
/// both transient deployments with no sensitive data.
pub const TEST_SUITE_PASSWORD: &str = "oxide";

pub struct ControlPlaneTestContext<N> {
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub external_client: ClientTestContext,
    pub internal_client: ClientTestContext,
    pub server: N,
    pub database: dev::db::CockroachInstance,
    pub clickhouse: dev::clickhouse::ClickHouseInstance,
    pub logctx: LogContext,
    pub sled_agent_storage: camino_tempfile::Utf8TempDir,
    pub sled_agent: sim::Server,
    pub sled_agent2_storage: camino_tempfile::Utf8TempDir,
    pub sled_agent2: sim::Server,
    pub oximeter: Oximeter,
    pub producer: ProducerServer,
    pub gateway: HashMap<SwitchLocation, GatewayTestContext>,
    pub dendrite: HashMap<SwitchLocation, dev::dendrite::DendriteInstance>,
    pub mgd: HashMap<SwitchLocation, dev::maghemite::MgdInstance>,
    pub external_dns_zone_name: String,
    pub external_dns: dns_server::TransientServer,
    pub internal_dns: dns_server::TransientServer,
    pub silo_name: Name,
    pub user_name: UserId,
}

impl<N: NexusServer> ControlPlaneTestContext<N> {
    pub fn wildcard_silo_dns_name(&self) -> String {
        format!("*.sys.{}", self.external_dns_zone_name)
    }

    pub async fn teardown(mut self) {
        self.server.close().await;
        self.database.cleanup().await.unwrap();
        self.clickhouse.cleanup().await.unwrap();
        self.sled_agent.http_server.close().await.unwrap();
        self.sled_agent2.http_server.close().await.unwrap();
        self.oximeter.close().await.unwrap();
        self.producer.close().await.unwrap();
        for (_, gateway) in self.gateway {
            gateway.teardown().await;
        }
        for (_, mut dendrite) in self.dendrite {
            dendrite.cleanup().await.unwrap();
        }
        for (_, mut mgd) in self.mgd {
            mgd.cleanup().await.unwrap();
        }
        self.logctx.cleanup_successful();
    }
}

pub fn load_test_config() -> NexusConfig {
    // We load as much configuration as we can from the test suite configuration
    // file.  In practice, TestContext requires that:
    //
    // - the Nexus TCP listen port be 0,
    // - the CockroachDB TCP listen port be 0, and
    // - if the log will go to a file then the path must be the sentinel value
    //   "UNUSED".
    // - each Nexus created for testing gets its own id so they don't see each
    //   others sagas and try to recover them
    //
    // (See LogContext::new() for details.)  Given these restrictions, it may
    // seem barely worth reading a config file at all.  However, developers can
    // change the logging level and local IP if they want, and as we add more
    // configuration options, we expect many of those can be usefully configured
    // (and reconfigured) for the test suite.
    let config_file_path = Utf8Path::new("tests/config.test.toml");
    let mut config = NexusConfig::from_file(config_file_path)
        .expect("failed to load config.test.toml");
    config.deployment.id = Uuid::new_v4();
    config
}

pub async fn test_setup<N: NexusServer>(
    test_name: &str,
) -> ControlPlaneTestContext<N> {
    let mut config = load_test_config();
    test_setup_with_config::<N>(
        test_name,
        &mut config,
        sim::SimMode::Explicit,
        None,
    )
    .await
}

struct RackInitRequestBuilder {
    services: Vec<nexus_types::internal_api::params::ServicePutRequest>,
    datasets: Vec<nexus_types::internal_api::params::DatasetCreateRequest>,
    internal_dns_config: internal_dns::DnsConfigBuilder,
    mac_addrs: Box<dyn Iterator<Item = MacAddr> + Send>,
}

impl RackInitRequestBuilder {
    fn new() -> Self {
        Self {
            services: vec![],
            datasets: vec![],
            internal_dns_config: internal_dns::DnsConfigBuilder::new(),
            mac_addrs: Box::new(MacAddr::iter_system()),
        }
    }

    // Keeps track of:
    // - The "ServicePutRequest" (for handoff to Nexus)
    // - The internal DNS configuration for this service
    fn add_service_with_id(
        &mut self,
        zone_id: Uuid,
        address: SocketAddrV6,
        kind: ServiceKind,
        service_name: internal_dns::ServiceName,
        sled_id: Uuid,
    ) {
        self.services.push(ServicePutRequest {
            address,
            kind,
            service_id: zone_id,
            sled_id,
            zone_id: Some(zone_id),
        });
        let zone = self
            .internal_dns_config
            .host_zone(zone_id, *address.ip())
            .expect("Failed to set up DNS for {kind}");
        self.internal_dns_config
            .service_backend_zone(service_name, &zone, address.port())
            .expect("Failed to set up DNS for {kind}");
    }

    fn add_service_without_dns(
        &mut self,
        zone_id: Uuid,
        address: SocketAddrV6,
        kind: ServiceKind,
        sled_id: Uuid,
    ) {
        self.services.push(ServicePutRequest {
            address,
            kind,
            service_id: zone_id,
            sled_id,
            zone_id: Some(zone_id),
        });
    }

    // Keeps track of:
    // - The "DatasetPutRequest" (for handoff to Nexus)
    // - The internal DNS configuration for this service
    fn add_dataset(
        &mut self,
        zpool_id: Uuid,
        dataset_id: Uuid,
        address: SocketAddrV6,
        kind: DatasetKind,
        service_name: internal_dns::ServiceName,
    ) {
        self.datasets.push(DatasetCreateRequest {
            zpool_id,
            dataset_id,
            request: DatasetPutRequest { address, kind },
        });
        let zone = self
            .internal_dns_config
            .host_zone(dataset_id, *address.ip())
            .expect("Failed to set up DNS for {kind}");
        self.internal_dns_config
            .service_backend_zone(service_name, &zone, address.port())
            .expect("Failed to set up DNS for {kind}");
    }
}

pub struct ControlPlaneTestContextBuilder<'a, N: NexusServer> {
    pub config: &'a mut NexusConfig,
    test_name: &'a str,
    rack_init_builder: RackInitRequestBuilder,

    pub start_time: chrono::DateTime<chrono::Utc>,
    pub logctx: LogContext,

    pub external_client: Option<ClientTestContext>,
    pub internal_client: Option<ClientTestContext>,

    pub server: Option<N>,
    pub database: Option<dev::db::CockroachInstance>,
    pub clickhouse: Option<dev::clickhouse::ClickHouseInstance>,
    pub sled_agent_storage: Option<camino_tempfile::Utf8TempDir>,
    pub sled_agent: Option<sim::Server>,
    pub sled_agent2_storage: Option<camino_tempfile::Utf8TempDir>,
    pub sled_agent2: Option<sim::Server>,
    pub oximeter: Option<Oximeter>,
    pub producer: Option<ProducerServer>,
    pub gateway: HashMap<SwitchLocation, GatewayTestContext>,
    pub dendrite: HashMap<SwitchLocation, dev::dendrite::DendriteInstance>,
    pub mgd: HashMap<SwitchLocation, dev::maghemite::MgdInstance>,

    // NOTE: Only exists after starting Nexus, until external Nexus is
    // initialized.
    nexus_internal: Option<<N as NexusServer>::InternalServer>,
    nexus_internal_addr: Option<SocketAddr>,

    pub external_dns_zone_name: Option<String>,
    pub external_dns: Option<dns_server::TransientServer>,
    pub internal_dns: Option<dns_server::TransientServer>,
    dns_config: Option<DnsConfigParams>,
    omicron_zones: Vec<OmicronZoneConfig>,
    omicron_zones2: Vec<OmicronZoneConfig>,

    pub silo_name: Option<Name>,
    pub user_name: Option<UserId>,
}

type StepInitFn<'a, N> = Box<
    dyn for<'b> FnOnce(
        &'b mut ControlPlaneTestContextBuilder<'a, N>,
    ) -> BoxFuture<'b, ()>,
>;

impl<'a, N: NexusServer> ControlPlaneTestContextBuilder<'a, N> {
    pub fn new(test_name: &'a str, config: &'a mut NexusConfig) -> Self {
        let start_time = chrono::Utc::now();
        let logctx = LogContext::new(test_name, &config.pkg.log);

        Self {
            config,
            test_name,
            rack_init_builder: RackInitRequestBuilder::new(),
            start_time,
            logctx,
            external_client: None,
            internal_client: None,
            server: None,
            database: None,
            clickhouse: None,
            sled_agent_storage: None,
            sled_agent: None,
            sled_agent2_storage: None,
            sled_agent2: None,
            oximeter: None,
            producer: None,
            gateway: HashMap::new(),
            dendrite: HashMap::new(),
            mgd: HashMap::new(),
            nexus_internal: None,
            nexus_internal_addr: None,
            external_dns_zone_name: None,
            external_dns: None,
            internal_dns: None,
            dns_config: None,
            omicron_zones: Vec::new(),
            omicron_zones2: Vec::new(),
            silo_name: None,
            user_name: None,
        }
    }

    pub async fn init_with_steps(
        &mut self,
        steps: Vec<(&str, StepInitFn<'a, N>)>,
        timeout: Duration,
    ) {
        let log = self.logctx.log.new(o!("component" => "init_with_steps"));
        for (step_name, step) in steps {
            debug!(log, "Running step {step_name}");
            let step_fut = step(self);
            match tokio::time::timeout(timeout, step_fut).await {
                Ok(()) => {}
                Err(_) => {
                    error!(
                        log,
                        "Timed out after {timeout:?} \
                         while running step {step_name}, failing test"
                    );
                    panic!(
                        "Timed out after {timeout:?} while running step {step_name}",
                    );
                }
            }
        }
    }

    pub async fn start_crdb(&mut self, populate: bool) {
        let populate = if populate {
            PopulateCrdb::FromEnvironmentSeed
        } else {
            PopulateCrdb::Empty
        };
        self.start_crdb_impl(populate).await;
    }

    /// Private implementation of `start_crdb` that allows for a seed tarball to
    /// be passed in. See [`PopulateCrdb`] for more details.
    async fn start_crdb_impl(&mut self, populate: PopulateCrdb) {
        let log = &self.logctx.log;
        debug!(log, "Starting CRDB");

        // Start up CockroachDB.
        let database = match populate {
            PopulateCrdb::FromEnvironmentSeed => {
                db::test_setup_database(log).await
            }
            #[cfg(feature = "omicron-dev")]
            PopulateCrdb::FromSeed { input_tar } => {
                db::test_setup_database_from_seed(log, input_tar).await
            }
            PopulateCrdb::Empty => db::test_setup_database_empty(log).await,
        };

        eprintln!("DB URL: {}", database.pg_config());
        let address = database
            .pg_config()
            .to_string()
            .split("postgresql://root@")
            .nth(1)
            .expect("Malformed URL: Missing postgresql prefix")
            .split('/')
            .next()
            .expect("Malformed URL: No slash after port")
            .parse::<std::net::SocketAddrV6>()
            .expect("Failed to parse port");

        let zpool_id = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        eprintln!("DB address: {}", address);
        self.rack_init_builder.add_dataset(
            zpool_id,
            dataset_id,
            address,
            DatasetKind::Cockroach,
            internal_dns::ServiceName::Cockroach,
        );
        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.omicron_zones.push(OmicronZoneConfig {
            id: dataset_id,
            underlay_address: *address.ip(),
            zone_type: OmicronZoneType::CockroachDb {
                address: address.to_string(),
                dataset: OmicronZoneDataset { pool_name },
            },
        });
        self.database = Some(database);
    }

    // Start ClickHouse database server.
    pub async fn start_clickhouse(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Starting Clickhouse");
        let clickhouse = dev::clickhouse::ClickHouseInstance::new_single_node(
            &self.logctx,
            0,
        )
        .await
        .unwrap();
        let port = clickhouse.port();

        let zpool_id = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);
        self.rack_init_builder.add_dataset(
            zpool_id,
            dataset_id,
            address,
            DatasetKind::Clickhouse,
            internal_dns::ServiceName::Clickhouse,
        );
        self.clickhouse = Some(clickhouse);

        // NOTE: We could pass this port information via DNS, rather than
        // requiring it to be known before Nexus starts.
        self.config
            .pkg
            .timeseries_db
            .address
            .as_mut()
            .expect("Tests expect to set a port of Clickhouse")
            .set_port(port);

        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.omicron_zones.push(OmicronZoneConfig {
            id: dataset_id,
            underlay_address: *address.ip(),
            zone_type: OmicronZoneType::Clickhouse {
                address: address.to_string(),
                dataset: OmicronZoneDataset { pool_name },
            },
        });
    }

    pub async fn start_gateway(
        &mut self,
        switch_location: SwitchLocation,
        port: Option<u16>,
    ) {
        debug!(&self.logctx.log, "Starting Management Gateway");
        let (mgs_config, sp_sim_config) =
            gateway_test_utils::setup::load_test_config();
        let mgs_addr =
            port.map(|port| SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0));
        let gateway = gateway_test_utils::setup::test_setup_with_config(
            self.test_name,
            gateway_messages::SpPort::One,
            mgs_config,
            &sp_sim_config,
            mgs_addr,
        )
        .await;
        self.gateway.insert(switch_location, gateway);
    }

    pub async fn start_dendrite(&mut self, switch_location: SwitchLocation) {
        let log = &self.logctx.log;
        debug!(log, "Starting Dendrite for {switch_location}");

        // Set up a stub instance of dendrite
        let dendrite = dev::dendrite::DendriteInstance::start(0).await.unwrap();
        let port = dendrite.port;
        self.dendrite.insert(switch_location, dendrite);

        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);

        // Update the configuration options for Nexus, if it's launched later.
        //
        // NOTE: If dendrite is started after Nexus, this is ignored.
        let config = DpdConfig { address: std::net::SocketAddr::V6(address) };
        self.config.pkg.dendrite.insert(switch_location, config);

        let sled_id = Uuid::parse_str(match switch_location {
            SwitchLocation::Switch0 => SLED_AGENT_UUID,
            SwitchLocation::Switch1 => SLED_AGENT2_UUID,
        })
        .unwrap();

        self.rack_init_builder.add_service_without_dns(
            sled_id,
            address,
            ServiceKind::Dendrite,
            sled_id,
        );
    }

    pub async fn start_mgd(&mut self, switch_location: SwitchLocation) {
        let log = &self.logctx.log;
        debug!(log, "Starting mgd for {switch_location}");

        // Set up an instance of mgd
        let mgd = dev::maghemite::MgdInstance::start(0).await.unwrap();
        let port = mgd.port;
        self.mgd.insert(switch_location, mgd);
        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);

        debug!(log, "mgd port is {port}");

        let config = MgdConfig { address: std::net::SocketAddr::V6(address) };
        self.config.pkg.mgd.insert(switch_location, config);

        let sled_id = Uuid::parse_str(match switch_location {
            SwitchLocation::Switch0 => SLED_AGENT_UUID,
            SwitchLocation::Switch1 => SLED_AGENT2_UUID,
        })
        .unwrap();

        self.rack_init_builder.add_service_without_dns(
            sled_id,
            address,
            ServiceKind::Mgd,
            sled_id,
        );
    }

    pub async fn record_switch_dns(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Recording DNS for the switch zones");
        for (sled_id, switch_location) in &[
            (SLED_AGENT_UUID, SwitchLocation::Switch0),
            (SLED_AGENT2_UUID, SwitchLocation::Switch1),
        ] {
            let id = sled_id.parse().unwrap();
            self.rack_init_builder
                .internal_dns_config
                .host_zone_switch(
                    id,
                    Ipv6Addr::LOCALHOST,
                    self.dendrite.get(switch_location).unwrap().port,
                    self.gateway
                        .get(switch_location)
                        .unwrap()
                        .client
                        .bind_address
                        .port(),
                    self.mgd.get(switch_location).unwrap().port,
                )
                .unwrap();
        }
    }

    pub async fn start_oximeter(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Starting Oximeter");

        let nexus_internal_addr = self
            .nexus_internal_addr
            .expect("Must start Nexus internally before Oximeter");

        let clickhouse = self
            .clickhouse
            .as_ref()
            .expect("Must start Clickhouse before oximeter");

        // Set up an Oximeter collector server
        let collector_id = Uuid::parse_str(OXIMETER_UUID).unwrap();
        let oximeter = start_oximeter(
            log.new(o!("component" => "oximeter")),
            nexus_internal_addr,
            clickhouse.port(),
            collector_id,
        )
        .await
        .unwrap();

        self.oximeter = Some(oximeter);
    }

    pub async fn start_producer_server(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Starting test metric Producer Server");

        let nexus_internal_addr = self
            .nexus_internal_addr
            .expect("Must start Nexus internally before producer server");

        // Set up a test metric producer server
        let producer_id = Uuid::parse_str(PRODUCER_UUID).unwrap();
        let producer = start_producer_server(nexus_internal_addr, producer_id)
            .await
            .unwrap();
        register_test_producer(&producer).unwrap();

        self.producer = Some(producer);
    }

    // Begin starting Nexus.
    pub async fn start_nexus_internal(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Starting Nexus (internal API)");

        self.config.deployment.internal_dns = InternalDns::FromAddress {
            address: self
                .internal_dns
                .as_ref()
                .expect("Must initialize internal DNS server first")
                .dns_server
                .local_address(),
        };
        self.config.deployment.database = Database::FromUrl {
            url: self
                .database
                .as_ref()
                .expect("Must start CRDB before Nexus")
                .pg_config()
                .clone(),
        };

        let (nexus_internal, nexus_internal_addr) =
            N::start_internal(&self.config, &log).await;

        let address = SocketAddrV6::new(
            match nexus_internal_addr.ip() {
                IpAddr::V4(addr) => addr.to_ipv6_mapped(),
                IpAddr::V6(addr) => addr,
            },
            nexus_internal_addr.port(),
            0,
            0,
        );

        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
        let mac = self
            .rack_init_builder
            .mac_addrs
            .next()
            .expect("ran out of MAC addresses");
        let external_address =
            self.config.deployment.dropshot_external.dropshot.bind_address.ip();
        let nexus_id = self.config.deployment.id;
        self.rack_init_builder.add_service_with_id(
            nexus_id,
            address,
            ServiceKind::Nexus {
                external_address,
                nic: ServiceNic {
                    id: Uuid::new_v4(),
                    name: "nexus".parse().unwrap(),
                    ip: NEXUS_OPTE_IPV4_SUBNET
                        .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
                        .unwrap()
                        .into(),
                    mac,
                    slot: 0,
                },
            },
            internal_dns::ServiceName::Nexus,
            sled_id,
        );

        self.omicron_zones.push(OmicronZoneConfig {
            id: nexus_id,
            underlay_address: *address.ip(),
            zone_type: OmicronZoneType::Nexus {
                external_dns_servers: self
                    .config
                    .deployment
                    .external_dns_servers
                    .clone(),
                external_ip: external_address,
                external_tls: self.config.deployment.dropshot_external.tls,
                internal_address: address.to_string(),
                nic: NetworkInterface {
                    id: Uuid::new_v4(),
                    ip: external_address,
                    kind: NetworkInterfaceKind::Service { id: nexus_id },
                    mac,
                    name: format!("nexus-{}", nexus_id).parse().unwrap(),
                    primary: true,
                    slot: 0,
                    subnet: (*NEXUS_OPTE_IPV4_SUBNET).into(),
                    vni: Vni::SERVICES_VNI,
                },
            },
        });

        self.nexus_internal = Some(nexus_internal);
        self.nexus_internal_addr = Some(nexus_internal_addr);
    }

    pub async fn populate_internal_dns(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Populating Internal DNS");

        // Populate the internal DNS system with all known DNS records
        let internal_dns_address = self
            .internal_dns
            .as_ref()
            .expect("Must start internal DNS server first")
            .dropshot_server
            .local_addr();
        let dns_config_client = dns_service_client::Client::new(
            &format!("http://{}", internal_dns_address),
            log.clone(),
        );

        let dns_config =
            self.rack_init_builder.internal_dns_config.clone().build();

        slog::info!(log, "DNS population: {:#?}", dns_config);
        dns_config_client.dns_config_put(&dns_config).await.expect(
            "Failed to send initial DNS records to internal DNS server",
        );
        self.dns_config = Some(dns_config);
    }

    // Perform RSS handoff
    pub async fn start_nexus_external(
        &mut self,
        tls_certificates: Vec<Certificate>,
    ) {
        let log = &self.logctx.log;
        debug!(log, "Starting Nexus (external API)");

        let dns_config = self.dns_config.clone().expect(
            "populate_internal_dns must be called before start_nexus_external",
        );

        // Create a recovery silo
        let external_dns_zone_name =
            internal_dns::names::DNS_ZONE_EXTERNAL_TESTING.to_string();
        let silo_name: Name = "test-suite-silo".parse().unwrap();
        let user_name =
            UserId::try_from("test-privileged".to_string()).unwrap();
        let user_password_hash = omicron_passwords::Hasher::default()
            .create_password(
                &omicron_passwords::Password::new(TEST_SUITE_PASSWORD).unwrap(),
            )
            .unwrap()
            .as_str()
            .parse()
            .unwrap();
        let recovery_silo = RecoverySiloConfig {
            silo_name: silo_name.clone(),
            user_name: user_name.clone(),
            user_password_hash,
        };

        // Handoff all known service information to Nexus
        let server = N::start(
            self.nexus_internal
                .take()
                .expect("Must launch internal nexus first"),
            self.config,
            Blueprint {
                id: Uuid::new_v4(),
                omicron_zones: BTreeMap::new(),
                zones_in_service: BTreeSet::new(),
                parent_blueprint_id: None,
                internal_dns_version: Generation::new(),
                time_created: Utc::now(),
                creator: "fixme".to_string(),
                comment: "fixme".to_string(),
            },
            self.rack_init_builder.services.clone(),
            // NOTE: We should probably hand off
            // "self.rack_init_builder.datasets" here, but Nexus won't be happy
            // if we pass it right now:
            //
            // - When we "call .add_dataset(...)", we need to keep track of
            // which zpool the dataset is coming from. For these synthetic
            // environments, we make this value up.
            // - When we tell Nexus about datasets, we need to provide the
            // parent zpool UUID, which must be known to Nexus's database.
            // - The sled agent we're creating to run alongside this test DOES
            // create synthetic zpools on boot, but
            //   (a) They're not the same zpools we're making up when we start
            //   Clickhouse / CRDB (we're basically making distinct calls to
            //   Uuid::new_v4()).
            //   (b) These sled-agent-created zpools are registered with Nexus
            //   asynchronously, and we're not making any effort (currently) to
            //   wait for them to be known to Nexus.
            vec![],
            dns_config,
            &external_dns_zone_name,
            recovery_silo,
            tls_certificates,
            SLED_AGENT2_UUID.parse().unwrap(),
        )
        .await;

        let external_server_addr =
            server.get_http_server_external_address().await;
        let internal_server_addr =
            server.get_http_server_internal_address().await;
        let testctx_external = ClientTestContext::new(
            external_server_addr,
            self.logctx
                .log
                .new(o!("component" => "external client test context")),
        );
        let testctx_internal = ClientTestContext::new(
            internal_server_addr,
            self.logctx
                .log
                .new(o!("component" => "internal client test context")),
        );

        self.external_dns_zone_name = Some(external_dns_zone_name);
        self.external_client = Some(testctx_external);
        self.internal_client = Some(testctx_internal);
        self.silo_name = Some(silo_name);
        self.user_name = Some(user_name);
        self.server = Some(server);
    }

    pub async fn start_sled(
        &mut self,
        switch_location: SwitchLocation,
        sim_mode: sim::SimMode,
    ) {
        let nexus_address =
            self.nexus_internal_addr.expect("Must launch Nexus first");

        // Set up a single sled agent.
        let sa_id: Uuid = if switch_location == SwitchLocation::Switch0 {
            SLED_AGENT_UUID
        } else {
            SLED_AGENT2_UUID
        }
        .parse()
        .unwrap();
        let tempdir = camino_tempfile::tempdir().unwrap();
        let sled_agent = start_sled_agent(
            self.logctx.log.new(o!(
                "component" => "omicron_sled_agent::sim::Server",
                "sled_id" => sa_id.to_string(),
            )),
            nexus_address,
            sa_id,
            tempdir.path(),
            sim_mode,
        )
        .await
        .expect("Failed to start sled agent");

        if switch_location == SwitchLocation::Switch0 {
            self.sled_agent = Some(sled_agent);
            self.sled_agent_storage = Some(tempdir);
        } else {
            self.sled_agent2 = Some(sled_agent);
            self.sled_agent2_storage = Some(tempdir);
        }
    }

    pub async fn configure_sled_agent(
        &mut self,
        switch_location: SwitchLocation,
    ) {
        let (field, zones) = if switch_location == SwitchLocation::Switch0 {
            (&self.sled_agent, &self.omicron_zones)
        } else {
            (&self.sled_agent2, &self.omicron_zones2)
        };

        // Tell our Sled Agent to report the zones that we configured.
        let Some(sled_agent) = field else {
            panic!("expected sled agent has not been created");
        };
        let client = sled_agent_client::Client::new(
            &format!("http://{}", sled_agent.http_server.local_addr()),
            self.logctx.log.clone(),
        );
        client
            .omicron_zones_put(&OmicronZonesConfig {
                zones: zones.clone(),
                generation: Generation::new().next(),
            })
            .await
            .expect("Failed to configure sled agent with our zones");
    }

    // Set up the Crucible Pantry on an existing Sled Agent.
    pub async fn start_crucible_pantry(&mut self) {
        let sled_agent = self
            .sled_agent
            .as_mut()
            .expect("Cannot start pantry without first starting sled agent");

        let pantry = sled_agent.start_pantry().await;
        let address = pantry.addr();

        let SocketAddr::V6(address) = address else {
            panic!("Expected IPv6 Pantry Address");
        };

        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
        let zone_id = Uuid::new_v4();
        self.rack_init_builder.add_service_with_id(
            zone_id,
            address,
            ServiceKind::CruciblePantry,
            internal_dns::ServiceName::CruciblePantry,
            sled_id,
        );
        self.omicron_zones.push(OmicronZoneConfig {
            id: zone_id,
            underlay_address: *address.ip(),
            zone_type: OmicronZoneType::CruciblePantry {
                address: address.to_string(),
            },
        });
    }

    // Set up an external DNS server.
    pub async fn start_external_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "external_dns_server"));
        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();

        let dns = dns_server::TransientServer::new(&log).await.unwrap();

        let SocketAddr::V6(dns_address) = dns.dns_server.local_address() else {
            panic!("Unsupported IPv4 DNS address");
        };
        let SocketAddr::V6(dropshot_address) = dns.dropshot_server.local_addr()
        else {
            panic!("Unsupported IPv4 Dropshot address");
        };

        let mac = self
            .rack_init_builder
            .mac_addrs
            .next()
            .expect("ran out of MAC addresses");
        let zone_id = Uuid::new_v4();
        self.rack_init_builder.add_service_with_id(
            zone_id,
            dropshot_address,
            ServiceKind::ExternalDns {
                external_address: (*dns_address.ip()).into(),
                nic: ServiceNic {
                    id: Uuid::new_v4(),
                    name: "external-dns".parse().unwrap(),
                    ip: DNS_OPTE_IPV4_SUBNET
                        .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
                        .unwrap()
                        .into(),
                    mac,
                    slot: 0,
                },
            },
            internal_dns::ServiceName::ExternalDns,
            sled_id,
        );

        let zpool_id = Uuid::new_v4();
        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.omicron_zones.push(OmicronZoneConfig {
            id: zone_id,
            underlay_address: *dropshot_address.ip(),
            zone_type: OmicronZoneType::ExternalDns {
                dataset: OmicronZoneDataset { pool_name },
                dns_address: dns_address.to_string(),
                http_address: dropshot_address.to_string(),
                nic: NetworkInterface {
                    id: Uuid::new_v4(),
                    ip: (*dns_address.ip()).into(),
                    kind: NetworkInterfaceKind::Service { id: zone_id },
                    mac,
                    name: format!("external-dns-{}", zone_id).parse().unwrap(),
                    primary: true,
                    slot: 0,
                    subnet: (*DNS_OPTE_IPV4_SUBNET).into(),
                    vni: Vni::SERVICES_VNI,
                },
            },
        });

        self.external_dns = Some(dns);
    }

    // Set up an internal DNS server.
    pub async fn start_internal_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "internal_dns_server"));
        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
        let dns = dns_server::TransientServer::new(&log).await.unwrap();

        let SocketAddr::V6(address) = dns.dropshot_server.local_addr() else {
            panic!("Unsupported IPv4 DNS address");
        };
        let zone_id = Uuid::new_v4();
        self.rack_init_builder.add_service_with_id(
            zone_id,
            address,
            ServiceKind::InternalDns,
            internal_dns::ServiceName::InternalDns,
            sled_id,
        );

        let zpool_id = Uuid::new_v4();
        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.omicron_zones.push(OmicronZoneConfig {
            id: zone_id,
            underlay_address: *address.ip(),
            zone_type: OmicronZoneType::InternalDns {
                dataset: OmicronZoneDataset { pool_name },
                dns_address: dns.dns_server.local_address().to_string(),
                http_address: address.to_string(),
                gz_address: Ipv6Addr::LOCALHOST,
                gz_address_index: 0,
            },
        });

        self.internal_dns = Some(dns);
    }

    pub fn build(self) -> ControlPlaneTestContext<N> {
        ControlPlaneTestContext {
            start_time: self.start_time,
            server: self.server.unwrap(),
            external_client: self.external_client.unwrap(),
            internal_client: self.internal_client.unwrap(),
            database: self.database.unwrap(),
            clickhouse: self.clickhouse.unwrap(),
            sled_agent_storage: self.sled_agent_storage.unwrap(),
            sled_agent: self.sled_agent.unwrap(),
            sled_agent2_storage: self.sled_agent2_storage.unwrap(),
            sled_agent2: self.sled_agent2.unwrap(),
            oximeter: self.oximeter.unwrap(),
            producer: self.producer.unwrap(),
            logctx: self.logctx,
            gateway: self.gateway,
            dendrite: self.dendrite,
            mgd: self.mgd,
            external_dns_zone_name: self.external_dns_zone_name.unwrap(),
            external_dns: self.external_dns.unwrap(),
            internal_dns: self.internal_dns.unwrap(),
            silo_name: self.silo_name.unwrap(),
            user_name: self.user_name.unwrap(),
        }
    }

    pub async fn teardown(self) {
        if let Some(server) = self.server {
            server.close().await;
        }
        if let Some(mut database) = self.database {
            database.cleanup().await.unwrap();
        }
        if let Some(mut clickhouse) = self.clickhouse {
            clickhouse.cleanup().await.unwrap();
        }
        if let Some(sled_agent) = self.sled_agent {
            sled_agent.http_server.close().await.unwrap();
        }
        if let Some(sled_agent2) = self.sled_agent2 {
            sled_agent2.http_server.close().await.unwrap();
        }
        if let Some(oximeter) = self.oximeter {
            oximeter.close().await.unwrap();
        }
        if let Some(producer) = self.producer {
            producer.close().await.unwrap();
        }
        for (_, gateway) in self.gateway {
            gateway.teardown().await;
        }
        for (_, mut dendrite) in self.dendrite {
            dendrite.cleanup().await.unwrap();
        }
        for (_, mut mgd) in self.mgd {
            mgd.cleanup().await.unwrap();
        }
        self.logctx.cleanup_successful();
    }
}

/// How to populate CockroachDB.
///
/// This is private because we want to ensure that tests use the setup script
/// rather than trying to create their own seed tarballs. This may need to be
/// revisited if circumstances change.
#[derive(Clone, Debug)]
enum PopulateCrdb {
    /// Populate Cockroach from the `CRDB_SEED_TAR_ENV` environment variable.
    ///
    /// Any tests that depend on nexus-test-utils should have this environment
    /// variable available.
    FromEnvironmentSeed,

    /// Populate Cockroach from the seed located at this path.
    #[cfg(feature = "omicron-dev")]
    FromSeed { input_tar: camino::Utf8PathBuf },

    /// Do not populate Cockroach.
    Empty,
}

/// Setup routine to use for `omicron-dev`. Use [`test_setup_with_config`] for
/// tests.
///
/// The main difference from tests is that this routine ensures the seed tarball
/// exists (or creates a seed tarball if it doesn't exist). For tests, this
/// should be done in the `crdb-seed` setup script.
#[cfg(feature = "omicron-dev")]
pub async fn omicron_dev_setup_with_config<N: NexusServer>(
    config: &mut NexusConfig,
) -> Result<ControlPlaneTestContext<N>> {
    let builder =
        ControlPlaneTestContextBuilder::<N>::new("omicron-dev", config);

    let log = &builder.logctx.log;
    debug!(log, "Ensuring seed tarball exists");

    // Start up a ControlPlaneTestContext, which tautologically sets up
    // everything needed for a simulated control plane.
    let why_invalidate =
        omicron_test_utils::dev::seed::should_invalidate_seed();
    let (seed_tar, status) =
        omicron_test_utils::dev::seed::ensure_seed_tarball_exists(
            log,
            why_invalidate,
        )
        .await
        .context("error ensuring seed tarball exists")?;
    status.log(log, &seed_tar);

    Ok(setup_with_config_impl(
        builder,
        PopulateCrdb::FromSeed { input_tar: seed_tar },
        sim::SimMode::Auto,
        None,
    )
    .await)
}

/// Setup routine to use for tests.
pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut NexusConfig,
    sim_mode: sim::SimMode,
    initial_cert: Option<Certificate>,
) -> ControlPlaneTestContext<N> {
    let builder = ControlPlaneTestContextBuilder::<N>::new(test_name, config);
    setup_with_config_impl(
        builder,
        PopulateCrdb::FromEnvironmentSeed,
        sim_mode,
        initial_cert,
    )
    .await
}

async fn setup_with_config_impl<N: NexusServer>(
    mut builder: ControlPlaneTestContextBuilder<'_, N>,
    populate: PopulateCrdb,
    sim_mode: sim::SimMode,
    initial_cert: Option<Certificate>,
) -> ControlPlaneTestContext<N> {
    const STEP_TIMEOUT: Duration = Duration::from_secs(60);

    builder
        .init_with_steps(
            vec![
                (
                    "start_crdb",
                    Box::new(|builder| {
                        builder.start_crdb_impl(populate).boxed()
                    }),
                ),
                (
                    "start_clickhouse",
                    Box::new(|builder| builder.start_clickhouse().boxed()),
                ),
                (
                    "start_gateway_switch0",
                    Box::new(|builder| {
                        builder
                            .start_gateway(SwitchLocation::Switch0, None)
                            .boxed()
                    }),
                ),
                (
                    "start_gateway_switch1",
                    Box::new(|builder| {
                        builder
                            .start_gateway(SwitchLocation::Switch1, None)
                            .boxed()
                    }),
                ),
                (
                    "start_dendrite_switch0",
                    Box::new(|builder| {
                        builder.start_dendrite(SwitchLocation::Switch0).boxed()
                    }),
                ),
                (
                    "start_dendrite_switch1",
                    Box::new(|builder| {
                        builder.start_dendrite(SwitchLocation::Switch1).boxed()
                    }),
                ),
                (
                    "start_mgd_switch0",
                    Box::new(|builder| {
                        builder.start_mgd(SwitchLocation::Switch0).boxed()
                    }),
                ),
                (
                    "start_mgd_switch1",
                    Box::new(|builder| {
                        builder.start_mgd(SwitchLocation::Switch1).boxed()
                    }),
                ),
                (
                    "record_switch_dns",
                    Box::new(|builder| builder.record_switch_dns().boxed()),
                ),
                (
                    "start_internal_dns",
                    Box::new(|builder| builder.start_internal_dns().boxed()),
                ),
                (
                    "start_external_dns",
                    Box::new(|builder| builder.start_external_dns().boxed()),
                ),
                (
                    "start_nexus_internal",
                    Box::new(|builder| builder.start_nexus_internal().boxed()),
                ),
                (
                    "start_sled1",
                    Box::new(move |builder| {
                        builder
                            .start_sled(SwitchLocation::Switch0, sim_mode)
                            .boxed()
                    }),
                ),
                (
                    "start_sled2",
                    Box::new(move |builder| {
                        builder
                            .start_sled(SwitchLocation::Switch1, sim_mode)
                            .boxed()
                    }),
                ),
                (
                    "start_crucible_pantry",
                    Box::new(|builder| builder.start_crucible_pantry().boxed()),
                ),
                (
                    "populate_internal_dns",
                    Box::new(|builder| builder.populate_internal_dns().boxed()),
                ),
                (
                    "configure_sled_agent1",
                    Box::new(|builder| {
                        builder
                            .configure_sled_agent(SwitchLocation::Switch0)
                            .boxed()
                    }),
                ),
                (
                    "configure_sled_agent2",
                    Box::new(|builder| {
                        builder
                            .configure_sled_agent(SwitchLocation::Switch1)
                            .boxed()
                    }),
                ),
                (
                    "start_nexus_external",
                    Box::new(|builder| {
                        builder
                            .start_nexus_external(
                                initial_cert.into_iter().collect(),
                            )
                            .boxed()
                    }),
                ),
                (
                    "start_oximeter",
                    Box::new(|builder| builder.start_oximeter().boxed()),
                ),
                (
                    "start_producer_server",
                    Box::new(|builder| builder.start_producer_server().boxed()),
                ),
            ],
            STEP_TIMEOUT,
        )
        .await;

    builder.build()
}

pub async fn start_sled_agent(
    log: Logger,
    nexus_address: SocketAddr,
    id: Uuid,
    update_directory: &Utf8Path,
    sim_mode: sim::SimMode,
) -> Result<sim::Server, String> {
    let config = sim::Config::for_testing(
        id,
        sim_mode,
        Some(nexus_address),
        Some(update_directory),
        None,
    );
    let server = sim::Server::start(&config, &log, true)
        .await
        .map_err(|e| e.to_string())?;
    Ok(server)
}

pub async fn start_oximeter(
    log: Logger,
    nexus_address: SocketAddr,
    db_port: u16,
    id: Uuid,
) -> Result<Oximeter, String> {
    let db = oximeter_collector::DbConfig {
        address: Some(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), db_port)),
        batch_size: 10,
        batch_interval: 1,
    };
    let config = oximeter_collector::Config {
        nexus_address: Some(nexus_address),
        db,
        log: ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Error },
    };
    let args = oximeter_collector::OximeterArguments {
        id,
        address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
    };
    Oximeter::with_logger(&config, &args, log).await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, oximeter::Target)]
struct IntegrationTarget {
    pub target_name: String,
}

#[derive(Debug, Clone, oximeter::Metric)]
struct IntegrationMetric {
    pub metric_name: String,
    pub datum: i64,
}

// A producer of simple counter metrics used in the integration tests
#[derive(Debug, Clone)]
struct IntegrationProducer {
    pub target: IntegrationTarget,
    pub metric: IntegrationMetric,
}

impl oximeter::Producer for IntegrationProducer {
    fn produce(
        &mut self,
    ) -> Result<
        Box<(dyn Iterator<Item = oximeter::types::Sample> + 'static)>,
        oximeter::MetricsError,
    > {
        use oximeter::Metric;
        let sample = oximeter::types::Sample::new(&self.target, &self.metric)?;
        *self.metric.datum_mut() += 1;
        Ok(Box::new(vec![sample].into_iter()))
    }
}

/// Creates and starts a producer server.
///
/// Actual producers can be registered with the [`register_producer`]
/// helper function.
pub async fn start_producer_server(
    nexus_address: SocketAddr,
    id: Uuid,
) -> Result<ProducerServer, String> {
    // Set up a producer server.
    //
    // This listens on any available port, and the server internally updates this to the actual
    // bound port of the Dropshot HTTP server.
    let producer_address = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0);
    let server_info = ProducerEndpoint {
        id,
        kind: ProducerKind::Service,
        address: producer_address,
        base_route: "/collect".to_string(),
        interval: Duration::from_secs(1),
    };
    let config = oximeter_producer::Config {
        server_info,
        registration_address: nexus_address,
        dropshot: ConfigDropshot {
            bind_address: producer_address,
            ..Default::default()
        },
        log: LogConfig::Config(ConfigLogging::StderrTerminal {
            level: ConfigLoggingLevel::Error,
        }),
    };
    let server =
        ProducerServer::start(&config).await.map_err(|e| e.to_string())?;
    Ok(server)
}

/// Registers an arbitrary producer with the test server.
pub fn register_producer(
    server: &ProducerServer,
    producer: impl oximeter::Producer,
) -> Result<(), String> {
    server.registry().register_producer(producer).map_err(|e| e.to_string())?;
    Ok(())
}

/// Registers a sample-generating test-specific producer.
pub fn register_test_producer(server: &ProducerServer) -> Result<(), String> {
    // Create and register an actual metric producer.
    let test_producer = IntegrationProducer {
        target: IntegrationTarget {
            target_name: "integration-test-target".to_string(),
        },
        metric: IntegrationMetric {
            metric_name: "integration-test-metric".to_string(),
            datum: 0,
        },
    };
    register_producer(server, test_producer)
}

/// Returns whether the two identity metadata objects are identical.
pub fn identity_eq(ident1: &IdentityMetadata, ident2: &IdentityMetadata) {
    assert_eq!(ident1.id, ident2.id);
    assert_eq!(ident1.name, ident2.name);
    assert_eq!(ident1.description, ident2.description);
    assert_eq!(ident1.time_created, ident2.time_created);
    assert_eq!(ident1.time_modified, ident2.time_modified);
}

/// Order-agnostic vec equality
pub fn assert_same_items<T: PartialEq + Debug>(v1: Vec<T>, v2: Vec<T>) {
    assert_eq!(v1.len(), v2.len(), "{:?} and {:?} don't match", v1, v2);
    for item in v1.iter() {
        assert!(v2.contains(item), "{:?} and {:?} don't match", v1, v2);
    }
}

pub async fn start_dns_server(
    log: slog::Logger,
    storage_path: &Utf8Path,
) -> Result<
    (
        dns_server::dns_server::ServerHandle,
        dropshot::HttpServer<dns_server::http_server::Context>,
        TokioAsyncResolver,
    ),
    anyhow::Error,
> {
    let config_store = dns_server::storage::Config {
        keep_old_generations: 3,
        storage_path: storage_path.into(),
    };
    let store = dns_server::storage::Store::new(
        log.new(o!("component" => "DnsStore")),
        &config_store,
    )
    .unwrap();

    let (dns_server, http_server) = dns_server::start_servers(
        log,
        store,
        &dns_server::dns_server::Config {
            bind_address: "[::1]:0".parse().unwrap(),
        },
        &dropshot::ConfigDropshot {
            bind_address: "[::1]:0".parse().unwrap(),
            request_body_max_bytes: 8 * 1024,
            default_handler_task_mode: HandlerTaskMode::Detached,
        },
    )
    .await
    .unwrap();

    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig {
        socket_addr: dns_server.local_address(),
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_nx_responses: false,
        bind_addr: None,
    });
    let resolver =
        TokioAsyncResolver::tokio(resolver_config, ResolverOpts::default())
            .context("creating DNS resolver")?;

    Ok((dns_server, http_server, resolver))
}
