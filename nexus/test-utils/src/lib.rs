// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

#[cfg(feature = "omicron-dev")]
use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::Utc;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HandlerTaskMode;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use gateway_test_utils::setup::GatewayTestContext;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::proto::xfer::Protocol;
use iddqd::IdOrdMap;
use internal_dns_types::config::DnsConfigBuilder;
use internal_dns_types::names::DNS_ZONE_EXTERNAL_TESTING;
use internal_dns_types::names::ServiceName;
use nexus_config::Database;
use nexus_config::DpdConfig;
use nexus_config::InternalDns;
use nexus_config::MgdConfig;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_config::NexusConfig;
use nexus_db_queries::db::pub_test_utils::crdb;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::SledCpuFamily;
use nexus_sled_agent_shared::recovery_silo::RecoverySiloConfig;
use nexus_test_interface::InternalServer;
use nexus_test_interface::NexusServer;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintSledConfig;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
use nexus_types::deployment::OmicronZoneExternalFloatingIp;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::deployment::OximeterReadMode;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::ReconfiguratorConfig;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::external_api::views::SledState;
use nexus_types::internal_api::params::DnsConfigParams;
use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
use omicron_common::address::DNS_OPTE_IPV6_SUBNET;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::address::NTP_OPTE_IPV4_SUBNET;
use omicron_common::address::NTP_PORT;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::UserId;
use omicron_common::api::external::Vni;
use omicron_common::api::external::{IdentityMetadata, Name};
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::zpool_name::ZpoolName;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use oximeter_collector::Oximeter;
use oximeter_producer::LogConfig;
use oximeter_producer::Server as ProducerServer;
use sled_agent_client::types::EarlyNetworkConfig;
use sled_agent_client::types::EarlyNetworkConfigBody;
use sled_agent_client::types::RackNetworkConfigV2;
use slog::{Logger, debug, error, o};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::iter::{once, repeat, zip};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

use nexus_types::deployment::PendingMgsUpdates;
pub use sim::TEST_HARDWARE_THREADS;
pub use sim::TEST_RESERVOIR_RAM;

pub mod background;
pub mod db;
pub mod http_testing;
pub mod resource_helpers;
pub mod sql;

pub const SLED_AGENT_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
pub const SLED_AGENT2_UUID: &str = "039be560-54cc-49e3-88df-1a29dadbf913";
pub const RACK_UUID: &str = nexus_db_queries::db::pub_test_utils::RACK_UUID;
pub const SWITCH_UUID: &str = "dae4e1f1-410e-4314-bff1-fec0504be07e";
pub const PHYSICAL_DISK_UUID: &str = "fbf4e1f1-410e-4314-bff1-fec0504be07e";
pub const OXIMETER_UUID: &str = "39e6175b-4df2-4730-b11d-cbc1e60a2e78";
pub const PRODUCER_UUID: &str = "a6458b7d-87c3-4483-be96-854d814c20de";
pub const RACK_SUBNET: &str = "fd00:1122:3344:0100::/56";

/// Password for the user created by the test suite
///
/// This is only used by the test suite and `omicron-dev run-all` (the latter of
/// which uses the test suite setup code for most of its operation).   These are
/// both transient deployments with no sensitive data.
pub const TEST_SUITE_PASSWORD: &str = "oxide";

pub struct ControlPlaneTestContextSledAgent {
    _storage: camino_tempfile::Utf8TempDir,

    server: sim::Server,
}

impl ControlPlaneTestContextSledAgent {
    pub fn sled_agent(&self) -> &Arc<sim::SledAgent> {
        &self.server.sled_agent
    }

    pub fn server(&self) -> &sim::Server {
        &self.server
    }

    pub fn sled_agent_id(&self) -> SledUuid {
        self.server.sled_agent.id
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.http_server.local_addr()
    }

    pub async fn start_pantry(&mut self) -> &sim::PantryServer {
        self.server.start_pantry().await
    }

    pub async fn teardown(self) {
        self.server.http_server.close().await.unwrap();
    }
}

pub struct ControlPlaneTestContext<N> {
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub external_client: ClientTestContext,
    pub techport_client: ClientTestContext,
    pub internal_client: ClientTestContext,
    pub lockstep_client: ClientTestContext,
    pub server: N,
    pub database: dev::db::CockroachInstance,
    pub database_admin: omicron_cockroach_admin::Server,
    pub clickhouse: dev::clickhouse::ClickHouseDeployment,
    pub logctx: LogContext,
    pub sled_agents: Vec<ControlPlaneTestContextSledAgent>,
    pub oximeter: Oximeter,
    pub producer: ProducerServer,
    pub gateway: BTreeMap<SwitchLocation, GatewayTestContext>,
    pub dendrite:
        RwLock<HashMap<SwitchLocation, dev::dendrite::DendriteInstance>>,
    pub mgd: HashMap<SwitchLocation, dev::maghemite::MgdInstance>,
    pub external_dns_zone_name: String,
    pub external_dns: dns_server::TransientServer,
    pub internal_dns: dns_server::TransientServer,
    pub initial_blueprint_id: BlueprintUuid,
    pub silo_name: Name,
    pub user_name: UserId,
    pub password: String,
}

impl<N: NexusServer> ControlPlaneTestContext<N> {
    /// Return the first simulated ['sim::Server']
    pub fn first_sim_server(&self) -> &sim::Server {
        self.sled_agents[0].server()
    }

    /// Return the first simulated Sled Agent
    pub fn first_sled_agent(&self) -> &Arc<sim::SledAgent> {
        self.sled_agents[0].sled_agent()
    }

    pub fn first_sled_id(&self) -> SledUuid {
        self.sled_agents[0].sled_agent_id()
    }

    pub fn second_sled_id(&self) -> SledUuid {
        self.sled_agents[1].sled_agent_id()
    }

    pub fn all_sled_agents(&self) -> impl Iterator<Item = &sim::Server> {
        self.sled_agents.iter().map(|sa| sa.server())
    }

    /// Return an iterator over all sled agents except the first one
    pub fn extra_sled_agents(&self) -> impl Iterator<Item = &sim::Server> {
        self.all_sled_agents().skip(1)
    }

    /// Find a sled agent that doesn't match the provided ID
    pub fn find_sled_agent(&self, exclude_sled: SledUuid) -> Option<SledUuid> {
        self.all_sled_agents()
            .find(|sa| sa.sled_agent.id != exclude_sled)
            .map(|sa| sa.sled_agent.id)
    }

    pub fn wildcard_silo_dns_name(&self) -> String {
        format!("*.sys.{}", self.external_dns_zone_name)
    }

    /// Wait until at least one inventory collection has been inserted into the
    /// datastore.
    ///
    /// # Panics
    ///
    /// Panics if an inventory collection is not found within `timeout`.
    pub async fn wait_for_at_least_one_inventory_collection(
        &self,
        timeout: Duration,
    ) {
        let mut inv_rx = self.server.inventory_load_rx();

        match wait_for_watch_channel_condition(
            &mut inv_rx,
            async |inv| {
                if inv.is_some() {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            timeout,
        )
        .await
        {
            Ok(()) => (),
            Err(poll::Error::TimedOut(elapsed)) => {
                panic!("no inventory collection found within {elapsed:?}");
            }
            Err(poll::Error::PermanentError(())) => {
                unreachable!("check can only fail via timeout")
            }
        }
    }

    pub fn internal_client(&self) -> nexus_client::Client {
        nexus_client::Client::new(
            &format!("http://{}", self.internal_client.bind_address),
            self.internal_client.client_log.clone(),
        )
    }

    pub fn lockstep_client(&self) -> nexus_lockstep_client::Client {
        nexus_lockstep_client::Client::new(
            &format!("http://{}", self.lockstep_client.bind_address),
            self.lockstep_client.client_log.clone(),
        )
    }

    /// Stop a Dendrite instance for testing failure scenarios.
    pub async fn stop_dendrite(
        &self,
        switch_location: omicron_common::api::external::SwitchLocation,
    ) {
        use slog::debug;
        let log = &self.logctx.log;
        debug!(log, "Stopping Dendrite for {switch_location}");

        let dendrite_opt =
            { self.dendrite.write().unwrap().remove(&switch_location) };
        if let Some(mut dendrite) = dendrite_opt {
            dendrite.cleanup().await.unwrap();
        }
    }

    pub async fn teardown(mut self) {
        self.server.close().await;
        self.database.cleanup().await.unwrap();
        self.clickhouse.cleanup().await.unwrap();

        for sled_agent in self.sled_agents {
            sled_agent.teardown().await;
        }

        self.oximeter.close().await.unwrap();
        self.producer.close().await.unwrap();
        for (_, gateway) in self.gateway {
            gateway.teardown().await;
        }
        for (_, mut dendrite) in self.dendrite.into_inner().unwrap() {
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
    //
    // (See LogContext::new() for details.)  Given these restrictions, it may
    // seem barely worth reading a config file at all.  However, developers can
    // change the logging level and local IP if they want, and as we add more
    // configuration options, we expect many of those can be usefully configured
    // (and reconfigured) for the test suite.
    let config_file_path = Utf8Path::new("tests/config.test.toml");
    NexusConfig::from_file(config_file_path)
        .expect("failed to load config.test.toml")
}

pub async fn test_setup<N: NexusServer>(
    test_name: &str,
    extra_sled_agents: u16,
) -> ControlPlaneTestContext<N> {
    let mut config = load_test_config();
    test_setup_with_config::<N>(
        test_name,
        &mut config,
        sim::SimMode::Explicit,
        None,
        extra_sled_agents,
        DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await
}

struct RackInitRequestBuilder {
    internal_dns_config: DnsConfigBuilder,
    mac_addrs: Box<dyn Iterator<Item = MacAddr> + Send>,
}

impl RackInitRequestBuilder {
    fn new() -> Self {
        Self {
            internal_dns_config: DnsConfigBuilder::new(),
            mac_addrs: Box::new(MacAddr::iter_system()),
        }
    }

    fn add_service_to_dns(
        &mut self,
        zone_id: OmicronZoneUuid,
        address: SocketAddrV6,
        service_name: ServiceName,
    ) {
        let zone = self
            .internal_dns_config
            .host_zone(zone_id, *address.ip())
            .expect("Failed to set up DNS for {kind}");
        self.internal_dns_config
            .service_backend_zone(service_name, &zone, address.port())
            .expect("Failed to set up DNS for {kind}");
    }

    fn add_gz_service_to_dns(
        &mut self,
        sled_id: SledUuid,
        address: SocketAddrV6,
        service_name: ServiceName,
    ) {
        let sled = self
            .internal_dns_config
            .host_sled(sled_id, *address.ip())
            .expect("Failed to set up DNS for GZ service");
        self.internal_dns_config
            .service_backend_sled(service_name, &sled, address.port())
            .expect("Failed to set up DNS for GZ service");
    }

    // Special handling of Nexus, which has multiple SRV records for its single
    // zone.
    fn add_nexus_to_dns(
        &mut self,
        zone_id: OmicronZoneUuid,
        address: SocketAddrV6,
        lockstep_port: u16,
    ) {
        self.internal_dns_config
            .host_zone_nexus(zone_id, address, lockstep_port)
            .expect("Failed to set up Nexus DNS");
    }

    // Special handling of ClickHouse, which has multiple SRV records for its
    // single zone.
    fn add_clickhouse_to_dns(
        &mut self,
        zone_id: OmicronZoneUuid,
        address: SocketAddrV6,
    ) {
        self.internal_dns_config
            .host_zone_clickhouse_single_node(
                zone_id,
                ServiceName::Clickhouse,
                address,
                true,
            )
            .expect("Failed to setup ClickHouse DNS");
    }

    // Special handling of internal DNS, which has a second A/AAAA record and an
    // NS record pointing to it.
    fn add_internal_name_server_to_dns(
        &mut self,
        zone_id: OmicronZoneUuid,
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
    ) {
        self.internal_dns_config
            .host_zone_internal_dns(
                zone_id,
                ServiceName::InternalDns,
                http_address,
                dns_address,
            )
            .expect("Failed to setup internal DNS");
    }
}

pub struct ControlPlaneTestContextBuilder<'a, N: NexusServer> {
    pub config: &'a mut NexusConfig,
    test_name: &'a str,
    rack_init_builder: RackInitRequestBuilder,

    pub start_time: chrono::DateTime<chrono::Utc>,
    pub logctx: LogContext,

    pub external_client: Option<ClientTestContext>,
    pub techport_client: Option<ClientTestContext>,
    pub internal_client: Option<ClientTestContext>,
    pub lockstep_client: Option<ClientTestContext>,

    pub server: Option<N>,
    pub database: Option<dev::db::CockroachInstance>,
    pub database_admin: Option<omicron_cockroach_admin::Server>,
    pub clickhouse: Option<dev::clickhouse::ClickHouseDeployment>,
    pub sled_agents: Vec<ControlPlaneTestContextSledAgent>,
    pub oximeter: Option<Oximeter>,
    pub producer: Option<ProducerServer>,
    pub gateway: BTreeMap<SwitchLocation, GatewayTestContext>,
    pub dendrite:
        RwLock<HashMap<SwitchLocation, dev::dendrite::DendriteInstance>>,
    pub mgd: HashMap<SwitchLocation, dev::maghemite::MgdInstance>,

    // NOTE: Only exists after starting Nexus, until external Nexus is
    // initialized.
    nexus_internal: Option<<N as NexusServer>::InternalServer>,
    nexus_internal_addr: Option<SocketAddr>,

    pub external_dns_zone_name: Option<String>,
    pub external_dns: Option<dns_server::TransientServer>,
    pub internal_dns: Option<dns_server::TransientServer>,
    dns_config: Option<DnsConfigParams>,
    initial_blueprint_id: Option<BlueprintUuid>,

    // Build sled configs as we go, ensuring that sled-agent's
    // initial configuration agrees with the blueprint we build.
    blueprint_zones: Vec<BlueprintZoneConfig>,
    blueprint_sleds: Option<BTreeMap<SledUuid, BlueprintSledConfig>>,

    pub silo_name: Option<Name>,
    pub user_name: Option<UserId>,
    pub password: Option<String>,

    pub simulated_upstairs: Arc<sim::SimulatedUpstairs>,
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

        let simulated_upstairs_log = logctx.log.new(o!(
            "component" => "omicron_sled_agent::sim::SimulatedUpstairs",
        ));

        Self {
            config,
            test_name,
            rack_init_builder: RackInitRequestBuilder::new(),
            start_time,
            logctx,
            external_client: None,
            techport_client: None,
            internal_client: None,
            lockstep_client: None,
            server: None,
            database: None,
            database_admin: None,
            clickhouse: None,
            sled_agents: vec![],
            oximeter: None,
            producer: None,
            gateway: BTreeMap::new(),
            dendrite: RwLock::new(HashMap::new()),
            mgd: HashMap::new(),
            nexus_internal: None,
            nexus_internal_addr: None,
            external_dns_zone_name: None,
            external_dns: None,
            internal_dns: None,
            dns_config: None,
            initial_blueprint_id: None,
            blueprint_zones: Vec::new(),
            blueprint_sleds: None,
            silo_name: None,
            user_name: None,
            password: None,
            simulated_upstairs: Arc::new(sim::SimulatedUpstairs::new(
                simulated_upstairs_log,
            )),
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
                crdb::test_setup_database(log).await
            }
            #[cfg(feature = "omicron-dev")]
            PopulateCrdb::FromSeed { input_tar } => {
                crdb::test_setup_database_from_seed(log, input_tar).await
            }
            PopulateCrdb::Empty => crdb::test_setup_database_empty(log).await,
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

        let zone_id = OmicronZoneUuid::new_v4();
        let zpool_id = ZpoolUuid::new_v4();
        eprintln!("DB address: {}", address);
        self.rack_init_builder.add_service_to_dns(
            zone_id,
            address,
            ServiceName::Cockroach,
        );
        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: ZpoolName::new_external(zpool_id),
            zone_type: BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb {
                    address,
                    dataset: OmicronZoneDataset { pool_name },
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });
        let http_address = database.http_addr();
        self.database = Some(database);

        let cli = omicron_cockroach_admin::CockroachCli::new(
            omicron_test_utils::dev::db::COCKROACHDB_BIN.into(),
            address,
            http_address,
        );
        let server = omicron_cockroach_admin::start_server(
            zone_id,
            cli,
            omicron_cockroach_admin::Config {
                dropshot: dropshot::ConfigDropshot::default(),
                log: ConfigLogging::StderrTerminal {
                    level: ConfigLoggingLevel::Error,
                },
            },
        )
        .await
        .expect("Failed to start CRDB admin server");

        self.database_admin = Some(server);
    }

    // Start ClickHouse database server.
    pub async fn start_clickhouse(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Starting Clickhouse");
        let clickhouse =
            dev::clickhouse::ClickHouseDeployment::new_single_node(
                &self.logctx,
            )
            .await
            .unwrap();

        let zone_id = OmicronZoneUuid::new_v4();
        let zpool_id = ZpoolUuid::new_v4();
        let http_address = clickhouse.http_address();
        let http_port = http_address.port();
        let native_address = clickhouse.native_address();
        self.rack_init_builder.add_clickhouse_to_dns(zone_id, http_address);
        self.clickhouse = Some(clickhouse);

        // NOTE: We could pass this port information via DNS, rather than
        // requiring it to be known before Nexus starts.
        //
        // See https://github.com/oxidecomputer/omicron/issues/6407.
        self.config
            .pkg
            .timeseries_db
            .address
            .as_mut()
            .expect("Tests expect to set a port of Clickhouse")
            .set_port(http_port);
        self.config.pkg.timeseries_db.address = Some(native_address.into());

        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: ZpoolName::new_external(zpool_id),
            zone_type: BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse {
                    address: http_address,
                    dataset: OmicronZoneDataset { pool_name },
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });
    }

    pub async fn start_gateway(
        &mut self,
        switch_location: SwitchLocation,
        port: Option<u16>,
        sp_sim_config_file: Utf8PathBuf,
    ) {
        debug!(&self.logctx.log, "Starting Management Gateway");
        let (mut mgs_config, sp_sim_config) =
            gateway_test_utils::setup::load_test_config(sp_sim_config_file);

        // The sp_sim_config_file contains suitable configuration information for a MGS daemon running on
        // switch0. For switch1, the port information needs to be flipped in order for MGS to correctly identify
        // itself as the switch1 MGS daemon.
        if switch_location == SwitchLocation::Switch1 {
            for config in mgs_config.switch.location.determination.iter_mut() {
                let swap = config.sp_port_1.clone();
                config.sp_port_1 = config.sp_port_2.clone();
                config.sp_port_2 = swap;
            }
        }

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
        let mgs = self.gateway.get(&switch_location).unwrap();
        let mgs_addr =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, mgs.port, 0, 0).into();

        // Set up a stub instance of dendrite
        let dendrite = dev::dendrite::DendriteInstance::start(
            0,
            self.nexus_internal_addr,
            Some(mgs_addr),
        )
        .await
        .unwrap();
        let port = dendrite.port;
        self.dendrite.write().unwrap().insert(switch_location, dendrite);

        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);

        // Update the configuration options for Nexus, if it's launched later.
        //
        // NOTE: If dendrite is started after Nexus, this is ignored.
        let config = DpdConfig { address: std::net::SocketAddr::V6(address) };
        self.config.pkg.dendrite.insert(switch_location, config);
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
    }

    pub async fn record_switch_dns(
        &mut self,
        sled_id: SledUuid,
        switch_location: SwitchLocation,
    ) {
        let log = &self.logctx.log;
        debug!(
            log,
            "Recording DNS for the switch zones";
            "sled_id" => sled_id.to_string(),
            "switch_location" => switch_location.to_string(),
        );

        self.rack_init_builder
            .internal_dns_config
            .host_zone_switch(
                sled_id,
                Ipv6Addr::LOCALHOST,
                self.dendrite
                    .read()
                    .unwrap()
                    .get(&switch_location)
                    .unwrap()
                    .port,
                self.gateway.get(&switch_location).unwrap().port,
                self.mgd.get(&switch_location).unwrap().port,
            )
            .unwrap()
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
            clickhouse.native_address().port(),
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
        let producer =
            start_producer_server(nexus_internal_addr, producer_id).unwrap();
        register_test_producer(&producer).unwrap();

        self.producer = Some(producer);
    }

    // Begin starting Nexus.
    pub async fn start_nexus_internal(&mut self) -> Result<(), String> {
        let log = &self.logctx.log;
        debug!(log, "Starting Nexus (internal API)");

        // In tests, disable blueprint planning.
        self.config.pkg.initial_reconfigurator_config =
            Some(ReconfiguratorConfig {
                planner_enabled: false,
                planner_config: PlannerConfig::default(),
                tuf_repo_pruner_enabled: true,
            });
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

        let nexus_internal = N::start_internal(&self.config, &log).await?;
        let nexus_internal_addr =
            nexus_internal.get_http_server_internal_address();
        let internal_address = match nexus_internal_addr {
            SocketAddr::V4(addr) => {
                SocketAddrV6::new(addr.ip().to_ipv6_mapped(), addr.port(), 0, 0)
            }
            SocketAddr::V6(addr) => addr,
        };
        let lockstep_address = match nexus_internal
            .get_http_server_lockstep_address()
        {
            SocketAddr::V4(addr) => {
                SocketAddrV6::new(addr.ip().to_ipv6_mapped(), addr.port(), 0, 0)
            }
            SocketAddr::V6(addr) => addr,
        };
        assert_eq!(internal_address.ip(), lockstep_address.ip());

        self.rack_init_builder.add_nexus_to_dns(
            self.config.deployment.id,
            internal_address,
            lockstep_address.port(),
        );
        self.record_nexus_zone(
            self.config.clone(),
            internal_address,
            lockstep_address.port(),
            0,
        );
        self.nexus_internal = Some(nexus_internal);
        self.nexus_internal_addr = Some(nexus_internal_addr);
        Ok(())
    }

    pub async fn configure_second_nexus(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Configuring second Nexus (not to run)");
        // Besides the Nexus that we just started, add an entry in the blueprint
        // for the Nexus that developers can start using
        // nexus/examples/config-second.toml.
        //
        // The details in its BlueprintZoneType mostly don't matter because
        // those are mostly used for DNS (which we don't usually need here) and
        // to tell sled agent how to start the zone (which isn't what's going on
        // here).  But it does need to be present for it to be able to determine
        // on startup if it needs to quiesce.
        let second_nexus_config_path =
            Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../examples/config-second.toml");
        let mut second_nexus_config =
            NexusConfig::from_file(&second_nexus_config_path).unwrap();
        // Okay, this is particularly awful.  The system does not allow multiple
        // zones to use the same external IP -- makes sense.  But it actually is
        // fine here because the IP is localhost and we're using host
        // networking, and we've already ensured that the ports will be unique.
        // Avoid tripping up the validation by using some other IP.  This won't
        // be used for anything.  Pick something that's not in use anywhere
        // else.  This range is guaranteed by RFC 6666 to discard traffic.
        second_nexus_config
            .deployment
            .dropshot_external
            .dropshot
            .bind_address
            .set_ip("100::1".parse().unwrap());
        let SocketAddr::V6(second_internal_address) =
            second_nexus_config.deployment.dropshot_internal.bind_address
        else {
            panic!(
                "expected IPv6 address for dropshot_internal in \
                 nexus/examples/config-second.toml"
            );
        };
        let second_lockstep_port = second_nexus_config
            .deployment
            .dropshot_lockstep
            .bind_address
            .port();
        self.record_nexus_zone(
            second_nexus_config,
            second_internal_address,
            second_lockstep_port,
            1,
        );
    }

    fn record_nexus_zone(
        &mut self,
        config: NexusConfig,
        internal_address: SocketAddrV6,
        lockstep_port: u16,
        which: usize,
    ) {
        let id = config.deployment.id;
        let mac = self
            .rack_init_builder
            .mac_addrs
            .next()
            .expect("ran out of MAC addresses");
        let ip_config = PrivateIpConfig::new_ipv4(
            NEXUS_OPTE_IPV4_SUBNET
                .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1 + which)
                .unwrap(),
            *NEXUS_OPTE_IPV4_SUBNET,
        )
        .unwrap();
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
            zone_type: BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_dns_servers: config
                    .deployment
                    .external_dns_servers
                    .clone(),
                external_ip: OmicronZoneExternalFloatingIp {
                    id: ExternalIpUuid::new_v4(),
                    ip: config
                        .deployment
                        .dropshot_external
                        .dropshot
                        .bind_address
                        .ip(),
                },
                external_tls: config.deployment.dropshot_external.tls,
                internal_address,
                lockstep_port,
                nic: NetworkInterface {
                    id: Uuid::new_v4(),
                    kind: NetworkInterfaceKind::Service {
                        id: id.into_untyped_uuid(),
                    },
                    mac,
                    name: format!("nexus-{}", id).parse().unwrap(),
                    ip_config,
                    primary: true,
                    slot: 0,
                    vni: Vni::SERVICES_VNI,
                },
                nexus_generation: Generation::new(),
            }),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });
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

        let dns_config = self
            .rack_init_builder
            .internal_dns_config
            .clone()
            .build_full_config_for_initial_generation();

        slog::info!(log, "DNS population: {:#?}", dns_config);
        dns_config_client.dns_config_put(&dns_config).await.expect(
            "Failed to send initial DNS records to internal DNS server",
        );
        self.dns_config = Some(dns_config);
    }

    /// Perform RSS handoff
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
        let external_dns_zone_name = DNS_ZONE_EXTERNAL_TESTING.to_string();
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

        // Construct an initial blueprint that agrees with the sled-agents'
        // post-initialization configuration (generation 2).
        let sleds = self
            .blueprint_sleds
            .take()
            .expect("should have already made blueprint sled configs");
        let id = BlueprintUuid::new_v4();
        let blueprint = Blueprint {
            id,
            sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            parent_blueprint_id: None,
            internal_dns_version: dns_config.generation,
            external_dns_version: Generation::new(),
            target_release_minimum_generation: Generation::new(),
            nexus_generation: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            // Clickhouse clusters are not generated by RSS. One must run
            // reconfigurator for that.
            clickhouse_cluster_config: None,
            oximeter_read_version: Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: Utc::now(),
            creator: "nexus-test-utils".to_string(),
            comment: "initial test blueprint".to_string(),
            source: BlueprintSource::Test,
        };

        self.initial_blueprint_id = Some(blueprint.id);

        // Handoff all known service information to Nexus
        let server = N::start(
            self.nexus_internal
                .take()
                .expect("Must launch internal nexus first"),
            self.config,
            blueprint,
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
            vec![],
            vec![],
            dns_config,
            &external_dns_zone_name,
            recovery_silo,
            tls_certificates,
        )
        .await;

        let external_server_addr =
            server.get_http_server_external_address().await;
        let techport_external_server_addr =
            server.get_http_server_techport_address().await;
        let internal_server_addr =
            server.get_http_server_internal_address().await;
        let lockstep_server_addr =
            server.get_http_server_lockstep_address().await;
        let testctx_external = ClientTestContext::new(
            external_server_addr,
            self.logctx
                .log
                .new(o!("component" => "external client test context")),
        );
        let testctx_techport = ClientTestContext::new(
            techport_external_server_addr,
            self.logctx.log.new(
                o!("component" => "techport external client test context"),
            ),
        );
        let testctx_internal = ClientTestContext::new(
            internal_server_addr,
            self.logctx
                .log
                .new(o!("component" => "internal client test context")),
        );
        let testctx_lockstep = ClientTestContext::new(
            lockstep_server_addr,
            self.logctx
                .log
                .new(o!("component" => "lockstep client test context")),
        );

        self.external_dns_zone_name = Some(external_dns_zone_name);
        self.external_client = Some(testctx_external);
        self.techport_client = Some(testctx_techport);
        self.internal_client = Some(testctx_internal);
        self.lockstep_client = Some(testctx_lockstep);
        self.silo_name = Some(silo_name);
        self.user_name = Some(user_name);
        self.password = Some(TEST_SUITE_PASSWORD.to_string());
        self.server = Some(server);
    }

    /// Set up a single sled agent.
    pub async fn start_sled(
        &mut self,
        sled_id: SledUuid,
        sled_index: u16,
        sim_mode: sim::SimMode,
    ) {
        let nexus_address =
            self.nexus_internal_addr.expect("Must launch Nexus first");

        let tempdir = camino_tempfile::tempdir().unwrap();
        let sled_agent = start_sled_agent(
            self.logctx.log.new(o!(
                "component" => "omicron_sled_agent::sim::Server",
                "sled_id" => sled_id.to_string(),
            )),
            nexus_address,
            sled_id,
            sled_index,
            tempdir.path(),
            sim_mode,
            &self.simulated_upstairs,
        )
        .await
        .expect("Failed to start sled agent");

        // Add a DNS entry for the TUF Repo Depot on this simulated sled agent.
        let SocketAddr::V6(server_addr_v6) = sled_agent.repo_depot_address
        else {
            panic!("expected sim sled agent to be listening on IPv6");
        };
        self.rack_init_builder.add_gz_service_to_dns(
            sled_id,
            server_addr_v6,
            ServiceName::RepoDepot,
        );

        self.sled_agents.push(ControlPlaneTestContextSledAgent {
            _storage: tempdir,
            server: sled_agent,
        });
    }

    /// Tell our first Sled Agent to report the zones that we configured, and
    /// tell the other Sled Agents to report they have no zones configured, and
    /// write the early network config to all sleds.
    pub async fn configure_sled_agents(&mut self) {
        let early_network_config = EarlyNetworkConfig {
            body: EarlyNetworkConfigBody {
                ntp_servers: Vec::new(),
                rack_network_config: Some(RackNetworkConfigV2 {
                    bfd: Vec::new(),
                    bgp: Vec::new(),
                    infra_ip_first: "192.0.2.10".parse().unwrap(),
                    infra_ip_last: "192.0.2.100".parse().unwrap(),
                    ports: Vec::new(),
                    rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
                }),
            },
            generation: 1,
            schema_version: 2,
        };

        macro_rules! from_clone {
            ($source: expr) => {
                $source.clone().into_iter().map(From::from).collect()
            };
        }

        // The first sled agent is the only one that'll have configured
        // blueprint zones, but the others all need to have disks.
        let zones = once(from_clone!(self.blueprint_zones))
            .chain(repeat(Vec::<BlueprintZoneConfig>::new()));

        // Compute the sled configurations once, and let the blueprint
        // builder copy them.
        self.make_sled_configs();
        let sled_configs = self
            .blueprint_sleds
            .as_ref()
            .expect("should have just made blueprint sled configs");

        // Send the sled-agents their new configurations.
        // This generation number should match the one in
        // `make_sled_configs`.
        let generation = Generation::from_u32(2);

        for (sled_agent, sled_zones) in zip(self.sled_agents.iter(), zones) {
            let sled_id = sled_agent.sled_agent_id();
            let client = sled_agent_client::Client::new(
                &format!("http://{}", sled_agent.local_addr()),
                self.logctx.log.clone(),
            );

            let disks = from_clone!(sled_configs[&sled_id].disks);
            let datasets = from_clone!(sled_configs[&sled_id].datasets);
            let zones = from_clone!(sled_zones);
            client
                .omicron_config_put(&OmicronSledConfig {
                    generation,
                    disks,
                    datasets,
                    zones,
                    remove_mupdate_override: None,
                    host_phase_2: HostPhase2DesiredSlots::current_contents(),
                })
                .await
                .expect("Failed to configure sled agent {sled_id} with zones");

            client
                .write_network_bootstore_config(&early_network_config)
                .await
                .expect(
                    "Failed to write early networking config \
                     to bootstore on sled {sled_id}",
                );
        }
    }

    /// Set up a single "extra" sled agent, meaning not the special first one.
    pub async fn extra_sled_agent(
        &mut self,
        sled_id: SledUuid,
        sled_index: u16,
        sim_mode: sim::SimMode,
    ) {
        let nexus_address =
            self.nexus_internal_addr.expect("Must launch Nexus first");

        let tempdir = camino_tempfile::tempdir().unwrap();
        let sled_agent = start_sled_agent(
            self.logctx.log.new(o!(
                "component" => "omicron_sled_agent::sim::Server",
                "sled_id" => sled_id.to_string(),
            )),
            nexus_address,
            sled_id,
            sled_index,
            tempdir.path(),
            sim_mode,
            &self.simulated_upstairs,
        )
        .await
        .expect("Failed to start sled agent");

        self.sled_agents.push(ControlPlaneTestContextSledAgent {
            _storage: tempdir,
            server: sled_agent,
        })
    }

    /// Configure a mock boundary-NTP server on the first sled agent
    pub async fn configure_boundary_ntp(&mut self) {
        let mac = self
            .rack_init_builder
            .mac_addrs
            .next()
            .expect("ran out of MAC addresses");
        let internal_ip = NTP_OPTE_IPV4_SUBNET
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
            .unwrap();
        let private_ip_config =
            PrivateIpConfig::new_ipv4(internal_ip, *NTP_OPTE_IPV4_SUBNET)
                .unwrap();
        let external_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let address = format!("[::1]:{NTP_PORT}").parse().unwrap(); // localhost
        let zone_id = OmicronZoneUuid::new_v4();
        let zpool_id = ZpoolUuid::new_v4();

        self.rack_init_builder.add_service_to_dns(
            zone_id,
            address,
            ServiceName::BoundaryNtp,
        );
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: ZpoolName::new_external(zpool_id),
            zone_type: BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp {
                    address,
                    ntp_servers: vec![],
                    dns_servers: vec![],
                    domain: None,
                    nic: NetworkInterface {
                        id: Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: zone_id.into_untyped_uuid(),
                        },
                        ip_config: private_ip_config,
                        mac,
                        name: format!("boundary-ntp-{zone_id}")
                            .parse()
                            .unwrap(),
                        primary: true,
                        slot: 0,
                        vni: Vni::SERVICES_VNI,
                    },
                    external_ip: OmicronZoneExternalSnatIp {
                        id: ExternalIpUuid::new_v4(),
                        snat_cfg: SourceNatConfig::new(external_ip, 0, 16383)
                            .unwrap(),
                    },
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });
    }

    /// Set up the Crucible Pantry on the first sled agent
    pub async fn start_crucible_pantry(&mut self) {
        let pantry = self.sled_agents[0].start_pantry().await;
        let address = pantry.addr();

        let SocketAddr::V6(address) = address else {
            panic!("Expected IPv6 Pantry Address");
        };

        let zone_id = OmicronZoneUuid::new_v4();
        self.rack_init_builder.add_service_to_dns(
            zone_id,
            address,
            ServiceName::CruciblePantry,
        );
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
            zone_type: BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });
    }

    /// Set up an external DNS server on the first sled agent.
    pub async fn start_external_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "external_dns_server"));

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
        let zone_id = OmicronZoneUuid::new_v4();
        self.rack_init_builder.add_service_to_dns(
            zone_id,
            dropshot_address,
            ServiceName::ExternalDns,
        );

        let zpool_id = ZpoolUuid::new_v4();
        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();

        let ip_config = if dns.dns_server.local_address().is_ipv4() {
            PrivateIpConfig::new_ipv4(
                DNS_OPTE_IPV4_SUBNET
                    .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES + 1)
                    .unwrap(),
                *DNS_OPTE_IPV4_SUBNET,
            )
            .unwrap()
        } else {
            PrivateIpConfig::new_ipv6(
                DNS_OPTE_IPV6_SUBNET
                    .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u128 + 1)
                    .unwrap(),
                *DNS_OPTE_IPV6_SUBNET,
            )
            .unwrap()
        };
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: ZpoolName::new_external(zpool_id),
            zone_type: BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns {
                    dataset: OmicronZoneDataset { pool_name },
                    dns_address: OmicronZoneExternalFloatingAddr {
                        id: ExternalIpUuid::new_v4(),
                        addr: dns_address.into(),
                    },
                    http_address: dropshot_address,
                    nic: NetworkInterface {
                        id: Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: zone_id.into_untyped_uuid(),
                        },
                        mac,
                        name: format!("external-dns-{}", zone_id)
                            .parse()
                            .unwrap(),
                        ip_config,
                        primary: true,
                        slot: 0,
                        vni: Vni::SERVICES_VNI,
                    },
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });

        self.external_dns = Some(dns);
    }

    /// Set up an internal DNS server on the first sled agent
    pub async fn start_internal_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "internal_dns_server"));
        let dns = dns_server::TransientServer::new(&log).await.unwrap();

        let SocketAddr::V6(dns_address) = dns.dns_server.local_address() else {
            panic!("Unsupported IPv4 DNS address");
        };
        let SocketAddr::V6(http_address) = dns.dropshot_server.local_addr()
        else {
            panic!("Unsupported IPv4 DNS address");
        };
        let zone_id = OmicronZoneUuid::new_v4();
        self.rack_init_builder.add_internal_name_server_to_dns(
            zone_id,
            http_address,
            dns_address,
        );

        let zpool_id = ZpoolUuid::new_v4();
        let pool_name = illumos_utils::zpool::ZpoolName::new_external(zpool_id)
            .to_string()
            .parse()
            .unwrap();
        self.blueprint_zones.push(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: ZpoolName::new_external(zpool_id),
            zone_type: BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    dataset: OmicronZoneDataset { pool_name },
                    dns_address,
                    http_address,
                    gz_address: Ipv6Addr::LOCALHOST,
                    gz_address_index: 0,
                },
            ),
            image_source: BlueprintZoneImageSource::InstallDataset,
        });

        self.internal_dns = Some(dns);
    }

    pub fn build(self) -> ControlPlaneTestContext<N> {
        ControlPlaneTestContext {
            start_time: self.start_time,
            server: self.server.unwrap(),
            external_client: self.external_client.unwrap(),
            techport_client: self.techport_client.unwrap(),
            internal_client: self.internal_client.unwrap(),
            lockstep_client: self.lockstep_client.unwrap(),
            database: self.database.unwrap(),
            database_admin: self.database_admin.unwrap(),
            clickhouse: self.clickhouse.unwrap(),
            sled_agents: self.sled_agents,
            oximeter: self.oximeter.unwrap(),
            producer: self.producer.unwrap(),
            logctx: self.logctx,
            gateway: self.gateway,
            dendrite: RwLock::new(self.dendrite.into_inner().unwrap()),
            mgd: self.mgd,
            external_dns_zone_name: self.external_dns_zone_name.unwrap(),
            external_dns: self.external_dns.unwrap(),
            internal_dns: self.internal_dns.unwrap(),
            initial_blueprint_id: self.initial_blueprint_id.unwrap(),
            silo_name: self.silo_name.unwrap(),
            user_name: self.user_name.unwrap(),
            password: self.password.unwrap(),
        }
    }

    pub async fn teardown(self) {
        if let Some(server) = self.server {
            server.close().await;
        }
        if let Some(nexus_internal) = self.nexus_internal {
            N::stop_internal(nexus_internal).await;
        }
        if let Some(mut database) = self.database {
            database.cleanup().await.unwrap();
        }
        if let Some(mut clickhouse) = self.clickhouse {
            clickhouse.cleanup().await.unwrap();
        }
        for sled_agent in self.sled_agents {
            sled_agent.teardown().await;
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
        for (_, mut dendrite) in self.dendrite.into_inner().unwrap() {
            dendrite.cleanup().await.unwrap();
        }
        for (_, mut mgd) in self.mgd {
            mgd.cleanup().await.unwrap();
        }
        self.logctx.cleanup_successful();
    }

    fn make_sled_configs(&mut self) {
        assert!(
            self.blueprint_sleds.is_none(),
            "should not have made sled configs yet"
        );

        let mut blueprint_sleds = BTreeMap::new();
        let mut disk_index = 0;

        // The first sled agent is the only one that'll have configured
        // blueprint zones, but the others all need to have disks.
        let maybe_zones = once(Some(&self.blueprint_zones)).chain(repeat(None));

        // The generation number that the sled-agents' configuration
        // will have when this blueprint is executed. Should match
        // the one in `configure_sled_agents`.
        let sled_agent_generation = Generation::from_u32(2);

        for (sled_agent, maybe_zones) in
            zip(self.sled_agents.iter(), maybe_zones)
        {
            let sled_id = sled_agent.sled_agent_id();

            let mut disks = IdOrdMap::new();
            let mut datasets = IdOrdMap::new();
            let zones = if let Some(zones) = maybe_zones {
                for zone in zones {
                    let zpool = &zone.filesystem_pool;
                    disks
                        .insert_unique(BlueprintPhysicalDiskConfig {
                            disposition:
                                BlueprintPhysicalDiskDisposition::InService,
                            identity: omicron_common::disk::DiskIdentity {
                                vendor: "nexus-tests".to_string(),
                                model: "nexus-test-model".to_string(),
                                serial: format!("nexus-test-disk-{disk_index}"),
                            },
                            id: PhysicalDiskUuid::new_v4(),
                            pool_id: zpool.id(),
                        })
                        .expect("freshly generated disk IDs are unique");
                    disk_index += 1;
                    let id = DatasetUuid::new_v4();
                    datasets
                        .insert_unique(BlueprintDatasetConfig {
                            disposition: BlueprintDatasetDisposition::InService,
                            id,
                            pool: *zpool,
                            kind: DatasetKind::TransientZone {
                                name: illumos_utils::zone::zone_name(
                                    zone.zone_type.kind().zone_prefix(),
                                    Some(zone.id),
                                ),
                            },
                            address: None,
                            quota: None,
                            reservation: None,
                            compression: CompressionAlgorithm::Off,
                        })
                        .expect("freshly generated dataset IDs are unique");
                }
                zones.iter().cloned().collect()
            } else {
                IdOrdMap::new()
            };

            // Populate extra fake disks, giving each sled 10 total.
            if disks.len() < 10 {
                for _ in disks.len()..10 {
                    disks
                        .insert_unique(BlueprintPhysicalDiskConfig {
                            disposition:
                                BlueprintPhysicalDiskDisposition::InService,
                            identity: omicron_common::disk::DiskIdentity {
                                vendor: "nexus-tests".to_string(),
                                model: "nexus-test-model".to_string(),
                                serial: format!("nexus-test-disk-{disk_index}"),
                            },
                            id: PhysicalDiskUuid::new_v4(),
                            pool_id: ZpoolUuid::new_v4(),
                        })
                        .expect("freshly generated disk IDs are unique");
                    disk_index += 1;
                }
            }
            blueprint_sleds.insert(
                sled_id,
                BlueprintSledConfig {
                    state: SledState::Active,
                    subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                    sled_agent_generation,
                    disks,
                    datasets,
                    zones,
                    remove_mupdate_override: None,
                    host_phase_2:
                        BlueprintHostPhase2DesiredSlots::current_contents(),
                },
            );
        }

        self.blueprint_sleds = Some(blueprint_sleds);
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
    extra_sled_agents: u16,
    gateway_config_file: Utf8PathBuf,
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
        extra_sled_agents,
        gateway_config_file,
        true,
    )
    .await)
}

/// Setup routine to use for tests.
pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut NexusConfig,
    sim_mode: sim::SimMode,
    initial_cert: Option<Certificate>,
    extra_sled_agents: u16,
    gateway_config_file: Utf8PathBuf,
) -> ControlPlaneTestContext<N> {
    let builder = ControlPlaneTestContextBuilder::<N>::new(test_name, config);
    setup_with_config_impl(
        builder,
        PopulateCrdb::FromEnvironmentSeed,
        sim_mode,
        initial_cert,
        extra_sled_agents,
        gateway_config_file,
        false,
    )
    .await
}

async fn setup_with_config_impl<N: NexusServer>(
    mut builder: ControlPlaneTestContextBuilder<'_, N>,
    populate: PopulateCrdb,
    sim_mode: sim::SimMode,
    initial_cert: Option<Certificate>,
    extra_sled_agents: u16,
    gateway_config_file: Utf8PathBuf,
    second_nexus: bool,
) -> ControlPlaneTestContext<N> {
    const STEP_TIMEOUT: Duration = Duration::from_secs(600);

    // All setups will start with CRDB and clickhouse
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
            ],
            STEP_TIMEOUT,
        )
        .await;

    // Usually our switch services rely on SMF updates to get information about
    // DNS and Nexus, but we currently don't use SMF to manage the services used in
    // the test context so we need to make the Nexus / DNS information available
    // to get the switch services working.
    builder
        .init_with_steps(
            vec![
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
                    Box::new(|builder| {
                        builder
                            .start_nexus_internal()
                            .map(|r| r.unwrap())
                            .boxed()
                    }),
                ),
            ],
            STEP_TIMEOUT,
        )
        .await;

    if second_nexus {
        builder
            .init_with_steps(
                vec![(
                    "configure_second_nexus",
                    Box::new(|builder| {
                        builder.configure_second_nexus().boxed()
                    }),
                )],
                STEP_TIMEOUT,
            )
            .await;
    }

    // By default there is only 1 sled agent, and this means only switch0 will
    // be configured. If extra sled agents are requested, then the second sled
    // agent will be for switch1.

    let mgs_config = gateway_config_file.clone();
    builder
        .init_with_steps(
            vec![
                (
                    "start_gateway_switch0",
                    Box::new(|builder| {
                        builder
                            .start_gateway(
                                SwitchLocation::Switch0,
                                None,
                                mgs_config,
                            )
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
                    "start_mgd_switch0",
                    Box::new(|builder| {
                        builder.start_mgd(SwitchLocation::Switch0).boxed()
                    }),
                ),
                (
                    "record_switch_dns",
                    Box::new(|builder| {
                        builder
                            .record_switch_dns(
                                SLED_AGENT_UUID.parse().unwrap(),
                                SwitchLocation::Switch0,
                            )
                            .boxed()
                    }),
                ),
            ],
            STEP_TIMEOUT,
        )
        .await;

    if extra_sled_agents > 0 {
        builder
            .init_with_steps(
                vec![
                    (
                        "start_gateway_switch1",
                        Box::new(|builder| {
                            builder
                                .start_gateway(
                                    SwitchLocation::Switch1,
                                    None,
                                    gateway_config_file,
                                )
                                .boxed()
                        }),
                    ),
                    (
                        "start_dendrite_switch1",
                        Box::new(|builder| {
                            builder
                                .start_dendrite(SwitchLocation::Switch1)
                                .boxed()
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
                        Box::new(|builder| {
                            builder
                                .record_switch_dns(
                                    SLED_AGENT2_UUID.parse().unwrap(),
                                    SwitchLocation::Switch1,
                                )
                                .boxed()
                        }),
                    ),
                ],
                STEP_TIMEOUT,
            )
            .await;
    }

    // The first and second sled agents have special UUIDs, and any extra ones
    // after that are random.

    builder
        .init_with_steps(
            vec![(
                "start_sled1",
                Box::new(move |builder| {
                    builder
                        .start_sled(
                            SLED_AGENT_UUID.parse().unwrap(),
                            0,
                            sim_mode,
                        )
                        .boxed()
                }),
            )],
            STEP_TIMEOUT,
        )
        .await;

    if extra_sled_agents > 0 {
        builder
            .init_with_steps(
                vec![(
                    "start_sled2",
                    Box::new(move |builder| {
                        builder
                            .start_sled(
                                SLED_AGENT2_UUID.parse().unwrap(),
                                1,
                                sim_mode,
                            )
                            .boxed()
                    }),
                )],
                STEP_TIMEOUT,
            )
            .await;
    }

    for index in 1..extra_sled_agents {
        builder
            .init_with_steps(
                vec![(
                    "add_extra_sled_agent",
                    Box::new(move |builder| {
                        builder
                            .extra_sled_agent(
                                SledUuid::new_v4(),
                                index.checked_add(1).unwrap(),
                                sim_mode,
                            )
                            .boxed()
                    }),
                )],
                STEP_TIMEOUT,
            )
            .await;
    }

    // Start expected services: these will all be allocated to the first sled
    // agent. Afterwards, configure the sled agents and start the rest of the
    // the required services.

    builder
        .init_with_steps(
            vec![
                (
                    "configure_boundary_ntp",
                    Box::new(|builder| {
                        builder.configure_boundary_ntp().boxed()
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
                    "configure_sled_agents",
                    Box::new(|builder| builder.configure_sled_agents().boxed()),
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

/// Starts a simulated sled agent
///
/// Note: you should probably use the `extra_sled_agents` macro parameter on
/// `nexus_test` instead!
pub async fn start_sled_agent(
    log: Logger,
    nexus_address: SocketAddr,
    id: SledUuid,
    sled_index: u16,
    update_directory: &Utf8Path,
    sim_mode: sim::SimMode,
    simulated_upstairs: &Arc<sim::SimulatedUpstairs>,
) -> Result<sim::Server, String> {
    // Generate a baseboard serial number that matches the SP configuration
    // (SimGimlet00, SimGimlet01, etc.) so that inventory can link sled agents
    // to their corresponding SPs via baseboard_id.
    let baseboard_serial = format!("SimGimlet{:02}", sled_index);

    let config = sim::Config::for_testing_with_baseboard(
        id,
        sim_mode,
        Some(nexus_address),
        Some(update_directory),
        sim::ZpoolConfig::None,
        SledCpuFamily::AmdMilan,
        Some(baseboard_serial),
    );
    start_sled_agent_with_config(log, &config, sled_index, simulated_upstairs)
        .await
}

pub async fn start_sled_agent_with_config(
    log: Logger,
    config: &sim::Config,
    sled_index: u16,
    simulated_upstairs: &Arc<sim::SimulatedUpstairs>,
) -> Result<sim::Server, String> {
    let server =
        sim::Server::start(&config, &log, true, simulated_upstairs, sled_index)
            .await
            .map_err(|e| e.to_string())?;
    Ok(server)
}

pub async fn start_oximeter(
    log: Logger,
    nexus_address: SocketAddr,
    native_port: u16,
    id: Uuid,
) -> Result<Oximeter, String> {
    let db = oximeter_collector::DbConfig {
        address: Some(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), native_port)),
        batch_size: 10,
        batch_interval: 1,
        replicated: false,
    };
    let config = oximeter_collector::Config {
        nexus_address: Some(nexus_address),
        db,
        // The collector only learns about producers when it refreshes its list
        // from Nexus. This interval is quite short, and much smaller than the
        // one we use in production. That's important for test latency, but not
        // strictly required for correctness.
        refresh_interval: Duration::from_secs(2),
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
pub fn start_producer_server(
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
        interval: Duration::from_secs(1),
    };
    let config = oximeter_producer::Config {
        server_info,
        registration_address: Some(nexus_address),
        default_request_body_max_bytes: 1024,
        log: LogConfig::Config(ConfigLogging::StderrTerminal {
            level: ConfigLoggingLevel::Error,
        }),
    };
    ProducerServer::start(&config).map_err(|e| e.to_string())
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
        TokioResolver,
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
            default_request_body_max_bytes: 8 * 1024,
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
        },
    )
    .await
    .unwrap();

    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig::new(
        dns_server.local_address(),
        Protocol::Udp,
    ));
    let mut resolver_opts = ResolverOpts::default();
    resolver_opts.edns0 = true;
    let resolver = TokioResolver::builder_with_config(
        resolver_config,
        TokioConnectionProvider::default(),
    )
    .with_options(resolver_opts)
    .build();

    Ok((dns_server, http_server, resolver))
}

/// Wait until a producer is registered with Oximeter.
///
/// This blocks until the producer is registered, for up to 60s. It panics if
/// the retry loop hits a permanent error.
pub async fn wait_for_producer<G: GenericUuid>(
    oximeter: &oximeter_collector::Oximeter,
    producer_id: G,
) {
    wait_for_producer_impl(oximeter, producer_id.into_untyped_uuid()).await;
}

// This function is outlined from wait_for_producer to avoid unnecessary
// monomorphization.
async fn wait_for_producer_impl(
    oximeter: &oximeter_collector::Oximeter,
    producer_id: Uuid,
) {
    wait_for_condition(
        || async {
            if oximeter
                .list_producers(None, usize::MAX)
                .iter()
                .any(|p| p.id == producer_id)
            {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await
    .expect("Failed to find producer within time limit");
}

/// Build a DPD client for test validation using the first running dendrite instance
pub fn dpd_client<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
) -> dpd_client::Client {
    // Get the first available dendrite instance and extract the values we need
    let dendrite_guard = cptestctx.dendrite.read().unwrap();
    let (switch_location, dendrite_instance) = dendrite_guard
        .iter()
        .next()
        .expect("No dendrite instances running for test");

    // Copy the values we need while the guard is still alive
    let switch_location = *switch_location;
    let port = dendrite_instance.port;
    drop(dendrite_guard);

    let client_state = dpd_client::ClientState {
        tag: String::from("nexus-test"),
        log: cptestctx.logctx.log.new(slog::o!(
            "component" => "DpdClient",
            "switch" => switch_location.to_string()
        )),
    };

    let addr = Ipv6Addr::LOCALHOST;
    dpd_client::Client::new(&format!("http://[{addr}]:{port}"), client_state)
}
