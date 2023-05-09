// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

use anyhow::Context;
use camino::Utf8Path;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
// TODO: Confirm we want this dep?
// It was used by the sled agent
use nexus_client::types as NexusTypes;
use nexus_test_interface::NexusServer;
use nexus_types::external_api::params::UserId;
use nexus_types::internal_api::params::RecoverySiloConfig;
use nexus_types::internal_api::params::ServiceKind;
use nexus_types::internal_api::params::ServicePutRequest;
use omicron_common::api::external::{IdentityMetadata, Name};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::nexus_config;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use oximeter_collector::Oximeter;
use oximeter_producer::Server as ProducerServer;
use slog::o;
use slog::Logger;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;
use trust_dns_resolver::config::NameServerConfig;
use trust_dns_resolver::config::Protocol;
use trust_dns_resolver::config::ResolverConfig;
use trust_dns_resolver::config::ResolverOpts;
use trust_dns_resolver::TokioAsyncResolver;
use uuid::Uuid;

pub mod db;
pub mod http_testing;
pub mod resource_helpers;

pub const SLED_AGENT_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
pub const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";
pub const OXIMETER_UUID: &str = "39e6175b-4df2-4730-b11d-cbc1e60a2e78";
pub const PRODUCER_UUID: &str = "a6458b7d-87c3-4483-be96-854d814c20de";

/// The reported amount of hardware threads for an emulated sled agent.
pub const TEST_HARDWARE_THREADS: u32 = 16;
/// The reported amount of physical RAM for an emulated sled agent.
pub const TEST_PHYSICAL_RAM: u64 = 32 * (1 << 30);

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
    pub oximeter: Oximeter,
    pub producer: ProducerServer,
    pub dendrite: dev::dendrite::DendriteInstance,

    pub external_dns_zone_name: String,
    pub external_dns: dns_server::InMemoryServer,
    pub internal_dns: dns_server::InMemoryServer,
    pub silo_name: Name,
    pub user_name: UserId,
}

impl<N: NexusServer> ControlPlaneTestContext<N> {
    pub async fn teardown(mut self) {
        self.server.close().await;
        self.database.cleanup().await.unwrap();
        self.clickhouse.cleanup().await.unwrap();
        self.sled_agent.http_server.close().await.unwrap();
        self.oximeter.close().await.unwrap();
        self.producer.close().await.unwrap();
        self.dendrite.cleanup().await.unwrap();
        self.logctx.cleanup_successful();
    }

    pub async fn external_http_client(&self) -> ClientTestContext {
        self.server
            .get_http_server_external_address()
            .await
            .map(|addr| {
                ClientTestContext::new(
                    addr,
                    self.logctx.log.new(
                        o!("component" => "external http client test context"),
                    ),
                )
            })
            .unwrap()
    }

    pub async fn external_https_client(&self) -> ClientTestContext {
        self.server
            .get_https_server_external_address()
            .await
            .map(|addr| {
                ClientTestContext::new(
                    addr,
                    self.logctx.log.new(
                        o!("component" => "external https client test context"),
                    ),
                )
            })
            .unwrap()
    }
}

pub fn load_test_config() -> omicron_common::nexus_config::Config {
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
    let mut config =
        omicron_common::nexus_config::Config::from_file(config_file_path)
            .expect("failed to load config.test.toml");
    config.deployment.id = Uuid::new_v4();
    config
}

pub async fn test_setup<N: NexusServer>(
    test_name: &str,
) -> ControlPlaneTestContext<N> {
    let mut config = load_test_config();
    test_setup_with_config::<N>(test_name, &mut config, sim::SimMode::Explicit)
        .await
}

struct RackInitRequestBuilder {
    services: Vec<NexusTypes::ServicePutRequest>,
    datasets: Vec<NexusTypes::DatasetCreateRequest>,
    internal_dns_config: internal_dns::DnsConfigBuilder,
}

impl RackInitRequestBuilder {
    fn new() -> Self {
        Self {
            services: vec![],
            datasets: vec![],
            internal_dns_config: internal_dns::DnsConfigBuilder::new(),
        }
    }

    fn add_service(
        &mut self,
        address: SocketAddrV6,
        kind: NexusTypes::ServiceKind,
        service_name: internal_dns::ServiceName,
        sled_id: Uuid,
    ) {
        let zone_id = Uuid::new_v4();
        self.services.push(
            NexusTypes::ServicePutRequest {
                address: address.to_string(),
                kind,
                service_id: Uuid::new_v4(),
                sled_id,
                zone_id: Some(zone_id),
            }
        );
        let zone = self.internal_dns_config.host_zone(
            zone_id,
            *address.ip(),
        ).expect("Failed to set up DNS for {kind}");
        self.internal_dns_config.service_backend_zone(
            service_name,
            &zone,
            address.port(),
        ).expect("Failed to set up DNS for {kind}");
    }

    fn add_dataset(
        &mut self,
        zpool_id: Uuid,
        dataset_id: Uuid,
        address: SocketAddrV6,
        kind: NexusTypes::DatasetKind,
        service_name: internal_dns::ServiceName,
    ) {
        self.datasets.push(NexusTypes::DatasetCreateRequest {
            zpool_id,
            dataset_id,
            request: NexusTypes::DatasetPutRequest {
                address: address.to_string(),
                kind: kind.into(),
            },
        });
        let zone = self.internal_dns_config.host_zone(
            dataset_id,
            *address.ip(),
        ).expect("Failed to set up DNS for {kind}");
        self.internal_dns_config.service_backend_zone(
            service_name,
            &zone,
            address.port(),
        ).expect("Failed to set up DNS for {kind}");
    }
}

// TODO: Maybe split this out into a few different files?

pub struct ControlPlaneTestContextBuilder<'a, N> {
    pub config: &'a mut omicron_common::nexus_config::Config,
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
    pub oximeter: Option<Oximeter>,
    pub producer: Option<ProducerServer>,
    pub dendrite: Option<dev::dendrite::DendriteInstance>,

    pub external_dns_zone_name: Option<String>,
    pub external_dns: Option<dns_server::InMemoryServer>,
    pub internal_dns: Option<dns_server::InMemoryServer>,

    pub silo_name: Option<Name>,
    pub user_name: Option<UserId>,
}

impl<'a, N: NexusServer> ControlPlaneTestContextBuilder<'a, N> {
    pub fn new(
        test_name: &str,
        config: &'a mut omicron_common::nexus_config::Config,
    ) -> Self {
        let start_time = chrono::Utc::now();
        let logctx = LogContext::new(test_name, &config.pkg.log);

        Self {
            config,
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
            oximeter: None,
            producer: None,
            dendrite: None,
            external_dns_zone_name: None,
            external_dns: None,
            internal_dns: None,
            silo_name: None,
            user_name: None,
        }
    }

    pub async fn start_crdb(&mut self) {
        let log = &self.logctx.log;
        // Start up CockroachDB.
        let database = db::test_setup_database(log).await;
        // Store actual address/port information for the databases after they start.
        // TODO: Use DNS, not a hard-coded config
        self.config.deployment.database =
            nexus_config::Database::FromUrl { url: database.pg_config().clone() };

        self.database = Some(database);

        // TODO: We don't know the port
        /*
        self.rack_init_builder.add_dataset(
            Uuid::new_v4(),
            Uuid::new_v4(),
            address: dataset
        );
        */
    }

    // Start ClickHouse database server.
    pub async fn start_clickhouse(&mut self) {
        let clickhouse = dev::clickhouse::ClickHouseInstance::new(0).await.unwrap();
        let port = clickhouse.port();

        let zpool_id = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);
        self.rack_init_builder.add_dataset(
            zpool_id,
            dataset_id,
            address,
            NexusTypes::DatasetKind::Clickhouse,
            internal_dns::ServiceName::Clickhouse,
        );
        self.clickhouse = Some(clickhouse);

        // TODO: Use DNS, not a hard-coded config
        self.config
            .pkg
            .timeseries_db
            .address
            .as_mut()
            .expect("Tests expect to set a port of Clickhouse")
            .set_port(port);
    }

    pub async fn start_dendrite(&mut self) {
        // Set up a stub instance of dendrite
        let dendrite = dev::dendrite::DendriteInstance::start(0).await.unwrap();
        let port = dendrite.port;
        self.dendrite = Some(dendrite);

        // TODO: Use DNS, not a hard-coded config
        self.config
            .pkg
            .dendrite
            .address
            .as_mut()
            .expect("Tests expect an explicit dendrite address")
            .set_port(port);
    }

    pub async fn start_nexus(&mut self) {
        // Begin starting Nexus.
        let (nexus_internal, nexus_internal_addr) =
            N::start_internal(&self.config, &self.logctx.log).await;

        // TODO: Finish?
    }

    pub async fn start_sled(
        &mut self,
        nexus_address: sim::NexusAddressSource,
        sim_mode: sim::SimMode,
    ) {
        // Set up a single sled agent.
        let sa_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
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
        .unwrap();

        self.sled_agent = Some(sled_agent);
        self.sled_agent_storage = Some(tempdir);
    }

    // Set up the Crucible Pantry on an existing Sled Agent.
    pub async fn start_crucible_pantry(
        &mut self,
        dns: &mut internal_dns::DnsConfigBuilder,
    ) {
        let sled_agent = self.sled_agent.as_mut()
            .expect("Cannot start pantry without first starting sled agent");

        // TODO: it's possible that we do the DNS management ourselves, and
        // *don't* let this be the responsibility of the simulated sled agent?
        sled_agent.start_pantry(dns).await;
    }

    // Set up an external DNS server.
    pub async fn start_external_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "external_dns_server"));
        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();

        let dns = dns_server::InMemoryServer::new(&log).await.unwrap();

        let SocketAddr::V6(address) = *dns.server.local_address() else {
            panic!("Unsupported IPv4 DNS address");
        };
        self.rack_init_builder.add_service(
            address,
            NexusTypes::ServiceKind::ExternalDns,
            internal_dns::ServiceName::ExternalDns,
            sled_id,
        );

        let SocketAddr::V6(address) = dns.dropshot_server.local_addr() else {
            panic!("Unsupported IPv4 DNS address");
        };
        self.rack_init_builder.add_service(
            address,
            NexusTypes::ServiceKind::ExternalDnsConfig,
            internal_dns::ServiceName::ExternalDns,
            sled_id,
        );

        self.external_dns = Some(dns);
    }

    // Set up an internal DNS server.
    pub async fn start_internal_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "internal_dns_server"));
        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
        let dns = dns_server::InMemoryServer::new(&log).await.unwrap();

        let SocketAddr::V6(address) = *dns.server.local_address() else {
            panic!("Unsupported IPv4 DNS address");
        };
        self.rack_init_builder.add_service(
            address,
            NexusTypes::ServiceKind::InternalDns,
            internal_dns::ServiceName::InternalDns,
            sled_id,
        );

        let SocketAddr::V6(address) = dns.dropshot_server.local_addr() else {
            panic!("Unsupported IPv4 DNS address");
        };
        self.rack_init_builder.add_service(
            address,
            NexusTypes::ServiceKind::InternalDnsConfig,
            internal_dns::ServiceName::InternalDns,
            sled_id,
        );

        self.internal_dns = Some(dns);
    }

    // TODO: aggregate "ServicePutRequest" objects, handoff to
    // nexus server via N::start (NexusServer::start)
    //
    // TODO: we aggregate the following:
    // - DNS Service(s) (internal/external config + DNS service)
    // - Nexus
    //
    // TODO: We could also handoff info about datasets / internal DNS config,
    // but we'll need to change the interface to "NexusServer::start"
    pub fn build(self) -> ControlPlaneTestContext<N> {
        todo!();
    }
}

pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut omicron_common::nexus_config::Config,
    sim_mode: sim::SimMode,
) -> ControlPlaneTestContext<N> {
    let builder = ControlPlaneTestContextBuilder::<N>::new(test_name, config);

    builder.start_crdb().await;
    builder.start_clickhouse().await;
    builder.start_dendrite().await;
    builder.start_internal_dns().await;
    builder.start_external_dns().await;

    // TODO TODO TODO TODO TODO: start here
    asdfsadf

    builder.start_sled(sim::NexusAddressSource::FromDns { internal_dns_address: () });
    

    // Begin starting Nexus.
    let (nexus_internal, nexus_internal_addr) =
        N::start_internal(&config, &logctx.log).await;

    // Set up a single sled agent.
    let sa_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
    let tempdir = camino_tempfile::tempdir().unwrap();
    let sled_agent = start_sled_agent(
        logctx.log.new(o!(
            "component" => "omicron_sled_agent::sim::Server",
            "sled_id" => sa_id.to_string(),
        )),
        sim::NexusAddressSource::Direct { address: nexus_internal_addr },
        sa_id,
        tempdir.path(),
        sim_mode,
    )
    .await
    .unwrap();

    // Finish setting up Nexus by initializing the rack.  We need to include
    // information about the internal DNS server started within the simulated
    // Sled Agent.
    let dns_server_address_internal = match internal_dns
        .dropshot_server
        .local_addr()
    {
        SocketAddr::V4(_) => panic!("expected DNS server to have IPv6 address"),
        SocketAddr::V6(addr) => addr,
    };
    let dns_server_dns_address_internal = match internal_dns
        .server
        .local_address()
    {
        SocketAddr::V4(_) => panic!("expected DNS server to have IPv6 address"),
        SocketAddr::V6(addr) => *addr,
    };
    let dns_server_zone = Uuid::new_v4();
    let dns_service_config = ServicePutRequest {
        service_id: Uuid::new_v4(),
        sled_id: sa_id,
        zone_id: Some(dns_server_zone),
        address: dns_server_address_internal,
        kind: ServiceKind::InternalDnsConfig,
    };
    let dns_service_dns = ServicePutRequest {
        service_id: Uuid::new_v4(),
        sled_id: sa_id,
        zone_id: Some(dns_server_zone),
        address: dns_server_dns_address_internal,
        kind: ServiceKind::InternalDns,
    };
    let dns_server_address_external = match external_dns.dropshot_server
        .local_addr()
    {
        SocketAddr::V4(_) => panic!("expected DNS server to have IPv6 address"),
        SocketAddr::V6(addr) => addr,
    };
    let dns_service_external = ServicePutRequest {
        service_id: Uuid::new_v4(),
        sled_id: sa_id,
        zone_id: Some(Uuid::new_v4()),
        address: dns_server_address_external,
        kind: ServiceKind::ExternalDnsConfig,
    };
    let nexus_service = ServicePutRequest {
        service_id: Uuid::new_v4(),
        sled_id: sa_id,
        zone_id: Some(Uuid::new_v4()),
        address: SocketAddrV6::new(
            match nexus_internal_addr.ip() {
                IpAddr::V4(addr) => addr.to_ipv6_mapped(),
                IpAddr::V6(addr) => addr,
            },
            nexus_internal_addr.port(),
            0,
            0,
        ),
        kind: ServiceKind::Nexus {
            external_address: config
                .deployment
                .dropshot_external
                .bind_address
                .ip(),
        },
    };
    let external_dns_zone_name =
        internal_dns::names::DNS_ZONE_EXTERNAL_TESTING.to_string();
    let silo_name: Name = "test-suite-silo".parse().unwrap();
    let user_name = UserId::try_from("test-privileged".to_string()).unwrap();
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

    // TODO: Could we just call the "handoff_to_nexus" in the simulated server?
    // This avoids a "backdoor" to nexus, and uses the same API as RSS would.
    //
    // TODO: Need to pass datasets, internal dns config here.
    let server = N::start(
        nexus_internal,
        &config,
        vec![
            dns_service_config,
            dns_service_dns,
            dns_service_external,
            nexus_service,
        ],
        &external_dns_zone_name,
        recovery_silo,
    )
    .await;

    let external_server_addr =
        server.get_http_server_external_address().await.unwrap();
    let internal_server_addr = server.get_http_server_internal_address().await;

    let testctx_external = ClientTestContext::new(
        external_server_addr,
        logctx.log.new(o!("component" => "external client test context")),
    );
    let testctx_internal = ClientTestContext::new(
        internal_server_addr,
        logctx.log.new(o!("component" => "internal client test context")),
    );

    // Set Nexus' shared resolver to point to the simulated sled agent's
    // internal DNS server
    server
        .set_resolver(
            internal_dns::resolver::Resolver::new_from_addrs(
                logctx.log.new(o!("component" => "DnsResolver")),
                vec![*internal_dns.server.local_address()],
            )
            .unwrap(),
        )
        .await;

    // Set up an Oximeter collector server
    let collector_id = Uuid::parse_str(OXIMETER_UUID).unwrap();
    let oximeter = start_oximeter(
        log.new(o!("component" => "oximeter")),
        internal_server_addr,
        clickhouse.port(),
        collector_id,
    )
    .await
    .unwrap();

    // Set up a test metric producer server
    let producer_id = Uuid::parse_str(PRODUCER_UUID).unwrap();
    let producer =
        start_producer_server(internal_server_addr, producer_id).await.unwrap();
    register_test_producer(&producer).unwrap();

    ControlPlaneTestContext {
        start_time,
        server,
        external_client: testctx_external,
        internal_client: testctx_internal,
        database,
        clickhouse,
        sled_agent_storage: tempdir,
        sled_agent,
        oximeter,
        producer,
        logctx,
        dendrite,
        external_dns_zone_name,
        external_dns,
        internal_dns,
        silo_name,
        user_name,
    }
}

pub async fn start_sled_agent(
    log: Logger,
    nexus_address_source: sim::NexusAddressSource,
    id: Uuid,
    update_directory: &Utf8Path,
    sim_mode: sim::SimMode,
) -> Result<sim::Server, String> {
    let config = sim::Config {
        id,
        sim_mode,
        nexus_address_source,
        dropshot: ConfigDropshot {
            bind_address: SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0),
            request_body_max_bytes: 1024 * 1024,
            ..Default::default()
        },
        // TODO-cleanup this is unused
        log: ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Debug },
        storage: sim::ConfigStorage {
            zpools: vec![],
            ip: IpAddr::from(Ipv6Addr::LOCALHOST),
        },
        updates: sim::ConfigUpdates {
            zone_artifact_path: update_directory.to_path_buf(),
        },
        hardware: sim::ConfigHardware {
            hardware_threads: TEST_HARDWARE_THREADS,
            physical_ram: TEST_PHYSICAL_RAM,
        },
    };
    let server = sim::Server::start(&config, &log).await.map_err(|e| e.to_string())?;
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
    pub name: String,
}

#[derive(Debug, Clone, oximeter::Metric)]
struct IntegrationMetric {
    pub name: String,
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
        let sample = oximeter::types::Sample::new(&self.target, &self.metric);
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
        address: producer_address,
        base_route: "/collect".to_string(),
        interval: Duration::from_secs(1),
    };
    let config = oximeter_producer::Config {
        server_info,
        registration_address: nexus_address,
        dropshot_config: ConfigDropshot {
            bind_address: producer_address,
            ..Default::default()
        },
        logging_config: ConfigLogging::StderrTerminal {
            level: ConfigLoggingLevel::Error,
        },
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
            name: "integration-test-target".to_string(),
        },
        metric: IntegrationMetric {
            name: "integration-test-metric".to_string(),
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
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig {
        socket_addr: *dns_server.local_address(),
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
