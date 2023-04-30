// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

use anyhow::Context;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use nexus_test_interface::NexusServer;
use nexus_types::internal_api::params::ServiceKind;
use nexus_types::internal_api::params::ServicePutRequest;
use omicron_common::api::external::IdentityMetadata;
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
use std::path::Path;
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

pub struct ControlPlaneTestContext<N> {
    pub external_client: ClientTestContext,
    pub internal_client: ClientTestContext,
    pub server: N,
    pub database: dev::db::CockroachInstance,
    pub clickhouse: dev::clickhouse::ClickHouseInstance,
    pub logctx: LogContext,
    pub sled_agent_storage: tempfile::TempDir,
    pub sled_agent: sim::Server,
    pub oximeter: Oximeter,
    pub producer: ProducerServer,
    pub dendrite: dev::dendrite::DendriteInstance,
    pub external_dns_zone_name: String,
    pub external_dns_server: dns_server::dns_server::ServerHandle,
    pub external_dns_config_server:
        dropshot::HttpServer<dns_server::http_server::Context>,
    pub external_dns_resolver: trust_dns_resolver::TokioAsyncResolver,
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
    let config_file_path = Path::new("tests/config.test.toml");
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

pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut omicron_common::nexus_config::Config,
    sim_mode: sim::SimMode,
) -> ControlPlaneTestContext<N> {
    let logctx = LogContext::new(test_name, &config.pkg.log);
    let log = &logctx.log;

    // Start up CockroachDB.
    let database = db::test_setup_database(log).await;

    // Start ClickHouse database server.
    let clickhouse = dev::clickhouse::ClickHouseInstance::new(0).await.unwrap();

    // Set up a stub instance of dendrite
    let dendrite = dev::dendrite::DendriteInstance::start(0).await.unwrap();

    // Store actual address/port information for the databases after they start.
    config.deployment.database =
        nexus_config::Database::FromUrl { url: database.pg_config().clone() };
    config
        .pkg
        .timeseries_db
        .address
        .as_mut()
        .expect("Tests expect to set a port of Clickhouse")
        .set_port(clickhouse.port());
    config
        .pkg
        .dendrite
        .address
        .as_mut()
        .expect("Tests expect an explicit dendrite address")
        .set_port(dendrite.port);

    // Begin starting Nexus.
    let (nexus_internal, nexus_internal_addr) =
        N::start_internal(&config, &logctx.log).await;

    // Set up a single sled agent.
    let sa_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let sled_agent = start_sled_agent(
        logctx.log.new(o!(
            "component" => "omicron_sled_agent::sim::Server",
            "sled_id" => sa_id.to_string(),
        )),
        nexus_internal_addr,
        sa_id,
        tempdir.path(),
        sim_mode,
    )
    .await
    .unwrap();

    // Set up an external DNS server.
    let (
        external_dns_server,
        external_dns_config_server,
        external_dns_resolver,
    ) = start_dns_server(
        logctx.log.new(o!(
            "component" => "external_dns_server",
        )),
        tempdir.path(),
    )
    .await
    .unwrap();

    // Finish setting up Nexus by initializing the rack.  We need to include
    // information about the internal DNS server started within the simulated
    // Sled Agent.
    let dns_server_address_internal = match sled_agent
        .dns_dropshot_server
        .local_addr()
    {
        SocketAddr::V4(_) => panic!("expected DNS server to have IPv6 address"),
        SocketAddr::V6(addr) => addr,
    };
    let dns_server_dns_address_internal = match sled_agent
        .dns_server
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
    let dns_server_address_external = match external_dns_config_server
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
                vec![*sled_agent.dns_server.local_address()],
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
        external_dns_server,
        external_dns_config_server,
        external_dns_resolver,
    }
}

pub async fn start_sled_agent(
    log: Logger,
    nexus_address: SocketAddr,
    id: Uuid,
    update_directory: &Path,
    sim_mode: sim::SimMode,
) -> Result<sim::Server, String> {
    let config = sim::Config {
        id,
        sim_mode,
        nexus_address,
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

    let (server, _rack_init_request) =
        sim::Server::start(&config, &log, &sim::RssArgs::default()).await?;
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
    storage_path: &Path,
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
        storage_path: storage_path.to_string_lossy().into_owned().into(),
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
