// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use nexus_test_interface::NexusServer;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::nexus_config;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use oximeter_collector::Oximeter;
use oximeter_producer::Server as ProducerServer;
use slog::o;
use slog::Logger;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

pub mod db;
pub mod http_testing;
pub mod resource_helpers;

pub const SLED_AGENT_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
pub const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";
pub const OXIMETER_UUID: &str = "39e6175b-4df2-4730-b11d-cbc1e60a2e78";
pub const PRODUCER_UUID: &str = "a6458b7d-87c3-4483-be96-854d814c20de";

pub struct ControlPlaneTestContext<N> {
    pub external_client: ClientTestContext,
    pub internal_client: ClientTestContext,
    pub server: N,
    pub database: dev::db::CockroachInstance,
    pub clickhouse: dev::clickhouse::ClickHouseInstance,
    pub logctx: LogContext,
    pub sled_agent: sim::Server,
    pub oximeter: Oximeter,
    pub producer: ProducerServer,
}

impl<N: NexusServer> ControlPlaneTestContext<N> {
    pub async fn teardown(mut self) {
        self.server.close().await;
        self.database.cleanup().await.unwrap();
        self.clickhouse.cleanup().await.unwrap();
        self.sled_agent.http_server.close().await.unwrap();
        self.oximeter.close().await.unwrap();
        self.producer.close().await.unwrap();
        self.logctx.cleanup_successful();
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
    test_setup_with_config::<N>(test_name, &mut config).await
}

pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut omicron_common::nexus_config::Config,
) -> ControlPlaneTestContext<N> {
    let logctx = LogContext::new(test_name, &config.pkg.log);
    let log = &logctx.log;

    // Start up CockroachDB.
    let database = db::test_setup_database(log).await;

    // Start ClickHouse database server.
    let clickhouse = dev::clickhouse::ClickHouseInstance::new(0).await.unwrap();

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

    let server = N::start_and_populate(&config, &logctx.log).await;

    let testctx_external = ClientTestContext::new(
        server.get_http_servers_external()[0],
        logctx.log.new(o!("component" => "external client test context")),
    );
    let testctx_internal = ClientTestContext::new(
        server.get_http_server_internal(),
        logctx.log.new(o!("component" => "internal client test context")),
    );

    // Set up a single sled agent.
    let sa_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
    let sled_agent = start_sled_agent(
        logctx.log.new(o!(
            "component" => "omicron_sled_agent::sim::Server",
            "sled_id" => sa_id.to_string(),
        )),
        server.get_http_server_internal(),
        sa_id,
    )
    .await
    .unwrap();

    // Set Nexus' shared resolver to point to the simulated sled agent's
    // internal DNS server
    server
        .set_resolver(
            internal_dns_client::multiclient::Resolver::new_from_addrs(vec![
                sled_agent.dns_server.address,
            ])
            .unwrap(),
        )
        .await;

    // Set up an Oximeter collector server
    let collector_id = Uuid::parse_str(OXIMETER_UUID).unwrap();
    let oximeter = start_oximeter(
        log.new(o!("component" => "oximeter")),
        server.get_http_server_internal(),
        clickhouse.port(),
        collector_id,
    )
    .await
    .unwrap();

    // Set up a test metric producer server
    let producer_id = Uuid::parse_str(PRODUCER_UUID).unwrap();
    let producer =
        start_producer_server(server.get_http_server_internal(), producer_id)
            .await
            .unwrap();
    register_test_producer(&producer).unwrap();

    ControlPlaneTestContext {
        server,
        external_client: testctx_external,
        internal_client: testctx_internal,
        database,
        clickhouse,
        sled_agent,
        oximeter,
        producer,
        logctx,
    }
}

pub async fn start_sled_agent(
    log: Logger,
    nexus_address: SocketAddr,
    id: Uuid,
) -> Result<sim::Server, String> {
    let config = sim::Config {
        id,
        sim_mode: sim::SimMode::Explicit,
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
    };

    let (server, _rack_init_request) =
        sim::Server::start(&config, &log).await?;
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
