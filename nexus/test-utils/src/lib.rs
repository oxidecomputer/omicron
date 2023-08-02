// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

use anyhow::Context;
use camino::Utf8Path;
use dns_service_client::types::DnsConfigParams;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HandlerTaskMode;
use nexus_test_interface::NexusServer;
use nexus_types::external_api::params::UserId;
use nexus_types::internal_api::params::Certificate;
use nexus_types::internal_api::params::DatasetCreateRequest;
use nexus_types::internal_api::params::DatasetKind;
use nexus_types::internal_api::params::DatasetPutRequest;
use nexus_types::internal_api::params::RecoverySiloConfig;
use nexus_types::internal_api::params::ServiceKind;
use nexus_types::internal_api::params::ServiceNic;
use nexus_types::internal_api::params::ServicePutRequest;
use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::{IdentityMetadata, Name};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::nexus_config;
use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use oximeter_collector::Oximeter;
use oximeter_producer::LogConfig;
use oximeter_producer::Server as ProducerServer;
use slog::{debug, o, Logger};
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

pub mod db;
pub mod http_testing;
pub mod resource_helpers;

pub const SLED_AGENT_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
pub const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";
pub const SWITCH_UUID: &str = "dae4e1f1-410e-4314-bff1-fec0504be07e";
pub const OXIMETER_UUID: &str = "39e6175b-4df2-4730-b11d-cbc1e60a2e78";
pub const PRODUCER_UUID: &str = "a6458b7d-87c3-4483-be96-854d814c20de";

/// The reported amount of hardware threads for an emulated sled agent.
pub const TEST_HARDWARE_THREADS: u32 = 16;
/// The reported amount of physical RAM for an emulated sled agent.
pub const TEST_PHYSICAL_RAM: u64 = 32 * (1 << 30);
/// The reported amount of VMM reservoir RAM for an emulated sled agent.
pub const TEST_RESERVOIR_RAM: u64 = 16 * (1 << 30);

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
    pub dendrite: HashMap<SwitchLocation, dev::dendrite::DendriteInstance>,
    pub external_dns_zone_name: String,
    pub external_dns: dns_server::TransientServer,
    pub internal_dns: dns_server::TransientServer,
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
        for (_, mut dendrite) in self.dendrite {
            dendrite.cleanup().await.unwrap();
        }
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
    mac_addrs: Box<dyn Iterator<Item = MacAddr>>,
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
    fn add_service(
        &mut self,
        address: SocketAddrV6,
        kind: ServiceKind,
        service_name: internal_dns::ServiceName,
        sled_id: Uuid,
    ) {
        let zone_id = Uuid::new_v4();
        self.services.push(ServicePutRequest {
            address,
            kind,
            service_id: Uuid::new_v4(),
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
    pub dendrite: HashMap<SwitchLocation, dev::dendrite::DendriteInstance>,

    // NOTE: Only exists after starting Nexus, until external Nexus is
    // initialized.
    nexus_internal: Option<<N as NexusServer>::InternalServer>,
    nexus_internal_addr: Option<SocketAddr>,

    pub external_dns_zone_name: Option<String>,
    pub external_dns: Option<dns_server::TransientServer>,
    pub internal_dns: Option<dns_server::TransientServer>,

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
            dendrite: HashMap::new(),
            nexus_internal: None,
            nexus_internal_addr: None,
            external_dns_zone_name: None,
            external_dns: None,
            internal_dns: None,
            silo_name: None,
            user_name: None,
        }
    }

    pub async fn start_crdb(&mut self, populate: bool) {
        let log = &self.logctx.log;
        debug!(log, "Starting CRDB");

        // Start up CockroachDB.
        let database = if populate {
            db::test_setup_database(log).await
        } else {
            db::test_setup_database_empty(log).await
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
        self.database = Some(database);
    }

    // Start ClickHouse database server.
    pub async fn start_clickhouse(&mut self) {
        let log = &self.logctx.log;
        debug!(log, "Starting Clickhouse");
        let clickhouse =
            dev::clickhouse::ClickHouseInstance::new(0).await.unwrap();
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
        let config = omicron_common::nexus_config::DpdConfig {
            address: std::net::SocketAddr::V6(address),
        };
        self.config.pkg.dendrite.insert(switch_location, config);

        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();
        self.rack_init_builder.add_service(
            address,
            ServiceKind::Dendrite,
            internal_dns::ServiceName::Dendrite,
            sled_id,
        );
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

        self.config.deployment.internal_dns =
            nexus_config::InternalDns::FromAddress {
                address: *self
                    .internal_dns
                    .as_ref()
                    .expect("Must initialize internal DNS server first")
                    .dns_server
                    .local_address(),
            };
        self.config.deployment.database = nexus_config::Database::FromUrl {
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
        self.rack_init_builder.add_service(
            address,
            ServiceKind::Nexus {
                external_address: self
                    .config
                    .deployment
                    .dropshot_external
                    .dropshot
                    .bind_address
                    .ip(),
                nic: ServiceNic {
                    id: Uuid::new_v4(),
                    name: "nexus".parse().unwrap(),
                    ip: NEXUS_OPTE_IPV4_SUBNET
                        .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as u32 + 1)
                        .unwrap()
                        .into(),
                    mac,
                },
            },
            internal_dns::ServiceName::Nexus,
            sled_id,
        );

        self.nexus_internal = Some(nexus_internal);
        self.nexus_internal_addr = Some(nexus_internal_addr);
    }

    pub async fn populate_internal_dns(&mut self) -> DnsConfigParams {
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
        dns_config_client.dns_config_put(&dns_config).await.expect(
            "Failed to send initial DNS records to internal DNS server",
        );
        dns_config
    }

    // Perform RSS handoff
    pub async fn start_nexus_external(
        &mut self,
        dns_config: DnsConfigParams,
        tls_certificates: Vec<Certificate>,
    ) {
        let log = &self.logctx.log;
        debug!(log, "Starting Nexus (external API)");

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
            &self.config,
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

    pub async fn start_sled(&mut self, sim_mode: sim::SimMode) {
        let nexus_address =
            self.nexus_internal_addr.expect("Must launch Nexus first");

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
        .expect("Failed to start sled agent");

        self.sled_agent = Some(sled_agent);
        self.sled_agent_storage = Some(tempdir);
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
        self.rack_init_builder.add_service(
            address,
            ServiceKind::CruciblePantry,
            internal_dns::ServiceName::CruciblePantry,
            sled_id,
        );
    }

    // Set up an external DNS server.
    pub async fn start_external_dns(&mut self) {
        let log = self.logctx.log.new(o!("component" => "external_dns_server"));
        let sled_id = Uuid::parse_str(SLED_AGENT_UUID).unwrap();

        let dns = dns_server::TransientServer::new(&log).await.unwrap();

        let SocketAddr::V6(dns_address) = *dns.dns_server.local_address() else {
            panic!("Unsupported IPv4 DNS address");
        };
        let SocketAddr::V6(dropshot_address) = dns.dropshot_server.local_addr() else {
            panic!("Unsupported IPv4 Dropshot address");
        };

        let mac = self
            .rack_init_builder
            .mac_addrs
            .next()
            .expect("ran out of MAC addresses");
        self.rack_init_builder.add_service(
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
                },
            },
            internal_dns::ServiceName::ExternalDns,
            sled_id,
        );
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
        self.rack_init_builder.add_service(
            address,
            ServiceKind::InternalDns,
            internal_dns::ServiceName::InternalDns,
            sled_id,
        );

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
            oximeter: self.oximeter.unwrap(),
            producer: self.producer.unwrap(),
            logctx: self.logctx,
            dendrite: self.dendrite,
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
        if let Some(oximeter) = self.oximeter {
            oximeter.close().await.unwrap();
        }
        if let Some(producer) = self.producer {
            producer.close().await.unwrap();
        }
        for (_, mut dendrite) in self.dendrite {
            dendrite.cleanup().await.unwrap();
        }
        self.logctx.cleanup_successful();
    }
}

pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut omicron_common::nexus_config::Config,
    sim_mode: sim::SimMode,
    initial_cert: Option<Certificate>,
) -> ControlPlaneTestContext<N> {
    let mut builder =
        ControlPlaneTestContextBuilder::<N>::new(test_name, config);

    let populate = true;
    builder.start_crdb(populate).await;
    builder.start_clickhouse().await;
    builder.start_dendrite(SwitchLocation::Switch0).await;
    builder.start_dendrite(SwitchLocation::Switch1).await;
    builder.start_internal_dns().await;
    builder.start_external_dns().await;
    builder.start_nexus_internal().await;
    builder.start_sled(sim_mode).await;
    builder.start_crucible_pantry().await;

    // Give Nexus necessary information to find the Crucible Pantry
    let dns_config = builder.populate_internal_dns().await;

    builder
        .start_nexus_external(dns_config, initial_cert.into_iter().collect())
        .await;

    builder.start_oximeter().await;
    builder.start_producer_server().await;

    builder.build()
}

pub async fn start_sled_agent(
    log: Logger,
    nexus_address: SocketAddr,
    id: Uuid,
    update_directory: &Utf8Path,
    sim_mode: sim::SimMode,
) -> Result<sim::Server, String> {
    let config = sim::Config {
        id,
        sim_mode,
        nexus_address,
        dropshot: ConfigDropshot {
            bind_address: SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0),
            request_body_max_bytes: 1024 * 1024,
            default_handler_task_mode: HandlerTaskMode::Detached,
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
            reservoir_ram: TEST_RESERVOIR_RAM,
        },
    };
    let server =
        sim::Server::start(&config, &log).await.map_err(|e| e.to_string())?;
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
            default_handler_task_mode: HandlerTaskMode::Detached,
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
