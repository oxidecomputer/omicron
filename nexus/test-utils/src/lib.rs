// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

#[cfg(feature = "omicron-dev")]
use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use gateway_test_utils::setup::GatewayTestContext;
use nexus_config::NexusConfig;
use nexus_test_interface::NexusServer;
use omicron_common::api::external::UserId;
use omicron_common::api::external::{IdentityMetadata, Name};
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use oximeter_collector::Oximeter;
use oximeter_producer::Server as ProducerServer;
use slog::debug;
use starter::PopulateCrdb;
use starter::setup_with_config_impl;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

pub use sim::TEST_HARDWARE_THREADS;
pub use sim::TEST_RESERVOIR_RAM;

pub mod background;
pub mod db;
pub mod http_testing;
pub mod resource_helpers;
pub mod sql;
pub mod starter;

pub use starter::ControlPlaneStarter;
pub use starter::register_test_producer;
pub use starter::start_oximeter;
pub use starter::start_producer_server;
pub use starter::start_sled_agent;
pub use starter::start_sled_agent_with_config;

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

/// Setup routine to use for tests.
pub async fn test_setup_with_config<N: NexusServer>(
    test_name: &str,
    config: &mut NexusConfig,
    sim_mode: sim::SimMode,
    initial_cert: Option<Certificate>,
    extra_sled_agents: u16,
    gateway_config_file: Utf8PathBuf,
) -> ControlPlaneTestContext<N> {
    let starter = ControlPlaneStarter::<N>::new(test_name, config);
    setup_with_config_impl(
        starter,
        PopulateCrdb::FromEnvironmentSeed,
        sim_mode,
        initial_cert,
        extra_sled_agents,
        gateway_config_file,
        false,
    )
    .await
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
    let starter = ControlPlaneStarter::<N>::new("omicron-dev", config);

    let log = &starter.logctx.log;
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
        starter,
        PopulateCrdb::FromSeed { input_tar: seed_tar },
        sim::SimMode::Auto,
        None,
        extra_sled_agents,
        gateway_config_file,
        true,
    )
    .await)
}

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
