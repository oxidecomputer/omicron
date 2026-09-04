// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for setting up the control plane for testing

use crate::ControlPlaneStarter;
use crate::ControlPlaneTestContextSledAgent;
use crate::starter::PopulateCrdb;
use crate::starter::SledIndexAllocator;
use crate::starter::setup_with_config_impl;
use crate::starter::start_sled_agent;
#[cfg(feature = "omicron-dev")]
use anyhow::Context;
#[cfg(feature = "omicron-dev")]
use anyhow::Result;
use camino::Utf8Path;
#[cfg(feature = "omicron-dev")]
use camino::Utf8PathBuf;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use gateway_test_utils::setup::GatewayTestContext;
use nexus_config::NexusConfig;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_test_interface::NexusServer;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintTarget;
use omicron_common::address::Ipv6Subnet;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::Name;
use omicron_common::api::external::UserId;
use omicron_common::api::internal::nexus::Certificate;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use omicron_test_utils::dev::TestTempDir;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use oximeter_collector::Oximeter;
use oximeter_producer::Server as ProducerServer;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::inventory::SledCpuFamily;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use transient_dns_server::TransientDnsServer;

pub struct ControlPlaneBuilder<'a> {
    // required
    test_name: &'a str,

    // defaults provided by the builder
    nextra_sled_agents: u16,
    tls_cert: Option<Certificate>,
    nexus_config: NexusConfig,
    configure_second_nexus: bool,
}

impl<'a> ControlPlaneBuilder<'a> {
    pub fn new(test_name: &'a str) -> Self {
        ControlPlaneBuilder {
            test_name,
            nextra_sled_agents: 0,
            tls_cert: None,
            nexus_config: load_test_config(),
            configure_second_nexus: false,
        }
    }

    pub fn with_extra_sled_agents(mut self, nextra: u16) -> Self {
        self.nextra_sled_agents = nextra;
        self
    }

    pub fn with_tls_cert(mut self, tls_cert: Option<Certificate>) -> Self {
        self.tls_cert = tls_cert;
        self
    }

    pub fn with_second_nexus_configured(mut self, do_configure: bool) -> Self {
        self.configure_second_nexus = do_configure;
        self
    }

    pub fn customize_nexus_config(
        mut self,
        f: &dyn Fn(&mut NexusConfig) -> (),
    ) -> Self {
        f(&mut self.nexus_config);
        self
    }

    pub async fn start<N: NexusServer>(self) -> ControlPlaneTestContext<N> {
        let mut nexus_config = self.nexus_config;
        let starter =
            ControlPlaneStarter::<N>::new(self.test_name, &mut nexus_config);
        setup_with_config_impl(
            starter,
            PopulateCrdb::FromEnvironmentSeed,
            sim::SimMode::Explicit,
            self.tls_cert,
            self.nextra_sled_agents,
            DEFAULT_SP_SIM_CONFIG.into(),
            self.configure_second_nexus,
        )
        .await
    }
}

/// Helper for setting up the control plane for testing and accessing its parts
///
/// See [`ControlPlaneBuilder`] for setting one up.
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
    pub(crate) sled_index_allocator: SledIndexAllocator,
    pub oximeter: Oximeter,
    pub producer: ProducerServer,
    pub gateway: BTreeMap<SwitchSlot, GatewayTestContext>,
    /// All dpd instances, whether currently running or not, indexed by switch
    /// slot.
    pub dendrite: RwLock<HashMap<SwitchSlot, dev::dendrite::DendriteInstance>>,
    pub mgd: HashMap<SwitchSlot, dev::maghemite::MgdInstance>,
    pub ddm: HashMap<SwitchSlot, dev::maghemite::DdmInstance>,
    pub external_dns_zone_name: String,
    pub external_dns: TransientDnsServer,
    pub internal_dns: TransientDnsServer,
    pub initial_blueprint_id: BlueprintUuid,
    pub silo_name: Name,
    pub user_name: UserId,
    pub password: String,

    pub(crate) debug_dropbox_dir: TestTempDir,
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

    pub fn debug_dropbox_path(&self) -> &Utf8Path {
        self.debug_dropbox_dir.path()
    }

    /// Wait until at least one inventory collection has been inserted into the
    /// datastore, and the inventory watch channel is populated.
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
            |inv| inv.is_some(),
            timeout,
        )
        .await
        {
            Ok(()) => (),
            Err(poll::WatchChannelError::TimedOut(elapsed)) => {
                panic!("no inventory collection found within {elapsed:?}");
            }
            Err(poll::WatchChannelError::SenderDropped) => {
                panic!(
                    "inventory watch channel sender dropped before a \
                     collection was available"
                );
            }
        }
    }

    /// Start a simulated sled agent partway through a test, waiting for the
    /// sled to be registered with Nexus and present in inventory. Also adds the
    /// sled to the target blueprint.
    ///
    /// The returned server must be held for as long as the sled should exist
    /// (dropping it shuts the sled agent down).
    ///
    /// Unlike sled agents started during setup, sleds added this way are not
    /// included in [`Self::all_sled_agents`].
    #[must_use = "dropping the returned server shuts the sled agent down"]
    pub async fn add_sled(
        &self,
        sled_id: SledUuid,
        sim_mode: sim::SimMode,
        cpu_family: SledCpuFamily,
    ) -> sim::Server {
        let sled_index = self.sled_index_allocator.next();
        let nexus_address = self.server.get_http_server_internal_address();

        // `start_sled_agent` uses `NexusRegistration::WaitForCompletion`, so
        // Nexus knows about the sled once this returns.
        let sled_agent = start_sled_agent(
            self.logctx.log.new(slog::o!(
                "component" => "omicron_sled_agent::sim::Server",
                "sled_id" => sled_id.to_string(),
            )),
            nexus_address,
            sled_id,
            sled_index,
            sim_mode,
            cpu_family,
            false,
            &self.first_sled_agent().simulated_upstairs,
        )
        .await
        .expect("started simulated sled agent");

        // Ensure the sled shows up in inventory.
        self.collect_inventory_with_sled(sled_id).await;

        // Reconfigurator autoplanning is disabled in tests, so we must add the
        // sled to the target blueprint manually.
        self.add_sled_to_target_blueprint(sled_id).await;

        sled_agent
    }

    /// Run an inventory collection and load it, ensuring that the loaded
    /// collection contains the sled ID.
    async fn collect_inventory_with_sled(&self, sled_id: SledUuid) {
        crate::background::run_inventory_collection(&self.lockstep_client)
            .await;
        crate::background::run_inventory_loader(&self.lockstep_client).await;

        let inv_rx = self.server.inventory_load_rx();
        let collection = inv_rx
            .borrow()
            .clone()
            .expect("inventory loader left a collection in the watch channel");
        assert!(
            collection.sled_agents.contains_key(&sled_id),
            "inventory collection {} contains sled {sled_id}",
            collection.id,
        );

        // We don't check that collection.id is the same as the CollectionUuid
        // returned from run_inventory_loader: it's technically possible (though
        // unlikely) that another collection ran in between. What we care about
        // is that the sled is visible.
    }

    /// Add a sled to the target blueprint, simulating part of what real Nexus
    /// would do.
    ///
    /// This marks the sled active and available for provisioning in the
    /// blueprint, though it doesn't add any disks or zones.
    ///
    /// Ideally this mangling with the lower-level `BlueprintBuilder` would go
    /// away and we would be able to do a full-fledged planning run instead, but
    /// that's a bit of a heavier lift.
    async fn add_sled_to_target_blueprint(&self, sled_id: SledUuid) {
        let datastore = self.server.datastore();
        let opctx =
            OpContext::for_tests(self.logctx.log.clone(), datastore.clone());

        let target = datastore
            .blueprint_target_get_current(&opctx)
            .await
            .expect("fetched current target blueprint");
        let authz_parent = authz::Blueprint::new(
            authz::FLEET,
            target.target_id.into_untyped_uuid(),
            LookupType::ById(target.target_id.into_untyped_uuid()),
        );
        let parent = datastore
            .blueprint_read(&opctx, &authz_parent)
            .await
            .expect("read current target blueprint");

        assert!(
            !parent.sleds.contains_key(&sled_id),
            "sled {sled_id} is not already in target blueprint {}",
            parent.id
        );

        let mut builder = BlueprintBuilder::new_based_on(
            &self.logctx.log,
            &parent,
            "nexus-test-utils",
            PlannerRng::from_entropy(),
        )
        .expect("created BlueprintBuilder from target blueprint");
        builder
            .ensure_sled_exists(sled_id, Ipv6Subnet::new(Ipv6Addr::LOCALHOST));
        builder.comment(format!("add sled {sled_id}"));
        let child = builder.build(BlueprintSource::Test);

        datastore
            .blueprint_insert(&opctx, &child)
            .await
            .expect("inserted child blueprint");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: child.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("set child blueprint as target");

        crate::background::run_blueprint_loader(&self.lockstep_client).await;
        crate::background::run_blueprint_rendezvous(&self.lockstep_client)
            .await;
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
    ///
    /// Panics if no Dendrite was found for the given switch slot, or it was
    /// already stopped. (But note that the Dendrite instance is temporarily
    /// removed from the switch slot while it is being stopped, so trying to
    /// call this function with the same switch slot concurrently may panic.)
    pub async fn stop_dendrite(&self, switch_slot: SwitchSlot) {
        use slog::debug;
        let log = &self.logctx.log;
        debug!(log, "Stopping Dendrite"; "switch_slot" => ?switch_slot);

        // Extract from mutex first to avoid holding lock across await
        let mut dendrite =
            self.dendrite.write().unwrap().remove(&switch_slot).unwrap_or_else(
                || {
                    panic!(
                        "a dendrite instance should exist \
                         for switch slot {switch_slot:?}"
                    );
                },
            );

        let prior_dpd_state = dendrite.stop_dpd().await.unwrap();
        assert_eq!(
            prior_dpd_state,
            dev::dendrite::PriorDpdState::Running,
            "dendrite should have been running before stop_dendrite"
        );
        self.dendrite.write().unwrap().insert(switch_slot, dendrite);
    }

    /// Restart a Dendrite instance for testing drift correction scenarios.
    ///
    /// Simulates a switch restart where DPD loses its programmed state.
    /// Restarts on the same port so test DNS stays valid.
    ///
    /// Works both when Dendrite is currently running (will stop and restart) or
    /// when it was previously stopped via [`Self::stop_dendrite`].
    ///
    /// Panics if no Dendrite was found for the given switch slot. (But note
    /// that the Dendrite instance is temporarily removed from the internal map
    /// while it is being restarted, so trying to call this function with the
    /// same switch slot concurrently may panic.)
    pub async fn restart_dendrite(&self, switch_slot: SwitchSlot) {
        use slog::debug;
        let log = self.logctx.log.new(slog::o!(
            "switch_slot" => format!("{switch_slot:?}"),
        ));
        debug!(log, "Restarting Dendrite");

        // Extract from mutex first to avoid holding the lock across an await
        // point. This does mean that while restart_dendrite is running, other
        // code wouldn't be able to find the dpd instance via the switch slot.
        let mut dendrite =
            self.dendrite.write().unwrap().remove(&switch_slot).unwrap_or_else(
                || {
                    panic!(
                        "a dendrite instance should exist \
                         for switch slot {switch_slot:?}"
                    );
                },
            );
        let port = dendrite.port();
        debug!(log, "Restarting dpd behind its proxy"; "port" => port);

        let prior_dpd_state = dendrite.restart_dpd().await.unwrap();
        debug!(log, "Restarted dpd"; "prior_dpd_state" => ?prior_dpd_state);

        // Wait for Dendrite to be ready before returning.
        // We check `switch_identifiers()` rather than just `dpd_uptime()`
        // because Nexus needs switch_identifiers to work to determine which
        // switch to program.
        let dpd_client = dpd_client::Client::new(
            &format!("http://[::1]:{port}"),
            dpd_client::ClientState {
                tag: String::from("test-restart-wait"),
                log: self.logctx.log.clone(),
            },
        );
        wait_for_condition(
            || async {
                match dpd_client.switch_identifiers().await {
                    Ok(_) => Ok(()),
                    Err(_) => Err(dev::poll::CondCheckError::<()>::NotYet {
                        status: None,
                    }),
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(60),
        )
        .await
        .expect("contacted dpd within timeout after restart");

        self.dendrite.write().unwrap().insert(switch_slot, dendrite);
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
        for (_, dendrite) in self.dendrite.into_inner().unwrap() {
            dendrite.cleanup().await.unwrap();
        }
        for (_, mut mgd) in self.mgd {
            mgd.cleanup().await.unwrap();
        }
        for (_, mut ddm) in self.ddm {
            ddm.cleanup().await.unwrap();
        }
        self.debug_dropbox_dir.cleanup_successful();
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

/// Setup routine to use for `omicron-dev`. Use [`ControlPlaneBuilder`] for
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
    slog::debug!(log, "Ensuring seed tarball exists");

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
