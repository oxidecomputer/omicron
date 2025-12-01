// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for setting up the control plane for testing

use crate::ControlPlaneStarter;
use crate::ControlPlaneTestContextSledAgent;
use crate::starter::PopulateCrdb;
use crate::starter::setup_with_config_impl;
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
use omicron_common::api::external::Name;
use omicron_common::api::external::UserId;
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_sled_agent::sim;
use omicron_test_utils::dev;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_watch_channel_condition;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::SledUuid;
use oximeter_collector::Oximeter;
use oximeter_producer::Server as ProducerServer;
use slog::debug;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub struct ControlPlaneBuilder<'a> {
    // required
    test_name: &'a str,

    // defaults provided by the builder
    nextra_sled_agents: u16,
    tls_cert: Option<Certificate>,
    nexus_config: NexusConfig,
}

impl<'a> ControlPlaneBuilder<'a> {
    pub fn new(test_name: &'a str) -> Self {
        ControlPlaneBuilder {
            test_name,
            nextra_sled_agents: 0,
            tls_cert: None,
            nexus_config: load_test_config(),
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
            false,
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
