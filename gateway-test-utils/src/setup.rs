// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use camino::Utf8Path;
use camino::Utf8PathBuf;
use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use gateway_messages::SpPort;
use omicron_gateway::MgsArguments;
use omicron_gateway::SpType;
use omicron_gateway::SwitchPortConfig;
pub use omicron_gateway::metrics::MetricsConfig;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::CondCheckError;
use qorb::resolver::AllBackends;
use qorb::resolver::Resolver;
use qorb::resolvers::fixed::FixedResolver;
use slog::o;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use std::collections::HashSet;
use std::convert::Infallible;
use std::future;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

// TODO this exact value is copy/pasted from `nexus/test-utils` - should we
// import it or have our own?
const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";
pub const DEFAULT_SP_SIM_CONFIG: &str = "configs/sp_sim_config.test.toml";

pub struct GatewayTestContext {
    pub client: ClientTestContext,
    pub server: omicron_gateway::Server,
    pub simrack: SimRack,
    pub logctx: LogContext,
    pub gateway_id: Uuid,
    resolver: FixedResolver,
    resolver_backends: watch::Receiver<AllBackends>,
}

impl GatewayTestContext {
    pub fn client(&self) -> gateway_client::Client {
        gateway_client::Client::new(
            &self.client.url("/").to_string(),
            self.logctx.log.new(slog::o!("component" => "MgsClient")),
        )
    }

    pub fn mgs_backends(&self) -> watch::Receiver<AllBackends> {
        self.resolver_backends.clone()
    }

    pub async fn teardown(mut self) {
        self.resolver.terminate().await;
        self.server.close().await.unwrap();
        self.logctx.cleanup_successful();
    }
}

pub fn load_test_config() -> (omicron_gateway::Config, sp_sim::Config) {
    // The test configs are located relative to the directory this file is in.
    // TODO: embed these with include_str! instead?
    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
    let server_config_file_path = manifest_dir.join("configs/config.test.toml");
    let server_config =
        match omicron_gateway::Config::from_file(&server_config_file_path) {
            Ok(config) => config,
            Err(e) => panic!("failed to load MGS config: {e}"),
        };

    let sp_sim_config_file_path =
        manifest_dir.join("configs/sp_sim_config.test.toml");
    let sp_sim_config =
        match sp_sim::Config::from_file(&sp_sim_config_file_path) {
            Ok(config) => config,
            Err(e) => panic!("failed to load SP simulator config: {e}"),
        };
    (server_config, sp_sim_config)
}

pub fn load_test_config_from(
    sp_sim_config_file: Utf8PathBuf,
) -> (omicron_gateway::Config, sp_sim::Config) {
    // The test configs are located relative to the directory this file is in.
    // TODO: embed these with include_str! instead?
    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
    let server_config_file_path = manifest_dir.join("configs/config.test.toml");
    let server_config =
        match omicron_gateway::Config::from_file(&server_config_file_path) {
            Ok(config) => config,
            Err(e) => panic!("failed to load MGS config: {e}"),
        };

    let sp_sim_config_file_path = manifest_dir.join(sp_sim_config_file);
    let sp_sim_config =
        match sp_sim::Config::from_file(&sp_sim_config_file_path) {
            Ok(config) => config,
            Err(e) => panic!("failed to load SP simulator config: {e}"),
        };
    (server_config, sp_sim_config)
}

pub async fn test_setup(
    test_name: &str,
    sp_port: SpPort,
) -> GatewayTestContext {
    let (server_config, sp_sim_config) = load_test_config();
    test_setup_with_config(
        test_name,
        sp_port,
        server_config,
        &sp_sim_config,
        None,
    )
    .await
}

// TODO-K: Use this
///// Helper function to load the main server config.
//fn load_server_config() -> omicron_gateway::Config {
//    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
//    let config_path = manifest_dir.join("configs/config.test.toml");
//    omicron_gateway::Config::from_file(&config_path)
//        .unwrap_or_else(|e| panic!("failed to load MGS config: {e}"))
//}

fn expected_location(
    config: &omicron_gateway::Config,
    sp_port: SpPort,
) -> String {
    let config = &config.switch.location;
    let mut locations = config.names.iter().cloned().collect::<HashSet<_>>();

    for determination in &config.determination {
        let refined = match sp_port {
            SpPort::One => &determination.sp_port_1,
            SpPort::Two => &determination.sp_port_2,
        };

        locations.retain(|name| refined.contains(name));
    }

    assert_eq!(locations.len(), 1);
    locations.into_iter().next().unwrap()
}

pub async fn test_setup_with_config(
    test_name: &str,
    sp_port: SpPort,
    mut server_config: omicron_gateway::Config,
    sp_sim_config: &sp_sim::Config,
    listen_addr: Option<SocketAddrV6>,
) -> GatewayTestContext {
    // Can't be `const` because `SocketAddrV6::new()` isn't const yet
    let localhost_port_0 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let mut addresses = vec![localhost_port_0];
    if let Some(addr) = listen_addr {
        addresses.push(addr);
    };

    // Use log settings from the server config and ignore log settings in
    // sp_sim_config; we'll give it the same logger as the server
    let logctx = LogContext::new(test_name, &server_config.log);
    let log = &logctx.log;

    // Start fake rack of simulated SPs
    let simrack = SimRack::start(sp_sim_config, log).await.unwrap();

    let expected_location = expected_location(&server_config, sp_port);

    // Update discovery addrs of `server_config` to point to the SP ports that
    // will identify us as the expected location
    for port_description in &mut server_config.switch.port {
        // we need to know whether this port points to a switch or sled; for now
        // assume that matches whether we end up as `switch0` or `switch1`
        let target_sp =
            port_description.location.get(&expected_location).unwrap();
        let (sp_addr, sp_ereport_addr) = match target_sp.typ {
            SpType::Switch => {
                let switch = &simrack.sidecars[usize::from(target_sp.slot)];
                (switch.local_addr(sp_port), switch.local_ereport_addr(sp_port))
            }
            SpType::Sled => {
                let sled = &simrack.gimlets[usize::from(target_sp.slot)];
                (sled.local_addr(sp_port), sled.local_ereport_addr(sp_port))
            }
            SpType::Power => todo!(),
        };
        match &mut port_description.config {
            SwitchPortConfig::Simulated { addr, ereport_addr, .. } => {
                *addr = sp_addr.unwrap();
                *ereport_addr =
                    sp_ereport_addr.expect("no ereport addr configured");
            }
            SwitchPortConfig::SwitchZoneInterface { .. } => {
                panic!("test config using `switch-zone-interface` config")
            }
        }
    }

    // Start gateway server
    let rack_id = Some(Uuid::parse_str(RACK_UUID).unwrap());
    let gateway_id = Uuid::new_v4();
    let args = MgsArguments { id: gateway_id, addresses, rack_id };
    let server = omicron_gateway::Server::start(
        server_config.clone(),
        args,
        log.clone(),
    )
    .await
    .unwrap();

    // Build a list of all SPs defined in our config
    let mut all_sp_ids = Vec::new();
    let mut local_switch = None;
    for port_config in &server_config.switch.port {
        let sp_id =
            port_config.location.get(&expected_location).copied().unwrap();
        all_sp_ids.push(sp_id);

        // Note the ID of our local ignition controller for use later in
        // checking that our self-location-discovery was correct.
        if port_config.config.interface()
            == server_config.switch.local_ignition_controller_interface
        {
            assert!(local_switch.is_none(), "ignition controller listed twice");
            local_switch = Some(sp_id);
        }
    }

    // Wait until the server has figured out the socket address of all those SPs
    let mgmt_switch = server.management_switch();
    poll::wait_for_condition::<(), Infallible, _, _>(
        || {
            let result = if mgmt_switch.is_discovery_complete()
                && all_sp_ids.iter().all(|&id| {
                    // All ids are valid; unwrap finding the handle to each one.
                    let sp = mgmt_switch.sp(id).unwrap();

                    // Have we found this SP?
                    sp.sp_addr_watch().borrow().is_some()
                }) {
                Ok(())
            } else {
                Err(CondCheckError::NotYet)
            };
            future::ready(result)
        },
        &Duration::from_millis(100),
        // This seems like a pretty long time to wait for MGS to discover the
        // simulated SPs, but we've seen tests fail due to timeouts here in the
        // past, so we may as well be generous:
        // https://github.com/oxidecomputer/omicron/issues/6877
        &Duration::from_secs(30),
    )
    .await
    .unwrap();

    // Make sure it discovered the location we expect
    assert_eq!(mgmt_switch.local_switch().unwrap(), local_switch.unwrap());

    let server_addr = server
        .dropshot_server_for_address(localhost_port_0)
        .unwrap()
        .local_addr();
    let client = ClientTestContext::new(
        server_addr,
        log.new(o!("component" => "client test context")),
    );

    let mut resolver = FixedResolver::new(std::iter::once(server_addr));
    let resolver_backends = resolver.monitor();

    GatewayTestContext {
        client,
        server,
        simrack,
        logctx,
        gateway_id,
        resolver,
        resolver_backends,
    }
}
