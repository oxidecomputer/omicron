// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use gateway_messages::SpPort;
use omicron_gateway::MgsArguments;
use omicron_gateway::SpType;
use omicron_gateway::SwitchPortConfig;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::CondCheckError;
use slog::o;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use std::collections::HashSet;
use std::convert::Infallible;
use std::future;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

// TODO this exact value is copy/pasted from `nexus/test-utils` - should we
// import it or have our own?
const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";

pub struct GatewayTestContext {
    pub client: ClientTestContext,
    pub server: omicron_gateway::Server,
    pub simrack: SimRack,
    pub logctx: LogContext,
}

impl GatewayTestContext {
    pub async fn teardown(self) {
        self.server.close().await.unwrap();
        self.logctx.cleanup_successful();
    }
}

pub fn load_test_config() -> (omicron_gateway::Config, sp_sim::Config) {
    let server_config_file_path = Path::new("tests/config.test.toml");
    let server_config =
        omicron_gateway::Config::from_file(server_config_file_path)
            .expect("failed to load config.test.toml");

    let sp_sim_config_file_path = Path::new("tests/sp_sim_config.test.toml");
    let sp_sim_config = sp_sim::Config::from_file(sp_sim_config_file_path)
        .expect("failed to load sp_sim_config.test.toml");
    (server_config, sp_sim_config)
}

pub async fn test_setup(
    test_name: &str,
    sp_port: SpPort,
) -> GatewayTestContext {
    let (server_config, mut sp_sim_config) = load_test_config();
    test_setup_with_config(
        test_name,
        sp_port,
        server_config,
        &mut sp_sim_config,
    )
    .await
}

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
) -> GatewayTestContext {
    // Can't be `const` because `SocketAddrV6::new()` isn't const yet
    let localhost_port_0 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);

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
        let sp_addr = match target_sp.typ {
            SpType::Switch => {
                simrack.sidecars[target_sp.slot].local_addr(sp_port)
            }
            SpType::Sled => simrack.gimlets[target_sp.slot].local_addr(sp_port),
            SpType::Power => todo!(),
        };
        match &mut port_description.config {
            SwitchPortConfig::Simulated { addr, .. } => {
                *addr = sp_addr.unwrap();
            }
            SwitchPortConfig::SwitchZoneInterface { .. } => {
                panic!("test config using `switch-zone-interface` config")
            }
        }
    }

    // Start gateway server
    let rack_id = Uuid::parse_str(RACK_UUID).unwrap();

    let args =
        MgsArguments { id: Uuid::new_v4(), addresses: vec![localhost_port_0] };
    let server = omicron_gateway::Server::start(
        server_config.clone(),
        args,
        rack_id,
        log.clone(),
    )
    .await
    .unwrap();

    // Build a list of all SPs defined in our config
    let mut all_sp_ids = Vec::new();
    for port_config in &server_config.switch.port {
        all_sp_ids.push(
            port_config.location.get(&expected_location).copied().unwrap(),
        );
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
        &Duration::from_secs(1),
    )
    .await
    .unwrap();

    // Make sure it discovered the location we expect
    assert_eq!(mgmt_switch.location_name().unwrap(), expected_location);

    let client = ClientTestContext::new(
        server
            .dropshot_server_for_address(localhost_port_0)
            .unwrap()
            .local_addr(),
        log.new(o!("component" => "client test context")),
    );

    GatewayTestContext { client, server, simrack, logctx }
}
