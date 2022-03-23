// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use gateway_sp_comms::KnownSp;
use gateway_sp_comms::KnownSps;
use slog::o;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use std::path::Path;
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
        self.server.http_server.close().await.unwrap();
        self.logctx.cleanup_successful();
    }
}

pub fn load_test_config() -> (omicron_gateway::Config, sp_sim::Config) {
    let server_config_file_path = Path::new("tests/config.test.toml");
    let mut server_config =
        omicron_gateway::Config::from_file(server_config_file_path)
            .expect("failed to load config.test.toml");
    server_config.id = Uuid::new_v4();

    let sp_sim_config_file_path = Path::new("tests/sp_sim_config.test.toml");
    let sp_sim_config = sp_sim::Config::from_file(sp_sim_config_file_path)
        .expect("failed to load sp_sim_config.test.toml");
    (server_config, sp_sim_config)
}

pub async fn test_setup(test_name: &str) -> GatewayTestContext {
    let (mut server_config, mut sp_sim_config) = load_test_config();
    test_setup_with_config(test_name, &mut server_config, &mut sp_sim_config)
        .await
}

pub async fn test_setup_with_config(
    test_name: &str,
    server_config: &mut omicron_gateway::Config,
    sp_sim_config: &mut sp_sim::Config,
) -> GatewayTestContext {
    // Use log settings from the server config and ignore log settings in
    // sp_sim_config; we'll give it the same logger as the server
    let logctx = LogContext::new(test_name, &server_config.log);
    let log = &logctx.log;

    // Start fake rack of simulated SPs
    let simrack = SimRack::start(sp_sim_config, log).await.unwrap();

    // Update gateway config to match the simulated rack.
    let sidecars = simrack
        .sidecars
        .iter()
        .map(|simsp| KnownSp {
            sp: simsp.local_addr(),
            switch_port: "127.0.0.1:0".parse().unwrap(),
        })
        .collect::<Vec<_>>();
    let gimlets = simrack
        .gimlets
        .iter()
        .map(|simsp| KnownSp {
            sp: simsp.local_addr(),
            switch_port: "127.0.0.1:0".parse().unwrap(),
        })
        .collect::<Vec<_>>();
    server_config.known_sps = KnownSps {
        switches: sidecars,
        sleds: gimlets,
        power_controllers: vec![], // TODO
    };

    // Start gateway server
    let rack_id = Uuid::parse_str(RACK_UUID).unwrap();

    let server = omicron_gateway::Server::start(server_config, rack_id, log)
        .await
        .unwrap();

    let client = ClientTestContext::new(
        server.http_server.local_addr(),
        log.new(o!("component" => "client test context")),
    );

    GatewayTestContext { client, server, simrack, logctx }
}
