// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::handle::BgpSocketConfig;

use super::*;
use assert_matches::assert_matches;
use httpmock::MockServer;
use omicron_test_utils::dev;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkPorts;
use std::collections::BTreeSet;

const OUR_SLOT: ThisSledSwitchSlot = ThisSledSwitchSlot::TEST_FAKE;

fn port(name: &str, switch: SwitchSlot, allow_ddm_traffic: bool) -> PortConfig {
    PortConfig {
        routes: Vec::new(),
        addresses: Vec::new(),
        switch,
        port: name.to_owned(),
        uplink_port_speed: LinkSpeed::Speed100G,
        uplink_port_fec: None,
        bgp_peers: Vec::new(),
        autoneg: false,
        lldp: None,
        tx_eq: None,
        allow_ddm_traffic,
    }
}

fn rack_network_config(ports: Vec<PortConfig>) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00:1122:3344:0100::/56".parse().unwrap(),
        infra_ip_first: "192.0.2.10".parse().unwrap(),
        infra_ip_last: "192.0.2.100".parse().unwrap(),
        ports: UplinkPorts::new(ports).unwrap(),
        bgp: Vec::new(),
        bfd: Vec::new(),
    }
}

fn test_reconciler(server: &MockServer, log: &Logger) -> DdmdReconciler {
    let dummy_addr = "0.0.0.0:0".parse().unwrap();
    DdmdReconciler::new(
        ScrimletReconcilersMode::Test {
            mgs_addr: dummy_addr,
            dpd_addr: dummy_addr,
            mgd_addr: dummy_addr,
            ddmd_addr: *server.address(),
            bgp_socket_config: BgpSocketConfig::for_test(dummy_addr),
        },
        OUR_SLOT,
        log,
    )
}

fn networking_config(config: RackNetworkConfig) -> SystemNetworkingConfig {
    SystemNetworkingConfig {
        rack_network_config: config,
        blueprint_external_networking_config: None,
    }
}

#[tokio::test]
async fn posts_external_peers() {
    let logctx = dev::test_setup_log("posts_external_peers");
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path("/external_peers").json_body(
            serde_json::json!({
                "address_objects": ["tfportqsfp0_0/ll"],
            }),
        );
        then.status(204);
    });

    let mut reconciler = test_reconciler(&server, &logctx.log);
    let status = reconciler
        .do_reconciliation(
            &networking_config(rack_network_config(vec![
                port("qsfp0", SwitchSlot::Switch0, true),
                port("qsfp1", SwitchSlot::Switch0, false),
            ])),
            &logctx.log,
        )
        .await;

    mock.assert();
    assert_matches!(
        status,
        DdmdReconcilerStatus::Reconciled { external_peers_address_objects }
            if external_peers_address_objects
                == BTreeSet::from(["tfportqsfp0_0/ll".to_string()])
    );

    logctx.cleanup_successful();
}

#[tokio::test]
async fn server_error_reports_failed() {
    let logctx = dev::test_setup_log("server_error_reports_failed");
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(httpmock::Method::POST).path("/external_peers");
        then.status(500).header("content-type", "application/json").body(
            serde_json::json!({
                "request_id": "test",
                "message": "boom",
            })
            .to_string(),
        );
    });

    let mut reconciler = test_reconciler(&server, &logctx.log);
    let status = reconciler
        .do_reconciliation(
            &networking_config(rack_network_config(vec![port(
                "qsfp0",
                SwitchSlot::Switch0,
                true,
            )])),
            &logctx.log,
        )
        .await;

    assert_matches!(status, DdmdReconcilerStatus::Failed(_));

    logctx.cleanup_successful();
}
