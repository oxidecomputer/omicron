// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use httpmock::MockServer;
use omicron_test_utils::dev;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkPorts;

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

fn base(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| (*s).to_owned()).collect()
}

// The regression that matters: dropping any base interface from an apply would
// stop the DDM sessions SMF started at switch zone boot.
#[test]
fn base_interfaces_are_never_dropped() {
    let real_sidecar_base: BTreeSet<String> =
        (0..32).map(|i| format!("tfportrear{i}_0")).collect();
    let config = rack_network_config(vec![
        port("qsfp0", SwitchSlot::Switch0, true),
        port("qsfp1", SwitchSlot::Switch0, false),
        port("qsfp2", SwitchSlot::Switch1, true),
    ]);

    let desired = desired_interfaces(&real_sidecar_base, &config, OUR_SLOT);

    assert!(real_sidecar_base.is_subset(&desired));
    assert!(desired.contains("tfportqsfp0_0"));
}

// On a4x2 / softnpu the base set comes from `switch_zone_maghemite_links`
// instead of the rear ports, and must survive just the same.
#[test]
fn softnpu_base_interfaces_survive() {
    let softnpu_base = base(&["net0", "net1"]);
    let config =
        rack_network_config(vec![port("qsfp0", SwitchSlot::Switch0, true)]);

    let desired = desired_interfaces(&softnpu_base, &config, OUR_SLOT);

    assert_eq!(desired, base(&["net0", "net1", "tfportqsfp0_0"]));
}

#[test]
fn excludes_other_switch_and_disallowed_ports() {
    let config = rack_network_config(vec![
        port("qsfp0", SwitchSlot::Switch0, false),
        port("qsfp1", SwitchSlot::Switch1, true),
    ]);

    let desired = desired_interfaces(&base(&["net0"]), &config, OUR_SLOT);

    assert_eq!(desired, base(&["net0"]));
}

// ddmd appends the link-local addrobj suffix itself; sending it here would
// produce `tfportqsfp0_0/ll/ll` and never match an existing FSM.
#[test]
fn interface_names_have_no_addrobj_suffix() {
    let config =
        rack_network_config(vec![port("qsfp0", SwitchSlot::Switch0, true)]);

    let desired =
        desired_interfaces(&base(&["tfportrear0_0"]), &config, OUR_SLOT);

    for interface in &desired {
        assert!(!interface.contains('/'), "unexpected addrobj: {interface}");
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
        },
        OUR_SLOT,
        base(&["tfportrear0_0"]),
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
async fn posts_base_union_front_ports() {
    let logctx = dev::test_setup_log("posts_base_union_front_ports");
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/ddm/omicron/apply")
            .json_body(serde_json::json!({
                "ddm_interfaces": ["tfportqsfp0_0", "tfportrear0_0"],
            }));
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
        DdmdReconcilerStatus::Reconciled { interfaces }
            if interfaces == base(&["tfportrear0_0", "tfportqsfp0_0"])
    );

    logctx.cleanup_successful();
}

#[tokio::test]
async fn server_error_reports_failed() {
    let logctx = dev::test_setup_log("server_error_reports_failed");
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(httpmock::Method::POST).path("/ddm/omicron/apply");
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
