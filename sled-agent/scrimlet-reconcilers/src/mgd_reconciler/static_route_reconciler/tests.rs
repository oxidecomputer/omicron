// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use mg_admin_client::types::Path as MgdPath;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::PortSpeed;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::SwitchSlot;

use crate::switch_zone_slot::ThisSledSwitchSlot;

/// Build a minimal `PortConfig` on the given switch with the given routes.
fn port_config(switch: SwitchSlot, routes: Vec<RouteConfig>) -> PortConfig {
    PortConfig {
        routes,
        addresses: Vec::new(),
        switch,
        port: "qsfp0".to_string(),
        uplink_port_speed: PortSpeed::Speed100G,
        uplink_port_fec: None,
        bgp_peers: Vec::new(),
        autoneg: false,
        lldp: None,
        tx_eq: None,
    }
}

fn rack_config(ports: Vec<PortConfig>) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00::/48".parse().unwrap(),
        infra_ip_first: "10.0.0.1".parse().unwrap(),
        infra_ip_last: "10.0.0.100".parse().unwrap(),
        ports,
        bgp: Vec::new(),
        bfd: Vec::new(),
    }
}

fn v4_route(
    destination: &str,
    nexthop: &str,
    vlan_id: Option<u16>,
    rib_priority: Option<u8>,
) -> RouteConfig {
    RouteConfig {
        destination: destination.parse().unwrap(),
        nexthop: nexthop.parse().unwrap(),
        vlan_id,
        rib_priority,
    }
}

fn v6_route(
    destination: &str,
    nexthop: &str,
    vlan_id: Option<u16>,
    rib_priority: Option<u8>,
) -> RouteConfig {
    RouteConfig {
        destination: destination.parse().unwrap(),
        nexthop: nexthop.parse().unwrap(),
        vlan_id,
        rib_priority,
    }
}

fn mgd_path(nexthop: &str, rib_priority: u8, vlan_id: Option<u16>) -> MgdPath {
    MgdPath {
        nexthop: nexthop.parse().unwrap(),
        rib_priority,
        vlan_id,
        shutdown: false,
        bgp: None,
        nexthop_interface: None,
    }
}

fn mgd_routes(
    v4: Vec<(&str, Vec<MgdPath>)>,
    v6: Vec<(&str, Vec<MgdPath>)>,
) -> MgdCurrentRoutes {
    MgdCurrentRoutes {
        v4: v4.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        v6: v6.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

#[test]
fn plan_all_unchanged() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_all_unchanged");
    let log = &logctx.log;

    // Desired: one v4 route on Switch0.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![v4_route("10.0.0.0/24", "10.0.0.1", None, None)],
    )]);

    // mgd has the same route (with default priority filled in).
    let current = mgd_routes(
        vec![("10.0.0.0/24", vec![mgd_path("10.0.0.1", 1, None)])],
        vec![],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 1);
    assert!(plan.to_delete.is_empty());
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_add_all() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_add_all");
    let log = &logctx.log;

    // Desired: two routes (one v4, one v6).
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![
            v4_route("10.0.0.0/24", "10.0.0.1", None, Some(5)),
            v6_route("2001:db8::/64", "2001:db8::1", None, Some(3)),
        ],
    )]);

    // mgd is empty.
    let current = mgd_routes(vec![], vec![]);

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 0);
    assert!(plan.to_delete.is_empty());
    assert_eq!(plan.to_add.len(), 2);

    logctx.cleanup_successful();
}

#[test]
fn plan_delete_all() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_delete_all");
    let log = &logctx.log;

    // Desired: no routes.
    let config = rack_config(vec![]);

    // mgd has two routes.
    let current = mgd_routes(
        vec![("10.0.0.0/24", vec![mgd_path("10.0.0.1", 1, None)])],
        vec![("2001:db8::/64", vec![mgd_path("2001:db8::1", 1, None)])],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 0);
    assert_eq!(plan.to_delete.len(), 2);
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_mix() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_mix");
    let log = &logctx.log;

    // Desired: keep 10.0.0.0/24, add 10.1.0.0/24, remove 10.2.0.0/24.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![
            v4_route("10.0.0.0/24", "10.0.0.1", None, None),
            v4_route("10.1.0.0/24", "10.1.0.1", None, None),
        ],
    )]);

    let current = mgd_routes(
        vec![
            ("10.0.0.0/24", vec![mgd_path("10.0.0.1", 1, None)]),
            ("10.2.0.0/24", vec![mgd_path("10.2.0.1", 1, None)]),
        ],
        vec![],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 1);
    assert_eq!(plan.to_delete.len(), 1);
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_filters_other_switch_slot() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_filters_other_switch_slot",
    );
    let log = &logctx.log;

    // Desired config has routes on both Switch0 and Switch1. Our reconciler is
    // Switch0 (TEST_FAKE), so Switch1 routes should be ignored entirely.
    let config = rack_config(vec![
        port_config(
            SwitchSlot::Switch0,
            vec![v4_route("10.0.0.0/24", "10.0.0.1", None, None)],
        ),
        port_config(
            SwitchSlot::Switch1,
            vec![v4_route("10.1.0.0/24", "10.1.0.1", None, None)],
        ),
    ]);

    // mgd is empty.
    let current = mgd_routes(vec![], vec![]);

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // Only the Switch0 route should be added.
    assert_eq!(plan.unchanged_count, 0);
    assert!(plan.to_delete.is_empty());
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_default_rib_priority() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("plan_default_rib_priority");
    let log = &logctx.log;

    // Desired route has rib_priority = None (should default to 1).
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![v4_route("10.0.0.0/24", "10.0.0.1", None, None)],
    )]);

    // mgd has the same route with priority 1 (the default).
    let current = mgd_routes(
        vec![(
            "10.0.0.0/24",
            vec![mgd_path("10.0.0.1", DEFAULT_RIB_PRIORITY_STATIC, None)],
        )],
        vec![],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // Should be unchanged because None defaults to 1.
    assert_eq!(plan.unchanged_count, 1);
    assert!(plan.to_delete.is_empty());
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_priority_change_is_delete_plus_add() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_priority_change_is_delete_plus_add",
    );
    let log = &logctx.log;

    // Desired: priority 5 for a route.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![v4_route("10.0.0.0/24", "10.0.0.1", None, Some(5))],
    )]);

    // mgd has the same route but with priority 1.
    let current = mgd_routes(
        vec![("10.0.0.0/24", vec![mgd_path("10.0.0.1", 1, None)])],
        vec![],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // Different priority means old route deleted, new route added.
    assert_eq!(plan.unchanged_count, 0);
    assert_eq!(plan.to_delete.len(), 1);
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_vlan_id_matters() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("plan_vlan_id_matters");
    let log = &logctx.log;

    // Desired: route with vlan_id = Some(100).
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![v4_route("10.0.0.0/24", "10.0.0.1", Some(100), None)],
    )]);

    // mgd has the same route but with no vlan_id.
    let current = mgd_routes(
        vec![("10.0.0.0/24", vec![mgd_path("10.0.0.1", 1, None)])],
        vec![],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // Different vlan_id: delete the old, add the new.
    assert_eq!(plan.unchanged_count, 0);
    assert_eq!(plan.to_delete.len(), 1);
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_multiple_nexthops_per_prefix() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_multiple_nexthops_per_prefix",
    );
    let log = &logctx.log;

    // Desired: two nexthops for the same prefix.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![
            v4_route("10.0.0.0/24", "10.0.0.1", None, None),
            v4_route("10.0.0.0/24", "10.0.0.2", None, None),
        ],
    )]);

    // mgd has one of them.
    let current = mgd_routes(
        vec![("10.0.0.0/24", vec![mgd_path("10.0.0.1", 1, None)])],
        vec![],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 1);
    assert!(plan.to_delete.is_empty());
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_rejects_bad_mgd_prefix() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("plan_rejects_bad_mgd_prefix");
    let log = &logctx.log;

    let config = rack_config(vec![]);

    // mgd returns an unparseable prefix.
    let current = mgd_routes(
        vec![("not-a-prefix", vec![mgd_path("10.0.0.1", 1, None)])],
        vec![],
    );

    let err = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect_err("plan should fail with bad prefix");

    assert!(
        err.contains("invalid route fetched from mgd"),
        "unexpected error: {err}",
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_rejects_mixed_ip_families_in_config() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_rejects_mixed_ip_families_in_config",
    );
    let log = &logctx.log;

    // A route with a v4 nexthop and a v6 destination.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![RouteConfig {
            destination: "2001:db8::/64".parse().unwrap(),
            nexthop: "10.0.0.1".parse().unwrap(),
            vlan_id: None,
            rib_priority: None,
        }],
    )]);

    let current = mgd_routes(vec![], vec![]);

    let err = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect_err("plan should fail with mixed families");

    assert!(err.contains("mixed IP families"), "unexpected error: {err}",);

    logctx.cleanup_successful();
}

#[test]
fn plan_rejects_wrong_nexthop_family_from_mgd() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_rejects_wrong_nexthop_family_from_mgd",
    );
    let log = &logctx.log;

    let config = rack_config(vec![]);

    // mgd has a v4 prefix but a v6 nexthop.
    let current = mgd_routes(
        vec![("10.0.0.0/24", vec![mgd_path("2001:db8::1", 1, None)])],
        vec![],
    );

    let err = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect_err("plan should fail with wrong nexthop family");

    assert!(
        err.contains("invalid route fetched from mgd"),
        "unexpected error: {err}",
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_both_empty() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_both_empty");
    let log = &logctx.log;

    let config = rack_config(vec![]);
    let current = mgd_routes(vec![], vec![]);

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 0);
    assert!(plan.to_delete.is_empty());
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_v6_routes() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_v6_routes");
    let log = &logctx.log;

    // Desired: one v6 route.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![v6_route("2001:db8::/64", "2001:db8::1", None, Some(2))],
    )]);

    // mgd has a different v6 route.
    let current = mgd_routes(
        vec![],
        vec![("fd00::/48", vec![mgd_path("fd00::1", 1, None)])],
    );

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged_count, 0);
    assert_eq!(plan.to_delete.len(), 1);
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_routes_from_multiple_ports() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_routes_from_multiple_ports",
    );
    let log = &logctx.log;

    // Desired: two ports on our switch, each with a route.
    let config = rack_config(vec![
        port_config(
            SwitchSlot::Switch0,
            vec![v4_route("10.0.0.0/24", "10.0.0.1", None, None)],
        ),
        port_config(
            SwitchSlot::Switch0,
            vec![v4_route("10.1.0.0/24", "10.1.0.1", None, None)],
        ),
    ]);

    // mgd is empty.
    let current = mgd_routes(vec![], vec![]);

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // Both routes from both ports should be added.
    assert_eq!(plan.unchanged_count, 0);
    assert!(plan.to_delete.is_empty());
    assert_eq!(plan.to_add.len(), 2);

    logctx.cleanup_successful();
}
