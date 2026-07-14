// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use gateway_messages::SpPort;
use mg_admin_client::types::Path as MgdPath;
use omicron_test_utils::dev;
use proptest::prelude::Arbitrary;
use proptest::prelude::Just;
use proptest::prelude::any;
use proptest::prelude::proptest as proptest_macro;
use proptest::strategy::Strategy;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkIpNet;
use sled_agent_types::early_networking::UplinkPorts;
use std::collections::BTreeMap;
use test_strategy::proptest;
use tokio::task::block_in_place;

impl Arbitrary for DiffableStaticRouteDescription {
    type Parameters = ();
    type Strategy = proptest::prelude::BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        // mgd enforces the same requirements as `UplinkIpNet` for prefixes, so
        // we'll lean on its `Arbitrary` implementation to avoid invalid IPs
        // like loopback and ipv4-mapped ipv6 addrs
        //
        // We have to do some work here to generate a matching v4-v4 or v6-v6
        // nexthop address; that address currently has no requirements (although
        // it may eventually:
        // <https://github.com/oxidecomputer/maghemite/issues/738>), so we
        // generate any arbitrary nexthop that matches the protocol family.
        any::<UplinkIpNet>()
            .prop_flat_map(|prefix| {
                let prefix = IpNet::from(prefix);

                // ipv4 prefixes can have either ipv4 or ipv6 for its nexthop
                // address, but ipv6 prefixes must have an ipv6 nexthop
                let nexthop_strategy = match prefix {
                    IpNet::V4(_) => any::<IpAddr>().boxed(),
                    IpNet::V6(_) => {
                        any::<Ipv6Addr>().prop_map(IpAddr::from).boxed()
                    }
                };
                (Just(prefix), nexthop_strategy)
            })
            .prop_map(|(prefix, nexthop)| match (prefix, nexthop) {
                (IpNet::V4(prefix), IpAddr::V4(nexthop)) => {
                    Self::V4 { nexthop, prefix }
                }
                (IpNet::V4(prefix), IpAddr::V6(nexthop)) => {
                    Self::V4OverV6 { nexthop, prefix }
                }
                (IpNet::V6(prefix), IpAddr::V6(nexthop)) => {
                    Self::V6 { nexthop, prefix }
                }
                (IpNet::V6(_), IpAddr::V4(_)) => {
                    unreachable!("invalid v4/v6 combo in Arbitrary impl")
                }
            })
            .boxed()
    }
}

/// Build a minimal `PortConfig` on the given switch with the given routes.
fn port_config(switch: SwitchSlot, routes: Vec<RouteConfig>) -> PortConfig {
    PortConfig {
        routes,
        addresses: Vec::new(),
        switch,
        port: "qsfp0".to_string(),
        uplink_port_speed: LinkSpeed::Speed100G,
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
        ports: UplinkPorts::new(ports).unwrap(),
        bgp: Vec::new(),
        bfd: Vec::new(),
    }
}

fn make_route(
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
    let logctx = dev::test_setup_log("plan_all_unchanged");
    let log = &logctx.log;

    // Desired: one v4 route on Switch0.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![make_route("10.0.0.0/24", "10.0.0.1", None, None)],
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
    let logctx = dev::test_setup_log("plan_add_all");
    let log = &logctx.log;

    // Desired: two routes (one v4, one v6).
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![
            make_route("10.0.0.0/24", "10.0.0.1", None, Some(5)),
            make_route("2001:db8::/64", "2001:db8::1", None, Some(3)),
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
    let logctx = dev::test_setup_log("plan_delete_all");
    let log = &logctx.log;

    // Desired config: no ports for our switch.
    let config =
        rack_config(vec![port_config(SwitchSlot::Switch1, Vec::new())]);

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
    let logctx = dev::test_setup_log("plan_mix");
    let log = &logctx.log;

    // Desired: keep 10.0.0.0/24, add 10.1.0.0/24, remove 10.2.0.0/24.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![
            make_route("10.0.0.0/24", "10.0.0.1", None, None),
            make_route("10.1.0.0/24", "10.1.0.1", None, None),
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
    let logctx = dev::test_setup_log("plan_filters_other_switch_slot");
    let log = &logctx.log;

    // Desired config has routes on both Switch0 and Switch1. Our reconciler is
    // Switch0 (TEST_FAKE), so Switch1 routes should be ignored entirely.
    let config = rack_config(vec![
        port_config(
            SwitchSlot::Switch0,
            vec![make_route("10.0.0.0/24", "10.0.0.1", None, None)],
        ),
        port_config(
            SwitchSlot::Switch1,
            vec![make_route("10.1.0.0/24", "10.1.0.1", None, None)],
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
    let logctx = dev::test_setup_log("plan_default_rib_priority");
    let log = &logctx.log;

    // Desired route has rib_priority = None (should default to 1).
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![make_route("10.0.0.0/24", "10.0.0.1", None, None)],
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
    let logctx = dev::test_setup_log("plan_priority_change_is_delete_plus_add");
    let log = &logctx.log;

    // Desired: priority 5 for a route.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![make_route("10.0.0.0/24", "10.0.0.1", None, Some(5))],
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
    let logctx = dev::test_setup_log("plan_vlan_id_matters");
    let log = &logctx.log;

    // Desired: route with vlan_id = Some(100).
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![make_route("10.0.0.0/24", "10.0.0.1", Some(100), None)],
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
    let logctx = dev::test_setup_log("plan_multiple_nexthops_per_prefix");
    let log = &logctx.log;

    // Desired: two nexthops for the same prefix.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![
            make_route("10.0.0.0/24", "10.0.0.1", None, None),
            make_route("10.0.0.0/24", "10.0.0.2", None, None),
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
    let logctx = dev::test_setup_log("plan_rejects_bad_mgd_prefix");
    let log = &logctx.log;

    // Desired config: no ports for our switch.
    let config =
        rack_config(vec![port_config(SwitchSlot::Switch1, Vec::new())]);

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
fn plan_rejects_ipv6_prefix_with_ipv4_nexthop_in_config() {
    let logctx = dev::test_setup_log(
        "plan_rejects_ipv6_prefix_with_ipv4_nexthop_in_config",
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
    .expect_err("plan should fail");

    assert!(
        err.contains("ipv4 nexthop 10.0.0.1 for ipv6 prefix 2001:db8::/64"),
        "unexpected error: {err}",
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_rejects_ipv6_prefix_with_ipv4_nexthop_from_mgd() {
    let logctx = dev::test_setup_log(
        "plan_rejects_ipv6_prefix_with_ipv4_nexthop_from_mgd",
    );
    let log = &logctx.log;

    // Desired config: no ports for our switch.
    let config =
        rack_config(vec![port_config(SwitchSlot::Switch1, Vec::new())]);

    // mgd has a v6 prefix but a v4 nexthop.
    let current = mgd_routes(
        vec![("2001:db8::/64", vec![mgd_path("10.0.0.1", 1, None)])],
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
    let logctx = dev::test_setup_log("plan_both_empty");
    let log = &logctx.log;

    // Desired config: no ports for our switch.
    let config =
        rack_config(vec![port_config(SwitchSlot::Switch1, Vec::new())]);

    // Current settings: no ports for our switch.
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
    let logctx = dev::test_setup_log("plan_v6_routes");
    let log = &logctx.log;

    // Desired: one v6 route.
    let config = rack_config(vec![port_config(
        SwitchSlot::Switch0,
        vec![make_route("2001:db8::/64", "2001:db8::1", None, Some(2))],
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
    let logctx = dev::test_setup_log("plan_routes_from_multiple_ports");
    let log = &logctx.log;

    // Desired: two ports on our switch, each with a route.
    let config = rack_config(vec![
        port_config(
            SwitchSlot::Switch0,
            vec![make_route("10.0.0.0/24", "10.0.0.1", None, None)],
        ),
        port_config(
            SwitchSlot::Switch0,
            vec![make_route("10.1.0.0/24", "10.1.0.1", None, None)],
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

#[derive(Debug, Clone, Copy, test_strategy::Arbitrary)]
enum StaticRouteTestInput {
    // route only exists in mgd
    MgdOnly,

    // route only exists in desired config for switch 0 or switch 1
    DesiredConfigOnly(SwitchSlot),

    // route exists in mgd and our desired switch 0 config
    MgdAndSwitch0,

    // route exists in mgd and our desired switch 0 config, but we want to
    // change the priority
    //
    // The `u8` here is the new priority we want in the desired config; the
    // prior mgd priority will come from our parent `StaticRouteTestValue`. They
    // may randomly be the same in some proptest runs.
    MgdAndSwitch0NewPriority(u8),
}

// mgd's API for describing static routes is looser than it should be; we'll
// generate a map using keys of the tuple (prefix, nexthop, vlan_id). This is
// more consistent with what mgd does internally:
// <https://github.com/oxidecomputer/maghemite/issues/739>.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, test_strategy::Arbitrary,
)]
struct StaticRouteTestKey {
    description: DiffableStaticRouteDescription,
    vlan_id: Option<u16>,
}

#[derive(Debug, Clone, Copy, test_strategy::Arbitrary)]
struct StaticRouteTestValue {
    input: StaticRouteTestInput,
    priority: u8,
}

impl StaticRouteTestKey {
    fn to_route(&self, priority: u8) -> DiffableStaticRoute {
        DiffableStaticRoute {
            description: self.description,
            vlan_id: self.vlan_id,
            priority,
        }
    }
}

#[derive(Debug, Clone, test_strategy::Arbitrary)]
struct TestInput {
    routes: BTreeMap<StaticRouteTestKey, StaticRouteTestValue>,
}

impl From<&'_ DiffableStaticRoute> for RouteConfig {
    fn from(value: &'_ DiffableStaticRoute) -> Self {
        let destination = match value.description {
            DiffableStaticRouteDescription::V4 { prefix, .. }
            | DiffableStaticRouteDescription::V4OverV6 { prefix, .. } => {
                IpNet::V4(prefix)
            }
            DiffableStaticRouteDescription::V6 { prefix, .. } => {
                IpNet::V6(prefix)
            }
        };
        let nexthop = match value.description {
            DiffableStaticRouteDescription::V4 { nexthop, .. } => {
                IpAddr::V4(nexthop)
            }
            DiffableStaticRouteDescription::V4OverV6 { nexthop, .. }
            | DiffableStaticRouteDescription::V6 { nexthop, .. } => {
                IpAddr::V6(nexthop)
            }
        };
        Self {
            destination,
            nexthop,
            vlan_id: value.vlan_id,
            rib_priority: Some(value.priority),
        }
    }
}

impl TestInput {
    fn initial_mgd_routes(&self) -> MgdCurrentRoutes {
        Self::routes_from_iter(
            self.routes
                .iter()
                .filter(|(_, val)| match val.input {
                    StaticRouteTestInput::MgdOnly
                    | StaticRouteTestInput::MgdAndSwitch0
                    | StaticRouteTestInput::MgdAndSwitch0NewPriority(_) => true,
                    StaticRouteTestInput::DesiredConfigOnly(_) => false,
                })
                .map(|(key, val)| key.to_route(val.priority)),
        )
    }

    fn post_reconciliation_mgd_routes(&self) -> MgdCurrentRoutes {
        Self::routes_from_iter(self.routes.iter().filter_map(|(key, val)| {
            match val.input {
                StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch1,
                )
                | StaticRouteTestInput::MgdOnly => None,

                StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch0,
                )
                | StaticRouteTestInput::MgdAndSwitch0 => {
                    Some(key.to_route(val.priority))
                }
                StaticRouteTestInput::MgdAndSwitch0NewPriority(priority) => {
                    Some(key.to_route(priority))
                }
            }
        }))
    }

    fn routes_from_iter(
        mgd_routes: impl Iterator<Item = DiffableStaticRoute>,
    ) -> MgdCurrentRoutes {
        let mut v4: HashMap<String, Vec<MgdPath>> = HashMap::new();
        let mut v6: HashMap<String, Vec<MgdPath>> = HashMap::new();
        for route in mgd_routes {
            match route.description {
                DiffableStaticRouteDescription::V4 { nexthop, prefix } => {
                    v4.entry(prefix.to_string()).or_default().push(MgdPath {
                        bgp: None,
                        nexthop: IpAddr::V4(nexthop),
                        nexthop_interface: None,
                        rib_priority: route.priority,
                        shutdown: false,
                        vlan_id: route.vlan_id,
                    });
                }
                DiffableStaticRouteDescription::V4OverV6 {
                    nexthop,
                    prefix,
                } => {
                    v4.entry(prefix.to_string()).or_default().push(MgdPath {
                        bgp: None,
                        nexthop: IpAddr::V6(nexthop),
                        nexthop_interface: None,
                        rib_priority: route.priority,
                        shutdown: false,
                        vlan_id: route.vlan_id,
                    });
                }
                DiffableStaticRouteDescription::V6 { nexthop, prefix } => {
                    v6.entry(prefix.to_string()).or_default().push(MgdPath {
                        bgp: None,
                        nexthop: IpAddr::V6(nexthop),
                        nexthop_interface: None,
                        rib_priority: route.priority,
                        shutdown: false,
                        vlan_id: route.vlan_id,
                    });
                }
            }
        }

        MgdCurrentRoutes { v4, v6 }
    }

    fn desired_rack_config(&self) -> RackNetworkConfig {
        let switch0 =
            self.routes.iter().filter_map(|(key, val)| match val.input {
                StaticRouteTestInput::MgdOnly
                | StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch1,
                ) => None,
                StaticRouteTestInput::MgdAndSwitch0
                | StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch0,
                ) => Some(RouteConfig::from(&key.to_route(val.priority))),
                StaticRouteTestInput::MgdAndSwitch0NewPriority(priority) => {
                    Some(RouteConfig::from(&key.to_route(priority)))
                }
            });
        let switch1 =
            self.routes.iter().filter_map(|(key, val)| match val.input {
                StaticRouteTestInput::MgdOnly
                | StaticRouteTestInput::MgdAndSwitch0
                | StaticRouteTestInput::MgdAndSwitch0NewPriority(_)
                | StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch0,
                ) => None,
                StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch1,
                ) => Some(RouteConfig::from(&key.to_route(val.priority))),
            });
        rack_config(vec![
            port_config(SwitchSlot::Switch0, switch0.collect()),
            port_config(SwitchSlot::Switch1, switch1.collect()),
        ])
    }

    fn expected_unchanged_count(&self) -> usize {
        self.routes
            .values()
            .filter(|val| match val.input {
                StaticRouteTestInput::MgdOnly
                | StaticRouteTestInput::DesiredConfigOnly(_) => false,
                StaticRouteTestInput::MgdAndSwitch0 => true,
                StaticRouteTestInput::MgdAndSwitch0NewPriority(priority) => {
                    val.priority == priority
                }
            })
            .count()
    }

    fn expected_to_add(&self) -> BTreeSet<DiffableStaticRoute> {
        self.routes
            .iter()
            .filter_map(|(key, val)| match val.input {
                StaticRouteTestInput::MgdOnly
                | StaticRouteTestInput::MgdAndSwitch0
                | StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch1,
                ) => None,
                StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch0,
                ) => Some(key.to_route(val.priority)),
                StaticRouteTestInput::MgdAndSwitch0NewPriority(priority) => {
                    if priority != val.priority {
                        Some(key.to_route(priority))
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    fn expected_to_delete(&self) -> BTreeSet<DiffableStaticRoute> {
        self.routes
            .iter()
            .filter_map(|(key, val)| match val.input {
                StaticRouteTestInput::MgdAndSwitch0
                | StaticRouteTestInput::DesiredConfigOnly(_) => None,
                StaticRouteTestInput::MgdOnly => {
                    Some(key.to_route(val.priority))
                }
                StaticRouteTestInput::MgdAndSwitch0NewPriority(priority) => {
                    if priority != val.priority {
                        Some(key.to_route(val.priority))
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    fn expected_reconciliation_status(&self) -> MgdStaticRouteReconcilerStatus {
        let mut unchanged = 0;
        let mut deleted_v4 = 0;
        let mut deleted_v6 = 0;
        let mut added_v4 = 0;
        let mut added_v6 = 0;

        for (key, val) in &self.routes {
            match val.input {
                StaticRouteTestInput::MgdOnly => match key.description {
                    DiffableStaticRouteDescription::V4 { .. }
                    | DiffableStaticRouteDescription::V4OverV6 { .. } => {
                        deleted_v4 += 1;
                    }
                    DiffableStaticRouteDescription::V6 { .. } => {
                        deleted_v6 += 1;
                    }
                },
                StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch0,
                ) => match key.description {
                    DiffableStaticRouteDescription::V4 { .. }
                    | DiffableStaticRouteDescription::V4OverV6 { .. } => {
                        added_v4 += 1;
                    }
                    DiffableStaticRouteDescription::V6 { .. } => {
                        added_v6 += 1;
                    }
                },
                StaticRouteTestInput::MgdAndSwitch0NewPriority(priority) => {
                    if priority == val.priority {
                        unchanged += 1;
                    } else {
                        match key.description {
                            DiffableStaticRouteDescription::V4 { .. }
                            | DiffableStaticRouteDescription::V4OverV6 {
                                ..
                            } => {
                                deleted_v4 += 1;
                                added_v4 += 1;
                            }
                            DiffableStaticRouteDescription::V6 { .. } => {
                                deleted_v6 += 1;
                                added_v6 += 1;
                            }
                        }
                    }
                }
                StaticRouteTestInput::DesiredConfigOnly(
                    SwitchSlot::Switch1,
                ) => (),
                StaticRouteTestInput::MgdAndSwitch0 => {
                    unchanged += 1;
                }
            }
        }

        MgdStaticRouteReconcilerStatus::Success {
            unchanged,
            deleted_v4,
            deleted_v6,
            added_v4,
            added_v6,
        }
    }
}

#[proptest]
fn proptest_plan(input: TestInput) {
    let logctx = dev::test_setup_log("proptest_plan");
    let log = &logctx.log;

    let plan = ReconciliationPlan::new(
        input.initial_mgd_routes(),
        &input.desired_rack_config(),
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    let ReconciliationPlan { unchanged_count, to_delete, to_add } = plan;

    assert_eq!(unchanged_count, input.expected_unchanged_count());
    assert_eq!(to_delete, input.expected_to_delete(), "incorrect to_delete");
    assert_eq!(to_add, input.expected_to_add(), "incorrect to_add");

    logctx.cleanup_successful();
}

struct MgdStaticRouteLists {
    v4: MgdStaticRoute4List,
    v6: MgdStaticRoute6List,
}

impl From<MgdCurrentRoutes> for MgdStaticRouteLists {
    fn from(value: MgdCurrentRoutes) -> Self {
        Self {
            v4: MgdStaticRoute4List {
                list: value
                    .v4
                    .into_iter()
                    .flat_map(|(prefix, paths)| {
                        let prefix =
                            prefix.parse::<Ipv4Net>().expect("valid prefix");
                        paths.into_iter().map(move |path| MgdStaticRoute4 {
                            nexthop: path.nexthop,
                            prefix,
                            rib_priority: path.rib_priority,
                            vlan_id: path.vlan_id,
                        })
                    })
                    .collect(),
            },
            v6: MgdStaticRoute6List {
                list: value
                    .v6
                    .into_iter()
                    .flat_map(|(prefix, paths)| {
                        let prefix =
                            prefix.parse::<Ipv6Net>().expect("valid prefix");
                        paths.into_iter().map(move |path| {
                            let nexthop = match path.nexthop {
                                IpAddr::V6(ip) => ip,
                                IpAddr::V4(_) => {
                                    panic!("invalid path: v6 with v4 nexthop")
                                }
                            };
                            MgdStaticRoute6 {
                                nexthop,
                                prefix,
                                rib_priority: path.rib_priority,
                                vlan_id: path.vlan_id,
                            }
                        })
                    })
                    .collect(),
            },
        }
    }
}

async fn remove_all_current_routes(
    client: &Client,
    current_routes: MgdCurrentRoutes,
) {
    let MgdStaticRouteLists { v4, v6 } = current_routes.into();

    client
        .static_remove_v4_route(&MgdDeleteStaticRoute4Request { routes: v4 })
        .await
        .expect("removed v4 routes");
    client
        .static_remove_v6_route(&MgdDeleteStaticRoute6Request { routes: v6 })
        .await
        .expect("removed v6 routes");
}

async fn create_initial_routes(
    client: &Client,
    initial_routes: MgdCurrentRoutes,
) {
    let MgdStaticRouteLists { v4, v6 } = initial_routes.into();

    client
        .static_add_v4_route(&MgdAddStaticRoute4Request { routes: v4 })
        .await
        .expect("added v4 routes");
    client
        .static_add_v6_route(&MgdAddStaticRoute6Request { routes: v6 })
        .await
        .expect("added v6 routes");
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ComparableMgdPath<'a> {
    nexthop: &'a IpAddr,
    nexthop_interface: &'a Option<String>,
    rib_priority: &'a u8,
    shutdown: &'a bool,
    vlan_id: &'a Option<u16>,
}

impl<'a> From<&'a MgdPath> for ComparableMgdPath<'a> {
    fn from(value: &'a MgdPath) -> Self {
        let MgdPath {
            bgp,
            nexthop,
            nexthop_interface,
            rib_priority,
            shutdown,
            vlan_id,
        } = value;
        assert!(bgp.is_none(), "static route tests never use BGP");

        Self { nexthop, nexthop_interface, rib_priority, shutdown, vlan_id }
    }
}

fn assert_routes_eq(
    actual: MgdCurrentRoutes,
    expected: MgdCurrentRoutes,
    description: &str,
) {
    // convert to BTree{Map,Set} for nicer output from assert_eq!() on failure
    #[derive(Debug, PartialEq, Eq)]
    struct BTreeMgdCurrentRoutes<'a> {
        v4: BTreeMap<&'a str, BTreeSet<ComparableMgdPath<'a>>>,
        v6: BTreeMap<&'a str, BTreeSet<ComparableMgdPath<'a>>>,
    }

    impl<'a> From<&'a MgdCurrentRoutes> for BTreeMgdCurrentRoutes<'a> {
        fn from(routes: &'a MgdCurrentRoutes) -> Self {
            fn convert_map<'b>(
                input: &'b HashMap<String, Vec<MgdPath>>,
            ) -> BTreeMap<&'b str, BTreeSet<ComparableMgdPath<'b>>>
            {
                let mut output: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
                for (k, paths) in input {
                    let paths = paths.iter().map(ComparableMgdPath::from);
                    output.insert(k.as_str(), paths.collect());
                }
                output
            }

            Self { v4: convert_map(&routes.v4), v6: convert_map(&routes.v6) }
        }
    }

    let actual = BTreeMgdCurrentRoutes::from(&actual);
    let expected = BTreeMgdCurrentRoutes::from(&expected);
    eprintln!("--- checking {description} ---");
    assert_eq!(actual, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn proptest_full_reconciliation() {
    let logctx = dev::test_setup_log("proptest_full_reconciliation");
    let mgsctx = gateway_test_utils::setup::test_setup(
        "proptest_full_reconciliation",
        SpPort::One,
    )
    .await;
    let mut mgdctx =
        dev::maghemite::MgdInstance::start(0, mgsctx.address().into())
            .await
            .expect("started mgd");
    let client = Client::new(
        &format!("http://{}", mgdctx.address()),
        logctx.log.clone(),
    );
    let rt = tokio::runtime::Handle::current();

    let one_test_invocation = async |input: TestInput| {
        // Clear all routes from a previous proptest invocation.
        let current_routes =
            MgdCurrentRoutes::fetch(&client).await.expect("fetched all routes");
        remove_all_current_routes(&client, current_routes).await;
        let current_routes =
            MgdCurrentRoutes::fetch(&client).await.expect("fetched all routes");
        assert_routes_eq(
            current_routes,
            MgdCurrentRoutes::default(),
            "cleared all routes",
        );

        // Apply all initial settings.
        create_initial_routes(&client, input.initial_mgd_routes()).await;
        let current_routes =
            MgdCurrentRoutes::fetch(&client).await.expect("fetched all routes");
        assert_routes_eq(
            current_routes,
            input.initial_mgd_routes(),
            "initial setup mismatch",
        );

        // Perform reconciliation.
        let status = reconcile(
            &client,
            &input.desired_rack_config(),
            ThisSledSwitchSlot::TEST_FAKE,
            &logctx.log,
        )
        .await;

        // Check reported status and mgd state
        assert_eq!(status, input.expected_reconciliation_status());
        let current_routes =
            MgdCurrentRoutes::fetch(&client).await.expect("fetched all routes");
        assert_routes_eq(
            current_routes,
            input.post_reconciliation_mgd_routes(),
            "post-reconciliation mismatch",
        );
    };

    proptest_macro!(|(input: TestInput)| {
        // Do a little dance to call our async `one_test_invocation` within the
        // non-async `proptest_macro!()` context.
        block_in_place(|| rt.block_on(one_test_invocation(input)));
    });

    mgdctx.cleanup().await.expect("mgd cleanup succeeded");
    mgsctx.teardown().await;
    logctx.cleanup_successful();
}
