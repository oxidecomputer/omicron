// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use dpd_client::types::LinkCreate as DpdLinkCreate;
use dpd_client::types::LinkId as DpdLinkId;
use dpd_client::types::LinkSettings as DpdLinkSettings;
use dpd_client::types::PortFec as DpdPortFec;
use dpd_client::types::PortSpeed as DpdPortSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::PortFec;
use sled_agent_types::early_networking::PortSpeed;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use sled_agent_types::early_networking::UplinkIpNet;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;

use crate::switch_zone_slot::ThisSledSwitchSlot;

/// Build a minimal `PortConfig` for a single port.
fn port_config(
    port: &DpdQsfp,
    switch: SwitchSlot,
    speed: PortSpeed,
    fec: Option<PortFec>,
    autoneg: bool,
    addrs: &[UplinkIpNet],
) -> PortConfig {
    PortConfig {
        routes: Vec::new(),
        addresses: addrs
            .iter()
            .copied()
            .map(|ip_net| UplinkAddressConfig {
                address: UplinkAddress::Static { ip_net },
                vlan_id: None,
            })
            .collect(),
        switch,
        port: port.to_string(),
        uplink_port_speed: speed,
        uplink_port_fec: fec,
        bgp_peers: Vec::new(),
        autoneg,
        lldp: None,
        tx_eq: None,
    }
}

/// Build `DpdPortSettings` with a single link (the only layout we support).
fn dpd_port_settings(
    speed: DpdPortSpeed,
    fec: Option<DpdPortFec>,
    autoneg: bool,
    addrs: Vec<IpAddr>,
) -> DpdPortSettings {
    let mut links = HashMap::new();
    let link_id = DpdLinkId(0);
    links.insert(
        link_id.to_string(),
        DpdLinkSettings {
            addrs,
            params: DpdLinkCreate {
                autoneg,
                fec,
                kr: false,
                lane: Some(link_id),
                speed,
                tx_eq: None,
            },
        },
    );
    DpdPortSettings { links }
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

#[test]
fn plan_all_unchanged() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_all_unchanged");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let addr: IpAddr = "10.0.0.1".parse().unwrap();
    let ip_net: UplinkIpNet = format!("{addr}/24").parse().unwrap();

    // Desired config: one port on Switch0.
    let config = rack_config(vec![port_config(
        &qsfp0,
        SwitchSlot::Switch0,
        PortSpeed::Speed100G,
        Some(PortFec::Rs),
        true,
        &[ip_net],
    )]);

    // Current dpd state matches exactly.
    let dpd_current = BTreeMap::from([(
        qsfp0.clone(),
        dpd_port_settings(
            DpdPortSpeed::Speed100G,
            Some(DpdPortFec::Rs),
            true,
            vec![addr],
        ),
    )]);

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged, BTreeSet::from([qsfp0]));
    assert!(plan.to_clear.is_empty());
    assert!(plan.to_create.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_create_all() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_create_all");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();

    // Desired config: two ports on Switch0.
    let config = rack_config(vec![
        port_config(
            &qsfp0,
            SwitchSlot::Switch0,
            PortSpeed::Speed100G,
            Some(PortFec::Rs),
            true,
            &["10.0.0.1/24".parse().unwrap()],
        ),
        port_config(
            &qsfp1,
            SwitchSlot::Switch0,
            PortSpeed::Speed25G,
            None,
            false,
            &["10.0.0.2/24".parse().unwrap()],
        ),
    ]);

    // dpd has no settings at all.
    let dpd_current = BTreeMap::new();

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert!(plan.unchanged.is_empty());
    assert!(plan.to_clear.is_empty());
    assert_eq!(plan.to_create.keys().collect::<Vec<_>>(), vec![&qsfp0, &qsfp1]);

    logctx.cleanup_successful();
}

#[test]
fn plan_clear_all() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_clear_all");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();

    // Desired config: no ports.
    let config = rack_config(vec![]);

    // dpd has settings for two ports.
    let dpd_current = BTreeMap::from([
        (
            qsfp0.clone(),
            dpd_port_settings(
                DpdPortSpeed::Speed100G,
                Some(DpdPortFec::Rs),
                true,
                vec!["10.0.0.1".parse().unwrap()],
            ),
        ),
        (
            qsfp1.clone(),
            dpd_port_settings(
                DpdPortSpeed::Speed25G,
                None,
                false,
                vec!["10.0.0.2".parse().unwrap()],
            ),
        ),
    ]);

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert!(plan.unchanged.is_empty());
    assert_eq!(plan.to_clear, BTreeSet::from([qsfp0, qsfp1]));
    assert!(plan.to_create.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_mix() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_mix");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();
    let qsfp2: DpdQsfp = "qsfp2".parse().unwrap();
    let qsfp3: DpdQsfp = "qsfp3".parse().unwrap();
    let ip0: IpAddr = "10.0.0.1".parse().unwrap();
    let ip1: IpAddr = "10.0.0.2".parse().unwrap();
    let ip2: IpAddr = "10.0.0.3".parse().unwrap();
    let ip3: IpAddr = "10.0.0.4".parse().unwrap();
    let ip_net0: UplinkIpNet = format!("{ip0}/24").parse().unwrap();
    let ip_net1: UplinkIpNet = format!("{ip1}/24").parse().unwrap();
    let ip_net3: UplinkIpNet = format!("{ip3}/24").parse().unwrap();

    // Desired config: qsfp0 unchanged, qsfp1 changed speed, qsfp3 new.
    // qsfp2 should be cleared (exists in dpd but not in desired config).
    let config = rack_config(vec![
        port_config(
            &qsfp0,
            SwitchSlot::Switch0,
            PortSpeed::Speed100G,
            Some(PortFec::Rs),
            true,
            &[ip_net0],
        ),
        port_config(
            &qsfp1,
            SwitchSlot::Switch0,
            PortSpeed::Speed100G,
            None,
            false,
            &[ip_net1],
        ),
        port_config(
            &qsfp3,
            SwitchSlot::Switch0,
            PortSpeed::Speed50G,
            None,
            true,
            &[ip_net3],
        ),
    ]);

    let dpd_current = BTreeMap::from([
        (
            qsfp0.clone(),
            dpd_port_settings(
                DpdPortSpeed::Speed100G,
                Some(DpdPortFec::Rs),
                true,
                vec![ip0],
            ),
        ),
        (
            qsfp1.clone(),
            dpd_port_settings(DpdPortSpeed::Speed25G, None, false, vec![ip1]),
        ),
        (
            qsfp2.clone(),
            dpd_port_settings(DpdPortSpeed::Speed10G, None, false, vec![ip2]),
        ),
    ]);

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // qsfp0: unchanged
    assert_eq!(plan.unchanged, BTreeSet::from([qsfp0]));
    // qsfp2: in dpd but not desired
    assert_eq!(plan.to_clear, BTreeSet::from([qsfp2]));
    // qsfp1: changed, qsfp3: new
    assert_eq!(
        plan.to_create.keys().collect::<BTreeSet<_>>(),
        BTreeSet::from([&qsfp1, &qsfp3]),
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_filters_other_switch_slot() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_filters_other_switch_slot",
    );
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();

    // Desired config has ports on both Switch0 and Switch1. Our reconciler is
    // Switch0 (TEST_FAKE), so Switch1 ports should be ignored entirely.
    let config = rack_config(vec![
        port_config(
            &qsfp0,
            SwitchSlot::Switch0,
            PortSpeed::Speed100G,
            None,
            true,
            &["10.0.0.1/24".parse().unwrap()],
        ),
        port_config(
            &qsfp1,
            SwitchSlot::Switch1, // different switch
            PortSpeed::Speed25G,
            None,
            false,
            &["10.0.0.2/24".parse().unwrap()],
        ),
    ]);

    // dpd is empty.
    let dpd_current = BTreeMap::new();

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    // Only qsfp0 should appear; qsfp1 is on the wrong switch.
    assert!(plan.unchanged.is_empty());
    assert!(plan.to_clear.is_empty());
    assert_eq!(plan.to_create.keys().collect::<Vec<_>>(), vec![&qsfp0],);

    logctx.cleanup_successful();
}

#[test]
fn plan_link_local_addrs_ignored_from_dpd() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_link_local_addrs_ignored_from_dpd",
    );
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let addr: IpAddr = "10.0.0.1".parse().unwrap();
    let ip_net: UplinkIpNet = format!("{addr}/24").parse().unwrap();
    let link_local: IpAddr = "fe80::1".parse().unwrap();

    // Desired config: one port with one address.
    let config = rack_config(vec![port_config(
        &qsfp0,
        SwitchSlot::Switch0,
        PortSpeed::Speed100G,
        None,
        true,
        &[ip_net],
    )]);

    // dpd has the same config but also reports a link-local address.
    // The reconciler should ignore the link-local and see no diff.
    let dpd_current = BTreeMap::from([(
        qsfp0.clone(),
        dpd_port_settings(
            DpdPortSpeed::Speed100G,
            None,
            true,
            vec![addr, link_local],
        ),
    )]);

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    assert_eq!(plan.unchanged, BTreeSet::from([qsfp0]));
    assert!(plan.to_clear.is_empty());
    assert!(plan.to_create.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_rejects_multi_link_dpd_port() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_rejects_multi_link_dpd_port",
    );
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let config = rack_config(vec![]);

    // Build DpdPortSettings with two links -- this is not representable in
    // RackNetworkConfig and should cause plan generation to fail.
    let mut links = HashMap::new();
    let link0 = DpdLinkId(0);
    let link1 = DpdLinkId(1);
    let link_params = DpdLinkCreate {
        autoneg: true,
        fec: None,
        kr: false,
        lane: Some(link0),
        speed: DpdPortSpeed::Speed100G,
        tx_eq: None,
    };
    links.insert(
        link0.to_string(),
        DpdLinkSettings {
            addrs: vec!["10.0.0.1".parse().unwrap()],
            params: link_params.clone(),
        },
    );
    links.insert(
        link1.to_string(),
        DpdLinkSettings {
            addrs: vec!["10.0.0.2".parse().unwrap()],
            params: link_params,
        },
    );
    let dpd_current = BTreeMap::from([(qsfp0, DpdPortSettings { links })]);

    let err = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect_err("plan should fail with multi-link port");

    assert!(err.contains("expected exactly 1 link"), "unexpected error: {err}",);

    logctx.cleanup_successful();
}

#[test]
fn plan_created_settings_round_trip_correctly() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_created_settings_round_trip_correctly",
    );
    let log = &logctx.log;

    let qsfp5: DpdQsfp = "qsfp5".parse().unwrap();
    let ip1: IpAddr = "10.0.0.1".parse().unwrap();
    let ip2: IpAddr = "10.0.0.2".parse().unwrap();
    let ip_net1: UplinkIpNet = format!("{ip1}/24").parse().unwrap();
    let ip_net2: UplinkIpNet = format!("{ip2}/24").parse().unwrap();

    // Desired config with various settings populated.
    let config = rack_config(vec![port_config(
        &qsfp5,
        SwitchSlot::Switch0,
        PortSpeed::Speed100G,
        Some(PortFec::Rs),
        true,
        &[ip_net1, ip_net2],
    )]);

    // dpd is empty, so qsfp5 should be in `to_create`.
    let dpd_current = BTreeMap::new();

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    let created = plan.to_create.get(&qsfp5).expect("qsfp5 should exist");

    // Verify the DpdPortSettings matches what we'd expect.
    assert_eq!(created.links.len(), 1);
    let link = created.links.values().next().unwrap();
    assert_eq!(link.params.speed, DpdPortSpeed::Speed100G);
    assert_eq!(link.params.fec, Some(DpdPortFec::Rs));
    assert!(link.params.autoneg);
    assert_eq!(
        link.addrs.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([ip1, ip2]),
    );

    logctx.cleanup_successful();
}
