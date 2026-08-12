// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use dpd_client::types::LinkCreate as DpdLinkCreate;
use dpd_client::types::LinkId as DpdLinkId;
use dpd_client::types::LinkSettings as DpdLinkSettings;
use dpd_client::types::PortFec as DpdPortFec;
use dpd_client::types::PortSpeed as DpdPortSpeed;
use gateway_messages::SpPort;
use omicron_test_utils::dev;
use proptest::prelude::proptest as proptest_macro;
use proptest::strategy::Strategy;
use sled_agent_types::early_networking::LinkFec;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use sled_agent_types::early_networking::UplinkIpNet;
use sled_agent_types::early_networking::UplinkPorts;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use test_strategy::Arbitrary;
use test_strategy::proptest;
use tokio::sync::Mutex;
use tokio::task::block_in_place;

use crate::switch_zone_slot::ThisSledSwitchSlot;

/// Build a minimal `PortConfig` for a single port.
fn port_config(
    port: &DpdQsfp,
    switch: SwitchSlot,
    speed: LinkSpeed,
    fec: Option<LinkFec>,
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
        ports: UplinkPorts::new(ports).unwrap(),
        bgp: Vec::new(),
        bfd: Vec::new(),
    }
}

#[test]
fn plan_all_unchanged() {
    let logctx = dev::test_setup_log("plan_all_unchanged");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let addr: IpAddr = "10.0.0.1".parse().unwrap();
    let ip_net: UplinkIpNet = format!("{addr}/24").parse().unwrap();

    // Desired config: one port on Switch0.
    let config = rack_config(vec![port_config(
        &qsfp0,
        SwitchSlot::Switch0,
        LinkSpeed::Speed100G,
        Some(LinkFec::Rs),
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

    assert_eq!(plan.unchanged, BTreeSet::from([qsfp0.to_string()]));
    assert!(plan.to_clear.is_empty());
    assert!(plan.to_apply.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_create_all() {
    let logctx = dev::test_setup_log("plan_create_all");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();

    // Desired config: two ports on Switch0.
    let config = rack_config(vec![
        port_config(
            &qsfp0,
            SwitchSlot::Switch0,
            LinkSpeed::Speed100G,
            Some(LinkFec::Rs),
            true,
            &["10.0.0.1/24".parse().unwrap()],
        ),
        port_config(
            &qsfp1,
            SwitchSlot::Switch0,
            LinkSpeed::Speed25G,
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
    assert_eq!(plan.to_apply.keys().collect::<Vec<_>>(), vec![&qsfp0, &qsfp1]);

    logctx.cleanup_successful();
}

#[test]
fn plan_clear_all() {
    let logctx = dev::test_setup_log("plan_clear_all");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();

    // Desired config: no ports for our switch.
    let config = rack_config(vec![port_config(
        &qsfp0,
        SwitchSlot::Switch1,
        LinkSpeed::Speed100G,
        Some(LinkFec::Rs),
        true,
        &[],
    )]);

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
    assert!(plan.to_apply.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_mix() {
    let logctx = dev::test_setup_log("plan_mix");
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
            LinkSpeed::Speed100G,
            Some(LinkFec::Rs),
            true,
            &[ip_net0],
        ),
        port_config(
            &qsfp1,
            SwitchSlot::Switch0,
            LinkSpeed::Speed100G,
            None,
            false,
            &[ip_net1],
        ),
        port_config(
            &qsfp3,
            SwitchSlot::Switch0,
            LinkSpeed::Speed50G,
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
    assert_eq!(plan.unchanged, BTreeSet::from([qsfp0.to_string()]));
    // qsfp2: in dpd but not desired
    assert_eq!(plan.to_clear, BTreeSet::from([qsfp2]));
    // qsfp1: changed, qsfp3: new
    assert_eq!(
        plan.to_apply.keys().collect::<BTreeSet<_>>(),
        BTreeSet::from([&qsfp1, &qsfp3]),
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_filters_other_switch_slot() {
    let logctx = dev::test_setup_log("plan_filters_other_switch_slot");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let qsfp1: DpdQsfp = "qsfp1".parse().unwrap();

    // Desired config has ports on both Switch0 and Switch1. Our reconciler is
    // Switch0 (TEST_FAKE), so Switch1 ports should be ignored entirely.
    let config = rack_config(vec![
        port_config(
            &qsfp0,
            SwitchSlot::Switch0,
            LinkSpeed::Speed100G,
            None,
            true,
            &["10.0.0.1/24".parse().unwrap()],
        ),
        port_config(
            &qsfp1,
            SwitchSlot::Switch1, // different switch
            LinkSpeed::Speed25G,
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
    assert_eq!(plan.to_apply.keys().collect::<Vec<_>>(), vec![&qsfp0],);

    logctx.cleanup_successful();
}

#[test]
fn plan_link_local_addrs_ignored_from_dpd() {
    let logctx = dev::test_setup_log("plan_link_local_addrs_ignored_from_dpd");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    let addr: IpAddr = "10.0.0.1".parse().unwrap();
    let ip_net: UplinkIpNet = format!("{addr}/24").parse().unwrap();
    let link_local: IpAddr = "fe80::1".parse().unwrap();

    // Desired config: one port with one address.
    let config = rack_config(vec![port_config(
        &qsfp0,
        SwitchSlot::Switch0,
        LinkSpeed::Speed100G,
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

    assert_eq!(plan.unchanged, BTreeSet::from([qsfp0.to_string()]));
    assert!(plan.to_clear.is_empty());
    assert!(plan.to_apply.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_rejects_multi_link_dpd_port() {
    let logctx = dev::test_setup_log("plan_rejects_multi_link_dpd_port");
    let log = &logctx.log;

    let qsfp0: DpdQsfp = "qsfp0".parse().unwrap();
    // Desired config: no ports for our switch.
    let config = rack_config(vec![port_config(
        &qsfp0,
        SwitchSlot::Switch1,
        LinkSpeed::Speed100G,
        Some(LinkFec::Rs),
        true,
        &[],
    )]);

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
    let logctx =
        dev::test_setup_log("plan_created_settings_round_trip_correctly");
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
        LinkSpeed::Speed100G,
        Some(LinkFec::Rs),
        true,
        &[ip_net1, ip_net2],
    )]);

    // dpd is empty, so qsfp5 should be in `to_apply`.
    let dpd_current = BTreeMap::new();

    let plan = ReconciliationPlan::new(
        dpd_current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    let created = plan.to_apply.get(&qsfp5).expect("qsfp5 should exist");

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

#[derive(Debug, Clone, PartialEq, Eq, Arbitrary)]
struct ArbitraryPortSettings {
    autoneg: bool,
    tx_eq: Option<TxEqConfig>,
    fec: Option<LinkFec>,
    speed: LinkSpeed,
    #[strategy(proptest::collection::btree_set(
        proptest::arbitrary::any::<UplinkAddressConfig>(),
        0..=4,
    ))]
    addrs: BTreeSet<UplinkAddressConfig>,
}

impl ArbitraryPortSettings {
    fn to_dpd_settings(&self, port_id: &PortId) -> DpdPortSettings {
        DpdPortSettings::from(&DiffablePortSettings {
            port_id: port_id.0.clone(),
            autoneg: self.autoneg,
            tx_eq: self.tx_eq,
            fec: self.fec,
            speed: self.speed,
            addrs: self
                .addrs
                .iter()
                .filter_map(|addr| match addr.address {
                    UplinkAddress::AddrConf => None,
                    UplinkAddress::Static { ip_net } => Some(ip_net.addr()),
                })
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
enum SwitchPortSettingsTestInput {
    // port only exists in dpd
    DpdOnly(ArbitraryPortSettings),

    // port only exists in desired config for switch 0 or switch 1
    DesiredSwitch0(ArbitraryPortSettings),
    DesiredSwitch1(ArbitraryPortSettings),

    // port exists in dpd and desired switch 0 with the same settings
    DpdAndSwitch0Same(ArbitraryPortSettings),

    // port exists in dpd and desired switch 0, but the settings may be
    // different (usually will be, but randomly could be the same still!)
    DpdAndSwitch0Changed {
        dpd: ArbitraryPortSettings,
        switch0: ArbitraryPortSettings,
    },
}

// We cap the arbitrary port generation here to at most `qsfp30`, because the
// `dpd` stub binary has a bug that makes `qsfp31` unusable:
// <https://github.com/oxidecomputer/dendrite/issues/271>.
#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, PartialOrd, Ord)]
struct PortId(#[strategy((0..31).prop_map(|n| format!("qsfp{n}")))] String);

impl PortId {
    fn to_dpd(&self) -> DpdQsfp {
        self.0.parse().unwrap()
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct TestInput {
    ports: BTreeMap<PortId, SwitchPortSettingsTestInput>,
}

impl TestInput {
    // Map all our arbitrary inputs into just the map that should exist in dpd
    // prior to reconciliation.
    fn initial_dpd_settings(&self) -> BTreeMap<DpdQsfp, DpdPortSettings> {
        self.ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DpdOnly(dpd)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Same(dpd)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    dpd,
                    ..
                } => Some((port_id.to_dpd(), dpd.to_dpd_settings(port_id))),
                SwitchPortSettingsTestInput::DesiredSwitch0(_)
                | SwitchPortSettingsTestInput::DesiredSwitch1(_) => None,
            })
            .collect()
    }

    // Map all our arbitrary inputs into the desired `RackNetworkConfig`.
    fn desired_rack_config(&self) -> RackNetworkConfig {
        let mut switch0 = self
            .ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DesiredSwitch0(switch0)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Same(switch0)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    switch0,
                    ..
                } => Some(diffable_to_port_config(
                    SwitchSlot::Switch0,
                    port_id,
                    switch0,
                )),
                SwitchPortSettingsTestInput::DpdOnly(_)
                | SwitchPortSettingsTestInput::DesiredSwitch1(_) => None,
            })
            .peekable();
        let mut switch1 = self
            .ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DesiredSwitch1(switch1) => {
                    Some(diffable_to_port_config(
                        SwitchSlot::Switch1,
                        port_id,
                        switch1,
                    ))
                }
                SwitchPortSettingsTestInput::DpdOnly(_)
                | SwitchPortSettingsTestInput::DesiredSwitch0(_)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Same(_)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    ..
                } => None,
            })
            .peekable();

        // RackNetworkConfig's ports must be nonempty. If we have no ports,
        // insert an arbitrary switch1 port; this won't affect the proptests
        // because they're exercising dpd on switch 0, but ensures we don't get
        // type construction errors from `rack_config()`.
        if switch0.peek().is_none() && switch1.peek().is_none() {
            rack_config(vec![PortConfig {
                routes: Vec::new(),
                addresses: Vec::new(),
                switch: SwitchSlot::Switch1,
                port: "qsfp0".to_owned(),
                uplink_port_speed: LinkSpeed::Speed0G,
                uplink_port_fec: None,
                bgp_peers: Vec::new(),
                autoneg: false,
                lldp: None,
                tx_eq: None,
            }])
        } else {
            rack_config(switch0.chain(switch1).collect())
        }
    }

    // Build the set of expected unchanged port names based on our arbitrary
    // inputs.
    fn expected_unchanged(&self) -> BTreeSet<String> {
        self.ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DpdOnly(_)
                | SwitchPortSettingsTestInput::DesiredSwitch0(_)
                | SwitchPortSettingsTestInput::DesiredSwitch1(_) => None,
                SwitchPortSettingsTestInput::DpdAndSwitch0Same(_) => {
                    Some(port_id.to_dpd().to_string())
                }
                SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    dpd,
                    switch0,
                } => {
                    // We have to convert to dpd settings for this comparison;
                    // if addrconf addresses are involved, they need to be
                    // filtered out.
                    if dpd.to_dpd_settings(port_id)
                        == switch0.to_dpd_settings(port_id)
                    {
                        Some(port_id.to_dpd().to_string())
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    // Build the set of ports we expect to clear in dpd.
    fn expected_to_clear(&self) -> BTreeSet<DpdQsfp> {
        self.ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DpdOnly(_) => {
                    Some(port_id.to_dpd())
                }
                SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    ..
                }
                | SwitchPortSettingsTestInput::DesiredSwitch0(_)
                | SwitchPortSettingsTestInput::DesiredSwitch1(_)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Same(_) => None,
            })
            .collect()
    }

    // Build the set of ports we expect to apply because they're new or changed.
    fn expected_to_apply(&self) -> BTreeMap<DpdQsfp, DpdPortSettings> {
        self.ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DpdOnly(_)
                | SwitchPortSettingsTestInput::DesiredSwitch1(_)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Same(_) => None,
                SwitchPortSettingsTestInput::DesiredSwitch0(switch0) => {
                    Some((port_id.to_dpd(), switch0.to_dpd_settings(port_id)))
                }
                SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    dpd,
                    switch0,
                } => {
                    // We have to convert to dpd settings for this comparison;
                    // if addrconf addresses are involved, they need to be
                    // filtered out.
                    if dpd.to_dpd_settings(port_id)
                        != switch0.to_dpd_settings(port_id)
                    {
                        Some((
                            port_id.to_dpd(),
                            switch0.to_dpd_settings(port_id),
                        ))
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    async fn validate_post_reconciliation(
        &self,
        client: &dpd_client::Client,
    ) -> anyhow::Result<()> {
        // Fetch all settings in dpd.
        let mut dpd_ports_with_settings = BTreeMap::new();
        for port_id in 0..32 {
            let port_id = format!("qsfp{port_id}").parse::<DpdQsfp>().unwrap();
            let settings = client
                .port_settings_get(&DpdPortId::Qsfp(port_id.clone()), DPD_TAG)
                .await?
                .into_inner();
            if !settings.links.is_empty() {
                dpd_ports_with_settings.insert(port_id, settings);
            }
        }

        // We expect to see all the settings we applied plus all unchanged
        // ports.
        let mut expected_settings = self
            .ports
            .iter()
            .filter_map(|(port_id, input)| match input {
                SwitchPortSettingsTestInput::DpdOnly(_)
                | SwitchPortSettingsTestInput::DesiredSwitch1(_) => None,
                SwitchPortSettingsTestInput::DesiredSwitch0(switch0)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Same(switch0)
                | SwitchPortSettingsTestInput::DpdAndSwitch0Changed {
                    switch0,
                    ..
                } => Some((port_id.to_dpd(), switch0.to_dpd_settings(port_id))),
            })
            .collect::<BTreeMap<DpdQsfp, DpdPortSettings>>();

        // To compare the state in dpd with what we expect, we need to ignore
        // ordering in the vec of addresses. We'll sort them all.
        for links in dpd_ports_with_settings
            .values_mut()
            .chain(expected_settings.values_mut())
            .flat_map(|settings| settings.links.values_mut())
        {
            links.addrs.sort_unstable();
        }

        assert_eq!(
            dpd_ports_with_settings, expected_settings,
            "unexpected settings in dpd after reconciliation"
        );

        Ok(())
    }
}

fn diffable_to_port_config(
    switch: SwitchSlot,
    port_id: &PortId,
    config: &ArbitraryPortSettings,
) -> PortConfig {
    let ArbitraryPortSettings { autoneg, tx_eq, fec, speed, addrs } = config;
    PortConfig {
        addresses: addrs.into_iter().copied().collect(),
        switch,
        port: port_id.0.clone(),
        uplink_port_speed: *speed,
        uplink_port_fec: *fec,
        autoneg: *autoneg,
        tx_eq: *tx_eq,

        // Fields that aren't involved in dpd configuration
        routes: Vec::new(),
        bgp_peers: Vec::new(),
        lldp: None,
    }
}

#[proptest]
fn proptest_plan(input: TestInput) {
    let logctx = dev::test_setup_log("proptest_plan");
    let log = &logctx.log;

    let plan = ReconciliationPlan::new(
        input.initial_dpd_settings(),
        &input.desired_rack_config(),
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    )
    .expect("plan should succeed");

    let ReconciliationPlan { unchanged, to_clear, to_apply } = plan;

    assert_eq!(unchanged, input.expected_unchanged(), "incorrect unchanged");
    assert_eq!(to_clear, input.expected_to_clear(), "incorrect to_clear");
    assert_eq!(to_apply, input.expected_to_apply(), "incorrect to_apply");

    logctx.cleanup_successful();
}

#[tokio::test(flavor = "multi_thread")]
async fn proptest_full_reconciliation() {
    let logctx = dev::test_setup_log("proptest_full_reconciliation");
    let mgsctx = gateway_test_utils::setup::test_setup(
        "proptest_full_reconciliation",
        SpPort::One,
    )
    .await;
    let dpdctx = dev::dendrite::DendriteInstance::start(
        None,
        mgsctx.address().into(),
        &logctx.log,
    )
    .await
    .expect("started dendrite");
    let client = dpd_client::Client::new(
        &format!("http://{}", dpdctx.address()),
        dpd_client::ClientState {
            tag: OMICRON_DPD_TAG.to_owned(),
            log: logctx.log.clone(),
        },
    );
    let rt = tokio::runtime::Handle::current();

    let reconciler = Mutex::new(PortReconciler::default());
    let one_test_invocation = async |input: TestInput| {
        // Clear all ports from a previous proptest invocation.
        for port_id in 0..32 {
            let port_id = format!("qsfp{port_id}").parse().unwrap();
            client
                .port_settings_clear(&DpdPortId::Qsfp(port_id), DPD_TAG)
                .await
                .expect("cleared port");
        }

        // Apply all initial settings.
        for (port_id, settings) in input.initial_dpd_settings() {
            client
                .port_settings_apply(
                    &DpdPortId::Qsfp(port_id),
                    DPD_TAG,
                    &settings,
                )
                .await
                .expect("applied initial settings");
        }

        // Perform reconciliation.
        let status = reconciler
            .lock()
            .await
            .reconcile(
                &client,
                &input.desired_rack_config(),
                ThisSledSwitchSlot::TEST_FAKE,
                &logctx.log,
            )
            .await;

        match status {
            DpdPortReconcilerStatus::FailedReadingCurrentSettings(_)
            | DpdPortReconcilerStatus::FailedGeneratingPlan(_)
            | DpdPortReconcilerStatus::PartialSuccess { .. } => {
                panic!("unexpected reconciler status: {status:?}");
            }
            DpdPortReconcilerStatus::Success {
                unchanged,
                cleared,
                applied,
            } => {
                assert_eq!(
                    unchanged,
                    input.expected_unchanged(),
                    "incorrect unchanged"
                );
                assert_eq!(
                    cleared,
                    input
                        .expected_to_clear()
                        .into_iter()
                        .map(|p| p.to_string())
                        .collect::<BTreeSet<_>>(),
                    "incorrect cleared"
                );
                let expected_applied = input
                    .expected_to_apply()
                    .keys()
                    .map(|p| p.to_string())
                    .collect::<BTreeSet<_>>();
                assert_eq!(applied, expected_applied, "incorrect applied");

                input
                    .validate_post_reconciliation(&client)
                    .await
                    .expect("validated post reconciliation settings");
            }
        }
    };

    proptest_macro!(|(input: TestInput)| {
        // Do a little dance to call our async `one_test_invocation` within the
        // non-async `proptest_macro!()` context.
        block_in_place(|| rt.block_on(one_test_invocation(input)));
    });

    dpdctx.cleanup().await.expect("dpd cleanup succeeded");
    mgsctx.teardown().await;
    logctx.cleanup_successful();
}
