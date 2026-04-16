// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use proptest::collection::vec;
use proptest::prelude::*;
use scuffle::AddPropertyGroupFlags;
use scuffle::EditPropertyGroups;
use scuffle::HasComposedPropertyGroups;
use scuffle::PropertyGroupType;
use scuffle::Scf;
use scuffle::Scope;
use scuffle::Value;
use scuffle::ValueKind;
use scuffle::isolated::IsolatedConfigd;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::PortSpeed;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkAddressConfig;
use std::collections::BTreeMap;
use test_strategy::Arbitrary;

use crate::switch_zone_slot::ThisSledSwitchSlot;

// arbitrary port name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
struct PortName(#[strategy(r"[a-z]{1,5}")] String);

// arbitrary port addresses (but always at least 1)
#[derive(Debug, Clone, Arbitrary)]
struct UplinkAddressConfigs(
    #[strategy(vec(any::<UplinkAddressConfig>(), 1..=5))]
    Vec<UplinkAddressConfig>,
);

#[derive(Debug, Clone, Arbitrary)]
enum PortSetup {
    // Port should exist with these addresses before reconciliation then be
    // removed
    Remove(UplinkAddressConfigs),
    // Port should not exist before reconciliation and should be added with
    // these addresses
    Add(UplinkAddressConfigs),
    // Port should exist with these addresses and remain unchanged after
    // reconciliation
    Unchanged(UplinkAddressConfigs),
    // Port should exist both before and after but  have different addresses
    // after reconciliation (except when proptest gives us the same values for
    // both, in which case this goes to "unchanged" instead).
    Change { before: UplinkAddressConfigs, after: UplinkAddressConfigs },
}

#[derive(Debug, Clone, Arbitrary)]
struct TestSetup {
    ports: BTreeMap<PortName, PortSetup>,
}

impl TestSetup {
    // Ports that should exist before reconciliation.
    fn before_ports(
        &self,
    ) -> impl Iterator<Item = (&str, &[UplinkAddressConfig])> {
        self.ports.iter().filter_map(|(port, port_setup)| match port_setup {
            PortSetup::Add(_) => None,
            PortSetup::Remove(addrs)
            | PortSetup::Unchanged(addrs)
            | PortSetup::Change { before: addrs, after: _ } => {
                Some((port.0.as_str(), addrs.0.as_slice()))
            }
        })
    }

    // Ports that should exist after reconciliation.
    fn after_ports(
        &self,
    ) -> impl Iterator<Item = (&str, &[UplinkAddressConfig])> {
        self.ports.iter().filter_map(|(port, port_setup)| match port_setup {
            PortSetup::Remove(_) => None,
            PortSetup::Add(addrs)
            | PortSetup::Unchanged(addrs)
            | PortSetup::Change { before: _, after: addrs } => {
                Some((port.0.as_str(), addrs.0.as_slice()))
            }
        })
    }

    fn expected_before(&self) -> BTreeMap<String, Vec<Value>> {
        Self::expected_common(self.before_ports())
    }

    fn expected_after(&self) -> BTreeMap<String, Vec<Value>> {
        Self::expected_common(self.after_ports())
    }

    fn expected_common<'a>(
        ports: impl Iterator<Item = (&'a str, &'a [UplinkAddressConfig])>,
    ) -> BTreeMap<String, Vec<Value>> {
        let mut expected = BTreeMap::new();

        for (port, addrs) in ports {
            expected.insert(
                format!("{port}_0"),
                addrs
                    .iter()
                    .map(|addr| Value::AString(addr.to_uplinkd_smf_property()))
                    .collect(),
            );
        }

        expected
    }

    fn rack_network_config(&self) -> RackNetworkConfig {
        let mut ports = Vec::new();

        for (port, addrs) in self.after_ports() {
            ports.push(PortConfig {
                routes: Vec::new(),
                addresses: addrs.to_vec(),
                switch: SwitchSlot::Switch0,
                port: port.to_owned(),
                uplink_port_speed: PortSpeed::Speed100G,
                uplink_port_fec: None,
                bgp_peers: Vec::new(),
                autoneg: false,
                lldp: None,
                tx_eq: None,
            });
        }

        RackNetworkConfig {
            rack_subnet: "fd00::/48".parse().unwrap(),
            infra_ip_first: "10.0.0.1".parse().unwrap(),
            infra_ip_last: "10.0.0.100".parse().unwrap(),
            ports,
            bgp: Vec::new(),
            bfd: Vec::new(),
        }
    }
}

fn apply_uplinks(
    scope: &Scope<'_>,
    uplinks: BTreeMap<String, Vec<Value>>,
    refresh_instance: bool,
) {
    let service = scope.service("oxide/uplink").unwrap().unwrap();
    let mut instance = service.instance("default").unwrap().unwrap();

    instance.delete_property_group("uplinks").unwrap();

    {
        let mut pg = instance
            .add_property_group(
                "uplinks",
                PropertyGroupType::Application,
                AddPropertyGroupFlags::Persistent,
            )
            .unwrap();

        let mut tx = pg.transaction().unwrap().start().unwrap();

        for (port, addrs) in uplinks {
            tx.property_new_multiple(
                &port,
                ValueKind::AString,
                addrs.iter().map(|addr| addr.as_value_ref()),
            )
            .unwrap();
        }

        tx.commit().unwrap();
    }

    if refresh_instance {
        instance.refresh().unwrap();
    }
}

fn config_of_running_snapshot(
    scope: &Scope<'_>,
) -> BTreeMap<String, Vec<Value>> {
    let service = scope.service("oxide/uplink").unwrap().unwrap();
    let instance = service.instance("default").unwrap().unwrap();
    let snapshot = instance.snapshot("running").unwrap().unwrap();
    let Some(pg) = snapshot.property_group_composed("uplinks").unwrap() else {
        return BTreeMap::new();
    };

    let mut config = BTreeMap::new();

    for property in pg.properties().unwrap() {
        let property = property.unwrap();
        let values =
            property.values().unwrap().map(|value| value.unwrap()).collect();
        config.insert(property.name().to_string(), values);
    }

    config
}

// Normal reconciliation.
#[test]
fn proptest_uplinkd_reconciliation() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "proptest_uplinkd_reconciliation",
    );
    let log = &logctx.log;
    let isolated =
        IsolatedConfigd::builder("oxide/uplink").unwrap().build().unwrap();
    let scf = Scf::connect_isolated(&isolated).unwrap();
    let scope = scf.scope_local().unwrap();

    proptest!(|(test_setup: TestSetup)| {
        apply_uplinks(&scope, test_setup.expected_before(), true);

        super::do_reconciliation_blocking_impl(
            &scf,
            &test_setup.rack_network_config(),
            ThisSledSwitchSlot::TEST_FAKE,
            log,
        ).unwrap();

        let reconciled_config = config_of_running_snapshot(&scope);
        let expected_config = test_setup.expected_after();

        assert_eq!(reconciled_config, expected_config);
    });
}

// Abnormal reconciliation: Apply the expected _reconciled_ config to the
// instance, but do not refresh. This emulates reconciliation succeeding in
// committing changes but failing to refresh the instance; the next
// reconciliation attempt should still work to refresh the instance.
#[test]
fn proptest_uplinkd_reconciliation_refresh() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "proptest_uplinkd_reconciliation_refresh",
    );
    let log = &logctx.log;
    let isolated =
        IsolatedConfigd::builder("oxide/uplink").unwrap().build().unwrap();
    let scf = Scf::connect_isolated(&isolated).unwrap();
    let scope = scf.scope_local().unwrap();

    proptest!(|(test_setup: TestSetup)| {
        apply_uplinks(&scope, test_setup.expected_before(), false);

        super::do_reconciliation_blocking_impl(
            &scf,
            &test_setup.rack_network_config(),
            ThisSledSwitchSlot::TEST_FAKE,
            log,
        ).unwrap();

        let reconciled_config = config_of_running_snapshot(&scope);
        let expected_config = test_setup.expected_after();

        assert_eq!(reconciled_config, expected_config);
    });
}
