// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use proptest::collection::btree_map;
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
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::LldpPortConfig;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use test_strategy::Arbitrary;

// arbitrary port name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
struct PortName(#[strategy(r"[a-z]{1,5}")] String);

#[derive(Debug, Clone, Arbitrary)]
enum PortSetup {
    // Port should exist before reconciliation then be removed
    Remove(LldpPortConfig),
    // Port should not exist before reconciliation and should be added
    Add(LldpPortConfig),
    // Port should exist and remain unchanged after reconciliation
    Unchanged(LldpPortConfig),
    // Port should exist both before and after but have different config
    // after reconciliation (except when proptest gives us the same values for
    // both, in which case this goes to "unchanged" instead).
    Change { before: LldpPortConfig, after: LldpPortConfig },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LldpSmfProperties {
    single_valued: BTreeMap<String, String>,
    management_addrs: BTreeSet<String>,
}

impl From<&'_ LldpPortConfig> for LldpSmfProperties {
    fn from(value: &'_ LldpPortConfig) -> Self {
        let LldpPortConfig {
            status,
            chassis_id,
            port_id,
            port_description,
            system_name,
            system_description,
            management_addrs,
        } = value;

        let mut single_valued = BTreeMap::new();
        single_valued.insert(
            property_name::STATUS.to_owned(),
            status.to_lldpd_smf_property().to_owned(),
        );
        for (name, val) in [
            (property_name::CHASSIS_ID, chassis_id),
            (property_name::PORT_ID, port_id),
            (property_name::PORT_DESCRIPTION, port_description),
            (property_name::SYSTEM_NAME, system_name),
            (property_name::SYSTEM_DESCRIPTION, system_description),
        ] {
            if let Some(val) = val {
                single_valued.insert(name.to_owned(), val.clone());
            }
        }

        let management_addrs = management_addrs
            .iter()
            .flatten()
            .map(|ip| ip.to_string())
            .collect();

        Self { single_valued, management_addrs }
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct TestSetup {
    #[strategy(btree_map(any::<PortName>(), any::<PortSetup>(), 1..=8))]
    ports: BTreeMap<PortName, PortSetup>,
}

impl TestSetup {
    // Ports that should exist before reconciliation.
    fn before_ports(&self) -> impl Iterator<Item = (&str, &LldpPortConfig)> {
        self.ports.iter().filter_map(|(port, port_setup)| match port_setup {
            PortSetup::Add(_) => None,
            PortSetup::Remove(config)
            | PortSetup::Unchanged(config)
            | PortSetup::Change { before: config, after: _ } => {
                Some((port.0.as_str(), config))
            }
        })
    }

    // Ports that should exist after reconciliation.
    fn after_ports(&self) -> impl Iterator<Item = (&str, &LldpPortConfig)> {
        self.ports.iter().filter_map(|(port, port_setup)| match port_setup {
            PortSetup::Remove(_) => None,
            PortSetup::Add(config)
            | PortSetup::Unchanged(config)
            | PortSetup::Change { before: _, after: config } => {
                Some((port.0.as_str(), config))
            }
        })
    }

    fn expected_before(&self) -> BTreeMap<String, LldpSmfProperties> {
        Self::expected_common(self.before_ports())
    }

    fn expected_after(&self) -> BTreeMap<String, LldpSmfProperties> {
        Self::expected_common(self.after_ports())
    }

    fn expected_common<'a>(
        ports: impl Iterator<Item = (&'a str, &'a LldpPortConfig)>,
    ) -> BTreeMap<String, LldpSmfProperties> {
        ports
            .map(|(port, config)| {
                (
                    format!("{LLDPD_PORT_PG_PREFIX}{port}"),
                    LldpSmfProperties::from(config),
                )
            })
            .collect()
    }

    fn rack_network_config(&self) -> RackNetworkConfig {
        let mut ports = Vec::new();

        for (port, config) in self.after_ports() {
            ports.push(PortConfig {
                routes: Vec::new(),
                addresses: Vec::new(),
                switch: SwitchSlot::Switch0,
                port: port.to_owned(),
                uplink_port_speed: LinkSpeed::Speed100G,
                uplink_port_fec: None,
                bgp_peers: Vec::new(),
                autoneg: false,
                lldp: Some(config.clone()),
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

fn erase_port_configs(scope: &Scope<'_>) {
    let service = scope.service(LLDPD_SERVICE_NAME).unwrap().unwrap();
    let mut instance = service.instance(LLDPD_INSTANCE_NAME).unwrap().unwrap();

    let mut pgs_to_delete = Vec::new();
    for pg in instance.property_groups_direct().unwrap() {
        let pg = pg.unwrap();
        if pg.name().starts_with(LLDPD_PORT_PG_PREFIX) {
            pgs_to_delete.push(pg.name().to_owned());
        }
    }
    for pg_name in pgs_to_delete {
        instance.delete_property_group(&pg_name).unwrap();
    }
}

fn apply_port_configs(
    scope: &Scope<'_>,
    configs: BTreeMap<String, LldpSmfProperties>,
    refresh_instance: bool,
) {
    let service = scope.service(LLDPD_SERVICE_NAME).unwrap().unwrap();
    let mut instance = service.instance(LLDPD_INSTANCE_NAME).unwrap().unwrap();

    for (pg_name, config) in configs {
        let mut pg = instance
            .add_property_group(
                &pg_name,
                PropertyGroupType::Application,
                AddPropertyGroupFlags::Persistent,
            )
            .unwrap();

        let mut tx = pg.transaction().unwrap().start().unwrap();

        for (name, val) in config.single_valued {
            tx.property_new(&name, ValueRef::AString(&val)).unwrap();
        }
        if !config.management_addrs.is_empty() {
            tx.property_new_multiple(
                property_name::MANAGEMENT_ADDRS,
                ValueKind::AString,
                config.management_addrs.iter().map(|ip| ValueRef::AString(ip)),
            )
            .unwrap();
        }

        tx.commit().unwrap();
    }

    if refresh_instance {
        instance.smf_refresh().unwrap();
    }
}

fn config_of_running_snapshot(
    scope: &Scope<'_>,
) -> BTreeMap<String, LldpSmfProperties> {
    let service = scope.service(LLDPD_SERVICE_NAME).unwrap().unwrap();
    let instance = service.instance(LLDPD_INSTANCE_NAME).unwrap().unwrap();
    let snapshot = instance.snapshot("running").unwrap().unwrap();

    let mut config_pgs = BTreeMap::new();
    for pg in snapshot.property_groups_composed().unwrap() {
        let pg = pg.unwrap();
        if !pg.name().starts_with(LLDPD_PORT_PG_PREFIX) {
            continue;
        }
        let mut props = LldpSmfProperties::default();
        for property in pg.properties().unwrap() {
            let property = property.unwrap();
            if property.name() == property_name::MANAGEMENT_ADDRS {
                for value in property.values().unwrap() {
                    match value.unwrap() {
                        Value::AString(value) => {
                            props.management_addrs.insert(value);
                        }
                        other => panic!("unexpected property value: {other:?}"),
                    }
                }
            } else {
                let value = property.single_value().unwrap();
                match value {
                    Value::AString(value) => {
                        props
                            .single_valued
                            .insert(property.name().to_owned(), value);
                    }
                    other => panic!("unexpected property value: {other:?}"),
                }
            }
        }
        config_pgs.insert(pg.name().to_owned(), props);
    }

    config_pgs
}

// Normal reconciliation.
#[test]
fn proptest_lldpd_reconciliation() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "proptest_lldpd_reconciliation",
    );
    let log = &logctx.log;
    let isolated =
        IsolatedConfigd::builder(LLDPD_SERVICE_NAME).unwrap().build().unwrap();
    let scf = Scf::connect_isolated(&isolated).unwrap();
    let scope = scf.scope_local().unwrap();

    proptest!(|(test_setup: TestSetup)| {
        erase_port_configs(&scope);
        apply_port_configs(&scope, test_setup.expected_before(), true);

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

    logctx.cleanup_successful();
}

// Abnormal reconciliation: Apply the expected _reconciled_ config to the
// instance, but do not refresh. This emulates reconciliation succeeding in
// committing changes but failing to refresh the instance; the next
// reconciliation attempt should still work to refresh the instance.
#[test]
fn proptest_lldpd_reconciliation_refresh() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "proptest_lldpd_reconciliation_refresh",
    );
    let log = &logctx.log;
    let isolated =
        IsolatedConfigd::builder(LLDPD_SERVICE_NAME).unwrap().build().unwrap();
    let scf = Scf::connect_isolated(&isolated).unwrap();
    let scope = scf.scope_local().unwrap();

    proptest!(|(test_setup: TestSetup)| {
        erase_port_configs(&scope);
        apply_port_configs(&scope, test_setup.expected_before(), false);

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

    logctx.cleanup_successful();
}
