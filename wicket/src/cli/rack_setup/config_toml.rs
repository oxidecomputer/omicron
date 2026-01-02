// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for the TOML file we give to and accept from clients for setting
//! (most of) the rack setup configuration.

use omicron_common::address::IpRange;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::internal::shared::BgpConfig;
use omicron_common::api::internal::shared::LldpPortConfig;
use omicron_common::api::internal::shared::RouteConfig;
use omicron_common::api::internal::shared::UplinkAddressConfig;
use serde::Serialize;
use sled_hardware_types::Baseboard;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;
use toml_edit::Array;
use toml_edit::ArrayOfTables;
use toml_edit::DocumentMut;
use toml_edit::Formatted;
use toml_edit::InlineTable;
use toml_edit::Item;
use toml_edit::Table;
use toml_edit::Value;
use wicket_common::inventory::SpType;
use wicket_common::rack_setup::BootstrapSledDescription;
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::UserSpecifiedBgpPeerConfig;
use wicket_common::rack_setup::UserSpecifiedImportExportPolicy;
use wicket_common::rack_setup::UserSpecifiedPortConfig;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;

static TEMPLATE: &str = include_str!("config_template.toml");

// Separator used between elements of multiline arrays to make them look
// nice/indented.
const ARRAY_SEP: &str = "\n    ";

pub(super) struct TomlTemplate {
    doc: DocumentMut,
}

impl TomlTemplate {
    pub(crate) fn populate(config: &CurrentRssUserConfigInsensitive) -> Self {
        let mut doc = TEMPLATE.parse::<DocumentMut>().unwrap();

        *doc.get_mut("external_dns_zone_name")
            .unwrap()
            .as_value_mut()
            .unwrap() = Value::String(Formatted::new(
            config.external_dns_zone_name.clone(),
        ));

        *doc.get_mut("ntp_servers").unwrap().as_array_mut().unwrap() = config
            .ntp_servers
            .iter()
            .map(|s| Value::String(Formatted::new(s.into())))
            .collect();

        *doc.get_mut("dns_servers").unwrap().as_array_mut().unwrap() = config
            .dns_servers
            .iter()
            .map(|s| Value::String(Formatted::new(s.to_string())))
            .collect();

        *doc.get_mut("internal_services_ip_pool_ranges")
            .unwrap()
            .as_array_mut()
            .unwrap() = config
            .internal_services_ip_pool_ranges
            .iter()
            .map(|r| {
                let mut t = InlineTable::new();
                let (first, last) = match r {
                    IpRange::V4(r) => (r.first.to_string(), r.last.to_string()),
                    IpRange::V6(r) => (r.first.to_string(), r.last.to_string()),
                };
                t.insert("first", Value::String(Formatted::new(first)));
                t.insert("last", Value::String(Formatted::new(last)));
                Value::InlineTable(t)
            })
            .collect();

        *doc.get_mut("external_dns_ips").unwrap().as_array_mut().unwrap() =
            config
                .external_dns_ips
                .iter()
                .map(|s| Value::String(Formatted::new(s.to_string())))
                .collect();

        for array in [
            "ntp_servers",
            "dns_servers",
            "internal_services_ip_pool_ranges",
            "external_dns_ips",
        ] {
            format_multiline_array(
                doc.get_mut(array).unwrap().as_array_mut().unwrap(),
            );
        }

        populate_allowed_source_ips(
            &mut doc,
            config.allowed_source_ips.as_ref(),
        );

        *doc.get_mut("bootstrap_sleds").unwrap().as_array_mut().unwrap() =
            build_sleds_array(&config.bootstrap_sleds);

        populate_network_table(
            doc.get_mut("rack_network_config").unwrap().as_table_mut().unwrap(),
            config.rack_network_config.as_ref(),
        );

        Self { doc }
    }
}

// Populate the allowed source IP list, which can be specified as a specified as
// a string "any" or a list of IP addresses.
fn populate_allowed_source_ips(
    doc: &mut DocumentMut,
    allowed_source_ips: Option<&AllowedSourceIps>,
) {
    const ALLOWLIST_COMMENT: &str = r#"
# Allowlist of source IPs that can make requests to user-facing services.
#
# Use the key:
#
# allow = "any"
#
# to indicate any external IPs are allowed to make requests. This is the default.
#
# Use the below two lines to only allow requests from the specified IP subnets.
# Requests from any other source IPs are refused. Note that individual addresses
# must include the netmask, e.g., "1.2.3.4/32".
#
# allow = "list"
# ips = [ "1.2.3.4/5", "5.6.7.8/10" ]
"#;

    let mut table = toml_edit::Table::new();
    table.decor_mut().set_prefix(ALLOWLIST_COMMENT);
    match allowed_source_ips {
        None | Some(AllowedSourceIps::Any) => {
            table.insert(
                "allow",
                Item::Value(Value::String(Formatted::new("any".to_string()))),
            );
        }
        Some(AllowedSourceIps::List(list)) => {
            let entries = list
                .iter()
                .map(|ip| Value::String(Formatted::new(ip.to_string())))
                .collect();
            table.insert(
                "allow",
                Item::Value(Value::String(Formatted::new("list".to_string()))),
            );
            table.insert("ips", Item::Value(Value::Array(entries)));
        }
    }
    doc.insert("allowed_source_ips", Item::Table(table)).unwrap();
}

impl fmt::Display for TomlTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.doc.fmt(f)
    }
}

fn format_multiline_array(array: &mut Array) {
    for element in array.iter_mut() {
        element.decor_mut().set_prefix(ARRAY_SEP);
    }
    array.set_trailing_comma(true);
    array.set_trailing("\n");
}

fn build_sleds_array(sleds: &BTreeSet<BootstrapSledDescription>) -> Array {
    // Helper function to build the comment attached to a given sled.
    fn sled_comment(sled: &BootstrapSledDescription, end: &str) -> String {
        let ip = sled
            .bootstrap_ip
            .map(|ip| Cow::from(format!("{ip}")))
            .unwrap_or_else(|| Cow::from("IP address UNKNOWN"));
        match &sled.baseboard {
            Baseboard::Gimlet { identifier, model, revision } => {
                format!(
                    " # {identifier} (model {model} revision {revision}, {ip})\
                     {end}"
                )
            }
            Baseboard::Unknown => {
                format!(" # UNKNOWN SLED ({ip}){end}")
            }
            Baseboard::Pc { identifier, model } => {
                format!(" # NON-OXIDE {identifier} (model {model}, {ip}){end}")
            }
        }
    }

    let mut array = Array::new();
    let mut prev: Option<&BootstrapSledDescription> = None;

    for sled in sleds {
        // We should never get a non-sled from wicketd; if we do, filter it out.
        if sled.id.type_ != SpType::Sled {
            continue;
        }

        let mut value = Formatted::new(i64::from(sled.id.slot));
        let decor = value.decor_mut();

        // We have to attach the comment for each sled on the _next_ item in the
        // array, so here we set our prefix to be the previous item's details.
        if let Some(prev) = prev {
            decor.set_prefix(sled_comment(prev, ARRAY_SEP));
        } else {
            decor.set_prefix(ARRAY_SEP);
        }

        array.push_formatted(Value::Integer(value));
        prev = Some(sled);
    }

    // Because we attach comments to previous items, we also need to add a
    // comment to the last element.
    if let Some(prev) = prev {
        array.set_trailing(sled_comment(prev, "\n"));
        array.set_trailing_comma(true);
    }

    array
}

fn populate_network_table(
    table: &mut Table,
    config: Option<&UserSpecifiedRackNetworkConfig>,
) {
    let Some(config) = config else {
        return;
    };

    for (property, value) in [
        ("infra_ip_first", config.infra_ip_first.to_string()),
        ("infra_ip_last", config.infra_ip_last.to_string()),
    ] {
        *table.get_mut(property).unwrap().as_value_mut().unwrap() =
            Value::String(Formatted::new(value));
    }

    if !config.switch0.is_empty() {
        let switch_table = table
            .get_mut("switch0")
            .expect("[rack_network_config.switch0] table exists in template")
            .as_table_mut()
            .expect("[rack_network_config.switch0] is a table");
        switch_table.clear();
        switch_table.set_implicit(true);

        for (port, cfg) in &config.switch0 {
            let uplink = populate_uplink_table(cfg);
            switch_table.insert(port, Item::Table(uplink));
        }
    }

    if !config.switch1.is_empty() {
        let switch_table = table
            .get_mut("switch1")
            .expect("[rack_network_config.switch1] table exists in template")
            .as_table_mut()
            .expect("[rack_network_config.switch1] is a table");
        switch_table.clear();
        switch_table.set_implicit(true);

        for (port, cfg) in &config.switch1 {
            let uplink = populate_uplink_table(cfg);
            switch_table.insert(port, Item::Table(uplink));
        }
    }

    if !config.bgp.is_empty() {
        *table.get_mut("bgp").unwrap().as_array_of_tables_mut().unwrap() =
            config
                .bgp
                .iter()
                .map(|cfg| {
                    // This style ensures that if a new field is added, this
                    // fails at compile time.
                    // XXX: shaper and checker are going to go away
                    let BgpConfig { asn, originate, shaper: _, checker: _ } =
                        cfg;

                    let mut bgp = Table::new();
                    bgp.insert("asn", i64_item(i64::from(*asn)));

                    let mut originate_out = Array::new();
                    for o in originate {
                        originate_out.push(string_value(o));
                    }
                    bgp.insert(
                        "originate",
                        Item::Value(Value::Array(originate_out)),
                    );
                    bgp
                })
                .collect();
    }
}

#[must_use]
fn populate_uplink_table(cfg: &UserSpecifiedPortConfig) -> Table {
    // This style ensures that if a new field is added, this fails loudly.
    let UserSpecifiedPortConfig {
        routes,
        addresses,
        uplink_port_speed,
        uplink_port_fec,
        autoneg,
        bgp_peers,
        lldp,
        tx_eq,
    } = cfg;

    let mut uplink = Table::new();

    // routes = []
    let mut routes_out = Array::new();
    for r in routes {
        let RouteConfig { destination, nexthop, vlan_id, rib_priority } = r;
        let mut route = InlineTable::new();
        route.insert("nexthop", string_value(nexthop));
        route.insert("destination", string_value(destination));
        if let Some(vlan_id) = vlan_id {
            route.insert("vlan_id", i64_value(i64::from(*vlan_id)));
        }
        if let Some(rib_priority) = rib_priority {
            route.insert("rib_priority", i64_value(i64::from(*rib_priority)));
        }
        routes_out.push(Value::InlineTable(route));
    }
    uplink.insert("routes", Item::Value(Value::Array(routes_out)));

    // addresses = []
    let mut addresses_out = Array::new();
    for a in addresses {
        let UplinkAddressConfig { address, vlan_id } = a;
        let mut x = InlineTable::new();
        x.insert("address", string_value(address));
        if let Some(vlan_id) = vlan_id {
            x.insert("vlan_id", i64_value(i64::from(*vlan_id)));
        }
        addresses_out.push(Value::InlineTable(x));
    }
    uplink.insert("addresses", Item::Value(Value::Array(addresses_out)));

    // General properties
    uplink.insert(
        "uplink_port_speed",
        string_item(enum_to_toml_string(&uplink_port_speed)),
    );

    if let Some(fec) = uplink_port_fec {
        uplink
            .insert("uplink_port_fec", string_item(enum_to_toml_string(&fec)));
    }
    uplink.insert("autoneg", bool_item(*autoneg));

    // [[rack_network_config.port.<port>.bgp_peers]]

    let mut peers = ArrayOfTables::new();
    for p in bgp_peers {
        // This style ensures that if a new field is added, this
        // fails at compile time.
        let UserSpecifiedBgpPeerConfig {
            asn,
            port,
            addr,
            hold_time,
            idle_hold_time,
            delay_open,
            connect_retry,
            keepalive,
            remote_asn,
            min_ttl,
            auth_key_id,
            multi_exit_discriminator,
            communities,
            local_pref,
            enforce_first_as,
            allowed_import,
            allowed_export,
            vlan_id,
        } = p;

        let mut peer = Table::new();

        // asn = 0
        peer.insert("asn", i64_item(i64::from(*asn)));

        // port = ""
        peer.insert("port", string_item(port));

        // addr = ""
        peer.insert("addr", string_item(addr));

        // hold_time
        if let Some(x) = hold_time {
            peer.insert("hold_time", i64_item(*x as i64));
        }

        // idle_hold_time
        if let Some(x) = idle_hold_time {
            peer.insert("idle_hold_time", i64_item(*x as i64));
        }

        // delay_open
        if let Some(x) = delay_open {
            peer.insert("delay_open", i64_item(*x as i64));
        }

        // connect_retry
        if let Some(x) = connect_retry {
            peer.insert("connect_retry", i64_item(*x as i64));
        }

        // keepalive
        if let Some(x) = keepalive {
            peer.insert("keepalive", i64_item(*x as i64));
        }

        // remote_asn
        if let Some(x) = remote_asn {
            peer.insert("remote_asn", i64_item(i64::from(*x)));
        }

        // min_ttl
        if let Some(x) = min_ttl {
            peer.insert("min_ttl", i64_item(i64::from(*x)));
        }

        // auth_key_id
        if let Some(x) = &auth_key_id {
            peer.insert("auth_key_id", string_item(x));
        }

        // multi_exit_discriminator
        if let Some(x) = multi_exit_discriminator {
            peer.insert("multi_exit_discriminator", i64_item(i64::from(*x)));
        }

        // communities
        if !communities.is_empty() {
            let mut out = Array::new();
            for c in communities {
                out.push(i64_value(i64::from(*c)));
            }
            peer.insert("communities", Item::Value(Value::Array(out)));
        }

        // allowed import policy
        if let UserSpecifiedImportExportPolicy::Allow(list) = allowed_import {
            let mut out = Array::new();
            for x in list.iter() {
                out.push(string_value(x.to_string()));
            }
            peer.insert("allowed_import", Item::Value(Value::Array(out)));
        }

        // allowed export policy
        if let UserSpecifiedImportExportPolicy::Allow(list) = allowed_export {
            let mut out = Array::new();
            for x in list.iter() {
                out.push(string_value(x.to_string()));
            }
            peer.insert("allowed_export", Item::Value(Value::Array(out)));
        }

        //vlan
        if let Some(x) = vlan_id {
            peer.insert("vlan_id", i64_item(i64::from(*x)));
        }

        // local_pref
        if let Some(x) = local_pref {
            peer.insert("local_pref", i64_item(i64::from(*x)));
        }

        // enforce_first_as
        peer.insert("enforce_first_as", bool_item(*enforce_first_as));

        peers.push(peer);
    }

    uplink.insert("bgp_peers", Item::ArrayOfTables(peers));

    if let Some(l) = lldp {
        let LldpPortConfig {
            status,
            chassis_id,
            port_id,
            system_name,
            system_description,
            port_description,
            management_addrs,
        } = l;
        let mut lldp = Table::new();
        lldp.insert("status", string_item(status));
        if let Some(x) = chassis_id {
            lldp.insert("chassis_id", string_item(x));
        }
        if let Some(x) = port_id {
            lldp.insert("port_id", string_item(x));
        }
        if let Some(x) = system_name {
            lldp.insert("system_name", string_item(x));
        }
        if let Some(x) = system_description {
            lldp.insert("system_description", string_item(x));
        }
        if let Some(x) = port_description {
            lldp.insert("port_description", string_item(x));
        }
        if let Some(addrs) = management_addrs {
            let mut addresses_out = Array::new();
            for a in addrs {
                addresses_out.push(string_value(a));
            }
            lldp.insert(
                "management_addrs",
                Item::Value(Value::Array(addresses_out)),
            );
        }
        uplink.insert("lldp", Item::Table(lldp));
    }

    if let Some(t) = tx_eq {
        let mut tx_eq = Table::new();
        if let Some(x) = t.pre1 {
            tx_eq.insert("pre1", i32_item(x));
        }
        if let Some(x) = t.pre2 {
            tx_eq.insert("pre2", i32_item(x));
        }
        if let Some(x) = t.main {
            tx_eq.insert("main", i32_item(x));
        }
        if let Some(x) = t.post1 {
            tx_eq.insert("post1", i32_item(x));
        }
        if let Some(x) = t.post2 {
            tx_eq.insert("post2", i32_item(x));
        }
        uplink.insert("tx_eq", Item::Table(tx_eq));
    }

    uplink
}

// Helper function to serialize enums into their appropriate string
// representations.
fn enum_to_toml_string<T: Serialize>(value: &T) -> String {
    let value = toml::Value::try_from(value).unwrap();
    match value {
        toml::Value::String(s) => s,
        other => {
            panic!("improper use of enum_to_toml_string: got {other:?}");
        }
    }
}

fn string_value(s: impl ToString) -> Value {
    Value::String(Formatted::new(s.to_string()))
}

fn string_item(s: impl ToString) -> Item {
    Item::Value(string_value(s))
}

fn i32_value(i: i32) -> Value {
    Value::Integer(Formatted::new(i.into()))
}

fn i32_item(i: i32) -> Item {
    Item::Value(i32_value(i))
}

fn i64_value(i: i64) -> Value {
    Value::Integer(Formatted::new(i))
}

fn i64_item(i: i64) -> Item {
    Item::Value(i64_value(i))
}

fn bool_value(b: bool) -> Value {
    Value::Boolean(Formatted::new(b))
}

fn bool_item(b: bool) -> Item {
    Item::Value(bool_value(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wicket_common::{
        example::ExampleRackSetupData, rack_setup::PutRssUserConfigInsensitive,
    };

    #[test]
    fn round_trip_nonempty_config() {
        let example = ExampleRackSetupData::non_empty();
        let template =
            TomlTemplate::populate(&example.current_insensitive).to_string();

        // Write out the template to a file as a snapshot test.
        expectorate::assert_contents(
            "tests/output/example_non_empty.toml",
            &template,
        );
        let parsed: PutRssUserConfigInsensitive =
            toml::de::from_str(&template).unwrap();
        assert_eq!(example.put_insensitive, parsed);
    }
}
