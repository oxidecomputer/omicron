// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for the TOML file we give to and accept from clients for setting
//! (most of) the rack setup configuration.

use serde::Serialize;
use std::borrow::Cow;
use std::fmt;
use toml_edit::Array;
use toml_edit::Document;
use toml_edit::Formatted;
use toml_edit::InlineTable;
use toml_edit::Table;
use toml_edit::Value;
use wicketd_client::types::BootstrapSledDescription;
use wicketd_client::types::CurrentRssUserConfigInsensitive;
use wicketd_client::types::IpRange;
use wicketd_client::types::RackNetworkConfig;
use wicketd_client::types::SpType;

static TEMPLATE: &str = include_str!("config_template.toml");

// Separator used between elements of multiline arrays to make them look
// nice/indented.
const ARRAY_SEP: &str = "\n    ";

pub(super) struct TomlTemplate {
    doc: Document,
}

impl TomlTemplate {
    pub(crate) fn populate(config: &CurrentRssUserConfigInsensitive) -> Self {
        let mut doc = TEMPLATE.parse::<Document>().unwrap();

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
            .map(|s| Value::String(Formatted::new(s.into())))
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

        for array in
            ["ntp_servers", "dns_servers", "internal_services_ip_pool_ranges"]
        {
            format_multiline_array(
                doc.get_mut(array).unwrap().as_array_mut().unwrap(),
            );
        }

        *doc.get_mut("bootstrap_sleds").unwrap().as_array_mut().unwrap() =
            build_sleds_array(&config.bootstrap_sleds);

        populate_network_table(
            doc.get_mut("rack_network_config").unwrap().as_table_mut().unwrap(),
            config.rack_network_config.as_ref(),
        );

        Self { doc }
    }
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

fn build_sleds_array(sleds: &[BootstrapSledDescription]) -> Array {
    // Helper function to build the comment attached to a given sled.
    fn sled_comment(sled: &BootstrapSledDescription, end: &str) -> String {
        use wicketd_client::types::Baseboard;
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
                format!(" # NON-GIMLET {identifier} (model {model}, {ip}){end}")
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
    config: Option<&RackNetworkConfig>,
) {
    // Helper function to serialize enums into their appropriate string
    // representations.
    fn enum_to_toml_string<T: Serialize>(value: &T) -> Cow<'static, str> {
        let value = toml::Value::try_from(value).unwrap();
        match value {
            toml::Value::String(s) => Cow::from(s),
            other => {
                panic!("improper use of enum_to_toml_string: got {other:?}");
            }
        }
    }

    let Some(config) = config else {
        return;
    };

    for (property, value) in [
        ("gateway_ip", Cow::from(&config.gateway_ip)),
        ("infra_ip_first", Cow::from(&config.infra_ip_first)),
        ("infra_ip_last", Cow::from(&config.infra_ip_last)),
        ("uplink_port", Cow::from(&config.uplink_port)),
        ("uplink_port_speed", enum_to_toml_string(&config.uplink_port_speed)),
        ("uplink_port_fec", enum_to_toml_string(&config.uplink_port_fec)),
        ("uplink_ip", Cow::from(&config.uplink_ip)),
    ] {
        *table.get_mut(property).unwrap().as_value_mut().unwrap() =
            Value::String(Formatted::new(value.into_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::api::internal::shared::RackNetworkConfig as InternalRackNetworkConfig;
    use std::net::Ipv6Addr;
    use wicket_common::rack_setup::PutRssUserConfigInsensitive;
    use wicketd_client::types::Baseboard;
    use wicketd_client::types::PortFec;
    use wicketd_client::types::PortSpeed;
    use wicketd_client::types::SpIdentifier;

    fn put_config_from_current_config(
        value: CurrentRssUserConfigInsensitive,
    ) -> PutRssUserConfigInsensitive {
        use omicron_common::api::internal::shared::PortFec as InternalPortFec;
        use omicron_common::api::internal::shared::PortSpeed as InternalPortSpeed;

        let rnc = value.rack_network_config.unwrap();

        PutRssUserConfigInsensitive {
            bootstrap_sleds: value
                .bootstrap_sleds
                .into_iter()
                .map(|sled| sled.id.slot)
                .collect(),
            dns_servers: value.dns_servers,
            external_dns_zone_name: value.external_dns_zone_name,
            internal_services_ip_pool_ranges: value
                .internal_services_ip_pool_ranges
                .into_iter()
                .map(|r| {
                    use omicron_common::address;
                    match r {
                        IpRange::V4(r) => address::IpRange::V4(
                            address::Ipv4Range::new(r.first, r.last).unwrap(),
                        ),
                        IpRange::V6(r) => address::IpRange::V6(
                            address::Ipv6Range::new(r.first, r.last).unwrap(),
                        ),
                    }
                })
                .collect(),
            ntp_servers: value.ntp_servers,
            rack_network_config: InternalRackNetworkConfig {
                gateway_ip: rnc.gateway_ip,
                infra_ip_first: rnc.infra_ip_first,
                infra_ip_last: rnc.infra_ip_last,
                uplink_port: rnc.uplink_port,
                uplink_port_speed: match rnc.uplink_port_speed {
                    PortSpeed::Speed0G => InternalPortSpeed::Speed0G,
                    PortSpeed::Speed1G => InternalPortSpeed::Speed1G,
                    PortSpeed::Speed10G => InternalPortSpeed::Speed10G,
                    PortSpeed::Speed25G => InternalPortSpeed::Speed25G,
                    PortSpeed::Speed40G => InternalPortSpeed::Speed40G,
                    PortSpeed::Speed50G => InternalPortSpeed::Speed50G,
                    PortSpeed::Speed100G => InternalPortSpeed::Speed100G,
                    PortSpeed::Speed200G => InternalPortSpeed::Speed200G,
                    PortSpeed::Speed400G => InternalPortSpeed::Speed400G,
                },
                uplink_port_fec: match rnc.uplink_port_fec {
                    PortFec::Firecode => InternalPortFec::Firecode,
                    PortFec::None => InternalPortFec::None,
                    PortFec::Rs => InternalPortFec::Rs,
                },
                uplink_ip: rnc.uplink_ip,
                uplink_vid: rnc.uplink_vid,
            },
        }
    }

    #[test]
    fn round_trip_nonempty_config() {
        let config = CurrentRssUserConfigInsensitive {
            bootstrap_sleds: vec![
                BootstrapSledDescription {
                    id: SpIdentifier { slot: 1, type_: SpType::Sled },
                    baseboard: Baseboard::Gimlet {
                        model: "model1".into(),
                        revision: 3,
                        identifier: "serial 1 2 3".into(),
                    },
                    bootstrap_ip: None,
                },
                BootstrapSledDescription {
                    id: SpIdentifier { slot: 5, type_: SpType::Sled },
                    baseboard: Baseboard::Gimlet {
                        model: "model2".into(),
                        revision: 5,
                        identifier: "serial 4 5 6".into(),
                    },
                    bootstrap_ip: Some(Ipv6Addr::LOCALHOST),
                },
            ],
            dns_servers: vec!["1.1.1.1".into(), "2.2.2.2".into()],
            external_dns_zone_name: "oxide.computer".into(),
            internal_services_ip_pool_ranges: vec![IpRange::V4(
                wicketd_client::types::Ipv4Range {
                    first: "10.0.0.1".parse().unwrap(),
                    last: "10.0.0.5".parse().unwrap(),
                },
            )],
            ntp_servers: vec!["ntp1.com".into(), "ntp2.com".into()],
            rack_network_config: Some(RackNetworkConfig {
                gateway_ip: "1.2.3.4".into(),
                infra_ip_first: "2.3.4.5".into(),
                infra_ip_last: "3.4.5.6".into(),
                uplink_ip: "4.5.6.7".into(),
                uplink_port_speed: PortSpeed::Speed400G,
                uplink_port_fec: PortFec::Firecode,
                uplink_port: "port0".into(),
                uplink_vid: None,
            }),
        };
        let template = TomlTemplate::populate(&config).to_string();
        let parsed: PutRssUserConfigInsensitive =
            toml::de::from_str(&template).unwrap();
        assert_eq!(put_config_from_current_config(config), parsed);
    }
}
