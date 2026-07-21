// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Computation of the desired bootstore rack network configuration.
//!
//! This crate holds the logic that the `sync_switch_configuration` Nexus
//! background task uses to build the [`RackNetworkConfig`] that is written to
//! the bootstore. The computation is a pure, in-memory function: all database
//! reads happen in the caller, which passes the results in via
//! [`RackNetworkConfigInput`].

use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use ipnetwork::IpNetwork;
use nexus_types::external_api::networking;
use omicron_uuid_kinds::BgpConfigUuid;
use oxnet::IpNet;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::BgpConfig as SledBgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig as SledBgpPeerConfig;
use sled_agent_types::early_networking::InvalidIpAddrError;
use sled_agent_types::early_networking::LinkFec;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::LldpAdminStatus;
use sled_agent_types::early_networking::LldpPortConfig;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouteConfig as SledRouteConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::TxEqConfig;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use sled_agent_types::early_networking::UplinkPorts;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;

/// Inputs to [`build_rack_network_config`].
pub struct RackNetworkConfigInput {
    /// The rack's subnet. Must be set and IPv6; the builder bails otherwise.
    pub rack_subnet: Option<IpNetwork>,
    /// The BGP configuration for each switch.
    pub bgp_configs: BTreeMap<SwitchSlot, SwitchBgpConfig>,
    /// The ports whose applied settings feed the bootstore config.
    pub ports: Vec<PortInput>,
    /// First address of the infrastructure IP range.
    pub infra_ip_first: IpAddr,
    /// Last address of the infrastructure IP range.
    pub infra_ip_last: IpAddr,
    /// The BFD sessions to configure.
    pub bfd: Vec<BfdPeerConfig>,
}

/// Per-switch BGP configuration.
pub struct BgpConfigInput {
    /// The config's ID, used to distinguish configs from each other.
    pub id: BgpConfigUuid,
    /// The local autonomous system number.
    pub asn: u32,
    /// The prefixes this switch originates, resolved from its announce set.
    pub originate: Vec<IpNet>,
    /// The optional checker program source.
    pub checker: Option<String>,
    /// The optional shaper program source.
    pub shaper: Option<String>,
    /// The maximum number of equal-cost paths (validated by the builder).
    pub max_paths: u8,
}

impl IdOrdItem for BgpConfigInput {
    type Key<'a> = BgpConfigUuid;
    fn key(&self) -> Self::Key<'_> {
        self.id
    }
    id_upcast!();
}

/// The BGP configuration a switch's peers reference.
pub enum SwitchBgpConfig {
    /// Every peer on the switch references a single BGP config.
    Single(BgpConfigInput),
    /// The switch's peers reference more than one distinct BGP config, so its
    /// ASN is ambiguous.
    ///
    /// In this case, [`build_rack_network_config`] reports a
    /// [`Problem::ConflictingBgpConfigForSwitch`].
    Conflicting(BTreeSet<BgpConfigUuid>),
}

/// Collapse the `(switch, config)` BGP associations a switch's peers reference
/// into one [`SwitchBgpConfig`] per switch.
///
/// A switch with more than one *distinct* config has peers that disagree on the
/// config (and thus the ASN) -- instead of silently picking the first, we
/// return `Conflicting`.
pub fn collapse_bgp_configs(
    associations: impl IntoIterator<Item = (SwitchSlot, BgpConfigInput)>,
) -> BTreeMap<SwitchSlot, SwitchBgpConfig> {
    let mut by_switch: BTreeMap<SwitchSlot, IdOrdMap<BgpConfigInput>> =
        BTreeMap::new();
    for (switch, config) in associations {
        by_switch.entry(switch).or_default().insert_overwrite(config);
    }

    by_switch
        .into_iter()
        .map(|(switch, configs)| {
            let collapsed = match configs.len() {
                0 => unreachable!(
                    "every switch in the map was inserted at least once"
                ),
                1 => SwitchBgpConfig::Single(
                    configs.into_iter().next().expect("len checked == 1"),
                ),
                _ => SwitchBgpConfig::Conflicting(
                    configs.into_iter().map(|config| config.id).collect(),
                ),
            };
            (switch, collapsed)
        })
        .collect()
}

/// A switch port's applied settings.
pub struct PortInput {
    /// The switch this port belongs to.
    pub switch: SwitchSlot,
    /// The name of the port (e.g. `qsfp0`).
    pub port_name: String,
    /// The BGP peers configured on this port.
    pub bgp_peers: Vec<networking::BgpPeer>,
    /// The IP addresses assigned to this port.
    pub addresses: Vec<AddressInput>,
    /// The port's links. Only the first is used today (no breakout support).
    pub links: Vec<LinkInput>,
    /// The static routes configured on this port.
    pub routes: Vec<SledRouteConfig>,
    /// The port's LLDP settings. Only the first entry is used today.
    pub lldp: Vec<LldpInput>,
    /// The port's TX-EQ overrides. Only the first entry is used today.
    pub tx_eq: Vec<TxEqConfig>,
}

/// An IP address assigned to a port.
pub struct AddressInput {
    pub address: IpNet,
    pub vlan_id: Option<u16>,
}

/// A port's link-level settings. The caller has already translated the FEC and
/// speed into the sled-agent enums.
pub struct LinkInput {
    pub autoneg: bool,
    pub fec: Option<LinkFec>,
    pub speed: LinkSpeed,
}

/// A port's LLDP settings.
pub struct LldpInput {
    pub enabled: bool,
    pub link_name: Option<String>,
    pub link_description: Option<String>,
    pub chassis_id: Option<String>,
    pub system_name: Option<String>,
    pub system_description: Option<String>,
    pub management_ip: Option<IpAddr>,
}

/// A single reason the bootstore config could not be built completely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    /// The rack's subnet was not set.
    RackSubnetMissing,
    /// The rack's subnet was set but is not IPv6.
    RackSubnetNotIpv6 { subnet: IpNetwork },
    /// A switch's BGP config has a `max_paths` value our types reject. This
    /// should be impossible given the database constraints.
    IllegalMaxPaths { switch: SwitchSlot, value: u8 },
    /// A port is on a switch that has no BGP config, so its peers' ASN is
    /// unknown.
    MissingBgpConfigForSwitch { switch: SwitchSlot, port: String },
    /// A switch's peers reference more than one distinct BGP config, so its
    /// ASN is ambiguous. Only one BGP config (ASN) per switch is allowed.
    ConflictingBgpConfigForSwitch {
        switch: SwitchSlot,
        configs: BTreeSet<BgpConfigUuid>,
    },
    /// A port has an address that can't be converted to an uplink address.
    MalformedUplinkAddress {
        switch: SwitchSlot,
        port: String,
        error: InvalidIpAddrError,
    },
    /// The rack ended up with no usable uplink ports, so the config would be
    /// empty. A rack with no uplinks can't reach external networks.
    NoUplinkPorts,
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Problem::RackSubnetMissing => write!(f, "rack subnet is not set"),
            Problem::RackSubnetNotIpv6 { subnet } => {
                write!(f, "rack subnet {subnet} must be ipv6")
            }
            Problem::IllegalMaxPaths { switch, value } => write!(
                f,
                "bgp config for switch {switch:?} has illegal max_paths \
                 value {value}",
            ),
            Problem::MissingBgpConfigForSwitch { switch, port } => write!(
                f,
                "port {port} is on switch {switch:?}, which has no bgp config",
            ),
            Problem::ConflictingBgpConfigForSwitch { switch, configs } => {
                write!(
                    f,
                    "switch {switch:?} has peers referencing more than one bgp \
                     config (only one asn per switch is allowed): {configs:?}",
                )
            }
            Problem::MalformedUplinkAddress { switch, port, error } => write!(
                f,
                "port {port} on switch {switch:?} has a malformed uplink \
                 address: {error}",
            ),
            Problem::NoUplinkPorts => {
                write!(
                    f,
                    "the rack has no usable uplink ports (possibly due to \
                     other errors, if reported above)"
                )
            }
        }
    }
}

/// The error returned when bootstore rack network config could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompleteBootstoreConfig {
    /// The list of problems encountered while building the config.
    pub problems: Vec<Problem>,
}

impl fmt::Display for IncompleteBootstoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "could not build a complete bootstore rack network config \
             ({} problem(s)):",
            self.problems.len(),
        )?;
        for problem in &self.problems {
            write!(f, "\n  - {problem}")?;
        }
        Ok(())
    }
}

impl std::error::Error for IncompleteBootstoreConfig {}

/// Build the desired [`RackNetworkConfig`] for the bootstore.
///
/// This is a pure, in-memory computation: all database reads happen while
/// creating the [`RackNetworkConfigInput`].
///
/// Returns an [`IncompleteBootstoreConfig`] listing the [`Problem`]s found if a
/// complete config cannot be built for this rack.
pub fn build_rack_network_config(
    input: RackNetworkConfigInput,
) -> Result<RackNetworkConfig, IncompleteBootstoreConfig> {
    let RackNetworkConfigInput {
        rack_subnet,
        bgp_configs,
        ports: port_inputs,
        infra_ip_first,
        infra_ip_last,
        bfd,
    } = input;

    let mut problems = Vec::new();

    // The rack subnet must be set and IPv6. Record the problem, but keep going
    // so we report everything else that's wrong too.
    let subnet = match rack_subnet {
        Some(IpNetwork::V6(subnet)) => Some(subnet.into()),
        Some(other) => {
            problems.push(Problem::RackSubnetNotIpv6 { subnet: other });
            None
        }
        None => {
            problems.push(Problem::RackSubnetMissing);
            None
        }
    };

    // The bootstore stores one BGP config per switch.
    let mut bgp: Vec<SledBgpConfig> = Vec::new();
    for (switch, switch_config) in &bgp_configs {
        let config = match switch_config {
            SwitchBgpConfig::Single(config) => config,
            SwitchBgpConfig::Conflicting(configs) => {
                problems.push(Problem::ConflictingBgpConfigForSwitch {
                    switch: *switch,
                    configs: configs.clone(),
                });
                continue;
            }
        };

        let max_paths = match MaxPathConfig::new(config.max_paths) {
            Ok(max_paths) => max_paths,
            Err(_) => {
                // TODO: We should have read_and_assemble handle this problem.
                // (Needs read_and_assemble to start accumulating a list of
                // problems.)
                problems.push(Problem::IllegalMaxPaths {
                    switch: *switch,
                    value: config.max_paths,
                });
                continue;
            }
        };

        bgp.push(SledBgpConfig {
            asn: config.asn,
            originate: config.originate.clone(),
            checker: config.checker.clone(),
            shaper: config.shaper.clone(),
            max_paths,
        });
    }

    let mut ports: Vec<PortConfig> = vec![];

    for port in port_inputs {
        // TODO https://github.com/oxidecomputer/omicron/issues/3062
        let tx_eq = port.tx_eq.get(0).copied();

        // Build the bootstore BGP peers from the port's peers, which include
        // each peer's communities and import/export policies, though not the
        // ASN.
        let bgp_peers: Vec<SledBgpPeerConfig> = if port.bgp_peers.is_empty() {
            Vec::new()
        } else {
            // The peer ASN comes from the switch's BGP config.
            let asn = match bgp_configs.get(&port.switch) {
                Some(SwitchBgpConfig::Single(config)) => config.asn,
                Some(SwitchBgpConfig::Conflicting(_)) => {
                    // The conflict was already recorded above -- we can skip
                    // this port here. This won't cause an incomplete bootstore
                    // config to be returned since we check for problems at the
                    // end.
                    continue;
                }
                None => {
                    // We attempt to enforce this constraint at the application
                    // layer, though this code hasn't been fully audited for
                    // transaction safety and it's possible there are customer
                    // systems with missing BGP configs. That is definitely data
                    // corruption, so produce a problem. We'll see how this
                    // shakes out in the field.
                    //
                    // TODO: RSS does check for missing BGP configs (by virtue
                    // of going through the datastore methods). But wicket could
                    // check for them even further in advance too.
                    problems.push(Problem::MissingBgpConfigForSwitch {
                        switch: port.switch,
                        port: port.port_name.clone(),
                    });
                    continue;
                }
            };
            port.bgp_peers
                .iter()
                .map(|peer| SledBgpPeerConfig {
                    asn,
                    port: port.port_name.clone(),
                    addr: peer.addr,
                    hold_time: Some(peer.hold_time.into()),
                    idle_hold_time: Some(peer.idle_hold_time.into()),
                    delay_open: Some(peer.delay_open.into()),
                    connect_retry: Some(peer.connect_retry.into()),
                    keepalive: Some(peer.keepalive.into()),
                    remote_asn: peer.remote_asn,
                    min_ttl: peer.min_ttl,
                    md5_auth_key: peer.md5_auth_key.clone(),
                    multi_exit_discriminator: peer.multi_exit_discriminator,
                    communities: peer.communities.clone(),
                    local_pref: peer.local_pref,
                    enforce_first_as: peer.enforce_first_as,
                    allowed_import: peer.allowed_import.clone(),
                    allowed_export: peer.allowed_export.clone(),
                    vlan_id: peer.vlan_id,
                    src_addr: peer.src_addr,
                })
                .collect()
        };

        let addresses = match port
            .addresses
            .iter()
            .map(|a| {
                 let address = UplinkAddress::try_from_ip_net_treating_unspecified_as_addrconf(a.address)?;
                 Ok(UplinkAddressConfig {
                     address,
                     vlan_id: a.vlan_id
                 })
            })
            .collect::<Result<_, InvalidIpAddrError>>()
        {
            Ok(addresses) => addresses,
            Err(err) => {
                problems.push(Problem::MalformedUplinkAddress {
                    switch: port.switch,
                    port: port.port_name.clone(),
                    error: err,
                });
                continue;
            }
        };

        let port_config = PortConfig {
            addresses,
            autoneg: port
                .links
                .get(0) //TODO breakout support
                .map(|l| l.autoneg)
                .unwrap_or(false),
            bgp_peers,
            port: port.port_name.clone(),
            routes: port.routes,
            switch: port.switch,
            uplink_port_fec: port
                .links
                .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                .map(|l| l.fec)
                .unwrap_or(None),
            uplink_port_speed: port
                .links
                .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                .map(|l| l.speed)
                .unwrap_or(LinkSpeed::Speed100G),
            lldp: port
                .lldp
                .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                .map(|c| LldpPortConfig {
                    status: match c.enabled {
                        true => LldpAdminStatus::Enabled,
                        false => LldpAdminStatus::Disabled,
                    },
                    port_id: c.link_name.clone(),
                    port_description: c.link_description.clone(),
                    chassis_id: c.chassis_id.clone(),
                    system_name: c.system_name.clone(),
                    system_description: c.system_description.clone(),
                    management_addrs: c.management_ip.map(|ip| vec![ip]),
                }),
            tx_eq,
        };

        ports.push(port_config);
    }

    // A rack with no uplink ports can't reach external networks; refuse to
    // build an empty config. The `UplinkPorts` newtype makes that invalid state
    // unrepresentable downstream (omicron#10640).
    let ports = match UplinkPorts::new(ports) {
        Ok(ports) => Some(ports),
        Err(_) => {
            problems.push(Problem::NoUplinkPorts);
            None
        }
    };

    if !problems.is_empty() {
        return Err(IncompleteBootstoreConfig { problems });
    }

    Ok(RackNetworkConfig {
        // `subnet` and `ports` are `Some` whenever there are no problems: a
        // missing/non-IPv6 subnet or an empty port list each push a problem
        // above, which we just returned on.
        rack_subnet: subnet
            .expect("rack subnet is set when there are no problems"),
        infra_ip_first,
        infra_ip_last,
        ports: ports.expect("ports is set when there are no problems"),
        bgp,
        bfd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::api::external::NameOrId;
    use proptest::prelude::*;
    use sled_agent_types::early_networking::ImportExportPolicy;
    use sled_agent_types::early_networking::RouterPeerIpAddr;
    use sled_agent_types::early_networking::RouterPeerType;
    use test_strategy::proptest;

    fn valid_bgp_config() -> BgpConfigInput {
        BgpConfigInput {
            id: BgpConfigUuid::from_u128(1),
            asn: 65000,
            originate: vec![],
            checker: None,
            shaper: None,
            max_paths: 1,
        }
    }

    fn input_with(
        bgp_configs: Vec<(SwitchSlot, BgpConfigInput)>,
        ports: Vec<PortInput>,
    ) -> RackNetworkConfigInput {
        RackNetworkConfigInput {
            rack_subnet: Some("fd00:1122:3344:1::/64".parse().unwrap()),
            bgp_configs: collapse_bgp_configs(bgp_configs),
            ports,
            infra_ip_first: "fd00:1122:3344:1::1".parse().unwrap(),
            infra_ip_last: "fd00:1122:3344:1::2".parse().unwrap(),
            bfd: vec![],
        }
    }

    fn bare_port(switch: SwitchSlot, name: &str) -> PortInput {
        PortInput {
            switch,
            port_name: name.to_string(),
            bgp_peers: vec![],
            addresses: vec![],
            links: vec![],
            routes: vec![],
            lldp: vec![],
            tx_eq: vec![],
        }
    }

    /// A minimal BGP peer for tests.
    ///
    /// The builder doesn't validate the values so they're all quite arbitrary.
    fn peer() -> networking::BgpPeer {
        networking::BgpPeer {
            // This is arbitrary, since configs are matched by switch slot and
            // not this name.
            bgp_config: NameOrId::Name("test-bgp".parse().unwrap()),
            addr: RouterPeerType::Numbered {
                ip: RouterPeerIpAddr::try_from(
                    "192.0.2.1".parse::<IpAddr>().unwrap(),
                )
                .expect("192.0.2.1 is a valid router peer ip"),
            },
            hold_time: 6,
            idle_hold_time: 3,
            delay_open: 0,
            connect_retry: 3,
            keepalive: 2,
            remote_asn: None,
            min_ttl: None,
            md5_auth_key: None,
            multi_exit_discriminator: None,
            communities: vec![],
            local_pref: None,
            enforce_first_as: false,
            allowed_import: ImportExportPolicy::NoFiltering,
            allowed_export: ImportExportPolicy::NoFiltering,
            vlan_id: None,
            src_addr: None,
        }
    }

    /// A port that has a single BGP peer (and so needs its switch's BGP config).
    fn port_with_peer(switch: SwitchSlot, name: &str) -> PortInput {
        PortInput { bgp_peers: vec![peer()], ..bare_port(switch, name) }
    }

    #[test]
    fn peerless_port_needs_no_switch_bgp_config() {
        // A port without BGP peers (e.g. a static-routing-only uplink) does not
        // need its switch to have a BGP config.
        let config = build_rack_network_config(input_with(
            // No switch has a BGP config.
            vec![],
            // A single peerless port.
            vec![bare_port(SwitchSlot::Switch0, "qsfp0")],
        ))
        .expect("a peerless port needs no switch bgp config");
        assert_eq!(
            config.ports.len(),
            1,
            "the peerless port must be included, not dropped",
        );
    }

    #[test]
    fn peered_port_requires_switch_bgp_config() {
        // A port that *does* have peers needs its switch's BGP config.
        let err = build_rack_network_config(input_with(
            vec![],
            vec![port_with_peer(SwitchSlot::Switch0, "qsfp0")],
        ))
        .unwrap_err();
        assert_eq!(
            err.problems,
            vec![
                Problem::MissingBgpConfigForSwitch {
                    switch: SwitchSlot::Switch0,
                    port: "qsfp0".to_string(),
                },
                // Skipping the only port leaves the rack with no uplinks.
                Problem::NoUplinkPorts,
            ],
        );
    }

    #[test]
    fn conflicting_bgp_configs_for_switch_is_reported() {
        // Two distinct BGP configs (distinct ids) on the same switch must be
        // reported as a problem.
        let bgp_configs = vec![
            (SwitchSlot::Switch0, valid_bgp_config()),
            (
                SwitchSlot::Switch0,
                BgpConfigInput {
                    id: BgpConfigUuid::from_u128(2),
                    asn: 65001,
                    ..valid_bgp_config()
                },
            ),
        ];
        let err = build_rack_network_config(input_with(bgp_configs, vec![]))
            .unwrap_err();
        assert_eq!(
            err.problems,
            vec![
                Problem::ConflictingBgpConfigForSwitch {
                    switch: SwitchSlot::Switch0,
                    configs: BTreeSet::from([
                        BgpConfigUuid::from_u128(1),
                        BgpConfigUuid::from_u128(2),
                    ]),
                },
                // No ports at all leaves the rack with no uplinks.
                Problem::NoUplinkPorts,
            ],
        );
    }

    #[test]
    fn collapse_bgp_configs_marks_only_conflicting_switches() {
        let configs = collapse_bgp_configs(vec![
            (SwitchSlot::Switch0, valid_bgp_config()),
            (
                SwitchSlot::Switch0,
                BgpConfigInput {
                    id: BgpConfigUuid::from_u128(2),
                    asn: 65001,
                    ..valid_bgp_config()
                },
            ),
            (SwitchSlot::Switch1, valid_bgp_config()),
        ]);
        assert_eq!(configs.len(), 2);
        match configs.get(&SwitchSlot::Switch0) {
            Some(SwitchBgpConfig::Conflicting(ids)) => assert_eq!(
                *ids,
                BTreeSet::from([
                    BgpConfigUuid::from_u128(1),
                    BgpConfigUuid::from_u128(2),
                ]),
            ),
            Some(SwitchBgpConfig::Single(_)) => {
                panic!("two distinct configs on a switch must conflict")
            }
            None => panic!("switch0 must have a config"),
        }
        match configs.get(&SwitchSlot::Switch1) {
            Some(SwitchBgpConfig::Single(_)) => {}
            Some(SwitchBgpConfig::Conflicting(_)) => {
                panic!("a single config must not conflict")
            }
            None => panic!("switch1 must have a config"),
        }
    }

    #[test]
    fn collapse_bgp_configs_dedups_one_config_referenced_many_times() {
        let configs = collapse_bgp_configs(vec![
            (SwitchSlot::Switch0, valid_bgp_config()),
            (SwitchSlot::Switch0, valid_bgp_config()),
            (SwitchSlot::Switch0, valid_bgp_config()),
        ]);
        assert_eq!(configs.len(), 1);
        match configs.get(&SwitchSlot::Switch0) {
            Some(SwitchBgpConfig::Single(_)) => {}
            Some(SwitchBgpConfig::Conflicting(_)) => {
                panic!(
                    "the same config referenced repeatedly must not conflict"
                )
            }
            None => panic!("switch0 must have a config"),
        }
    }

    #[test]
    fn missing_rack_subnet_is_reported() {
        let input = RackNetworkConfigInput {
            rack_subnet: None,
            ..input_with(vec![], vec![])
        };
        let err = build_rack_network_config(input).unwrap_err();
        // No subnet and (incidentally) no ports: both are reported.
        assert_eq!(
            err.problems,
            vec![Problem::RackSubnetMissing, Problem::NoUplinkPorts],
        );
    }

    #[test]
    fn non_ipv6_rack_subnet_is_reported() {
        let v4: IpNetwork = "10.0.0.0/24".parse().unwrap();
        let input = RackNetworkConfigInput {
            rack_subnet: Some(v4),
            ..input_with(vec![], vec![])
        };
        let err = build_rack_network_config(input).unwrap_err();
        assert_eq!(
            err.problems,
            vec![
                Problem::RackSubnetNotIpv6 { subnet: v4 },
                Problem::NoUplinkPorts,
            ],
        );
    }

    #[test]
    fn empty_port_list_is_reported() {
        // A valid subnet but no ports at all: the rack has no uplinks, so the
        // builder must bail rather than emit an empty config.
        let err =
            build_rack_network_config(input_with(vec![], vec![])).unwrap_err();
        assert_eq!(err.problems, vec![Problem::NoUplinkPorts]);
    }

    #[test]
    fn all_problems_are_accumulated() {
        let v4: IpNetwork = "10.0.0.0/24".parse().unwrap();
        let input = RackNetworkConfigInput {
            rack_subnet: Some(v4),
            ..input_with(
                vec![],
                vec![port_with_peer(SwitchSlot::Switch0, "qsfp0")],
            )
        };
        let err = build_rack_network_config(input).unwrap_err();
        assert_eq!(
            err.problems,
            // This order is fixed.
            vec![
                Problem::RackSubnetNotIpv6 { subnet: v4 },
                Problem::MissingBgpConfigForSwitch {
                    switch: SwitchSlot::Switch0,
                    port: "qsfp0".to_string(),
                },
                Problem::NoUplinkPorts,
            ],
        );
    }

    #[test]
    fn peered_port_switch_asn() {
        // A peered port needs its switch's BGP config to source the peer ASN.
        // Use two switches with distinct ASNs to ensure that the matching up
        // occurs properly.
        let bgp_configs = vec![
            (
                SwitchSlot::Switch0,
                BgpConfigInput { asn: 64999, ..valid_bgp_config() },
            ),
            (
                SwitchSlot::Switch1,
                BgpConfigInput { asn: 65111, ..valid_bgp_config() },
            ),
        ];

        let config = build_rack_network_config(input_with(
            bgp_configs,
            vec![
                port_with_peer(SwitchSlot::Switch0, "qsfp0"),
                port_with_peer(SwitchSlot::Switch1, "qsfp1"),
            ],
        ))
        .expect("both peered ports' switches have a bgp config");

        let p0 = config
            .ports
            .iter()
            .find(|p| p.port == "qsfp0")
            .expect("qsfp0 is in the built config");
        assert_eq!(p0.bgp_peers.len(), 1);
        assert_eq!(
            p0.bgp_peers[0].asn, 64999,
            "the peer ASN must come from switch 0's bgp config",
        );

        let p1 = config
            .ports
            .iter()
            .find(|p| p.port == "qsfp1")
            .expect("qsfp1 is in the built config");
        assert_eq!(p1.bgp_peers.len(), 1);
        assert_eq!(
            p1.bgp_peers[0].asn, 65111,
            "the peer ASN must come from switch 1's bgp config",
        );
    }

    #[test]
    fn malformed_uplink_address_is_reported() {
        // A loopback address can't be converted to an uplink address, so a port
        // carrying one must be reported, not silently dropped from the config.
        let loopback: IpNet = "127.0.0.1/8".parse().unwrap();
        let port = PortInput {
            addresses: vec![AddressInput { address: loopback, vlan_id: None }],
            ..bare_port(SwitchSlot::Switch0, "qsfp0")
        };
        let err = build_rack_network_config(input_with(vec![], vec![port]))
            .unwrap_err();
        assert_eq!(
            err.problems,
            vec![
                Problem::MalformedUplinkAddress {
                    switch: SwitchSlot::Switch0,
                    port: "qsfp0".to_string(),
                    error: InvalidIpAddrError::LoopbackAddress,
                },
                // Skipping the only port leaves the rack with no uplinks.
                Problem::NoUplinkPorts,
            ],
        );
    }

    #[test]
    fn unspecified_address_becomes_addrconf_not_a_problem() {
        // An unspecified address is a sentinel value that's converted to
        // `UplinkAddress::AddrConf`.
        let unspecified: IpNet = "0.0.0.0/0".parse().unwrap();
        let port = PortInput {
            addresses: vec![AddressInput {
                address: unspecified,
                vlan_id: None,
            }],
            ..bare_port(SwitchSlot::Switch0, "qsfp0")
        };
        let config = build_rack_network_config(input_with(vec![], vec![port]))
            .expect("built network config successfully");
        assert_eq!(config.ports.len(), 1);
        assert_eq!(config.ports.first().addresses.len(), 1);
        assert_eq!(
            config.ports.first().addresses[0].address,
            UplinkAddress::AddrConf,
        );
    }

    #[test]
    fn illegal_max_paths_is_reported() {
        // The builder consumes an unvalidated `max_paths`, so an out-of-range
        // value must surface as a problem.
        let bgp_configs = vec![(
            SwitchSlot::Switch0,
            BgpConfigInput { max_paths: 0, ..valid_bgp_config() },
        )];
        let err = build_rack_network_config(input_with(bgp_configs, vec![]))
            .unwrap_err();
        assert_eq!(
            err.problems,
            vec![
                Problem::IllegalMaxPaths {
                    switch: SwitchSlot::Switch0,
                    value: 0,
                },
                Problem::NoUplinkPorts,
            ],
        );
    }

    /// The builder must never silently drop a port (see #10640).
    #[proptest]
    fn no_silent_port_drops(
        switch0_has_bgp: bool,
        switch1_has_bgp: bool,
        // Each port is (switch, has_peer).
        #[strategy(prop::collection::vec(
            (
                prop_oneof![
                    Just(SwitchSlot::Switch0),
                    Just(SwitchSlot::Switch1),
                ],
                any::<bool>(),
            ),
            0..8,
        ))]
        ports_spec: Vec<(SwitchSlot, bool)>,
    ) {
        let mut bgp_configs = Vec::new();
        if switch0_has_bgp {
            bgp_configs.push((SwitchSlot::Switch0, valid_bgp_config()));
        }
        if switch1_has_bgp {
            bgp_configs.push((SwitchSlot::Switch1, valid_bgp_config()));
        }

        let ports: Vec<PortInput> = ports_spec
            .iter()
            .enumerate()
            .map(|(i, &(switch, has_peer))| {
                let name = format!("qsfp{i}");
                if has_peer {
                    port_with_peer(switch, &name)
                } else {
                    bare_port(switch, &name)
                }
            })
            .collect();

        // The names we fed in. On a successful build the output ports must be
        // exactly these.
        let mut input_names: Vec<String> =
            (0..ports.len()).map(|i| format!("qsfp{i}")).collect();
        input_names.sort();

        // A port needs its switch's BGP config only if it has peers.
        let switch_has_bgp = |switch: SwitchSlot| match switch {
            SwitchSlot::Switch0 => switch0_has_bgp,
            SwitchSlot::Switch1 => switch1_has_bgp,
        };
        let all_peered_have_bgp = ports_spec
            .iter()
            .all(|&(switch, has_peer)| !has_peer || switch_has_bgp(switch));

        let result = build_rack_network_config(input_with(bgp_configs, ports));

        if input_names.is_empty() {
            // No ports at all -> a NoUplinkPorts bail, never an empty
            // (silently truncated) config.
            let err = result.expect_err("an empty port list must force a bail");
            prop_assert_eq!(err.problems, vec![Problem::NoUplinkPorts]);
        } else if all_peered_have_bgp {
            let config = result.expect(
                "every peered port's switch has a bgp config, so it builds",
            );
            let mut got_names: Vec<String> =
                config.ports.iter().map(|p| p.port.clone()).collect();
            got_names.sort();
            prop_assert_eq!(got_names, input_names);
        } else {
            let err = result.expect_err(
                "a peered port whose switch has no bgp config must bail",
            );
            // This mirrors the implementation, which generally isn't great in a
            // PBT, but it's hard to describe this in a way that doesn't weaken
            // the test.
            let mut expected: Vec<Problem> = ports_spec
                .iter()
                .enumerate()
                .filter_map(|(i, &(switch, has_peer))| {
                    (has_peer && !switch_has_bgp(switch)).then(|| {
                        Problem::MissingBgpConfigForSwitch {
                            switch,
                            port: format!("qsfp{i}"),
                        }
                    })
                })
                .collect();
            // A port survives unless it's peered with no switch bgp config. If
            // none survive, the built list is empty, so the builder appends
            // NoUplinkPorts after the per-port problems.
            let any_port_survives = ports_spec
                .iter()
                .any(|&(switch, has_peer)| !has_peer || switch_has_bgp(switch));
            if !any_port_survives {
                expected.push(Problem::NoUplinkPorts);
            }
            prop_assert_eq!(err.problems, expected);
        }
    }
}
