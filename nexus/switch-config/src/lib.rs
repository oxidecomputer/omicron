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

use ipnetwork::IpNetwork;
use nexus_types::external_api::networking;
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
use slog::Logger;
use slog::error;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::IpAddr;

/// Inputs to [`build_rack_network_config`].
pub struct RackNetworkConfigInput {
    /// The rack's subnet. Must be set and IPv6; the builder bails otherwise.
    pub rack_subnet: Option<IpNetwork>,
    /// The BGP configuration for each switch.
    pub bgp_configs: BTreeMap<SwitchSlot, BgpConfigInput>,
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

/// Build the desired [`RackNetworkConfig`] for the bootstore.
///
/// This is a pure, in-memory computation: all database reads happen while
/// creating the [`RackNetworkConfigInput`].
///
/// Returns `None` if a complete config cannot be built for this rack (in which
/// case the caller should skip the rack and retry on the next activation). Note
/// that, matching the historical behavior, individual ports are silently
/// skipped on conversion errors. Later changes will address this.
pub fn build_rack_network_config(
    log: &Logger,
    input: RackNetworkConfigInput,
) -> Option<RackNetworkConfig> {
    let RackNetworkConfigInput {
        rack_subnet,
        bgp_configs,
        ports: port_inputs,
        infra_ip_first,
        infra_ip_last,
        bfd,
    } = input;

    // build the desired bootstore config from the records we've fetched
    let subnet = match rack_subnet {
        Some(IpNetwork::V6(subnet)) => subnet.into(),
        Some(IpNetwork::V4(_)) => {
            error!(log, "rack subnet must be ipv6"; "rack_subnet" => ?rack_subnet);
            return None;
        }
        None => {
            error!(log, "rack subnet not set");
            return None;
        }
    };

    // TODO: is this correct? Do we place the BgpConfig for both switches in a single Vec to send to the bootstore?
    let mut bgp: Vec<SledBgpConfig> = bgp_configs
        .values()
        .filter_map(|config| {
            let max_paths = match MaxPathConfig::new(config.max_paths) {
                Ok(max_paths) => max_paths,
                Err(err) => {
                    // This should be impossible - our db constraints
                    // should ensure legal values.
                    error!(
                        log,
                        "database contains illegal max_paths value";
                        InlineErrorChain::new(&err),
                    );
                    return None;
                }
            };

            Some(SledBgpConfig {
                asn: config.asn,
                originate: config.originate.clone(),
                checker: config.checker.clone(),
                shaper: config.shaper.clone(),
                max_paths,
            })
        })
        .collect();

    bgp.dedup();

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
                Some(config) => config.asn,
                None => {
                    // XXX: The port has BGP peers but no ASN configured. This
                    // is an error, but we continue for now. We should
                    // completely fail the task instead.
                    error!(
                        log,
                        "no bgp config for switch; skipping port for bootstore";
                        "switch_slot" => ?port.switch,
                        "port" => &port.port_name,
                    );
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
                error!(
                    log,
                    "failed to convert database uplink addresses \
                     to API uplink addresses";
                    "switch_slot" => ?port.switch,
                    "port" => &port.port_name,
                    InlineErrorChain::new(&err),
                );
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

    Some(RackNetworkConfig {
        rack_subnet: subnet,
        infra_ip_first,
        infra_ip_last,
        ports,
        bgp,
        bfd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peerless_port_needs_no_switch_bgp_config() {
        // A port with no BGP peers (e.g. a static-routing-only uplink) should
        // not be dropped.
        let log = slog::Logger::root(slog::Discard, slog::o!());
        let input = RackNetworkConfigInput {
            rack_subnet: Some("fd00:1122:3344:1::/64".parse().unwrap()),
            // No switch has a BGP config.
            bgp_configs: BTreeMap::new(),
            // A single peerless port.
            ports: vec![PortInput {
                switch: SwitchSlot::Switch0,
                port_name: "qsfp0".to_string(),
                bgp_peers: vec![],
                addresses: vec![],
                links: vec![],
                routes: vec![],
                lldp: vec![],
                tx_eq: vec![],
            }],
            infra_ip_first: "fd00:1122:3344:1::1".parse().unwrap(),
            infra_ip_last: "fd00:1122:3344:1::2".parse().unwrap(),
            bfd: vec![],
        };
        let config = build_rack_network_config(&log, input)
            .expect("a valid subnet builds a config");
        assert_eq!(
            config.ports.len(),
            1,
            "the peerless port must be included, not dropped",
        );
    }
}
