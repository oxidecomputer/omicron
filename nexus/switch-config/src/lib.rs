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
//!
//! The crate still depends on `nexus-db-model` and `nexus-db-queries` for input
//! types; later changes will replace those with domain types to drop the
//! dependency.

use ipnetwork::IpNetwork;
use nexus_db_model::{BgpConfig, SwitchLinkSpeed};
use nexus_db_queries::db::datastore::SwitchPortSettingsCombinedResult;
use oxnet::IpNet;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::BgpConfig as SledBgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig as SledBgpPeerConfig;
use sled_agent_types::early_networking::InvalidIpAddrError;
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
use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;

/// Inputs to [`build_rack_network_config`].
pub struct RackNetworkConfigInput<'a> {
    pub rack: &'a nexus_db_model::Rack,
    pub applied_ports: &'a [(
        SwitchSlot,
        &'a nexus_db_model::SwitchPort,
        &'a SwitchPortSettingsCombinedResult,
    )],
    pub switch_bgp_config: &'a HashMap<SwitchSlot, (Uuid, BgpConfig)>,
    pub bgp_announce_prefixes: &'a HashMap<Uuid, Vec<IpNet>>,
    pub infra_ip_first: IpAddr,
    pub infra_ip_last: IpAddr,
    pub bfd: Vec<BfdPeerConfig>,
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
    input: RackNetworkConfigInput<'_>,
) -> Option<RackNetworkConfig> {
    let RackNetworkConfigInput {
        rack,
        applied_ports,
        switch_bgp_config,
        bgp_announce_prefixes,
        infra_ip_first,
        infra_ip_last,
        bfd,
    } = input;

    // build the desired bootstore config from the records we've fetched
    let subnet = match rack.rack_subnet {
        Some(IpNetwork::V6(subnet)) => subnet.into(),
        Some(IpNetwork::V4(_)) => {
            error!(log, "rack subnet must be ipv6"; "rack" => ?rack);
            return None;
        }
        None => {
            error!(log, "rack subnet not set"; "rack" => ?rack);
            return None;
        }
    };

    // TODO: is this correct? Do we place the BgpConfig for both switches in a single Vec to send to the bootstore?
    let mut bgp: Vec<SledBgpConfig> = switch_bgp_config
        .iter()
        .filter_map(|(_location, (_id, config))| {
            let announcements =
                bgp_announce_prefixes.get(&config.bgp_announce_set_id).expect(
                    "bgp config is present but announce set is not populated",
                );

            let max_paths = match MaxPathConfig::new(*config.max_paths) {
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
                asn: config.asn.0,
                originate: announcements.clone(),
                checker: config.checker.clone(),
                shaper: config.shaper.clone(),
                max_paths,
            })
        })
        .collect();

    bgp.dedup();

    let mut ports: Vec<PortConfig> = vec![];

    for &(switch_slot, port, info) in applied_ports {
        // TODO https://github.com/oxidecomputer/omicron/issues/3062
        let tx_eq = if let Some(c) = info.tx_eq.get(0) {
            Some(TxEqConfig {
                pre1: c.pre1,
                pre2: c.pre2,
                main: c.main,
                post2: c.post2,
                post1: c.post1,
            })
        } else {
            None
        };

        // Build the bootstore BGP peers from the switch port settings we
        // already fetched. `info.bgp_peers` includes each peer's communities
        // and import/export policies, though not the ASN.
        let bgp_peers: Vec<SledBgpPeerConfig> = if info.bgp_peers.is_empty() {
            Vec::new()
        } else {
            // The peer ASN comes from the switch's BGP config.
            let asn = match switch_bgp_config.get(&switch_slot) {
                Some((_id, config)) => config.asn.0,
                None => {
                    // XXX: The port has BGP peers but no ASN configured. This
                    // is an error, but we continue for now. We should
                    // completely fail the task instead.
                    error!(
                        log,
                        "no bgp config for switch; skipping port for bootstore";
                        "switch_slot" => ?switch_slot,
                        "port" => &port.port_name.to_string(),
                    );
                    continue;
                }
            };
            info.bgp_peers
                .iter()
                .map(|peer| {
                    let peer = peer.as_bgp_peer();
                    SledBgpPeerConfig {
                        asn,
                        port: port.port_name.to_string(),
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
                    }
                })
                .collect()
        };

        let addresses = match info
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
                    "switch_slot" => ?switch_slot,
                    "port" => &port.port_name.to_string(),
                    InlineErrorChain::new(&err),
                );
                continue;
            }
        };

        let port_config = PortConfig {
            addresses,
            autoneg: info
                .links
                .get(0) //TODO breakout support
                .map(|l| l.autoneg)
                .unwrap_or(false),
            bgp_peers,
            port: port.port_name.to_string(),
            routes: info
                .routes
                .iter()
                .map(|r| SledRouteConfig {
                    destination: r.dst.into(),
                    nexthop: r.gw.ip(),
                    vlan_id: r.vid.map(|x| x.0),
                    rib_priority: r.rib_priority.map(|x| x.0),
                })
                .collect(),
            switch: switch_slot,
            uplink_port_fec: info
                .links
                .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                .map(|l| l.fec.map(|fec| fec.into()))
                .unwrap_or(None),
            uplink_port_speed: info
                .links
                .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                .map(|l| l.speed)
                .unwrap_or(SwitchLinkSpeed::Speed100G)
                .into(),
            lldp: info
                .link_lldp
                .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                .map(|c| LldpPortConfig {
                    status: match c.enabled {
                        true => LldpAdminStatus::Enabled,
                        false => LldpAdminStatus::Disabled,
                    },
                    port_id: c.link_name.clone().map(|p| p.to_string()),
                    port_description: c.link_description.clone(),
                    chassis_id: c.chassis_id.clone(),
                    system_name: c.system_name.clone(),
                    system_description: c.system_description.clone(),
                    management_addrs: c.management_ip.map(|a| vec![a.ip()]),
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
