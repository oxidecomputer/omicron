// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Assembly of the bootstore config builder's inputs from database records.

use ipnetwork::IpNetwork;
use nexus_bootstore_config::{
    AddressInput, BgpConfigInput, LinkInput, LldpInput, PortInput,
    RackNetworkConfigInput, RouteInput, TxEqInput,
};
use nexus_db_model::BgpConfig;
use nexus_db_model::SwitchPort;
use nexus_db_queries::db::datastore::SwitchPortSettingsCombinedResult;
use oxnet::IpNet;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::SwitchSlot;
use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;

/// Assemble the [`RackNetworkConfigInput`] for the bootstore config builder
/// from already-read database records.
///
/// This is a pure translation step (db row to owned domain type). In the
/// future, we'll move the reads into this crate so that the whole assembly
/// happens in a single transaction.
pub fn assemble(
    rack_subnet: Option<IpNetwork>,
    applied_ports: &[(
        SwitchSlot,
        &SwitchPort,
        &SwitchPortSettingsCombinedResult,
    )],
    switch_bgp_config: &HashMap<SwitchSlot, (Uuid, BgpConfig)>,
    bgp_announce_prefixes: &HashMap<Uuid, Vec<IpNet>>,
    infra_ip_first: IpAddr,
    infra_ip_last: IpAddr,
    bfd: Vec<BfdPeerConfig>,
) -> RackNetworkConfigInput {
    // One BGP config per switch, with its announce-set prefixes resolved into
    // the prefixes the switch originates.
    let bgp_configs = switch_bgp_config
        .iter()
        .map(|(switch_slot, (_id, config))| {
            let originate = bgp_announce_prefixes
                .get(&config.bgp_announce_set_id)
                .expect(
                    "bgp config is present but announce set is not populated",
                )
                .clone();
            (
                *switch_slot,
                BgpConfigInput {
                    asn: config.asn.0,
                    originate,
                    checker: config.checker.clone(),
                    shaper: config.shaper.clone(),
                    max_paths: *config.max_paths,
                },
            )
        })
        .collect();

    let ports = applied_ports
        .iter()
        .map(|&(switch, port, info)| port_input_from_db(switch, port, info))
        .collect();

    RackNetworkConfigInput {
        rack_subnet,
        bgp_configs,
        ports,
        infra_ip_first,
        infra_ip_last,
        bfd,
    }
}

fn port_input_from_db(
    switch: SwitchSlot,
    port: &SwitchPort,
    info: &SwitchPortSettingsCombinedResult,
) -> PortInput {
    PortInput {
        switch,
        port_name: port.port_name.to_string(),
        bgp_peers: info
            .bgp_peers
            .iter()
            .map(|peer| peer.as_bgp_peer().clone())
            .collect(),
        addresses: info
            .addresses
            .iter()
            .map(|a| AddressInput { address: a.address, vlan_id: a.vlan_id })
            .collect(),
        links: info
            .links
            .iter()
            .map(|l| LinkInput {
                autoneg: l.autoneg,
                fec: l.fec.map(Into::into),
                speed: l.speed.into(),
            })
            .collect(),
        routes: info
            .routes
            .iter()
            .map(|r| RouteInput {
                destination: r.dst.into(),
                nexthop: r.gw.ip(),
                vlan_id: r.vid.map(|x| x.0),
                rib_priority: r.rib_priority.map(|x| x.0),
            })
            .collect(),
        lldp: info
            .link_lldp
            .iter()
            .map(|c| LldpInput {
                enabled: c.enabled,
                link_name: c.link_name.clone(),
                link_description: c.link_description.clone(),
                chassis_id: c.chassis_id.clone(),
                system_name: c.system_name.clone(),
                system_description: c.system_description.clone(),
                management_ip: c.management_ip.map(|a| a.ip()),
            })
            .collect(),
        tx_eq: info
            .tx_eq
            .iter()
            .map(|c| TxEqInput {
                pre1: c.pre1,
                pre2: c.pre2,
                main: c.main,
                post2: c.post2,
                post1: c.post1,
            })
            .collect(),
    }
}
