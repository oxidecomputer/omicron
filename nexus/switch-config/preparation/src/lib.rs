// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Assembly of the bootstore config builder's inputs from database records.

use ipnetwork::IpNetwork;
use nexus_db_model::BgpConfig;
use nexus_db_model::SwitchPort;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SwitchConfigData;
use nexus_db_queries::db::datastore::SwitchPortSettingsCombinedResult;
use nexus_switch_config::{
    AddressInput, BgpConfigInput, LinkInput, LldpInput, PortInput,
    RackNetworkConfigInput,
};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::BgpAnnounceSetUuid;
use omicron_uuid_kinds::BgpConfigUuid;
use oxnet::IpNet;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::TxEqConfig;
use slog::Logger;
use slog::warn;
use std::collections::HashMap;
use std::net::IpAddr;

/// Read the bootstore network config inputs from the database and assemble them
/// into a [`RackNetworkConfigInput`].
pub async fn read_and_assemble(
    datastore: &DataStore,
    opctx: &OpContext,
    log: &Logger,
    rack_subnet: Option<IpNetwork>,
) -> Result<RackNetworkConfigInput, Error> {
    let SwitchConfigData {
        applied_ports,
        bgp_configs,
        infra_blocks,
        bfd_sessions,
    } = datastore.switch_config_read(opctx).await?;

    // One BGP config per switch. Every peer on a switch references that switch's
    // single BGP config, so associate the first one we see per switch.
    let mut switch_bgp_config: HashMap<SwitchSlot, (BgpConfigUuid, BgpConfig)> =
        HashMap::new();
    for (port, settings) in &applied_ports {
        let switch = SwitchSlot::from(port.switch_slot);
        for peer in &settings.bgp_peers {
            let id = peer.bgp_config_id();
            if let Some(cfg) = bgp_configs.get(&id) {
                switch_bgp_config
                    .entry(switch)
                    .or_insert_with(|| (id, cfg.config.clone()));
            }
        }
    }

    // Resolve each config's announce set into the prefixes the switch
    // originates.
    let bgp_announce_prefixes: HashMap<BgpAnnounceSetUuid, Vec<IpNet>> =
        bgp_configs
            .iter()
            .map(|cfg| {
                let prefixes = cfg
                    .announcements
                    .iter()
                    .map(|a| a.network.into())
                    .collect();
                (cfg.config.bgp_announce_set_id(), prefixes)
            })
            .collect();

    // The infra IP range comes from the (single) block of the infra address
    // lot.
    if infra_blocks.len() > 1 {
        warn!(
            log,
            "more than one block assigned to infra lot";
            "blocks" => ?infra_blocks,
        );
    }
    let (infra_ip_first, infra_ip_last) = match infra_blocks.first() {
        Some(block) => (block.first_address.ip(), block.last_address.ip()),
        None => {
            return Err(Error::internal_error(
                "no blocks assigned to infra lot",
            ));
        }
    };

    // Translate the BFD sessions into the bootstore's peer configs.
    let mut bfd = Vec::with_capacity(bfd_sessions.len());
    for session in bfd_sessions {
        bfd.push(BfdPeerConfig {
            local: session.local.map(|x| x.ip()),
            remote: session.remote.ip(),
            detection_threshold: session
                .detection_threshold
                .0
                .try_into()
                .map_err(|_| {
                    Error::internal_error(&format!(
                        "bfd detection threshold overflow: {}",
                        session.detection_threshold.0,
                    ))
                })?,
            required_rx: session.required_rx.0.into(),
            mode: session.mode.into(),
            switch: session.switch_slot.into(),
        });
    }

    let applied_port_refs: Vec<(
        SwitchSlot,
        &SwitchPort,
        &SwitchPortSettingsCombinedResult,
    )> = applied_ports
        .iter()
        .map(|(port, settings)| {
            (SwitchSlot::from(port.switch_slot), port, settings)
        })
        .collect();

    Ok(assemble(
        rack_subnet,
        &applied_port_refs,
        &switch_bgp_config,
        &bgp_announce_prefixes,
        infra_ip_first,
        infra_ip_last,
        bfd,
    ))
}

/// Assemble the [`RackNetworkConfigInput`] for the bootstore config builder
/// from already-read database records.
///
/// This is a pure translation step (db row to owned domain type). The reads
/// happen in [`read_and_assemble`], which calls this.
pub fn assemble(
    rack_subnet: Option<IpNetwork>,
    applied_ports: &[(
        SwitchSlot,
        &SwitchPort,
        &SwitchPortSettingsCombinedResult,
    )],
    switch_bgp_config: &HashMap<SwitchSlot, (BgpConfigUuid, BgpConfig)>,
    bgp_announce_prefixes: &HashMap<BgpAnnounceSetUuid, Vec<IpNet>>,
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
                .get(&config.bgp_announce_set_id())
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
            .map(|r| RouteConfig {
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
            .map(|c| TxEqConfig {
                pre1: c.pre1,
                pre2: c.pre2,
                main: c.main,
                post2: c.post2,
                post1: c.post1,
            })
            .collect(),
    }
}
