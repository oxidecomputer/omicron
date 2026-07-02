// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Assembly of the bootstore config builder's inputs from database records.

use ipnetwork::IpNetwork;
use nexus_db_model::SwitchPort;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SwitchConfigData;
use nexus_db_queries::db::datastore::SwitchPortSettingsCombinedResult;
use nexus_switch_config::{
    AddressInput, BgpConfigInput, LinkInput, LldpInput, PortInput,
    RackNetworkConfigInput, collapse_bgp_configs,
};
use omicron_common::api::external::Error;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::TxEqConfig;
use slog::Logger;
use slog::warn;
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

    // Each `(switch, BGP config)` association a peer makes.
    //
    // We have to be a bit careful here -- note that many peers reference the
    // same config, so an association can appear more than once. `assemble`
    // dedups these configs, marking disagreements with `Conflicting`.
    let mut switch_bgp_configs: Vec<(SwitchSlot, BgpConfigInput)> = Vec::new();
    for (port, settings) in &applied_ports {
        let switch = SwitchSlot::from(port.switch_slot);
        for peer in &settings.bgp_peers {
            let id = peer.bgp_config_id();
            if let Some(cfg) = bgp_configs.get(&id) {
                // Resolve the config's announce set into the prefixes the
                // switch originates.
                let originate = cfg
                    .announcements
                    .iter()
                    .map(|a| a.network.into())
                    .collect();
                switch_bgp_configs.push((
                    switch,
                    BgpConfigInput {
                        id,
                        asn: cfg.config.asn.0,
                        originate,
                        checker: cfg.config.checker.clone(),
                        shaper: cfg.config.shaper.clone(),
                        max_paths: *cfg.config.max_paths,
                    },
                ));
            }
        }
    }

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
        switch_bgp_configs,
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
    switch_bgp_configs: Vec<(SwitchSlot, BgpConfigInput)>,
    infra_ip_first: IpAddr,
    infra_ip_last: IpAddr,
    bfd: Vec<BfdPeerConfig>,
) -> RackNetworkConfigInput {
    // Collapse the per-switch BGP associations into one `SwitchBgpConfig` per
    // switch, flagging conflicting entries.
    let bgp_configs = collapse_bgp_configs(switch_bgp_configs);

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
