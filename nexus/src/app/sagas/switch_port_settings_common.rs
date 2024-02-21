// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use crate::app::map_switch_zone_addrs;
use crate::Nexus;
use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortFec, PortSettings, PortSpeed,
};
use internal_dns::ServiceName;
use ipnetwork::IpNetwork;
use nexus_db_model::{SwitchLinkFec, SwitchLinkSpeed};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_types::external_api::params;
use omicron_common::address::SLED_AGENT_PORT;
use omicron_common::api::external::NameOrId;
use omicron_common::api::internal::shared::{
    ParseSwitchLocationError, SwitchLocation,
};
use sled_agent_client::types::BgpPeerConfig as OmicronBgpPeerConfig;
use sled_agent_client::types::PortConfigV1;
use sled_agent_client::types::RouteConfig;
use sled_agent_client::types::{BgpConfig, EarlyNetworkConfig};
use std::collections::HashMap;
use std::net::SocketAddrV6;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

pub(crate) fn api_to_dpd_port_settings(
    settings: &SwitchPortSettingsCombinedResult,
) -> Result<PortSettings, String> {
    let mut dpd_port_settings = PortSettings {
        links: HashMap::new(),
        v4_routes: HashMap::new(),
        v6_routes: HashMap::new(),
    };

    //TODO breakouts
    let link_id = LinkId(0);

    for l in settings.links.iter() {
        dpd_port_settings.links.insert(
            link_id.to_string(),
            LinkSettings {
                params: LinkCreate {
                    autoneg: l.autoneg,
                    lane: Some(LinkId(0)),
                    kr: false,
                    fec: match l.fec {
                        SwitchLinkFec::Firecode => PortFec::Firecode,
                        SwitchLinkFec::Rs => PortFec::Rs,
                        SwitchLinkFec::None => PortFec::None,
                    },
                    speed: match l.speed {
                        SwitchLinkSpeed::Speed0G => PortSpeed::Speed0G,
                        SwitchLinkSpeed::Speed1G => PortSpeed::Speed1G,
                        SwitchLinkSpeed::Speed10G => PortSpeed::Speed10G,
                        SwitchLinkSpeed::Speed25G => PortSpeed::Speed25G,
                        SwitchLinkSpeed::Speed40G => PortSpeed::Speed40G,
                        SwitchLinkSpeed::Speed50G => PortSpeed::Speed50G,
                        SwitchLinkSpeed::Speed100G => PortSpeed::Speed100G,
                        SwitchLinkSpeed::Speed200G => PortSpeed::Speed200G,
                        SwitchLinkSpeed::Speed400G => PortSpeed::Speed400G,
                    },
                },
                //TODO won't work for breakouts
                addrs: settings
                    .addresses
                    .iter()
                    .map(|a| a.address.ip())
                    .collect(),
            },
        );
    }

    Ok(dpd_port_settings)
}

/// This function updates the bootstore with the information required to provide external
/// connectivity to the rack during cold boot. This should be called whenever changes
/// to uplink configurations / external routing are made
pub(crate) fn apply_bootstore_update(
    config: &mut EarlyNetworkConfig,
    update: &EarlyNetworkPortUpdate,
) -> Result<BootstoreNetworkPortChange, ActionError> {
    let mut change = BootstoreNetworkPortChange::default();

    let rack_net_config = match &mut config.body.rack_network_config {
        Some(cfg) => cfg,
        None => {
            return Err(ActionError::action_failed(
                "rack network config not yet initialized".to_string(),
            ))
        }
    };

    for port in &mut rack_net_config.ports {
        if port.port == update.port.port {
            change.previous_port_config = Some(port.clone());
            *port = update.port.clone();
            break;
        }
    }
    if change.previous_port_config.is_none() {
        rack_net_config.ports.push(update.port.clone());
    }

    for updated_bgp in &update.bgp_configs {
        let mut exists = false;
        for resident_bgp in &mut rack_net_config.bgp {
            if resident_bgp.asn == updated_bgp.asn {
                change.changed_bgp_configs.push(resident_bgp.clone());
                *resident_bgp = updated_bgp.clone();
                exists = true;
                break;
            }
        }
        if !exists {
            change.added_bgp_configs.push(updated_bgp.clone());
        }
    }
    rack_net_config.bgp.extend_from_slice(&change.added_bgp_configs);

    Ok(change)
}

pub(crate) async fn bootstore_update(
    nexus: &Arc<Nexus>,
    opctx: &OpContext,
    switch_port_id: Uuid,
    switch_port_name: &str,
    settings: &SwitchPortSettingsCombinedResult,
) -> Result<EarlyNetworkPortUpdate, ActionError> {
    let switch_port =
        nexus.get_switch_port(&opctx, switch_port_id).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for uplink: {e}"
            ))
        })?;

    let switch_location: SwitchLocation =
        switch_port.switch_location.parse().map_err(
            |e: ParseSwitchLocationError| {
                ActionError::action_failed(format!(
                    "get switch location for uplink: {e:?}",
                ))
            },
        )?;

    let mut peer_info = Vec::new();
    let mut bgp_configs = Vec::new();
    for p in &settings.bgp_peers {
        let bgp_config = nexus
            .bgp_config_get(&opctx, p.bgp_config_id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!("get bgp config: {e}"))
            })?;

        let announcements = nexus
            .bgp_announce_list(
                &opctx,
                &params::BgpAnnounceSetSelector {
                    name_or_id: NameOrId::Id(bgp_config.bgp_announce_set_id),
                },
            )
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "get bgp announcements: {e}"
                ))
            })?;

        peer_info.push((p, bgp_config.asn.0));
        bgp_configs.push(BgpConfig {
            asn: bgp_config.asn.0,
            originate: announcements
                .iter()
                .filter_map(|a| match a.network {
                    IpNetwork::V4(net) => Some(net.into()),
                    //TODO v6
                    _ => None,
                })
                .collect(),
        });
    }

    let update = EarlyNetworkPortUpdate {
        port: PortConfigV1 {
            routes: settings
                .routes
                .iter()
                .map(|r| RouteConfig { destination: r.dst, nexthop: r.gw.ip() })
                .collect(),
            addresses: settings.addresses.iter().map(|a| a.address).collect(),
            switch: switch_location,
            port: switch_port_name.into(),
            uplink_port_fec: settings
                .links
                .get(0)
                .map(|l| l.fec)
                .unwrap_or(SwitchLinkFec::None)
                .into(),
            uplink_port_speed: settings
                .links
                .get(0)
                .map(|l| l.speed)
                .unwrap_or(SwitchLinkSpeed::Speed100G)
                .into(),
            autoneg: settings.links.get(0).map(|l| l.autoneg).unwrap_or(false),
            bgp_peers: peer_info
                .iter()
                .filter_map(|(p, asn)| {
                    //TODO v6
                    match p.addr.ip() {
                        IpAddr::V4(addr) => Some(OmicronBgpPeerConfig {
                            asn: *asn,
                            port: switch_port_name.into(),
                            addr,
                            hold_time: Some(p.hold_time.0.into()),
                            connect_retry: Some(p.connect_retry.0.into()),
                            delay_open: Some(p.delay_open.0.into()),
                            idle_hold_time: Some(p.idle_hold_time.0.into()),
                            keepalive: Some(p.keepalive.0.into()),
                        }),
                        IpAddr::V6(_) => {
                            warn!(opctx.log, "IPv6 peers not yet supported");
                            None
                        }
                    }
                })
                .collect(),
        },
        bgp_configs,
    };

    Ok(update)
}

/// Updates SMF properties in switch zone on scrimlets
/// These SMF properties are used to notify rack networking services which
/// ports are being used as uplinks, as they required additional configuration
/// for external connectivity

pub(crate) async fn read_bootstore_config(
    sa: &sled_agent_client::Client,
) -> Result<EarlyNetworkConfig, ActionError> {
    Ok(sa
        .read_network_bootstore_config_cache()
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "read bootstore network config: {e}"
            ))
        })?
        .into_inner())
}

pub(crate) async fn write_bootstore_config(
    sa: &sled_agent_client::Client,
    config: &EarlyNetworkConfig,
) -> Result<(), ActionError> {
    sa.write_network_bootstore_config(config).await.map_err(|e| {
        ActionError::action_failed(format!(
            "write bootstore network config: {e}"
        ))
    })?;
    Ok(())
}

pub(crate) async fn switch_sled_agent(
    location: SwitchLocation,
    sagactx: &NexusActionContext,
) -> Result<sled_agent_client::Client, ActionError> {
    let nexus = sagactx.user_data().nexus();
    let sled_agent_addr = get_scrimlet_address(location, nexus).await?;
    Ok(sled_agent_client::Client::new(
        &format!("http://{}", sled_agent_addr),
        sagactx.user_data().log().clone(),
    ))
}

pub(crate) async fn get_scrimlet_address(
    location: SwitchLocation,
    nexus: &Arc<Nexus>,
) -> Result<SocketAddrV6, ActionError> {
    /* TODO this depends on DNS entries only coming from RSS, it's broken
            on the upgrade path
    nexus
        .resolver()
        .await
        .lookup_socket_v6(ServiceName::Scrimlet(location))
        .await
        .map_err(|e| e.to_string())
        .map_err(|e| {
            ActionError::action_failed(format!(
                "scrimlet dns lookup failed {e}",
            ))
        })
    */
    let result = nexus
        .resolver()
        .await
        .lookup_all_ipv6(ServiceName::Dendrite)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "scrimlet dns lookup failed {e}",
            ))
        });

    let mappings = match result {
        Ok(addrs) => map_switch_zone_addrs(&nexus.log, addrs).await,
        Err(e) => {
            warn!(nexus.log, "Failed to lookup Dendrite address: {e}");
            return Err(ActionError::action_failed(format!(
                "switch mapping failed {e}",
            )));
        }
    };

    let addr = match mappings.get(&location) {
        Some(addr) => addr,
        None => {
            return Err(ActionError::action_failed(format!(
                "address for switch at location: {location} not found",
            )));
        }
    };

    let mut segments = addr.segments();
    segments[7] = 1;
    let addr = Ipv6Addr::from(segments);

    Ok(SocketAddrV6::new(addr, SLED_AGENT_PORT, 0, 0))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BootstoreNetworkPortChange {
    previous_port_config: Option<PortConfigV1>,
    changed_bgp_configs: Vec<BgpConfig>,
    added_bgp_configs: Vec<BgpConfig>,
}

#[derive(Clone, Debug)]
pub struct EarlyNetworkPortUpdate {
    port: PortConfigV1,
    bgp_configs: Vec<BgpConfig>,
}
