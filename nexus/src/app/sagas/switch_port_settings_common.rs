use super::NexusActionContext;
use crate::app::map_switch_zone_addrs;
use crate::Nexus;
use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortFec, PortSettings, PortSpeed,
    RouteSettingsV4, RouteSettingsV6,
};
use dpd_client::{Ipv4Cidr, Ipv6Cidr};
use internal_dns::ServiceName;
use ipnetwork::IpNetwork;
use mg_admin_client::types::Prefix4;
use mg_admin_client::types::{ApplyRequest, BgpPeerConfig};
use nexus_db_model::{SwitchLinkFec, SwitchLinkSpeed};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_types::external_api::params;
use omicron_common::address::SLED_AGENT_PORT;
use omicron_common::api::external::NameOrId;
use omicron_common::api::internal::shared::{
    ParseSwitchLocationError, SwitchLocation,
};
use sled_agent_client::types::PortConfigV1;
use sled_agent_client::types::RouteConfig;
use sled_agent_client::types::{BgpConfig, EarlyNetworkConfig};
use sled_agent_client::types::{
    BgpPeerConfig as OmicronBgpPeerConfig, HostPortConfig,
};
use std::collections::HashMap;
use std::net::SocketAddrV6;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

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
                    autoneg: false,
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

    for r in &settings.routes {
        match &r.dst {
            IpNetwork::V4(n) => {
                let gw = match r.gw.ip() {
                    IpAddr::V4(gw) => gw,
                    IpAddr::V6(_) => {
                        return Err(
                            "IPv4 destination cannot have IPv6 nexthop".into()
                        )
                    }
                };
                dpd_port_settings.v4_routes.insert(
                    Ipv4Cidr { prefix: n.ip(), prefix_len: n.prefix() }
                        .to_string(),
                    vec![RouteSettingsV4 { link_id: link_id.0, nexthop: gw }],
                );
            }
            IpNetwork::V6(n) => {
                let gw = match r.gw.ip() {
                    IpAddr::V6(gw) => gw,
                    IpAddr::V4(_) => {
                        return Err(
                            "IPv6 destination cannot have IPv4 nexthop".into()
                        )
                    }
                };
                dpd_port_settings.v6_routes.insert(
                    Ipv6Cidr { prefix: n.ip(), prefix_len: n.prefix() }
                        .to_string(),
                    vec![RouteSettingsV6 { link_id: link_id.0, nexthop: gw }],
                );
            }
        }
    }

    Ok(dpd_port_settings)
}

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

pub(crate) async fn ensure_switch_port_uplink(
    sagactx: NexusActionContext,
    opctx: &OpContext,
    skip_self: bool,
    inject: Option<Uuid>,
    switch_port_id: Uuid,
    switch_port_name: String,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();

    let switch_port =
        nexus.get_switch_port(&opctx, switch_port_id).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for uplink: {e}"
            ))
        })?;

    let switch_location: SwitchLocation =
        switch_port.switch_location.parse().map_err(|e| {
            ActionError::action_failed(format!(
                "get switch location for uplink: {e:?}",
            ))
        })?;

    let mut uplinks: Vec<HostPortConfig> = Vec::new();

    // The sled agent uplinks interface is an all or nothing interface, so we
    // need to get all the uplink configs for all the ports.
    let active_ports =
        nexus.active_port_settings(&opctx).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get active switch port settings: {e}"
            ))
        })?;

    for (port, info) in &active_ports {
        // Since we are undoing establishing uplinks for the settings
        // associated with this port we skip adding this ports uplinks
        // to the list - effectively removing them.
        if skip_self && port.id == switch_port.id {
            continue;
        }
        uplinks.push(HostPortConfig {
            port: port.port_name.clone(),
            addrs: info.addresses.iter().map(|a| a.address).collect(),
        })
    }

    if let Some(id) = inject {
        let settings = nexus
            .switch_port_settings_get(&opctx, &id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "get switch port settings for injection: {e}"
                ))
            })?;
        uplinks.push(HostPortConfig {
            port: switch_port_name.clone(),
            addrs: settings.addresses.iter().map(|a| a.address).collect(),
        })
    }

    let sc = switch_sled_agent(switch_location, &sagactx).await?;
    sc.uplink_ensure(&sled_agent_client::types::SwitchPorts { uplinks })
        .await
        .map_err(|e| {
            ActionError::action_failed(format!("ensure uplink: {e}"))
        })?;

    Ok(())
}

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

pub(crate) async fn select_mg_client(
    sagactx: &NexusActionContext,
    opctx: &OpContext,
    switch_port_id: Uuid,
) -> Result<Arc<mg_admin_client::Client>, ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();

    let switch_port =
        nexus.get_switch_port(&opctx, switch_port_id).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for mg client selection: {e}"
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

    let mg_client: Arc<mg_admin_client::Client> = osagactx
        .nexus()
        .mg_clients
        .get(&switch_location)
        .ok_or_else(|| {
            ActionError::action_failed(format!(
                "requested switch not available: {switch_location}"
            ))
        })?
        .clone();
    Ok(mg_client)
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

pub(crate) async fn ensure_switch_port_bgp_settings(
    sagactx: NexusActionContext,
    opctx: &OpContext,
    settings: SwitchPortSettingsCombinedResult,
    switch_port_name: String,
    switch_port_id: Uuid,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let mg_client: Arc<mg_admin_client::Client> =
        select_mg_client(&sagactx, opctx, switch_port_id).await.map_err(
            |e| ActionError::action_failed(format!("select mg client: {e}")),
        )?;

    let mut bgp_peer_configs = Vec::new();

    for peer in settings.bgp_peers {
        let config = nexus
            .bgp_config_get(&opctx, peer.bgp_config_id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!("get bgp config: {e}"))
            })?;

        let announcements = nexus
            .bgp_announce_list(
                &opctx,
                &params::BgpAnnounceSetSelector {
                    name_or_id: NameOrId::Id(config.bgp_announce_set_id),
                },
            )
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "get bgp announcements: {e}"
                ))
            })?;

        let mut prefixes = Vec::new();
        for a in &announcements {
            let value = match a.network.ip() {
                IpAddr::V4(value) => Ok(value),
                IpAddr::V6(_) => Err(ActionError::action_failed(
                    "IPv6 announcement not yet supported".to_string(),
                )),
            }?;
            prefixes.push(Prefix4 { value, length: a.network.prefix() });
        }

        let bpc = BgpPeerConfig {
            asn: *config.asn,
            name: format!("{}", peer.addr.ip()), //TODO user defined name?
            host: format!("{}:179", peer.addr.ip()),
            hold_time: peer.hold_time.0.into(),
            idle_hold_time: peer.idle_hold_time.0.into(),
            delay_open: peer.delay_open.0.into(),
            connect_retry: peer.connect_retry.0.into(),
            keepalive: peer.keepalive.0.into(),
            resolution: BGP_SESSION_RESOLUTION,
            originate: prefixes,
        };

        bgp_peer_configs.push(bpc);
    }

    mg_client
        .inner
        .bgp_apply(&ApplyRequest {
            peer_group: switch_port_name,
            peers: bgp_peer_configs,
        })
        .await
        .map_err(|e| {
            ActionError::action_failed(format!("apply bgp settings: {e}"))
        })?;

    Ok(())
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

pub(crate) async fn select_dendrite_client(
    sagactx: &NexusActionContext,
    opctx: &OpContext,
    switch_port_id: Uuid,
) -> Result<Arc<dpd_client::Client>, ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();

    let switch_port =
        nexus.get_switch_port(&opctx, switch_port_id).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for dendrite client selection {e}"
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

    let dpd_client: Arc<dpd_client::Client> = osagactx
        .nexus()
        .dpd_clients
        .get(&switch_location)
        .ok_or_else(|| {
            ActionError::action_failed(format!(
                "requested switch not available: {switch_location}"
            ))
        })?
        .clone();
    Ok(dpd_client)
}
