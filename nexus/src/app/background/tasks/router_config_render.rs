// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rendering router configurations (RFD 662) into the bootstore specs.
//!
//! Reads all `RouterConfiguration` rows (plus their BGP peers, static routes
//! and BFD peers) and renders them into the omicron-owned
//! [`RouterConfigSpec`] mirror types stored in the bootstore's
//! `SystemNetworkingConfig`. The scrimlet mgd reconciler turns them (plus
//! the uplink config) into one declarative mgd apply per switch.
//!
//! The built-in per-switch configurations ("default-switch0" /
//! "default-switch1") steer the daemon-owned default router of their own
//! switch's mgd in place (rendered as a spec named "default"). Each is only
//! included while non-empty: absent means the scrimlet reconciler keeps
//! rendering the default router from the uplink config, so the uplink path
//! keeps owning it until an operator puts contents here — at which point
//! the operator owns that switch's whole default router.

use mg_api_types::rdb::DEFAULT_RIB_PRIORITY_STATIC;
use nexus_db_model::{
    RouterConfiguration, RouterConfigurationBfdPeer,
    RouterConfigurationBgpPeer, RouterConfigurationStaticRoute,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::external_api::networking::BgpAnnounceSetSelector;
use nexus_types::identity::Resource;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::{BgpAnnounceSetUuid, GenericUuid};
use oxnet::IpNet;
use sled_agent_types::early_networking::{BfdMode, SwitchSlot};
use sled_agent_types::router_config::{
    RouterConfigBfdPeer, RouterConfigBgpPeer, RouterConfigBgpPeerParameters,
    RouterConfigBgpSpec, RouterConfigSpec, RouterConfigStaticRoute4,
    RouterConfigStaticRoute6, RouterConfigUnnumberedBgpPeer,
    SwitchRouterConfigs,
};
use slog_error_chain::InlineErrorChain;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::{NonZeroU8, NonZeroU32};

/// Render all router configurations into per-switch bootstore specs.
///
/// Fails as a whole if the configurations or their contents cannot be
/// loaded: a partial render must never reach the bootstore, because the
/// downstream mgd apply is total — a missing router would be torn down and
/// a router whose announce set failed to load would originate nothing.
pub async fn render_switch_router_configs(
    datastore: &DataStore,
    opctx: &OpContext,
    errors: &mut Vec<String>,
) -> Result<SwitchRouterConfigs, String> {
    let configs = load_configurations(datastore, opctx).await?;

    let ids: Vec<_> = configs.iter().map(|c| c.id()).collect();
    let (bgp_peers, static_routes, bfd_peers) = futures::try_join!(
        datastore.router_configuration_bgp_peer_list_batch(opctx, &ids),
        datastore.router_configuration_static_route_list_batch(opctx, &ids),
        datastore.router_configuration_bfd_peer_list_batch(opctx, &ids),
    )
    .map_err(|e| {
        format!(
            "failed to list router configuration contents: {}",
            InlineErrorChain::new(&e)
        )
    })?;

    let originate = load_originate_prefixes(datastore, opctx, &configs).await?;

    let mut rendered = SwitchRouterConfigs::new();
    for config in &configs {
        let is_builtin =
            nexus_types::router_configuration::is_builtin_router_configuration_id(
                &config.id().into_untyped_uuid(),
            );
        let id = config.id().into_untyped_uuid();
        let my = |cid: &nexus_db_model::DbTypedUuid<
            omicron_uuid_kinds::RouterConfigurationKind,
        >| cid.into_untyped_uuid() == id;
        let originate = config
            .bgp_config
            .as_ref()
            .map(|bgp| {
                originate
                    .get(&BgpAnnounceSetUuid::from(bgp.bgp_announce_set_id))
                    .cloned()
                    .expect("load_originate_prefixes covers every announce set")
            })
            .unwrap_or_default();
        let bgp = build_bgp_spec(
            config,
            &bgp_peers
                .iter()
                .filter(|p| my(&p.router_configuration_id))
                .collect::<Vec<_>>(),
            originate,
            errors,
        );
        let (static4, static6) = build_static_routes(
            config,
            &static_routes
                .iter()
                .filter(|r| my(&r.router_configuration_id))
                .collect::<Vec<_>>(),
            errors,
        );
        let bfd_peers = build_bfd_peers(
            config,
            &bfd_peers
                .iter()
                .filter(|p| my(&p.router_configuration_id))
                .collect::<Vec<_>>(),
            errors,
        );
        let spec = RouterConfigSpec {
            // mgd maps a spec named "default" onto its daemon-owned default
            // router; the built-ins configure their own switch's default
            // router in place.
            name: if is_builtin {
                "default".to_string()
            } else {
                config.name().to_string()
            },
            id,
            bgp,
            static4,
            static6,
            bfd_peers,
        };
        // Only once the operator has put contents in it: a "default" spec is
        // the router's complete desired state, so an empty one would wipe
        // the uplink BGP/static state the scrimlet reconciler renders from
        // the rack network config.
        if is_builtin && spec.is_empty() {
            continue;
        }
        rendered.entry(config.switch.into()).or_default().push(spec);
    }
    // Give every switch an entry, even an empty one: the scrimlet reconciler
    // always applies, so removed routers are torn down.
    for slot in [SwitchSlot::Switch0, SwitchSlot::Switch1] {
        rendered.entry(slot).or_default();
    }
    Ok(rendered)
}

async fn load_configurations(
    datastore: &DataStore,
    opctx: &OpContext,
) -> Result<Vec<RouterConfiguration>, String> {
    let mut configs = Vec::new();
    let mut paginator = Paginator::new(
        NonZeroU32::new(200).unwrap(),
        dropshot::PaginationOrder::Ascending,
    );
    while let Some(p) = paginator.next() {
        let batch = datastore
            .router_configuration_list(
                opctx,
                &PaginatedBy::Id(p.current_pagparams()),
            )
            .await
            .map_err(|e| {
                format!(
                    "failed to list router configurations: {}",
                    InlineErrorChain::new(&e)
                )
            })?;
        paginator = p.found_batch(&batch, &|c: &RouterConfiguration| {
            c.id().into_untyped_uuid()
        });
        configs.extend(batch);
    }
    Ok(configs)
}

/// Resolve the originated prefixes of each distinct announce set referenced
/// by the given configurations. Any failed load fails the render: rendering
/// such a configuration anyway would tell its BGP peers to originate
/// nothing.
async fn load_originate_prefixes(
    datastore: &DataStore,
    opctx: &OpContext,
    configs: &[RouterConfiguration],
) -> Result<HashMap<BgpAnnounceSetUuid, Vec<IpNet>>, String> {
    let mut originate = HashMap::new();
    for config in configs {
        let Some(bgp) = &config.bgp_config else {
            continue;
        };
        let set_id = BgpAnnounceSetUuid::from(bgp.bgp_announce_set_id);
        if originate.contains_key(&set_id) {
            continue;
        }
        let announcements = datastore
            .bgp_announcement_list(
                opctx,
                &BgpAnnounceSetSelector {
                    announce_set: NameOrId::Id(set_id.into_untyped_uuid()),
                },
            )
            .await
            .map_err(|e| {
                format!(
                    "failed to list announcements of announce set {set_id}: {}",
                    InlineErrorChain::new(&e)
                )
            })?;
        originate.insert(
            set_id,
            announcements.iter().map(|a| IpNet::from(a.network)).collect(),
        );
    }
    Ok(originate)
}

fn allowed_prefixes(
    list: &Option<Vec<ipnetwork::IpNetwork>>,
) -> Option<Vec<IpNet>> {
    list.as_ref().map(|list| list.iter().map(|n| IpNet::from(*n)).collect())
}

fn peer_parameters(
    peer: &RouterConfigurationBgpPeer,
) -> RouterConfigBgpPeerParameters {
    RouterConfigBgpPeerParameters {
        hold_time: u64::from(*peer.hold_time),
        idle_hold_time: u64::from(*peer.idle_hold_time),
        delay_open: u64::from(*peer.delay_open),
        connect_retry: u64::from(*peer.connect_retry),
        keepalive: u64::from(*peer.keepalive),
        remote_asn: peer.remote_asn.map(|v| *v),
        min_ttl: peer.min_ttl.map(|v| *v),
        md5_auth_key: peer.md5_auth_key.clone(),
        multi_exit_discriminator: peer.multi_exit_discriminator.map(|v| *v),
        communities: peer.communities.iter().map(|v| **v).collect(),
        local_pref: peer.local_pref.map(|v| *v),
        enforce_first_as: peer.enforce_first_as,
        vlan_id: peer.vlan_id.map(|v| *v),
        allowed_import: allowed_prefixes(&peer.allowed_import),
        allowed_export: allowed_prefixes(&peer.allowed_export),
    }
}

fn build_bgp_spec(
    config: &RouterConfiguration,
    peers: &[&RouterConfigurationBgpPeer],
    originate: Vec<IpNet>,
    errors: &mut Vec<String>,
) -> Option<RouterConfigBgpSpec> {
    let bgp = config.bgp_config.as_ref()?;
    let asn = *bgp.bgp_asn;
    let mut numbered = Vec::new();
    let mut unnumbered = Vec::new();
    for peer in peers {
        let parameters = peer_parameters(peer);
        match (peer.addr, &peer.port_name, peer.router_lifetime) {
            (Some(addr), None, None) => {
                numbered.push(RouterConfigBgpPeer {
                    name: peer.name.to_string(),
                    addr: addr.ip(),
                    parameters,
                });
            }
            (None, Some(port), Some(lifetime)) => {
                unnumbered.push(RouterConfigUnnumberedBgpPeer {
                    name: peer.name.to_string(),
                    port: port.to_string(),
                    router_lifetime: *lifetime,
                    parameters,
                });
            }
            _ => errors.push(format!(
                "router configuration {}: bgp peer {} is neither numbered \
                 nor unnumbered",
                config.name(),
                peer.name,
            )),
        }
    }
    Some(RouterConfigBgpSpec {
        asn,
        originate,
        // The RC data model has no max-paths knob yet; 1 (mgd's default).
        max_paths: None,
        checker: None,
        shaper: None,
        peers: numbered,
        unnumbered_peers: unnumbered,
    })
}

fn build_static_routes(
    config: &RouterConfiguration,
    routes: &[&RouterConfigurationStaticRoute],
    errors: &mut Vec<String>,
) -> (Vec<RouterConfigStaticRoute4>, Vec<RouterConfigStaticRoute6>) {
    let mut static4 = Vec::new();
    let mut static6 = Vec::new();
    for route in routes {
        let rib_priority = route
            .rib_priority
            .map(|v| *v)
            .unwrap_or(DEFAULT_RIB_PRIORITY_STATIC);
        let vlan_id = route.vlan_id.map(|v| *v);
        match IpNet::from(route.dst) {
            IpNet::V4(prefix) => static4.push(RouterConfigStaticRoute4 {
                prefix,
                nexthop: route.gw.ip(),
                vlan_id,
                rib_priority,
            }),
            IpNet::V6(prefix) => match route.gw.ip() {
                IpAddr::V6(nexthop) => static6.push(RouterConfigStaticRoute6 {
                    prefix,
                    nexthop,
                    vlan_id,
                    rib_priority,
                }),
                IpAddr::V4(gw) => errors.push(format!(
                    "router configuration {}: static route {} has an IPv6 \
                     destination but IPv4 gateway {gw}",
                    config.name(),
                    route.name,
                )),
            },
        }
    }
    (static4, static6)
}

fn build_bfd_peers(
    config: &RouterConfiguration,
    peers: &[&RouterConfigurationBfdPeer],
    errors: &mut Vec<String>,
) -> Vec<RouterConfigBfdPeer> {
    let mut bfd_peers = Vec::new();
    for peer in peers {
        let remote = peer.remote.ip();
        let Some(detection_threshold) =
            NonZeroU8::new(*peer.detection_threshold)
        else {
            errors.push(format!(
                "router configuration {}: bfd peer {} has a zero detection \
                 threshold",
                config.name(),
                peer.name,
            ));
            continue;
        };
        bfd_peers.push(RouterConfigBfdPeer {
            peer: remote,
            listen: peer.local.map(|l| l.ip()).unwrap_or(match remote {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            }),
            required_rx: u64::from(*peer.required_rx),
            detection_threshold,
            mode: match peer.mode {
                nexus_db_model::BfdMode::SingleHop => BfdMode::SingleHop,
                nexus_db_model::BfdMode::MultiHop => BfdMode::MultiHop,
            },
        });
    }
    bfd_peers
}
