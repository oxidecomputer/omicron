// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task propagating router configurations (RFD 662) to mgd.
//!
//! Reads all `RouterConfiguration` rows (plus their BGP peers, static routes
//! and BFD peers), builds one declarative `MultiRouterApplyRequest` per
//! switch, and PUTs it to that switch's mgd. The apply is total: routers
//! absent from the request are torn down by mgd (except the daemon-owned
//! "default" router), so an empty request is meaningful and is still sent.

use crate::app::background::BackgroundTask;
use crate::app::switch_zone_address_mappings;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use mg_api_types::bfd::{BfdPeerConfig, SessionMode};
use mg_api_types::bgp::config::{
    BgpPeerConfig, BgpPeerParameters, Ipv4UnicastConfig, Ipv6UnicastConfig,
    JitterRange, UnnumberedBgpPeerConfig,
};
use mg_api_types::bgp::policy::{ImportExportPolicy4, ImportExportPolicy6};
use mg_api_types::rdb::DEFAULT_RIB_PRIORITY_STATIC;
use mg_api_types::router::{
    BgpSpec, MultiRouterApplyRequest, RouterId, RouterSpec,
};
use mg_api_types::static_routes::{StaticRoute4, StaticRoute6};
use nexus_db_model::{
    RouterConfiguration, RouterConfigurationBfdPeer,
    RouterConfigurationBgpPeer, RouterConfigurationStaticRoute,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::external_api::networking::BgpAnnounceSetSelector;
use nexus_types::identity::Resource;
use omicron_common::address::MGD_PORT;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::{BgpAnnounceSetUuid, GenericUuid};
use oxnet::{IpNet, SocketAddrJson};
use serde_json::json;
use sled_agent_types::early_networking::SwitchSlot;
use slog_error_chain::InlineErrorChain;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::num::{NonZeroU8, NonZeroU32};
use std::sync::Arc;

const BGP_PORT: u16 = 179;
// See sled-agent/scrimlet-reconcilers: max time the peering engine waits for
// external messages before checking shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

pub struct RouterConfigurationReconciler {
    datastore: Arc<DataStore>,
    resolver: Resolver,
}

impl RouterConfigurationReconciler {
    pub fn new(datastore: Arc<DataStore>, resolver: Resolver) -> Self {
        Self { datastore, resolver }
    }

    async fn load_configurations(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RouterConfiguration>, String> {
        let mut configs = Vec::new();
        let mut paginator = Paginator::new(
            NonZeroU32::new(200).unwrap(),
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .datastore
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

    /// Resolve the originated prefixes of each distinct announce set
    /// referenced by the given configurations.
    async fn load_originate_prefixes(
        &self,
        opctx: &OpContext,
        configs: &[RouterConfiguration],
        errors: &mut Vec<String>,
    ) -> HashMap<BgpAnnounceSetUuid, Vec<IpNet>> {
        let mut originate = HashMap::new();
        for config in configs {
            let Some(bgp) = &config.bgp_config else {
                continue;
            };
            let set_id = BgpAnnounceSetUuid::from(bgp.bgp_announce_set_id);
            if originate.contains_key(&set_id) {
                continue;
            }
            match self
                .datastore
                .bgp_announcement_list(
                    opctx,
                    &BgpAnnounceSetSelector {
                        announce_set: NameOrId::Id(set_id.into_untyped_uuid()),
                    },
                )
                .await
            {
                Ok(announcements) => {
                    originate.insert(
                        set_id,
                        announcements
                            .iter()
                            .map(|a| IpNet::from(a.network))
                            .collect(),
                    );
                }
                Err(e) => errors.push(format!(
                    "failed to list announcements of announce set {set_id}: {}",
                    InlineErrorChain::new(&e)
                )),
            }
        }
        originate
    }
}

fn policy4(list: &Option<Vec<ipnetwork::IpNetwork>>) -> ImportExportPolicy4 {
    match list {
        None => ImportExportPolicy4::NoFiltering,
        Some(list) => ImportExportPolicy4::Allow(
            list.iter()
                .filter_map(|n| match IpNet::from(*n) {
                    IpNet::V4(net) => Some(net),
                    IpNet::V6(_) => None,
                })
                .collect(),
        ),
    }
}

fn policy6(list: &Option<Vec<ipnetwork::IpNetwork>>) -> ImportExportPolicy6 {
    match list {
        None => ImportExportPolicy6::NoFiltering,
        Some(list) => ImportExportPolicy6::Allow(
            list.iter()
                .filter_map(|n| match IpNet::from(*n) {
                    IpNet::V6(net) => Some(net),
                    IpNet::V4(_) => None,
                })
                .collect(),
        ),
    }
}

fn peer_parameters(peer: &RouterConfigurationBgpPeer) -> BgpPeerParameters {
    BgpPeerParameters {
        hold_time: u64::from(*peer.hold_time),
        idle_hold_time: u64::from(*peer.idle_hold_time),
        delay_open: u64::from(*peer.delay_open),
        connect_retry: u64::from(*peer.connect_retry),
        keepalive: u64::from(*peer.keepalive),
        resolution: BGP_SESSION_RESOLUTION,
        passive: false,
        remote_asn: peer.remote_asn.map(|v| *v),
        min_ttl: peer.min_ttl.map(|v| *v),
        md5_auth_key: peer.md5_auth_key.clone(),
        multi_exit_discriminator: peer.multi_exit_discriminator.map(|v| *v),
        communities: peer.communities.iter().map(|v| **v).collect(),
        local_pref: peer.local_pref.map(|v| *v),
        enforce_first_as: peer.enforce_first_as,
        vlan_id: peer.vlan_id.map(|v| *v),
        ipv4_unicast: Some(Ipv4UnicastConfig {
            nexthop: None,
            import_policy: policy4(&peer.allowed_import),
            export_policy: policy4(&peer.allowed_export),
        }),
        ipv6_unicast: Some(Ipv6UnicastConfig {
            nexthop: None,
            import_policy: policy6(&peer.allowed_import),
            export_policy: policy6(&peer.allowed_export),
        }),
        deterministic_collision_resolution: false,
        idle_hold_jitter: None,
        connect_retry_jitter: Some(JitterRange { min: 0.75, max: 1.0 }),
        src_addr: None,
        src_port: None,
    }
}

fn build_bgp_spec(
    config: &RouterConfiguration,
    peers: &[&RouterConfigurationBgpPeer],
    originate: Vec<IpNet>,
    errors: &mut Vec<String>,
) -> Option<BgpSpec> {
    let bgp = config.bgp_config.as_ref()?;
    let asn = *bgp.bgp_asn;
    let mut numbered: HashMap<String, Vec<BgpPeerConfig>> = HashMap::new();
    let mut unnumbered: HashMap<String, Vec<UnnumberedBgpPeerConfig>> =
        HashMap::new();
    for peer in peers {
        let parameters = peer_parameters(peer);
        match (peer.addr, &peer.port_name, peer.router_lifetime) {
            (Some(addr), None, None) => {
                numbered.entry("default".to_string()).or_default().push(
                    BgpPeerConfig {
                        host: SocketAddrJson::from(SocketAddr::new(
                            addr.ip(),
                            BGP_PORT,
                        )),
                        name: peer.name.to_string(),
                        parameters,
                    },
                );
            }
            (None, Some(port), Some(lifetime)) => {
                unnumbered.entry(port.to_string()).or_default().push(
                    UnnumberedBgpPeerConfig {
                        interface: format!("tfport{port}_0"),
                        name: peer.name.to_string(),
                        router_lifetime: *lifetime,
                        parameters,
                    },
                );
            }
            _ => errors.push(format!(
                "router configuration {}: bgp peer {} is neither numbered \
                 nor unnumbered",
                config.name(),
                peer.name,
            )),
        }
    }
    Some(BgpSpec {
        asn,
        // The DB has no distinct BGP router-id; reuse the ASN like the
        // scrimlet mgd reconciler does.
        id: asn,
        listen: SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, BGP_PORT, 0, 0)
            .to_string(),
        originate,
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
) -> (Vec<StaticRoute4>, Vec<StaticRoute6>) {
    let mut static4 = Vec::new();
    let mut static6 = Vec::new();
    for route in routes {
        let rib_priority = route
            .rib_priority
            .map(|v| *v)
            .unwrap_or(DEFAULT_RIB_PRIORITY_STATIC);
        let vlan_id = route.vlan_id.map(|v| *v);
        match IpNet::from(route.dst) {
            IpNet::V4(prefix) => static4.push(StaticRoute4 {
                prefix,
                nexthop: route.gw.ip(),
                vlan_id,
                rib_priority,
            }),
            IpNet::V6(prefix) => match route.gw.ip() {
                IpAddr::V6(nexthop) => static6.push(StaticRoute6 {
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
) -> Vec<BfdPeerConfig> {
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
        bfd_peers.push(BfdPeerConfig {
            peer: remote,
            listen: peer.local.map(|l| l.ip()).unwrap_or(match remote {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            }),
            required_rx: u64::from(*peer.required_rx),
            detection_threshold,
            mode: match peer.mode {
                nexus_db_model::BfdMode::SingleHop => SessionMode::SingleHop,
                nexus_db_model::BfdMode::MultiHop => SessionMode::MultiHop,
            },
        });
    }
    bfd_peers
}

impl BackgroundTask for RouterConfigurationReconciler {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let log = &opctx.log;
            let mut errors = Vec::new();

            let configs = match self.load_configurations(opctx).await {
                Ok(configs) => configs,
                Err(e) => return json!({ "error": e }),
            };

            let ids: Vec<_> = configs.iter().map(|c| c.id()).collect();
            let (bgp_peers, static_routes, bfd_peers) = match futures::try_join!(
                self.datastore
                    .router_configuration_bgp_peer_list_batch(opctx, &ids),
                self.datastore
                    .router_configuration_static_route_list_batch(opctx, &ids),
                self.datastore
                    .router_configuration_bfd_peer_list_batch(opctx, &ids),
            ) {
                Ok(children) => children,
                Err(e) => {
                    return json!({
                        "error": format!(
                            "failed to list router configuration contents: {}",
                            InlineErrorChain::new(&e)
                        )
                    });
                }
            };

            let originate = self
                .load_originate_prefixes(opctx, &configs, &mut errors)
                .await;

            // Build the desired router set for each switch. Start from an
            // empty set per switch so that switches whose last configuration
            // was deleted still receive an (empty) apply request.
            let mut desired: BTreeMap<SwitchSlot, Vec<RouterSpec>> =
                BTreeMap::from([
                    (SwitchSlot::Switch0, Vec::new()),
                    (SwitchSlot::Switch1, Vec::new()),
                ]);
            for config in &configs {
                let id = config.id().into_untyped_uuid();
                let my = |cid: &nexus_db_model::DbTypedUuid<
                    omicron_uuid_kinds::RouterConfigurationKind,
                >| cid.into_untyped_uuid() == id;
                let originate = config
                    .bgp_config
                    .as_ref()
                    .and_then(|bgp| {
                        originate
                            .get(&BgpAnnounceSetUuid::from(
                                bgp.bgp_announce_set_id,
                            ))
                            .cloned()
                    })
                    .unwrap_or_default();
                let bgp = build_bgp_spec(
                    config,
                    &bgp_peers
                        .iter()
                        .filter(|p| my(&p.router_configuration_id))
                        .collect::<Vec<_>>(),
                    originate,
                    &mut errors,
                );
                let (static4, static6) = build_static_routes(
                    config,
                    &static_routes
                        .iter()
                        .filter(|r| my(&r.router_configuration_id))
                        .collect::<Vec<_>>(),
                    &mut errors,
                );
                let bfd_peers = build_bfd_peers(
                    config,
                    &bfd_peers
                        .iter()
                        .filter(|p| my(&p.router_configuration_id))
                        .collect::<Vec<_>>(),
                    &mut errors,
                );
                desired.entry(config.switch.into()).or_default().push(
                    RouterSpec {
                        name: config.name().to_string(),
                        id: RouterId(id),
                        bgp,
                        static4,
                        static6,
                        bfd_peers,
                    },
                );
            }

            let mappings =
                match switch_zone_address_mappings(&self.resolver, log).await {
                    Ok(mappings) => mappings,
                    Err(e) => {
                        return json!({
                            "error": format!(
                                "failed to resolve switch zone addresses: {e}"
                            )
                        });
                    }
                };

            let mut applied = BTreeMap::new();
            for (slot, routers) in desired {
                let Some(addr) = mappings.get(&slot) else {
                    errors.push(format!(
                        "no mgd address resolved for {slot:?}; skipping"
                    ));
                    continue;
                };
                let client = mg_admin_client::Client::new(
                    &format!("http://[{addr}]:{MGD_PORT}"),
                    log.clone(),
                );
                let router_count = routers.len();
                match client
                    .multi_router_apply(&MultiRouterApplyRequest { routers })
                    .await
                {
                    Ok(_) => {
                        applied.insert(format!("{slot:?}"), router_count);
                    }
                    Err(e) => errors.push(format!(
                        "multi-router apply to mgd on {slot:?} failed: {}",
                        InlineErrorChain::new(&e)
                    )),
                }
            }

            json!({
                "router_configurations": configs.len(),
                "applied": applied,
                "errors": errors,
            })
        }
        .boxed()
    }
}
