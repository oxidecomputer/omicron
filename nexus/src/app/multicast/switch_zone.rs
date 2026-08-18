// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Switch zone service clients for multicast operations.
//!
//! Wraps MGD (for MRIB programming) and DDM (for the read-only peer
//! view) on the switch zone. Built per reconciliation pass.
//!
//! - **MRIB**: Nexus → MGD MRIB → mg-lower → DDM → peer sleds
//! - **Peers**: under RFD 488 mg-lower derives underlay members from DDM
//!   peer subscriptions, so the reconciler does not resolve sled→port; the
//!   DDM client only backs the `omdb nexus multicast ddm-peers` view

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use std::time::Duration;

use anyhow::anyhow;
use futures::future::{join_all, try_join_all};
use internal_dns_resolver::Resolver;
use sled_agent_types::early_networking::SwitchSlot;
use slog::{Logger, debug, warn};

use internal_dns_types::names::ServiceName;
use mg_admin_client::types::{
    MribAddStaticRequest, MribDeleteStaticRequest, MulticastRouteKey,
    MulticastRouteKeyV4, MulticastRouteKeyV6, StaticMulticastRouteInput,
};
use omicron_common::address::{DDMD_PORT, MGD_PORT};
use omicron_ddm_admin_client::PeerInfo;

use crate::app::switch_zone_targets;

/// Client for switch zone services used by the multicast reconciler.
///
/// Provides access to MGD (MRIB route programming) and DDM (the
/// read-only peer view; not used for sled-to-port resolution).
///
/// Built per reconciliation pass, similar to [`MulticastDataplaneClient`].
///
/// Note: per [omicron#10167], system-level networking (uplinkd, system-zone
/// NAT, BGP, BFD) is migrating from Nexus RPWs to sled-agent reconcilers
/// that operate based on data in the bootstore. Multicast is
/// **instance networking** (group state derives from per-instance memberships),
/// so this client's direct-to-MGD path is intentional and should be preserved
/// by the migration.
///
/// If a follow-on to [omicron#10167] routes MRIB writes through
/// sled-agent, the reconciler logic stays in Nexus and only the wire
/// surface changes: Nexus would call a sled-agent endpoint that fronts
/// MGD instead of MGD directly.
///
/// [`MulticastDataplaneClient`]: super::dataplane::MulticastDataplaneClient
/// [omicron#10167]: https://github.com/oxidecomputer/omicron/issues/10167
pub(crate) struct MulticastSwitchZoneClient {
    mgd_clients: HashMap<SwitchSlot, mg_admin_client::Client>,
    ddm_clients: HashMap<SwitchSlot, omicron_ddm_admin_client::Client>,
    log: Logger,
}

pub(crate) type MribRouteIndex =
    HashMap<IpAddr, HashMap<Option<IpAddr>, HashMap<SwitchSlot, Ipv6Addr>>>;

// Mirrors `MulticastDataplaneClient::new`'s timeout.
const SWITCH_ZONE_BUILD_TIMEOUT: Duration = Duration::from_secs(5);

// Per-switch bound on the live DDM peer query so an unreachable switch zone
// degrades to a skipped switch rather than stalling the whole request.
//
// Budget: progenitor-generated clients (omdb's lockstep client included)
// default to a 15s reqwest timeout, so the handler's worst case of
// SWITCH_ZONE_BUILD_TIMEOUT + one concurrent DDM_PEERS_QUERY_TIMEOUT (13s)
// must stay under it with margin for DNS and HTTP overhead.
const DDM_PEERS_QUERY_TIMEOUT: Duration = Duration::from_secs(8);

impl MulticastSwitchZoneClient {
    /// Build MGD and DDM clients for all switch zones.
    ///
    /// Resolves service ports from DNS rather than hardcoding them,
    /// falling back to the well-known port constants when DNS lookup
    /// fails. This allows the test harness to run MGD and DDM on
    /// dynamic ports.
    ///
    /// Returns an error when no switch zones resolve, so the reconciler
    /// retries rather than silently treating writes as noops.
    pub(crate) async fn new(
        resolver: Resolver,
        log: Logger,
    ) -> Result<Self, String> {
        match tokio::time::timeout(
            SWITCH_ZONE_BUILD_TIMEOUT,
            Self::build(resolver, log.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(format!(
                "timed out building switch-zone clients after \
                 {SWITCH_ZONE_BUILD_TIMEOUT:?}"
            )),
        }
    }

    async fn build(resolver: Resolver, log: Logger) -> Result<Self, String> {
        let switch_zones = switch_zone_targets(&resolver, &log).await?;

        if switch_zones.is_empty() {
            return Err(
                "no switch zones resolved for multicast operations".to_string()
            );
        }

        // Resolve MGD and DDM sockets from DNS, keyed by SRV target. This
        // preserves distinct switch zones that share an IPv6 address in tests
        // and differ only by port.
        let mgd_socket_map =
            resolve_service_sockets(&resolver, &log, ServiceName::Mgd).await;
        let ddm_socket_map =
            resolve_service_sockets(&resolver, &log, ServiceName::Ddm).await;

        let mgd_clients = switch_zones
            .iter()
            .map(|(slot, endpoint)| {
                let socketaddr = mgd_socket_map
                    .get(&endpoint.target)
                    .copied()
                    .unwrap_or_else(|| {
                        SocketAddrV6::new(endpoint.addr, MGD_PORT, 0, 0)
                    });
                (
                    *slot,
                    mg_admin_client::Client::new(
                        &format!("http://{socketaddr}"),
                        log.clone(),
                    ),
                )
            })
            .collect();

        let ddm_clients = switch_zones
            .iter()
            .filter_map(|(slot, endpoint)| {
                let from_dns = ddm_socket_map.get(&endpoint.target).copied();
                let socketaddr = from_dns.unwrap_or_else(|| {
                    SocketAddrV6::new(endpoint.addr, DDMD_PORT, 0, 0)
                });
                match omicron_ddm_admin_client::Client::new(&log, socketaddr) {
                    Ok(c) => Some((*slot, c)),
                    Err(e) => {
                        warn!(
                            log,
                            "failed to build DDM client for switch zone";
                            "switch" => ?slot,
                            "error" => %e,
                        );
                        None
                    }
                }
            })
            .collect();

        Ok(Self { mgd_clients, ddm_clients, log })
    }

    /// Add a multicast route to the MRIB on all switches in parallel.
    ///
    /// `mg-lower` watches the MRIB and automatically advertises the
    /// route via DDM to peer sleds. Short-circuits on the first switch
    /// failure as the reconciler retries the full set on the next pass.
    ///
    /// # Arguments
    ///
    /// * `group_ip`: Overlay multicast group address.
    /// * `underlay_ip`: Underlay IPv6 address (ff04::/64) the route
    ///   forwards to.
    /// * `source`: SSM source for source-specific routes; `None` for ASM.
    pub(crate) async fn add_route(
        &self,
        group_ip: IpAddr,
        underlay_ip: Ipv6Addr,
        source: Option<IpAddr>,
    ) -> Result<(), anyhow::Error> {
        let route_key = make_route_key(group_ip, source);

        let request = MribAddStaticRequest {
            routes: vec![StaticMulticastRouteInput {
                key: route_key,
                underlay_group: underlay_ip,
            }],
        };

        try_join_all(self.mgd_clients.iter().map(|(slot, client)| {
            let request = &request;
            async move {
                client.static_add_mcast_route(request).await.map_err(|e| {
                    warn!(
                        self.log,
                        "mgd static_add_mcast_route failed";
                        "switch" => ?slot,
                        "group_ip" => %group_ip,
                        "error" => %e,
                    );
                    anyhow!(
                        "mgd static_add_mcast_route failed on switch {slot:?}: {e}"
                    )
                })?;
                debug!(
                    self.log,
                    "added multicast route to MRIB";
                    "switch" => ?slot,
                    "group_ip" => %group_ip,
                    "underlay_ip" => %underlay_ip,
                );
                Ok::<(), anyhow::Error>(())
            }
        }))
        .await?;
        Ok(())
    }

    /// Remove a multicast route from the MRIB on all switches in parallel.
    ///
    /// `mg-lower` detects the removal and withdraws the DDM
    /// advertisement from peer sleds. Short-circuits on the first
    /// switch failure as the reconciler retries on the next pass.
    pub(crate) async fn remove_route(
        &self,
        group_ip: IpAddr,
        source: Option<IpAddr>,
    ) -> Result<(), anyhow::Error> {
        let route_key = make_route_key(group_ip, source);

        let request = MribDeleteStaticRequest { keys: vec![route_key] };

        try_join_all(self.mgd_clients.iter().map(|(slot, client)| {
            let request = &request;
            async move {
                client.static_remove_mcast_route(request).await.map_err(
                    |e| {
                        warn!(
                            self.log,
                            "mgd static_remove_mcast_route failed";
                            "switch" => ?slot,
                            "group_ip" => %group_ip,
                            "error" => %e,
                        );
                        anyhow!(
                            "mgd static_remove_mcast_route failed on switch {slot:?}: {e}"
                        )
                    },
                )?;
                debug!(
                    self.log,
                    "removed multicast route from MRIB";
                    "switch" => ?slot,
                    "group_ip" => %group_ip,
                );
                Ok::<(), anyhow::Error>(())
            }
        }))
        .await?;
        Ok(())
    }

    /// List static multicast routes from all reachable switches and
    /// index them by group|source|switch.
    pub(crate) async fn list_routes_indexed(
        &self,
    ) -> Result<MribRouteIndex, anyhow::Error> {
        let mut index = MribRouteIndex::new();

        for (slot, client) in &self.mgd_clients {
            match client.static_list_mcast_routes().await {
                Ok(routes) => {
                    for route in routes.into_inner() {
                        let (group_ip, source) = route_identifier(&route.key);
                        index
                            .entry(group_ip)
                            .or_default()
                            .entry(source)
                            .or_default()
                            .insert(*slot, route.underlay_group);
                    }
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "failed to list multicast routes from switch zone";
                        "switch" => ?slot,
                        "error" => %e,
                    );
                }
            }
        }

        Ok(index)
    }

    pub(crate) fn switch_count(&self) -> usize {
        self.mgd_clients.len()
    }

    /// Whether a multicast route is present in `mrib_loc` (RPF-verified)
    /// on every configured switch.
    ///
    /// Returns `false` when the route is missing on any switch, including
    /// switches that fail the RPC. The reconciler interprets `false` as
    /// not-yet-forwarding (still in `mrib_in`, de-promoted by the RPF
    /// revalidator, or simply unreachable) and retries on the next pass.
    pub(crate) async fn route_active_on_all_switches(
        &self,
        group_ip: IpAddr,
        source: Option<IpAddr>,
    ) -> bool {
        let vni = u32::from(
            omicron_common::api::external::Vni::DEFAULT_MULTICAST_VNI,
        );

        for (slot, client) in &self.mgd_clients {
            match client
                .get_mrib_selected(
                    None,
                    Some(&group_ip),
                    None,
                    source.as_ref(),
                    Some(vni),
                )
                .await
            {
                Ok(resp) => {
                    if resp.into_inner().is_empty() {
                        return false;
                    }
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "mgd get_mrib_selected failed";
                        "switch" => ?slot,
                        "group_ip" => %group_ip,
                        "error" => %e,
                    );
                    return false;
                }
            }
        }

        true
    }

    /// Query DDM peers from all switch zones, tagged with the switch slot
    /// they were observed from so callers can present per-switch topology.
    ///
    /// Errors from individual switch zones are logged and skipped rather
    /// than failing the whole query.
    ///
    /// Switches are queried concurrently so the worst-case latency is one
    /// per-switch [`DDM_PEERS_QUERY_TIMEOUT`] rather than the sum across
    /// switches. A sequential scan of two slow switches exceeds omdb's
    /// client timeout.
    pub(crate) async fn ddm_peers_by_switch(
        &self,
    ) -> Vec<(SwitchSlot, PeerInfo)> {
        let per_switch =
            self.ddm_clients.iter().map(|(slot, client)| async move {
                match tokio::time::timeout(
                    DDM_PEERS_QUERY_TIMEOUT,
                    client.get_peers(),
                )
                .await
                {
                    Ok(Ok(peers)) => peers
                        .into_values()
                        .map(|peer| (*slot, peer))
                        .collect::<Vec<_>>(),
                    Ok(Err(e)) => {
                        warn!(
                            self.log,
                            "failed to get DDM peers from switch zone";
                            "switch" => ?slot,
                            "error" => %e,
                        );
                        Vec::new()
                    }
                    Err(_) => {
                        warn!(
                            self.log,
                            "timed out querying DDM peers from switch zone";
                            "switch" => ?slot,
                            "timeout" => ?DDM_PEERS_QUERY_TIMEOUT,
                        );
                        Vec::new()
                    }
                }
            });

        join_all(per_switch).await.into_iter().flatten().collect()
    }
}

fn make_route_key(
    group_ip: IpAddr,
    source: Option<IpAddr>,
) -> MulticastRouteKey {
    let vni =
        u32::from(omicron_common::api::external::Vni::DEFAULT_MULTICAST_VNI);
    match group_ip {
        IpAddr::V4(v4) => MulticastRouteKey::V4(MulticastRouteKeyV4 {
            group: v4,
            source: source.and_then(|s| match s {
                IpAddr::V4(s4) => Some(s4),
                _ => None,
            }),
            vni,
        }),
        IpAddr::V6(v6) => MulticastRouteKey::V6(MulticastRouteKeyV6 {
            group: v6,
            source: source.and_then(|s| match s {
                IpAddr::V6(s6) => Some(s6),
                _ => None,
            }),
            vni,
        }),
    }
}

/// Resolve service sockets from DNS, returning a map of SRV target to socket.
async fn resolve_service_sockets(
    resolver: &Resolver,
    log: &Logger,
    service: ServiceName,
) -> HashMap<String, SocketAddrV6> {
    match resolver.lookup_all_socket_v6_by_target(service).await {
        Ok(pairs) => pairs.into_iter().collect(),
        Err(e) => {
            warn!(
                log,
                "failed to resolve service sockets from DNS, using defaults";
                "service" => ?service,
                "error" => %e,
            );
            HashMap::new()
        }
    }
}

fn route_identifier(key: &MulticastRouteKey) -> (IpAddr, Option<IpAddr>) {
    match key {
        MulticastRouteKey::V4(k) => {
            (IpAddr::V4(k.group), k.source.map(IpAddr::V4))
        }
        MulticastRouteKey::V6(k) => {
            (IpAddr::V6(k.group), k.source.map(IpAddr::V6))
        }
    }
}
