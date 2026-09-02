// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `mgd` within a scrimlet's
//! switch zone.
//!
//! mgd is configured declaratively: each reconciliation sends one
//! `multi_router_apply` request carrying the complete desired router list
//! for this switch. mgd tears down routers absent from the request and
//! creates or updates the ones present; the daemon-owned "default" router
//! is emptied rather than torn down when absent (which never happens here,
//! since we always render it).
//!
//! The "default" router carries the rack's uplink configuration (BGP,
//! static routes, BFD) rendered from `RackNetworkConfig`, filtered to this
//! switch slot. Operator router configurations (RFD 662) arrive
//! pre-rendered from Nexus via the bootstore (`switch_router_configs`); an
//! operator spec named "default" (only present while non-empty) replaces
//! the uplink render, at which point the operator owns this switch's
//! default router.

use crate::ScrimletReconcilersMode;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use anyhow::Context;
use anyhow::bail;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdReconcilerStatus;
use mg_admin_client::Client;
use mg_api_types::bfd::BfdPeerConfig as MgdBfdPeerConfig;
use mg_api_types::bfd::SessionMode as MgdSessionMode;
use mg_api_types::bgp::config::BgpPeerConfig as MgdBgpPeerConfig;
use mg_api_types::bgp::config::BgpPeerParameters as MgdBgpPeerParameters;
use mg_api_types::bgp::config::CheckerSource as MgdCheckerSource;
use mg_api_types::bgp::config::Ipv4UnicastConfig as MgdIpv4UnicastConfig;
use mg_api_types::bgp::config::Ipv6UnicastConfig as MgdIpv6UnicastConfig;
use mg_api_types::bgp::config::JitterRange as MgdJitterRange;
use mg_api_types::bgp::config::ShaperSource as MgdShaperSource;
use mg_api_types::bgp::config::UnnumberedBgpPeerConfig as MgdUnnumberedBgpPeerConfig;
use mg_api_types::bgp::policy::ImportExportPolicy4 as MgdImportExportPolicy4;
use mg_api_types::bgp::policy::ImportExportPolicy6 as MgdImportExportPolicy6;
use mg_api_types::rdb::DEFAULT_RIB_PRIORITY_STATIC;
use mg_api_types::router::BgpSpec;
use mg_api_types::router::MultiRouterApplyRequest;
use mg_api_types::router::RouterId;
use mg_api_types::router::RouterSpec;
use mg_api_types::static_routes::StaticRoute4 as MgdStaticRoute4;
use mg_api_types::static_routes::StaticRoute6 as MgdStaticRoute6;
use oxnet::IpNet;
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::BgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig as UplinkBgpPeerConfig;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouterPeerType;
use sled_agent_types::router_config::RouterConfigBgpPeerParameters;
use sled_agent_types::router_config::RouterConfigBgpSpec;
use sled_agent_types::router_config::RouterConfigSpec;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::num::NonZeroU8;
use std::time::Duration;

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

const BGP_PORT: u16 = 179;

/// Name of the daemon-owned default router in mgd.
const DEFAULT_ROUTER_NAME: &str = "default";


#[derive(Debug)]
pub(crate) struct MgdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
}

impl Reconciler for MgdReconciler {
    type Status = MgdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "MgdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        mode: ScrimletReconcilersMode,
        switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        Self { client: mode.mgd_client(parent_log), switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let routers = match render_desired_routers(
            system_networking_config,
            self.switch_slot,
        ) {
            Ok(routers) => routers,
            Err(err) => {
                let err = InlineErrorChain::new(&*err).to_string();
                warn!(
                    log, "failed to render desired mgd router set";
                    "error" => &err,
                );
                return MgdReconcilerStatus::FailedGeneratingDesiredConfig(
                    err,
                );
            }
        };

        let router_count = routers.len();
        match self
            .client
            .multi_router_apply(&MultiRouterApplyRequest { routers })
            .await
        {
            Ok(_) => MgdReconcilerStatus::Success { routers: router_count },
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(log, "multi-router apply to mgd failed"; &err);
                MgdReconcilerStatus::FailedApplying {
                    routers: router_count,
                    error: err.to_string(),
                }
            }
        }
    }
}

/// Build the complete desired router list for this switch's mgd.
fn render_desired_routers(
    config: &SystemNetworkingConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> anyhow::Result<Vec<RouterSpec>> {
    let operator_specs = config
        .switch_router_configs
        .get(&our_switch_slot.slot())
        .map(Vec::as_slice)
        .unwrap_or_default();

    let mut routers = Vec::with_capacity(operator_specs.len() + 1);
    let mut have_default = false;
    for spec in operator_specs {
        if spec.name == DEFAULT_ROUTER_NAME {
            have_default = true;
        }
        routers.push(convert_operator_spec(spec));
    }
    if !have_default {
        routers.push(render_uplink_default_router(
            &config.rack_network_config,
            our_switch_slot,
        )?);
    }
    Ok(routers)
}

/// Render the default router's spec from the rack's uplink configuration.
fn render_uplink_default_router(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> anyhow::Result<RouterSpec> {
    let bgp = render_uplink_bgp_spec(config, our_switch_slot)?;
    let (static4, static6) =
        render_uplink_static_routes(config, our_switch_slot)?;
    let bfd_peers = render_uplink_bfd_peers(config, our_switch_slot);
    Ok(RouterSpec {
        name: DEFAULT_ROUTER_NAME.to_string(),
        // mgd ignores the id for the "default" router; its identity is
        // daemon-owned.
        id: RouterId(uuid::Uuid::nil()),
        bgp,
        static4,
        static6,
        bfd_peers,
    })
}

fn render_uplink_bgp_spec(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> anyhow::Result<Option<BgpSpec>> {
    // Filter down to just the peers of the ports matching our switch slot.
    let our_bgp_peers = config
        .ports
        .iter()
        .filter(|port| port.switch == our_switch_slot)
        .flat_map(|port| port.bgp_peers.iter().map(|peer| (&port.port, peer)))
        .collect::<Vec<_>>();

    let Some((_, first_peer)) = our_bgp_peers.first() else {
        return Ok(None);
    };

    // A router has exactly one BGP configuration (single ASN); in practice
    // each rack runs a single ASN anyway.
    let asn = first_peer.asn;
    for (_, peer) in &our_bgp_peers {
        if peer.asn != asn {
            bail!(
                "uplink BGP peers on this switch span multiple ASNs \
                 ({asn} and {}), but a router has a single BGP configuration",
                peer.asn,
            );
        }
    }

    let BgpConfig { asn: _, originate, shaper, checker, max_paths } = config
        .bgp
        .iter()
        .find(|c| c.asn == asn)
        .with_context(|| {
            format!(
                "invalid RackNetworkConfig: peers reference ASN {asn} \
                 which doesn't have a corresponding BGP config",
            )
        })?;

    let mut numbered: HashMap<String, Vec<MgdBgpPeerConfig>> = HashMap::new();
    let mut unnumbered: HashMap<String, Vec<MgdUnnumberedBgpPeerConfig>> =
        HashMap::new();
    let mut seen_numbered = BTreeSet::new();
    let mut seen_unnumbered = BTreeSet::new();
    for (port_name, peer) in our_bgp_peers {
        let parameters = uplink_peer_parameters(peer);
        match &peer.addr {
            RouterPeerType::Numbered(numbered_router) => {
                let ip = numbered_router.target_addr();
                let host = SocketAddr::new(ip.into(), BGP_PORT);
                if !seen_numbered.insert(host) {
                    bail!(
                        "invalid config: multiple numbered peers \
                         for address {host}"
                    );
                }
                numbered.entry(port_name.clone()).or_default().push(
                    MgdBgpPeerConfig {
                        host: host.into(),
                        name: ip.to_string(),
                        parameters,
                    },
                );
            }
            RouterPeerType::Unnumbered(unnumbered_router) => {
                let interface = format!("tfport{port_name}_0");
                if !seen_unnumbered.insert(interface.clone()) {
                    bail!(
                        "invalid config: multiple unnumbered peers \
                         for interface {interface}"
                    );
                }
                unnumbered.entry(port_name.clone()).or_default().push(
                    MgdUnnumberedBgpPeerConfig {
                        interface,
                        name: format!("unnumbered-{port_name}"),
                        router_lifetime: unnumbered_router
                            .router_lifetime
                            .as_u16(),
                        parameters,
                    },
                );
            }
        }
    }

    Ok(Some(BgpSpec {
        asn,
        // The uplink config has no distinct BGP router-id; reuse the ASN.
        id: asn,
        listen: bgp_listen_addr(),
        originate: originate.clone(),
        checker: checker
            .clone()
            .map(|code| MgdCheckerSource { asn, code }),
        shaper: shaper.clone().map(|code| MgdShaperSource { asn, code }),
        peers: numbered,
        unnumbered_peers: unnumbered,
        max_paths: Some(max_paths.as_nonzero_u8()),
    }))
}

fn render_uplink_static_routes(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> anyhow::Result<(Vec<MgdStaticRoute4>, Vec<MgdStaticRoute6>)> {
    let mut static4 = Vec::new();
    let mut static6 = Vec::new();
    for route in config
        .ports
        .iter()
        .filter(|port| port.switch == our_switch_slot)
        .flat_map(|port| port.routes.iter())
    {
        let vlan_id = route.vlan_id;
        // TODO The rack network config uses `None` as a sentinel for "the
        // default priority". This isn't what we want long-term; see
        // https://github.com/oxidecomputer/maghemite/issues/646#issuecomment-3948331208.
        let rib_priority =
            route.rib_priority.unwrap_or(DEFAULT_RIB_PRIORITY_STATIC);
        match (route.nexthop, route.destination) {
            // An IPv6 nexthop for an IPv4 prefix is valid (v4-over-v6).
            (nexthop, IpNet::V4(prefix)) => static4.push(MgdStaticRoute4 {
                prefix,
                nexthop,
                vlan_id,
                rib_priority,
            }),
            (IpAddr::V6(nexthop), IpNet::V6(prefix)) => {
                static6.push(MgdStaticRoute6 {
                    prefix,
                    nexthop,
                    vlan_id,
                    rib_priority,
                })
            }
            (IpAddr::V4(nexthop), IpNet::V6(prefix)) => bail!(
                "rack network config route has unsupported mix: \
                 ipv4 nexthop {nexthop} for ipv6 prefix {prefix}"
            ),
        }
    }
    Ok((static4, static6))
}

fn render_uplink_bfd_peers(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> Vec<MgdBfdPeerConfig> {
    config
        .bfd
        .iter()
        .filter(|peer| peer.switch == our_switch_slot)
        .map(|peer| MgdBfdPeerConfig {
            peer: peer.remote,
            listen: peer.local.unwrap_or_else(|| match peer.remote {
                IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
                IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
            }),
            required_rx: peer.required_rx,
            // TODO-cleanup We should extend NonZeroU8 up the stack.
            // <https://github.com/oxidecomputer/omicron/issues/10657>
            detection_threshold: NonZeroU8::new(peer.detection_threshold)
                .unwrap_or(NonZeroU8::MIN),
            mode: convert_bfd_mode(peer.mode),
        })
        .collect()
}

/// Convert an operator router-configuration spec (pre-rendered by Nexus)
/// into the mgd API type, filling in the daemon tunables the control plane
/// doesn't decide.
fn convert_operator_spec(spec: &RouterConfigSpec) -> RouterSpec {
    RouterSpec {
        name: spec.name.clone(),
        id: RouterId(spec.id),
        bgp: spec.bgp.as_ref().map(convert_operator_bgp),
        static4: spec
            .static4
            .iter()
            .map(|route| MgdStaticRoute4 {
                prefix: route.prefix,
                nexthop: route.nexthop,
                vlan_id: route.vlan_id,
                rib_priority: route.rib_priority,
            })
            .collect(),
        static6: spec
            .static6
            .iter()
            .map(|route| MgdStaticRoute6 {
                prefix: route.prefix,
                nexthop: route.nexthop,
                vlan_id: route.vlan_id,
                rib_priority: route.rib_priority,
            })
            .collect(),
        bfd_peers: spec
            .bfd_peers
            .iter()
            .map(|peer| MgdBfdPeerConfig {
                peer: peer.peer,
                listen: peer.listen,
                required_rx: peer.required_rx,
                detection_threshold: peer.detection_threshold,
                mode: convert_bfd_mode(peer.mode),
            })
            .collect(),
    }
}

fn convert_operator_bgp(bgp: &RouterConfigBgpSpec) -> BgpSpec {
    let asn = bgp.asn;
    let mut numbered: HashMap<String, Vec<MgdBgpPeerConfig>> = HashMap::new();
    for peer in &bgp.peers {
        numbered.entry(DEFAULT_ROUTER_NAME.to_string()).or_default().push(
            MgdBgpPeerConfig {
                host: SocketAddr::new(peer.addr, BGP_PORT).into(),
                name: peer.name.clone(),
                parameters: convert_operator_peer_parameters(&peer.parameters),
            },
        );
    }
    let mut unnumbered: HashMap<String, Vec<MgdUnnumberedBgpPeerConfig>> =
        HashMap::new();
    for peer in &bgp.unnumbered_peers {
        unnumbered.entry(peer.port.clone()).or_default().push(
            MgdUnnumberedBgpPeerConfig {
                interface: format!("tfport{}_0", peer.port),
                name: peer.name.clone(),
                router_lifetime: peer.router_lifetime,
                parameters: convert_operator_peer_parameters(&peer.parameters),
            },
        );
    }
    BgpSpec {
        asn,
        // The spec has no distinct BGP router-id; reuse the ASN.
        id: asn,
        listen: bgp_listen_addr(),
        originate: bgp.originate.clone(),
        checker: bgp
            .checker
            .clone()
            .map(|code| MgdCheckerSource { asn, code }),
        shaper: bgp.shaper.clone().map(|code| MgdShaperSource { asn, code }),
        peers: numbered,
        unnumbered_peers: unnumbered,
        max_paths: bgp.max_paths,
    }
}

fn uplink_peer_parameters(
    peer: &UplinkBgpPeerConfig,
) -> MgdBgpPeerParameters {
    fn as_list(policy: &ImportExportPolicy) -> Option<&[IpNet]> {
        match policy {
            ImportExportPolicy::NoFiltering => None,
            ImportExportPolicy::Allow(list) => Some(list),
        }
    }

    MgdBgpPeerParameters {
        hold_time: peer
            .hold_time
            .unwrap_or(UplinkBgpPeerConfig::DEFAULT_HOLD_TIME),
        idle_hold_time: peer
            .idle_hold_time
            .unwrap_or(UplinkBgpPeerConfig::DEFAULT_IDLE_HOLD_TIME),
        delay_open: peer
            .delay_open
            .unwrap_or(UplinkBgpPeerConfig::DEFAULT_DELAY_OPEN),
        connect_retry: peer
            .connect_retry
            .unwrap_or(UplinkBgpPeerConfig::DEFAULT_CONNECT_RETRY),
        keepalive: peer
            .keepalive
            .unwrap_or(UplinkBgpPeerConfig::DEFAULT_KEEPALIVE),
        remote_asn: peer.remote_asn,
        min_ttl: peer.min_ttl,
        md5_auth_key: peer.md5_auth_key.clone(),
        multi_exit_discriminator: peer.multi_exit_discriminator,
        communities: peer.communities.clone(),
        local_pref: peer.local_pref,
        enforce_first_as: peer.enforce_first_as,
        vlan_id: peer.vlan_id,
        ..daemon_tunables(
            as_list(&peer.allowed_import),
            as_list(&peer.allowed_export),
        )
    }
}

fn convert_operator_peer_parameters(
    parameters: &RouterConfigBgpPeerParameters,
) -> MgdBgpPeerParameters {
    MgdBgpPeerParameters {
        hold_time: parameters.hold_time,
        idle_hold_time: parameters.idle_hold_time,
        delay_open: parameters.delay_open,
        connect_retry: parameters.connect_retry,
        keepalive: parameters.keepalive,
        remote_asn: parameters.remote_asn,
        min_ttl: parameters.min_ttl,
        md5_auth_key: parameters.md5_auth_key.clone(),
        multi_exit_discriminator: parameters.multi_exit_discriminator,
        communities: parameters.communities.clone(),
        local_pref: parameters.local_pref,
        enforce_first_as: parameters.enforce_first_as,
        vlan_id: parameters.vlan_id,
        ..daemon_tunables(
            parameters.allowed_import.as_deref(),
            parameters.allowed_export.as_deref(),
        )
    }
}

/// The peer parameters the control plane doesn't decide, identical for
/// every peer we configure.
fn daemon_tunables(
    allowed_import: Option<&[IpNet]>,
    allowed_export: Option<&[IpNet]>,
) -> MgdBgpPeerParameters {
    MgdBgpPeerParameters {
        hold_time: 0,
        idle_hold_time: 0,
        delay_open: 0,
        connect_retry: 0,
        keepalive: 0,
        remote_asn: None,
        min_ttl: None,
        md5_auth_key: None,
        multi_exit_discriminator: None,
        communities: Vec::new(),
        local_pref: None,
        enforce_first_as: false,
        vlan_id: None,
        resolution: BGP_SESSION_RESOLUTION,
        passive: false,
        ipv4_unicast: Some(MgdIpv4UnicastConfig {
            nexthop: None,
            import_policy: policy4(allowed_import),
            export_policy: policy4(allowed_export),
        }),
        ipv6_unicast: Some(MgdIpv6UnicastConfig {
            nexthop: None,
            import_policy: policy6(allowed_import),
            export_policy: policy6(allowed_export),
        }),
        deterministic_collision_resolution: false,
        idle_hold_jitter: None,
        connect_retry_jitter: Some(MgdJitterRange { min: 0.75, max: 1.0 }),
        src_addr: None,
        src_port: None,
    }
}

fn policy4(list: Option<&[IpNet]>) -> MgdImportExportPolicy4 {
    match list {
        None => MgdImportExportPolicy4::NoFiltering,
        Some(list) => MgdImportExportPolicy4::Allow(
            list.iter()
                .filter_map(|net| match net {
                    IpNet::V4(net) => Some(*net),
                    IpNet::V6(_) => None,
                })
                .collect(),
        ),
    }
}

fn policy6(list: Option<&[IpNet]>) -> MgdImportExportPolicy6 {
    match list {
        None => MgdImportExportPolicy6::NoFiltering,
        Some(list) => MgdImportExportPolicy6::Allow(
            list.iter()
                .filter_map(|net| match net {
                    IpNet::V6(net) => Some(*net),
                    IpNet::V4(_) => None,
                })
                .collect(),
        ),
    }
}

fn convert_bfd_mode(mode: BfdMode) -> MgdSessionMode {
    match mode {
        BfdMode::SingleHop => MgdSessionMode::SingleHop,
        BfdMode::MultiHop => MgdSessionMode::MultiHop,
    }
}

fn bgp_listen_addr() -> String {
    SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, BGP_PORT, 0, 0).to_string()
}

#[cfg(test)]
mod tests;
