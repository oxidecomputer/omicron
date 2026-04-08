// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Submodule responsible for "reconciliation" of BGP config in mgd.
//!
//! Unlike other submodules in this crate, BGP config isn't really reconciled -
//! it's unconditionally applied any time the reconciler runs. There's no
//! mechanism for reading the current BGP config from mgd.

use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::switch_zone_slot::ThisSledSwitchSlot;
use mg_admin_client::Client;
use mg_admin_client::types::ApplyRequest as MgdBgpApplyRequest;
use mg_admin_client::types::BestpathFanoutRequest as MgdBestpathFanoutRequest;
use mg_admin_client::types::BgpPeerConfig as MgdBgpPeerConfig;
use mg_admin_client::types::CheckerSource as MgdCheckerSource;
use mg_admin_client::types::ImportExportPolicy4 as MgdImportExportPolicy4;
use mg_admin_client::types::ImportExportPolicy6 as MgdImportExportPolicy6;
use mg_admin_client::types::Ipv4UnicastConfig as MgdIpv4UnicastConfig;
use mg_admin_client::types::Ipv6UnicastConfig as MgdIpv6UnicastConfig;
use mg_admin_client::types::JitterRange as MgdJitterRange;
use mg_admin_client::types::ShaperSource as MgdShaperSource;
use mg_admin_client::types::UnnumberedBgpPeerConfig as MgdUnnumberedBgpPeerConfig;
use oxnet::IpNet;
use rdb_types::Prefix as MgdPrefix;
use rdb_types::Prefix4 as MgdPrefix4;
use rdb_types::Prefix6 as MgdPrefix6;
use sled_agent_types::early_networking::BgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::RouterPeerType;
use slog::Logger;
use slog::error;
use slog_error_chain::InlineErrorChain;

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

#[derive(Debug, Clone)]
pub enum MgdBgpReconcilerApplyResult {
    Ok,
    Err(String),
}

#[derive(Debug, Clone)]
pub enum MgdBgpReconcilerStatus {
    SkippedNoBgpConfig,
    SkippedInvalidBgpConfig {
        bad_peer_asns: BTreeSet<u32>,
    },
    Reconciled {
        bgp_config: MgdBgpReconcilerApplyResult,
        fanout: MgdBgpReconcilerApplyResult,
        bad_peer_asns: BTreeSet<u32>,
    },
}

impl slog::KV for MgdBgpReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            MgdBgpReconcilerStatus::SkippedNoBgpConfig => {
                serializer.emit_str("bgp".into(), "skipped: no bgp config")
            }
            MgdBgpReconcilerStatus::SkippedInvalidBgpConfig {
                bad_peer_asns,
            } => serializer.emit_arguments(
                "bgp".into(),
                &format_args!(
                    "skipped: INVALID bgp config ({} bad peer ASNs)",
                    bad_peer_asns.len()
                ),
            ),
            MgdBgpReconcilerStatus::Reconciled {
                bgp_config,
                fanout,
                bad_peer_asns,
            } => {
                for (key, result) in
                    [("bgp-apply", bgp_config), ("rib-fanout-update", fanout)]
                {
                    let s = match result {
                        MgdBgpReconcilerApplyResult::Ok => "success",
                        MgdBgpReconcilerApplyResult::Err(err) => err.as_str(),
                    };
                    serializer.emit_str(key.into(), s)?;
                }
                serializer
                    .emit_usize("bgp-bad-peer-asns".into(), bad_peer_asns.len())
            }
        }
    }
}

pub(super) async fn reconcile(
    client: &Client,
    desired_config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> MgdBgpReconcilerStatus {
    let (bgp_request, fanout_request, bad_peer_asns) =
        match build_apply_request(desired_config, our_switch_slot, log) {
            BuildApplyRequestResult::SkippedNoConfig => {
                return MgdBgpReconcilerStatus::SkippedNoBgpConfig;
            }
            BuildApplyRequestResult::SkippedInvalidConfig { bad_peer_asns } => {
                return MgdBgpReconcilerStatus::SkippedInvalidBgpConfig {
                    bad_peer_asns,
                };
            }
            BuildApplyRequestResult::Ok {
                bgp_request,
                fanout_request,
                bad_peer_asns,
            } => (bgp_request, fanout_request, bad_peer_asns),
        };

    let bgp_config = match client.bgp_apply_v2(&bgp_request).await {
        Ok(_) => MgdBgpReconcilerApplyResult::Ok,
        Err(err) => MgdBgpReconcilerApplyResult::Err(format!(
            "failed to apply bgp config to mgd: {}",
            InlineErrorChain::new(&err)
        )),
    };

    let fanout = match client.update_rib_bestpath_fanout(&fanout_request).await
    {
        Ok(_) => MgdBgpReconcilerApplyResult::Ok,
        Err(err) => MgdBgpReconcilerApplyResult::Err(format!(
            "failed to apply rib bestpath fanout to mgd: {}",
            InlineErrorChain::new(&err)
        )),
    };

    MgdBgpReconcilerStatus::Reconciled { bgp_config, fanout, bad_peer_asns }
}

#[derive(Debug)]
enum BuildApplyRequestResult {
    SkippedNoConfig,
    SkippedInvalidConfig {
        bad_peer_asns: BTreeSet<u32>,
    },
    Ok {
        bgp_request: MgdBgpApplyRequest,
        fanout_request: MgdBestpathFanoutRequest,
        bad_peer_asns: BTreeSet<u32>,
    },
}

fn build_apply_request(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> BuildApplyRequestResult {
    // The top-level `RackNetworkConfig` contains a `Vec` of `BgpConfig`s. If we
    // have any ports that have BGP peers, we expect to identify the correct
    // top-level `BgpConfig` by matching the peer's ASN against the config's
    // ASN. Additionally, we only support one ASN per switch, which means we
    // only expect one common ASN among all our ports.
    //
    // We maintain prior behavior here: if we have a port with an ASN that
    // doesn't match a top-level `BgpConfig`, or if we have two ports with
    // different ASNs, we log errors but still attempt to apply some subset of
    // the config. We should consider restructuring these types to make this
    // less error-prone.
    //
    // Once we've found one port <-> `BgpConfig` ASN match, store that config in
    // `matched_bgp_config` and reuse it for all subsequent ports.
    let mut matched_bgp_config: Option<&BgpConfig> = None;

    // Filter down to just the peers of the ports matching our switch slot.
    let our_bgp_peers = config
        .ports
        .iter()
        .filter(|port| port.switch == our_switch_slot)
        .flat_map(|port| port.bgp_peers.iter().map(|peer| (&port.port, peer)));

    // Maps containing all our peers converted to mgd API types.
    let mut numbered_peers: HashMap<String, Vec<MgdBgpPeerConfig>> =
        HashMap::new();
    let mut unnumbered_peers: HashMap<String, Vec<MgdUnnumberedBgpPeerConfig>> =
        HashMap::new();

    // Keep track of the ASNs of any peers we skip because they don't match the
    // top-level `BgpConfig`.
    let mut bad_peer_asns = BTreeSet::new();

    for (port_name, peer) in our_bgp_peers {
        // See the comment at the top of this function; for this peer, find the
        // matching top-level config.
        match matched_bgp_config.as_mut() {
            Some(prev_match) => {
                if peer.asn != prev_match.asn {
                    error!(
                        log,
                        "only one ASN per switch is supported";
                        "port-name" => port_name,
                        "peer-asn" => peer.asn,
                        "config-asn" => prev_match.asn,
                    );
                    bad_peer_asns.insert(peer.asn);
                    continue;
                }
            }
            None => {
                matched_bgp_config =
                    config.bgp.iter().find(|c| c.asn == peer.asn);
                if matched_bgp_config.is_none() {
                    error!(
                        log,
                        "ASN referenced by peer is not present in bgp config";
                        "port-name" => port_name,
                        "peer-asn" => peer.asn,
                    );
                    bad_peer_asns.insert(peer.asn);
                    continue;
                }
            }
        }

        // We're now guaranteed that `matched_bgp_config` contains
        // `Some(config)` where `config.asn == peer.asn`; we can accumulate the
        // actual peer config we want.
        match peer.addr {
            RouterPeerType::Unnumbered { router_lifetime } => {
                unnumbered_peers.entry(port_name.clone()).or_default().push(
                    build_unnumbered_config(port_name, peer, router_lifetime),
                );
            }
            RouterPeerType::Numbered { ip } => {
                numbered_peers
                    .entry(port_name.clone())
                    .or_default()
                    .push(build_numbered_config(peer, ip));
            }
        }
    }

    // `matched_bgp_config` is `Some(_)` if and only if we added at least one
    // peer to one of the two {un,}numbered_peers maps. If we didn't, we have no
    // BGP config to apply.
    let config = match matched_bgp_config {
        Some(config) => config,
        None => {
            return if bad_peer_asns.is_empty() {
                BuildApplyRequestResult::SkippedNoConfig
            } else {
                BuildApplyRequestResult::SkippedInvalidConfig { bad_peer_asns }
            };
        }
    };

    let asn = config.asn;
    let checker = config
        .checker
        .as_ref()
        .map(|code| MgdCheckerSource { asn, code: code.clone() });
    let shaper = config
        .shaper
        .as_ref()
        .map(|code| MgdShaperSource { asn, code: code.clone() });
    let originate = config
        .originate
        .iter()
        .map(|ip_net| match ip_net {
            IpNet::V4(ipv4_net) => MgdPrefix::V4(MgdPrefix4 {
                length: ipv4_net.width(),
                value: ipv4_net.addr(),
            }),
            IpNet::V6(ipv6_net) => MgdPrefix::V6(MgdPrefix6 {
                length: ipv6_net.width(),
                value: ipv6_net.addr(),
            }),
        })
        .collect();

    let bgp_request = MgdBgpApplyRequest {
        asn: config.asn,
        checker,
        originate,
        peers: numbered_peers,
        shaper,
        unnumbered_peers,
    };

    let fanout_request =
        MgdBestpathFanoutRequest { fanout: config.max_paths.as_nonzero_u8() };

    BuildApplyRequestResult::Ok { bgp_request, fanout_request, bad_peer_asns }
}

fn build_unnumbered_config(
    port_name: &str,
    peer: &BgpPeerConfig,
    router_lifetime: RouterLifetimeConfig,
) -> MgdUnnumberedBgpPeerConfig {
    MgdUnnumberedBgpPeerConfig {
        name: format!("unnumbered-{port_name}"),
        interface: format!("tfport{port_name}_0"),
        hold_time: peer.hold_time.unwrap_or(BgpPeerConfig::DEFAULT_HOLD_TIME),
        idle_hold_time: peer
            .idle_hold_time
            .unwrap_or(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME),
        delay_open: peer
            .delay_open
            .unwrap_or(BgpPeerConfig::DEFAULT_DELAY_OPEN),
        connect_retry: peer
            .connect_retry
            .unwrap_or(BgpPeerConfig::DEFAULT_CONNECT_RETRY),
        keepalive: peer.keepalive.unwrap_or(BgpPeerConfig::DEFAULT_KEEPALIVE),
        resolution: BGP_SESSION_RESOLUTION,
        passive: false,
        remote_asn: peer.remote_asn,
        min_ttl: peer.min_ttl,
        md5_auth_key: peer.md5_auth_key.clone(),
        multi_exit_discriminator: peer.multi_exit_discriminator,
        communities: peer.communities.clone(),
        local_pref: peer.local_pref,
        enforce_first_as: peer.enforce_first_as,
        ipv4_unicast: Some(build_ipv4_unicast(peer)),
        ipv6_unicast: Some(build_ipv6_unicast(peer)),
        vlan_id: peer.vlan_id,
        connect_retry_jitter: Some(MgdJitterRange { max: 1.0, min: 0.75 }),
        deterministic_collision_resolution: false,
        idle_hold_jitter: None,
        router_lifetime: router_lifetime.as_u16(),
    }
}

fn build_numbered_config(
    peer: &BgpPeerConfig,
    addr: RouterPeerIpAddr,
) -> MgdBgpPeerConfig {
    MgdBgpPeerConfig {
        name: format!("{addr}"),
        host: format!("{addr}:179"),
        hold_time: peer.hold_time.unwrap_or(BgpPeerConfig::DEFAULT_HOLD_TIME),
        idle_hold_time: peer
            .idle_hold_time
            .unwrap_or(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME),
        delay_open: peer
            .delay_open
            .unwrap_or(BgpPeerConfig::DEFAULT_DELAY_OPEN),
        connect_retry: peer
            .connect_retry
            .unwrap_or(BgpPeerConfig::DEFAULT_CONNECT_RETRY),
        keepalive: peer.keepalive.unwrap_or(BgpPeerConfig::DEFAULT_KEEPALIVE),
        resolution: BGP_SESSION_RESOLUTION,
        passive: false,
        remote_asn: peer.remote_asn,
        min_ttl: peer.min_ttl,
        md5_auth_key: peer.md5_auth_key.clone(),
        multi_exit_discriminator: peer.multi_exit_discriminator,
        communities: peer.communities.clone(),
        local_pref: peer.local_pref,
        enforce_first_as: peer.enforce_first_as,
        ipv4_unicast: Some(build_ipv4_unicast(peer)),
        ipv6_unicast: Some(build_ipv6_unicast(peer)),
        vlan_id: peer.vlan_id,
        connect_retry_jitter: Some(MgdJitterRange { max: 1.0, min: 0.75 }),
        deterministic_collision_resolution: false,
        idle_hold_jitter: None,
    }
}

fn build_ipv4_unicast(peer: &BgpPeerConfig) -> MgdIpv4UnicastConfig {
    MgdIpv4UnicastConfig {
        nexthop: None,
        import_policy: match &peer.allowed_import {
            ImportExportPolicy::NoFiltering => {
                MgdImportExportPolicy4::NoFiltering
            }
            ImportExportPolicy::Allow(list) => MgdImportExportPolicy4::Allow(
                list.iter()
                    .filter_map(|x| match x {
                        IpNet::V4(p) => Some(MgdPrefix4 {
                            length: p.width(),
                            value: p.addr(),
                        }),
                        IpNet::V6(_) => None,
                    })
                    .collect(),
            ),
        },
        export_policy: match &peer.allowed_export {
            ImportExportPolicy::NoFiltering => {
                MgdImportExportPolicy4::NoFiltering
            }
            ImportExportPolicy::Allow(list) => MgdImportExportPolicy4::Allow(
                list.iter()
                    .filter_map(|x| match x {
                        IpNet::V4(p) => Some(MgdPrefix4 {
                            length: p.width(),
                            value: p.addr(),
                        }),
                        IpNet::V6(_) => None,
                    })
                    .collect(),
            ),
        },
    }
}

fn build_ipv6_unicast(peer: &BgpPeerConfig) -> MgdIpv6UnicastConfig {
    MgdIpv6UnicastConfig {
        nexthop: None,
        import_policy: match &peer.allowed_import {
            ImportExportPolicy::NoFiltering => {
                MgdImportExportPolicy6::NoFiltering
            }
            ImportExportPolicy::Allow(list) => MgdImportExportPolicy6::Allow(
                list.iter()
                    .filter_map(|x| match x {
                        IpNet::V6(p) => Some(MgdPrefix6 {
                            length: p.width(),
                            value: p.addr(),
                        }),
                        IpNet::V4(_) => None,
                    })
                    .collect(),
            ),
        },
        export_policy: match &peer.allowed_export {
            ImportExportPolicy::NoFiltering => {
                MgdImportExportPolicy6::NoFiltering
            }
            ImportExportPolicy::Allow(list) => MgdImportExportPolicy6::Allow(
                list.iter()
                    .filter_map(|x| match x {
                        IpNet::V6(p) => Some(MgdPrefix6 {
                            length: p.width(),
                            value: p.addr(),
                        }),
                        IpNet::V4(_) => None,
                    })
                    .collect(),
            ),
        },
    }
}

#[cfg(test)]
mod tests;
