// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Submodule responsible for reconciliation of BGP configuration in mgd.

use crate::switch_zone_slot::ThisSledSwitchSlot;
use anyhow::Context;
use anyhow::bail;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::{
    MgdBgpReconcilerStatus, MgdBgpReconcilerStatusOpCount,
};
use daft::Diffable;
use mg_admin_client::Client;
use mg_admin_client::ResponseValue as MgdResponseValue;
use mg_admin_client::types::Neighbor as MgdNeighbor;
use mg_admin_client::types::UnnumberedNeighbor as MgdUnnumberedNeighbor;
use mg_api_types::bgp::config::CheckerSource as MgdCheckerSource;
use mg_api_types::bgp::config::Ipv4UnicastConfig as MgdIpv4UnicastConfig;
use mg_api_types::bgp::config::Ipv6UnicastConfig as MgdIpv6UnicastConfig;
use mg_api_types::bgp::config::JitterRange as MgdJitterRange;
use mg_api_types::bgp::config::Origin4 as MgdOrigin4;
use mg_api_types::bgp::config::Router as MgdRouter;
use mg_api_types::bgp::config::ShaperSource as MgdShaperSource;
use mg_api_types::bgp::history::Origin6 as MgdOrigin6;
use mg_api_types::bgp::policy::ImportExportPolicy4 as MgdImportExportPolicy4;
use mg_api_types::bgp::policy::ImportExportPolicy6 as MgdImportExportPolicy6;
use mg_api_types::rib::BestpathFanoutRequest as MgdBestpathFanoutRequest;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use sled_agent_types::early_networking::BgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouterPeerType;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

type MgdClientError = mg_admin_client::Error<mg_admin_client::types::Error>;

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

const BGP_PORT: u16 = 179;

pub(super) async fn reconcile(
    client: &Client,
    desired_config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> MgdBgpReconcilerStatus {
    let current_config = match DiffableBgpConfig::fetch_current(client).await {
        Ok(config) => config,
        Err(err) => {
            return MgdBgpReconcilerStatus::FailedReadingBgpConfig(
                InlineErrorChain::new(&*err).to_string(),
            );
        }
    };

    let desired_config = match DiffableBgpConfig::from_desired_config(
        &desired_config,
        our_switch_slot,
        log,
    ) {
        Ok(config) => config,
        Err(err) => {
            return MgdBgpReconcilerStatus::FailedGeneratingDesiredConfig(
                InlineErrorChain::new(&*err).to_string(),
            );
        }
    };

    let plan = ReconciliationPlan::new(&current_config, &desired_config);

    apply_plan(client, plan, log).await
}

async fn apply_plan(
    client: &Client,
    plan: ReconciliationPlan<'_>,
    log: &Logger,
) -> MgdBgpReconcilerStatus {
    let ReconciliationPlan {
        set_max_paths,
        routers_to_delete,
        routers_to_update,
        routers_to_create,
        originate4_to_update,
        originate6_to_update,
        originate4_to_create,
        originate6_to_create,
        shapers_to_delete,
        shapers_to_update,
        shapers_to_create,
        checkers_to_delete,
        checkers_to_update,
        checkers_to_create,
        numbered_peers_to_delete,
        numbered_peers_to_update,
        numbered_peers_to_create,
        unnumbered_peers_to_delete,
        unnumbered_peers_to_update,
        unnumbered_peers_to_create,
    } = plan;

    // Helper type to keep track of all the operations we perform, and for any
    // errors, both `warn!`-log them and append them into `errors`.
    #[derive(Debug, Default)]
    struct StatusBuilder {
        counts: MgdBgpReconcilerStatusOpCount,
        did_change_max_paths: bool,
        errors: Vec<String>,
    }
    let mut status_builder = StatusBuilder::default();
    macro_rules! record_count {
        ($op:expr, $counter:ident, $ctx:expr,) => {
            let result = $op;
            match result {
                Ok(_) => {
                    status_builder.counts.$counter += 1;
                }
                Err(err) => {
                    let ctx = $ctx;
                    let err = InlineErrorChain::new(&err);
                    warn!(log, "{ctx}"; &err);
                    status_builder.errors.push(format!("{ctx}: {err}"));
                }
            }
        };
    }
    macro_rules! record_bool {
        ($op:expr, $field:ident, $ctx:expr,) => {
            let result = $op;
            match result {
                Ok(_) => {
                    status_builder.$field = true;
                }
                Err(err) => {
                    let ctx = $ctx;
                    let err = InlineErrorChain::new(&err);
                    warn!(log, "{ctx}"; &err);
                    status_builder.errors.push(format!("{ctx}: {err}"));
                }
            }
        };
    }

    // Perform all deletions. Each of these calls is wrapped in
    // `notfound_to_none()` - if we try to delete something that's already gone,
    // that's fine. (E.g., we start by deleting the routers - that may or may
    // not delete all the associated objects in that same ASN.)
    //
    // The order here matters, both due to bugs in current mgd and for
    // correctness after those bugs are fixed! Specifically:
    //
    // * We must delete peers before attempting to create new peers, because
    //   it's possible a peer has changed ASNs, which means we must delete the
    //   old one before creating the new one. This is not due to any bugs in
    //   mgd, but rather how it organizes peers by ASN.
    // * Prior to deleting a router by ASN, we need to delete all the entities
    //   contained in it. This is a workaround for
    //   <https://github.com/oxidecomputer/maghemite/issues/783>, where deleting
    //   a router sometimes leaves orphaned entities that cause subsequent
    //   operations to fail.
    for (asn, ip) in numbered_peers_to_delete {
        let peer = ip.to_string();
        record_count!(
            notfound_to_none(client.delete_neighbor(asn, &peer).await),
            numbered_peers_deleted,
            format!("failed to delete numbered peer {ip} for asn {asn}"),
        );
    }
    for (asn, iface) in unnumbered_peers_to_delete {
        record_count!(
            notfound_to_none(
                client.delete_unnumbered_neighbor(asn, &iface).await
            ),
            unnumbered_peers_deleted,
            format!("failed to delete unnumbered peer {iface} for asn {asn}"),
        );
    }
    for asn in routers_to_delete {
        let error_count = status_builder.errors.len();
        record_count!(
            notfound_to_none(client.delete_origin4(asn).await),
            origin4_deleted,
            format!("failed to delete ipv4 origins for asn {asn}"),
        );
        record_count!(
            notfound_to_none(client.delete_origin6(asn).await),
            origin6_deleted,
            format!("failed to delete ipv6 origins for asn {asn}"),
        );
        record_count!(
            notfound_to_none(client.delete_shaper(asn).await),
            shapers_deleted,
            format!("failed to delete shaper for asn {asn}"),
        );
        record_count!(
            notfound_to_none(client.delete_checker(asn).await),
            checkers_deleted,
            format!("failed to delete checker for asn {asn}"),
        );

        // mgd bug workaround: deleting a router doesn't clean up all its
        // children, so we should only delete the router if we've successfully
        // deleted all the children ourself first.
        // <https://github.com/oxidecomputer/maghemite/issues/783>
        if status_builder.errors.len() == error_count {
            record_count!(
                notfound_to_none(client.delete_router(asn).await),
                routers_deleted,
                format!("failed to delete router for asn {asn}"),
            );
        } else {
            status_builder.errors.push(format!(
                "skipped attempting to delete router \
                 with asn {asn} due to prior deletion errors"
            ));
        }
    }
    for asn in shapers_to_delete {
        record_count!(
            notfound_to_none(client.delete_shaper(asn).await),
            shapers_deleted,
            format!("failed to delete shaper for asn {asn}"),
        );
    }
    for asn in checkers_to_delete {
        record_count!(
            notfound_to_none(client.delete_checker(asn).await),
            checkers_deleted,
            format!("failed to delete checker for asn {asn}"),
        );
    }

    // Perform all updates to modified entities.
    for (asn, config) in routers_to_update {
        record_count!(
            client.update_router(&config.to_mgd_router(asn)).await,
            routers_updated,
            format!("failed to update router with asn {asn}"),
        );
    }
    for (asn, prefixes) in originate4_to_update {
        record_count!(
            client
                .update_origin4(&MgdOrigin4 {
                    asn,
                    prefixes: prefixes.iter().copied().collect()
                })
                .await,
            origin4_updated,
            format!("failed to update ipv4 origin prefixes for asn {asn}"),
        );
    }
    for (asn, prefixes) in originate6_to_update {
        record_count!(
            client
                .update_origin6(&MgdOrigin6 {
                    asn,
                    prefixes: prefixes.iter().copied().collect()
                })
                .await,
            origin6_updated,
            format!("failed to update ipv6 origin prefixes for asn {asn}"),
        );
    }
    for (asn, shaper) in shapers_to_update {
        record_count!(
            client
                .update_shaper(&MgdShaperSource {
                    asn,
                    code: shaper.to_owned()
                })
                .await,
            shapers_updated,
            format!("failed to update shaper for asn {asn}"),
        );
    }
    for (asn, checker) in checkers_to_update {
        record_count!(
            client
                .update_checker(&MgdCheckerSource {
                    asn,
                    code: checker.to_owned()
                })
                .await,
            checkers_updated,
            format!("failed to update checker for asn {asn}"),
        );
    }
    for (addr, config) in numbered_peers_to_update {
        record_count!(
            client
                .update_neighbor(&config.to_mgd_numbered_neighbor(addr))
                .await,
            numbered_peers_updated,
            format!(
                "failed to update numbered peer {addr} for asn {}",
                config.asn
            ),
        );
    }
    for (iface, config) in unnumbered_peers_to_update {
        record_count!(
            client
                .update_unnumbered_neighbor(
                    &config.to_mgd_unnumbered_neighbor(iface.to_string()),
                )
                .await,
            unnumbered_peers_updated,
            format!(
                "failed to update unnumbered peer {iface} for asn {}",
                config.common.asn
            ),
        );
    }

    // Perform all creations.
    //
    // Keep track of routers we fail to create to avoid spuriously trying to
    // create child entities related to routers we couldn't create.
    let mut routers_that_failed_to_create = BTreeSet::new();
    for (asn, config) in routers_to_create {
        let prev_created = status_builder.counts.routers_created;
        record_count!(
            client.create_router(&config.to_mgd_router(*asn)).await,
            routers_created,
            format!("failed to create router with asn {asn}"),
        );

        // If we _didn't_ successfully create the router, add it to
        // `routers_that_failed_to_create` and skip all related creates.
        if status_builder.counts.routers_created == prev_created {
            routers_that_failed_to_create.insert(*asn);
        }
    }
    for (asn, prefixes) in originate4_to_create {
        if routers_that_failed_to_create.contains(asn) {
            continue;
        }
        record_count!(
            client
                .create_origin4(&MgdOrigin4 {
                    asn: *asn,
                    prefixes: prefixes.iter().copied().collect()
                })
                .await,
            origin4_created,
            format!("failed to create ipv4 origin prefixes for asn {asn}"),
        );
    }
    for (asn, prefixes) in originate6_to_create {
        if routers_that_failed_to_create.contains(asn) {
            continue;
        }
        record_count!(
            client
                .create_origin6(&MgdOrigin6 {
                    asn: *asn,
                    prefixes: prefixes.iter().copied().collect()
                })
                .await,
            origin6_created,
            format!("failed to create ipv6 origin prefixes for asn {asn}"),
        );
    }
    for (asn, shaper) in shapers_to_create {
        if routers_that_failed_to_create.contains(asn) {
            continue;
        }
        record_count!(
            client
                .create_shaper(&MgdShaperSource {
                    asn: *asn,
                    code: shaper.to_owned()
                })
                .await,
            shapers_created,
            format!("failed to create shaper for asn {asn}"),
        );
    }
    for (asn, checker) in checkers_to_create {
        if routers_that_failed_to_create.contains(asn) {
            continue;
        }
        record_count!(
            client
                .create_checker(&MgdCheckerSource {
                    asn: *asn,
                    code: checker.to_owned()
                })
                .await,
            checkers_created,
            format!("failed to create checker for asn {asn}"),
        );
    }
    for (addr, config) in numbered_peers_to_create {
        if routers_that_failed_to_create.contains(&config.asn) {
            continue;
        }
        record_count!(
            client
                .create_neighbor(&config.to_mgd_numbered_neighbor(*addr))
                .await,
            numbered_peers_created,
            format!(
                "failed to create numbered peer {addr} for asn {}",
                config.asn
            ),
        );
    }
    for (iface, config) in unnumbered_peers_to_create {
        if routers_that_failed_to_create.contains(&config.common.asn) {
            continue;
        }
        record_count!(
            client
                .create_unnumbered_neighbor(
                    &config.to_mgd_unnumbered_neighbor(iface.to_string()),
                )
                .await,
            unnumbered_peers_created,
            format!(
                "failed to create unnumbered peer {iface} for asn {}",
                config.common.asn
            ),
        );
    }

    // Set the global bestpath fanout setting, if it changed.
    if let Some(max_paths) = set_max_paths {
        record_bool!(
            client
                .update_bestpath_fanout(&MgdBestpathFanoutRequest {
                    fanout: max_paths.as_nonzero_u8(),
                })
                .await,
            did_change_max_paths,
            "update_bestpath_fanout() failed",
        );
    }

    let StatusBuilder { counts, did_change_max_paths, errors } = status_builder;
    if errors.is_empty() {
        MgdBgpReconcilerStatus::Success { counts, did_change_max_paths }
    } else {
        MgdBgpReconcilerStatus::PartialSuccess {
            counts,
            did_change_max_paths,
            errors,
        }
    }
}

#[derive(Debug)]
struct ReconciliationPlan<'a> {
    // If `Some(_)`, we need to update the (global) max_paths value.
    set_max_paths: Option<MaxPathConfig>,

    // Routers keyed by ASN.
    //
    // For each of the `routers_to_delete`, we're also expected to delete its
    // origin4, origin6, checker, and shaper.
    routers_to_delete: BTreeSet<u32>,
    routers_to_update: BTreeMap<u32, &'a DiffableBgpRouterConfig>,
    routers_to_create: BTreeMap<&'a u32, &'a DiffableBgpRouterConfig>,

    // IPv4 / IPv6 originate prefixes keyed by ASN.
    originate4_to_update: BTreeMap<u32, &'a BTreeSet<Ipv4Net>>,
    originate6_to_update: BTreeMap<u32, &'a BTreeSet<Ipv6Net>>,
    originate4_to_create: BTreeMap<&'a u32, &'a BTreeSet<Ipv4Net>>,
    originate6_to_create: BTreeMap<&'a u32, &'a BTreeSet<Ipv6Net>>,

    // Shapers / checkers keyed by ASN.
    shapers_to_delete: BTreeSet<u32>,
    shapers_to_update: BTreeMap<u32, &'a str>,
    shapers_to_create: BTreeMap<&'a u32, &'a String>,
    checkers_to_delete: BTreeSet<u32>,
    checkers_to_update: BTreeMap<u32, &'a str>,
    checkers_to_create: BTreeMap<&'a u32, &'a String>,

    // Numbered peers keyed by (ASN, address) or ASN.
    numbered_peers_to_delete: BTreeSet<(u32, IpAddr)>,
    numbered_peers_to_update:
        BTreeMap<SocketAddr, &'a DiffableBgpCommonPeerConfig>,
    numbered_peers_to_create:
        BTreeMap<&'a SocketAddr, &'a DiffableBgpCommonPeerConfig>,

    // Unnumbered peers keyed by (ASN, interface) or ASN.
    unnumbered_peers_to_delete: BTreeSet<(u32, &'a str)>,
    unnumbered_peers_to_update:
        BTreeMap<&'a str, &'a DiffableBgpUnnumberedPeerConfig>,
    unnumbered_peers_to_create:
        BTreeMap<&'a String, &'a DiffableBgpUnnumberedPeerConfig>,
}

impl<'a> ReconciliationPlan<'a> {
    fn new(
        current_config: &'a DiffableBgpConfig,
        desired_config: &'a DiffableBgpConfig,
    ) -> Self {
        let DiffableBgpConfigDiff {
            max_paths,
            routers,
            originate4,
            originate6,
            shapers,
            checkers,
            numbered_peers,
            unnumbered_peers,
        } = current_config.diff(desired_config);

        let set_max_paths = if max_paths.is_modified() {
            // If `max_paths.after` is itself `None`, we don't have a BGP config
            // and will leave mgd with whatever its current (presumably
            // default) setting is. This might be wrong if we previously had a
            // BGP config that changed the setting and now we ought to reset it
            // to a default, but (a) that's an unlikely edge case and (b) we
            // need to rework how this attached to BGP configs anyway:
            // <https://github.com/oxidecomputer/omicron/issues/10463>.
            max_paths.after.copied()
        } else {
            None
        };

        // Build the set of routers that have been deleted.
        let routers_to_delete =
            routers.removed.keys().copied().copied().collect::<BTreeSet<_>>();

        // For "to delete" checkers and shapers, filter out any that are already
        // covered by `routers_to_delete`.
        let shapers_to_delete = shapers
            .removed
            .keys()
            .copied()
            .copied()
            .filter(|asn| !routers_to_delete.contains(&asn))
            .collect();
        let checkers_to_delete = checkers
            .removed
            .keys()
            .copied()
            .copied()
            .filter(|asn| !routers_to_delete.contains(&asn))
            .collect();

        // Peers are _not_ filtered out; we need the ID of the peer to delete
        // it. (Deleting the router _should_ do this, but currently doesn't:
        // https://github.com/oxidecomputer/maghemite/issues/783.)
        let mut numbered_peers_to_delete = numbered_peers
            .removed
            .iter()
            .map(|(addr, config)| (config.asn, addr.ip()))
            .collect::<BTreeSet<_>>();
        let mut unnumbered_peers_to_delete = unnumbered_peers
            .removed
            .iter()
            .map(|(interface, config)| (config.common.asn, interface.as_str()))
            .collect::<BTreeSet<_>>();

        // For peers, our diff is keyed by the peer identifier (IP or
        // interface). But peers live under a router in MGD, so we have to
        // check: if the ASN changed, we need to treat the change as a "delete /
        // create" instead of as an "update".
        let mut numbered_peers_to_create = numbered_peers.added.clone();
        let mut numbered_peers_to_update = BTreeMap::new();
        for (key, leaf) in numbered_peers.modified() {
            if leaf.before.asn == leaf.after.asn {
                numbered_peers_to_update.insert(*key, leaf.after);
            } else {
                numbered_peers_to_delete.insert((leaf.before.asn, key.ip()));
                numbered_peers_to_create.insert(key, leaf.after);
            }
        }

        let mut unnumbered_peers_to_create = unnumbered_peers.added.clone();
        let mut unnumbered_peers_to_update = BTreeMap::new();
        for (key, leaf) in unnumbered_peers.modified() {
            if leaf.before.common.asn == leaf.after.common.asn {
                unnumbered_peers_to_update.insert(key.as_str(), leaf.after);
            } else {
                unnumbered_peers_to_delete
                    .insert((leaf.before.common.asn, key.as_str()));
                unnumbered_peers_to_create.insert(key, leaf.after);
            }
        }

        Self {
            set_max_paths,

            routers_to_delete,
            routers_to_update: routers
                .modified()
                .map(|(key, leaf)| (*key, leaf.after))
                .collect(),
            routers_to_create: routers.added,

            originate4_to_update: originate4
                .modified()
                .map(|(key, leaf)| (*key, leaf.after))
                .collect(),
            originate6_to_update: originate6
                .modified()
                .map(|(key, leaf)| (*key, leaf.after))
                .collect(),
            originate4_to_create: originate4.added,
            originate6_to_create: originate6.added,

            shapers_to_delete,
            shapers_to_update: shapers
                .modified()
                .map(|(key, leaf)| (*key, leaf.after.as_str()))
                .collect(),
            shapers_to_create: shapers.added,

            checkers_to_delete,
            checkers_to_update: checkers
                .modified()
                .map(|(key, leaf)| (*key, leaf.after.as_str()))
                .collect(),
            checkers_to_create: checkers.added,

            numbered_peers_to_delete,
            numbered_peers_to_update,
            numbered_peers_to_create,

            unnumbered_peers_to_delete,
            unnumbered_peers_to_update,
            unnumbered_peers_to_create,
        }
    }
}

// All the `Diffable*` types defined here define a common structure between what
// we can fetch from mgd and what we can construct from a `RackNetworkConfig`.
// They all derive `daft::Diffable` for easy checking of "what changed".
#[derive(Debug, Diffable, Eq, PartialEq)]
struct DiffableBgpRouterConfig {
    id: u32,
    graceful_shutdown: bool,
    listen: String,
}

impl DiffableBgpRouterConfig {
    fn to_mgd_router(&self, asn: u32) -> MgdRouter {
        MgdRouter {
            asn,
            graceful_shutdown: self.graceful_shutdown,
            id: self.id,
            listen: self.listen.clone(),
        }
    }
}

#[derive(Debug, Diffable, PartialEq, Eq)]
struct DiffableBgpCommonPeerConfig {
    asn: u32,
    name: String,
    group: String,
    hold_time: u64,
    idle_hold_time: u64,
    delay_open: u64,
    connect_retry: u64,
    keepalive: u64,
    resolution: u64,
    passive: bool,
    remote_asn: Option<u32>,
    min_ttl: Option<u8>,
    md5_auth_key: Option<String>,
    multi_exit_discriminator: Option<u32>,
    communities: Vec<u32>,
    local_pref: Option<u32>,
    enforce_first_as: bool,
    vlan_id: Option<u16>,
    ipv4_unicast: Option<DiffableUnicastConfig<Ipv4Net>>,
    ipv6_unicast: Option<DiffableUnicastConfig<Ipv6Net>>,
    deterministic_collision_resolution: bool,
    idle_hold_jitter: Option<DiffableJitterRange>,
    connect_retry_jitter: Option<DiffableJitterRange>,
    src_addr: Option<IpAddr>,
    src_port: Option<u16>,
}

impl DiffableBgpCommonPeerConfig {
    fn to_mgd_numbered_neighbor(&self, addr: SocketAddr) -> MgdNeighbor {
        MgdNeighbor {
            asn: self.asn,
            communities: self.communities.clone(),
            connect_retry: self.connect_retry,
            connect_retry_jitter: self.connect_retry_jitter.map(From::from),
            delay_open: self.delay_open,
            deterministic_collision_resolution: self
                .deterministic_collision_resolution,
            enforce_first_as: self.enforce_first_as,
            group: self.group.clone(),
            hold_time: self.hold_time,
            host: addr,
            idle_hold_jitter: self.idle_hold_jitter.map(From::from),
            idle_hold_time: self.idle_hold_time,
            ipv4_unicast: self.ipv4_unicast.clone().map(From::from),
            ipv6_unicast: self.ipv6_unicast.clone().map(From::from),
            keepalive: self.keepalive,
            local_pref: self.local_pref,
            md5_auth_key: self.md5_auth_key.clone(),
            min_ttl: self.min_ttl,
            multi_exit_discriminator: self.multi_exit_discriminator,
            name: self.name.clone(),
            passive: self.passive,
            remote_asn: self.remote_asn,
            resolution: self.resolution,
            src_addr: self.src_addr,
            src_port: self.src_port,
            vlan_id: self.vlan_id,
        }
    }
}

#[derive(Debug, Diffable, PartialEq, Eq)]
struct DiffableBgpUnnumberedPeerConfig {
    router_lifetime: u16,
    common: DiffableBgpCommonPeerConfig,
}

impl DiffableBgpUnnumberedPeerConfig {
    fn to_mgd_unnumbered_neighbor(
        &self,
        interface: String,
    ) -> MgdUnnumberedNeighbor {
        let common = &self.common;
        MgdUnnumberedNeighbor {
            interface,
            act_as_a_default_ipv6_router: self.router_lifetime,
            asn: common.asn,
            communities: common.communities.clone(),
            connect_retry: common.connect_retry,
            connect_retry_jitter: common.connect_retry_jitter.map(From::from),
            delay_open: common.delay_open,
            deterministic_collision_resolution: common
                .deterministic_collision_resolution,
            enforce_first_as: common.enforce_first_as,
            group: common.group.clone(),
            hold_time: common.hold_time,
            idle_hold_jitter: common.idle_hold_jitter.map(From::from),
            idle_hold_time: common.idle_hold_time,
            ipv4_unicast: common.ipv4_unicast.clone().map(From::from),
            ipv6_unicast: common.ipv6_unicast.clone().map(From::from),
            keepalive: common.keepalive,
            local_pref: common.local_pref,
            md5_auth_key: common.md5_auth_key.clone(),
            min_ttl: common.min_ttl,
            multi_exit_discriminator: common.multi_exit_discriminator,
            name: common.name.clone(),
            passive: common.passive,
            remote_asn: common.remote_asn,
            resolution: common.resolution,
            src_addr: common.src_addr,
            src_port: common.src_port,
            vlan_id: common.vlan_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Diffable, PartialEq, Eq)]
struct DiffableJitterRange {
    min: F64Bits,
    max: F64Bits,
}

impl From<MgdJitterRange> for DiffableJitterRange {
    fn from(value: MgdJitterRange) -> Self {
        let MgdJitterRange { max, min } = value;
        Self { min: min.into(), max: max.into() }
    }
}

impl From<DiffableJitterRange> for MgdJitterRange {
    fn from(value: DiffableJitterRange) -> Self {
        let DiffableJitterRange { min, max } = value;
        Self { min: min.into(), max: max.into() }
    }
}

// `daft` diffing requires implementing `Eq`, which isn't implemented on f64.
// We're not concerned with the differences between -0.0/0.0 or the various
// flavors of NaN, so we'll just store the raw bits to do our diff.
#[derive(Debug, Clone, Copy, Diffable, PartialEq, Eq)]
struct F64Bits(u64);

impl From<f64> for F64Bits {
    fn from(value: f64) -> Self {
        Self(value.to_bits())
    }
}

impl From<F64Bits> for f64 {
    fn from(value: F64Bits) -> Self {
        f64::from_bits(value.0)
    }
}

#[derive(Debug, Clone, Diffable, PartialEq, Eq)]
struct DiffableUnicastConfig<Proto> {
    nexthop: Option<IpAddr>,
    import_policy: DiffableImportExportPolicy<Proto>,
    export_policy: DiffableImportExportPolicy<Proto>,
}

impl From<MgdIpv4UnicastConfig> for DiffableUnicastConfig<Ipv4Net> {
    fn from(value: MgdIpv4UnicastConfig) -> Self {
        let MgdIpv4UnicastConfig { export_policy, import_policy, nexthop } =
            value;
        Self {
            nexthop,
            import_policy: import_policy.into(),
            export_policy: export_policy.into(),
        }
    }
}

impl From<MgdIpv6UnicastConfig> for DiffableUnicastConfig<Ipv6Net> {
    fn from(value: MgdIpv6UnicastConfig) -> Self {
        let MgdIpv6UnicastConfig { export_policy, import_policy, nexthop } =
            value;
        Self {
            nexthop,
            import_policy: import_policy.into(),
            export_policy: export_policy.into(),
        }
    }
}

impl From<DiffableUnicastConfig<Ipv4Net>> for MgdIpv4UnicastConfig {
    fn from(value: DiffableUnicastConfig<Ipv4Net>) -> Self {
        let DiffableUnicastConfig { export_policy, import_policy, nexthop } =
            value;
        Self {
            nexthop,
            import_policy: import_policy.into(),
            export_policy: export_policy.into(),
        }
    }
}

impl From<DiffableUnicastConfig<Ipv6Net>> for MgdIpv6UnicastConfig {
    fn from(value: DiffableUnicastConfig<Ipv6Net>) -> Self {
        let DiffableUnicastConfig { export_policy, import_policy, nexthop } =
            value;
        Self {
            nexthop,
            import_policy: import_policy.into(),
            export_policy: export_policy.into(),
        }
    }
}

#[derive(Debug, Clone, Diffable, PartialEq, Eq)]
enum DiffableImportExportPolicy<Proto> {
    NoFiltering,
    Allow(BTreeSet<Proto>),
}

impl From<&'_ ImportExportPolicy> for DiffableImportExportPolicy<Ipv4Net> {
    fn from(value: &'_ ImportExportPolicy) -> Self {
        match value {
            ImportExportPolicy::NoFiltering => Self::NoFiltering,
            ImportExportPolicy::Allow(nets) => Self::Allow(
                nets.iter()
                    .filter_map(|net| match net {
                        IpNet::V4(net) => Some(*net),
                        IpNet::V6(_) => None,
                    })
                    .collect(),
            ),
        }
    }
}

impl From<&'_ ImportExportPolicy> for DiffableImportExportPolicy<Ipv6Net> {
    fn from(value: &'_ ImportExportPolicy) -> Self {
        match value {
            ImportExportPolicy::NoFiltering => Self::NoFiltering,
            ImportExportPolicy::Allow(nets) => Self::Allow(
                nets.iter()
                    .filter_map(|net| match net {
                        IpNet::V6(net) => Some(*net),
                        IpNet::V4(_) => None,
                    })
                    .collect(),
            ),
        }
    }
}

impl From<MgdImportExportPolicy4> for DiffableImportExportPolicy<Ipv4Net> {
    fn from(value: MgdImportExportPolicy4) -> Self {
        match value {
            MgdImportExportPolicy4::NoFiltering => Self::NoFiltering,
            MgdImportExportPolicy4::Allow(prefixes) => Self::Allow(prefixes),
        }
    }
}

impl From<MgdImportExportPolicy6> for DiffableImportExportPolicy<Ipv6Net> {
    fn from(value: MgdImportExportPolicy6) -> Self {
        match value {
            MgdImportExportPolicy6::NoFiltering => Self::NoFiltering,
            MgdImportExportPolicy6::Allow(prefixes) => Self::Allow(prefixes),
        }
    }
}

impl From<DiffableImportExportPolicy<Ipv4Net>> for MgdImportExportPolicy4 {
    fn from(value: DiffableImportExportPolicy<Ipv4Net>) -> Self {
        match value {
            DiffableImportExportPolicy::NoFiltering => Self::NoFiltering,
            DiffableImportExportPolicy::Allow(prefixes) => {
                Self::Allow(prefixes)
            }
        }
    }
}

impl From<DiffableImportExportPolicy<Ipv6Net>> for MgdImportExportPolicy6 {
    fn from(value: DiffableImportExportPolicy<Ipv6Net>) -> Self {
        match value {
            DiffableImportExportPolicy::NoFiltering => Self::NoFiltering,
            DiffableImportExportPolicy::Allow(prefixes) => {
                Self::Allow(prefixes)
            }
        }
    }
}

#[derive(Debug, Diffable, PartialEq, Eq)]
struct DiffableBgpConfig {
    // Global max_paths / bestpath fanout setting.
    //
    // TODO-correctness Nexus specifies this as part of the BGP config, but it's
    // a global setting (not specific to BGP). For now we reconcile it as part
    // of this reconciler. We keep it as an `Option` to represent "we have no
    // BGP config". <https://github.com/oxidecomputer/omicron/issues/10463>
    max_paths: Option<MaxPathConfig>,

    // Routers keyed by ASN.
    routers: BTreeMap<u32, DiffableBgpRouterConfig>,

    // IPv4/IPv6 originate prefixes keyed by ASN.
    originate4: BTreeMap<u32, BTreeSet<Ipv4Net>>,
    originate6: BTreeMap<u32, BTreeSet<Ipv6Net>>,

    // Shapers keyed by ASN.
    shapers: BTreeMap<u32, String>,

    // Checkers keyed by ASN.
    checkers: BTreeMap<u32, String>,

    // Numbered peers keyed by host.
    numbered_peers: BTreeMap<SocketAddr, DiffableBgpCommonPeerConfig>,

    // Unnumbered peers keyed by interface.
    unnumbered_peers: BTreeMap<String, DiffableBgpUnnumberedPeerConfig>,
}

impl DiffableBgpConfig {
    async fn fetch_current(client: &Client) -> anyhow::Result<Self> {
        let mut routers = BTreeMap::new();
        let mut numbered_peers = BTreeMap::new();
        let mut unnumbered_peers = BTreeMap::new();
        let mut originate4 = BTreeMap::new();
        let mut originate6 = BTreeMap::new();
        let mut shapers = BTreeMap::new();
        let mut checkers = BTreeMap::new();

        for router in client
            .read_routers()
            .await
            .context("read_routers() failed")?
            .into_inner()
        {
            let MgdRouter { asn, graceful_shutdown, id, listen } = router;

            {
                let MgdOrigin4 { asn: _, prefixes } = client
                    .read_origin4(asn)
                    .await
                    .with_context(|| format!("read_origin4({asn}) failed"))?
                    .into_inner();
                originate4.insert(asn, prefixes.into_iter().collect());
            }
            {
                let MgdOrigin6 { asn: _, prefixes } = client
                    .read_origin6(asn)
                    .await
                    .with_context(|| format!("read_origin6({asn}) failed"))?
                    .into_inner();
                originate6.insert(asn, prefixes.into_iter().collect());
            }

            // Unlike `read_origin{4,6}` above, which return an empty set of
            // prefixes if there are no originating prefixes,
            // `read_{shaper,checker}` return an HTTP 404 if there is no
            // associated shaper/checker. We convert those to `None`.
            let shaper = notfound_to_none(client.read_shaper(asn).await)
                .with_context(|| format!("read_shaper({asn}) failed"))?
                .map(|resp| resp.code);
            let checker = notfound_to_none(client.read_checker(asn).await)
                .with_context(|| format!("read_checker({asn}) failed"))?
                .map(|resp| resp.code);

            routers.insert(
                asn,
                DiffableBgpRouterConfig { graceful_shutdown, id, listen },
            );
            if let Some(shaper) = shaper {
                shapers.insert(asn, shaper);
            }
            if let Some(checker) = checker {
                checkers.insert(asn, checker);
            }
        }

        // Keep consistency with Nexus's API shape around max_paths: it attaches
        // it to BGP, for now, so we only query it to see if we need to change
        // it if we have any BGP configs.
        // <https://github.com/oxidecomputer/omicron/issues/10463>
        let max_paths = if routers.is_empty() {
            None
        } else {
            let max_paths = client
                .read_bestpath_fanout()
                .await
                .context("read_bestpath_fanout() failed")?
                .into_inner()
                .fanout;
            Some(MaxPathConfig::new_saturating(max_paths))
        };

        for &asn in routers.keys() {
            // read numbered peers
            for neighbor in client
                .read_neighbors(asn)
                .await
                .with_context(|| format!("read_neighbors({asn}) failed"))?
                .into_inner()
            {
                let MgdNeighbor {
                    asn,
                    communities,
                    connect_retry,
                    connect_retry_jitter,
                    delay_open,
                    deterministic_collision_resolution,
                    enforce_first_as,
                    group,
                    hold_time,
                    host,
                    idle_hold_jitter,
                    idle_hold_time,
                    ipv4_unicast,
                    ipv6_unicast,
                    keepalive,
                    local_pref,
                    md5_auth_key,
                    min_ttl,
                    multi_exit_discriminator,
                    name,
                    passive,
                    remote_asn,
                    resolution,
                    src_addr,
                    src_port,
                    vlan_id,
                } = neighbor;

                numbered_peers.insert(
                    host,
                    DiffableBgpCommonPeerConfig {
                        asn,
                        name,
                        group,
                        hold_time,
                        idle_hold_time,
                        delay_open,
                        connect_retry,
                        keepalive,
                        resolution,
                        passive,
                        remote_asn,
                        min_ttl,
                        md5_auth_key,
                        multi_exit_discriminator,
                        communities,
                        local_pref,
                        enforce_first_as,
                        vlan_id,
                        ipv4_unicast: ipv4_unicast
                            .map(TryFrom::try_from)
                            .transpose()
                            .with_context(|| {
                                format!(
                                    "invalid ipv4 unicast network \
                                     on numbered peer {host}"
                                )
                            })?,
                        ipv6_unicast: ipv6_unicast
                            .map(TryFrom::try_from)
                            .transpose()
                            .with_context(|| {
                                format!(
                                    "invalid ipv6 unicast network \
                                     on numbered peer {host}"
                                )
                            })?,
                        deterministic_collision_resolution,
                        idle_hold_jitter: idle_hold_jitter.map(From::from),
                        connect_retry_jitter: connect_retry_jitter
                            .map(From::from),
                        src_addr,
                        src_port,
                    },
                );
            }

            // read unnumbered peers
            for neighbor in client
                .read_unnumbered_neighbors(asn)
                .await
                .with_context(|| {
                    format!("read_unnumbered_neighbors_v2({asn}) failed")
                })?
                .into_inner()
            {
                let MgdUnnumberedNeighbor {
                    act_as_a_default_ipv6_router,
                    asn,
                    communities,
                    connect_retry,
                    connect_retry_jitter,
                    delay_open,
                    deterministic_collision_resolution,
                    enforce_first_as,
                    group,
                    hold_time,
                    idle_hold_jitter,
                    idle_hold_time,
                    interface,
                    ipv4_unicast,
                    ipv6_unicast,
                    keepalive,
                    local_pref,
                    md5_auth_key,
                    min_ttl,
                    multi_exit_discriminator,
                    name,
                    passive,
                    remote_asn,
                    resolution,
                    src_addr,
                    src_port,
                    vlan_id,
                } = neighbor;

                let common = DiffableBgpCommonPeerConfig {
                    asn,
                    name,
                    group,
                    hold_time,
                    idle_hold_time,
                    delay_open,
                    connect_retry,
                    keepalive,
                    resolution,
                    passive,
                    remote_asn,
                    min_ttl,
                    md5_auth_key,
                    multi_exit_discriminator,
                    communities,
                    local_pref,
                    enforce_first_as,
                    vlan_id,
                    ipv4_unicast: ipv4_unicast
                        .map(TryFrom::try_from)
                        .transpose()
                        .with_context(|| {
                            format!(
                                "invalid ipv4 unicast network \
                                 on unnumbered interface {interface}"
                            )
                        })?,
                    ipv6_unicast: ipv6_unicast
                        .map(TryFrom::try_from)
                        .transpose()
                        .with_context(|| {
                            format!(
                                "invalid ipv6 unicast network \
                                 on unnumbered interface {interface}"
                            )
                        })?,
                    deterministic_collision_resolution,
                    idle_hold_jitter: idle_hold_jitter.map(From::from),
                    connect_retry_jitter: connect_retry_jitter.map(From::from),
                    src_addr,
                    src_port,
                };

                unnumbered_peers.insert(
                    interface,
                    DiffableBgpUnnumberedPeerConfig {
                        router_lifetime: act_as_a_default_ipv6_router,
                        common,
                    },
                );
            }
        }

        Ok(Self {
            max_paths,
            routers,
            originate4,
            originate6,
            shapers,
            checkers,
            numbered_peers,
            unnumbered_peers,
        })
    }

    fn from_desired_config(
        config: &RackNetworkConfig,
        our_switch_slot: ThisSledSwitchSlot,
        log: &Logger,
    ) -> anyhow::Result<Self> {
        // Filter down to just the peers of the ports matching our switch slot.
        let our_bgp_peers = config
            .ports
            .iter()
            .filter(|port| port.switch == our_switch_slot)
            .flat_map(|port| {
                port.bgp_peers.iter().map(|peer| (&port.port, peer))
            });

        let mut max_paths: Option<MaxPathConfig> = None;
        let mut routers = BTreeMap::new();
        let mut originate4 = BTreeMap::new();
        let mut originate6 = BTreeMap::new();
        let mut shapers = BTreeMap::new();
        let mut checkers = BTreeMap::new();
        let mut numbered_peers = BTreeMap::new();
        let mut unnumbered_peers = BTreeMap::new();

        for (port_name, peer_config) in our_bgp_peers {
            let BgpPeerConfig {
                asn,
                // TODO-cleanup We expect this field to contain the same value
                // as `port_name` from the parent map; we should restructure the
                // port config to make this relationship more precise.
                port: _,
                addr,
                hold_time,
                idle_hold_time,
                delay_open,
                connect_retry,
                keepalive,
                remote_asn,
                min_ttl,
                md5_auth_key,
                multi_exit_discriminator,
                communities,
                local_pref,
                enforce_first_as,
                allowed_import,
                allowed_export,
                vlan_id,
            } = peer_config;

            // Find the router config for this peer's ASN, unless we already did
            // so in a previous iteration from another peer on the same ASN.
            if let btree_map::Entry::Vacant(entry) = routers.entry(*asn) {
                let BgpConfig {
                    asn: _,
                    originate,
                    shaper,
                    checker,
                    max_paths: this_config_max_paths,
                } = config.bgp.iter().find(|c| c.asn == *asn).with_context(
                    || {
                        format!(
                            "invalid RackNetworkConfig: \
                             peer {addr:?} references ASN {asn} \
                             which doesn't have a corresponding BGP config",
                        )
                    },
                )?;

                // We only expect to have at most one BGP config, and don't
                // really have a way of handling multiple configs with different
                // max_paths settings anyway, so just take the last one we find
                // in this loop.
                if let Some(prev_max_paths) = max_paths
                    && prev_max_paths != *this_config_max_paths
                {
                    warn!(
                        log,
                        "found multiple BGP configs with different max_paths, \
                         but can only choose one";
                        "max-paths-ignored" => prev_max_paths.as_u8(),
                        "max-paths-chosen" => this_config_max_paths.as_u8(),
                    );
                }
                max_paths = Some(*this_config_max_paths);

                // TODO-correctness The values here should come from Nexus, but
                // aren't currently available in the config, so we have to pick
                // reasonable defaults.
                //
                // The ID and the ASN are supposed to be able to be different
                // values, we just don't expose that yet. In BGP, Router IDs are
                // distinct values from the ASN and both are required for BGP
                // peering. Since we only support eBGP, the ASNs on each switch
                // is different, but in iBGP the ASNs would be the same, and
                // thus the Router ID would be a value that can be used to
                // distinguish between two BGP routers in the same ASN. I think
                // some vendor implementations default to using the interface IP
                // Address or the highest IP Address configured to a loopback
                // interface for the Router ID.
                entry.insert(DiffableBgpRouterConfig {
                    id: *asn,
                    graceful_shutdown: false,
                    listen: SocketAddrV6::new(
                        Ipv6Addr::UNSPECIFIED,
                        BGP_PORT,
                        0,
                        0,
                    )
                    .to_string(),
                });

                originate4.insert(
                    *asn,
                    originate
                        .iter()
                        .filter_map(|net| match net {
                            IpNet::V4(net) => Some(*net),
                            IpNet::V6(_) => None,
                        })
                        .collect(),
                );
                originate6.insert(
                    *asn,
                    originate
                        .iter()
                        .filter_map(|net| match net {
                            IpNet::V6(net) => Some(*net),
                            IpNet::V4(_) => None,
                        })
                        .collect(),
                );
                if let Some(shaper) = shaper {
                    shapers.insert(*asn, shaper.clone());
                }
                if let Some(checker) = checker {
                    checkers.insert(*asn, checker.clone());
                }
            }

            let name = match addr {
                RouterPeerType::Unnumbered { .. } => {
                    format!("unnumbered-{port_name}")
                }
                RouterPeerType::Numbered { ip } => ip.to_string(),
            };

            let common = DiffableBgpCommonPeerConfig {
                asn: *asn,
                name,
                // TODO-correctness Nexus historically used the port name as the
                // group name. Does this matter?
                group: port_name.clone(),
                hold_time: hold_time
                    .unwrap_or(BgpPeerConfig::DEFAULT_HOLD_TIME),
                idle_hold_time: idle_hold_time
                    .unwrap_or(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME),
                delay_open: delay_open
                    .unwrap_or(BgpPeerConfig::DEFAULT_DELAY_OPEN),
                connect_retry: connect_retry
                    .unwrap_or(BgpPeerConfig::DEFAULT_CONNECT_RETRY),
                keepalive: keepalive
                    .unwrap_or(BgpPeerConfig::DEFAULT_KEEPALIVE),
                resolution: BGP_SESSION_RESOLUTION,
                passive: false,
                remote_asn: *remote_asn,
                min_ttl: *min_ttl,
                md5_auth_key: md5_auth_key.clone(),
                multi_exit_discriminator: *multi_exit_discriminator,
                communities: communities.clone(),
                local_pref: *local_pref,
                enforce_first_as: *enforce_first_as,
                vlan_id: *vlan_id,
                ipv4_unicast: Some(DiffableUnicastConfig {
                    nexthop: None,
                    import_policy: allowed_import.into(),
                    export_policy: allowed_export.into(),
                }),
                ipv6_unicast: Some(DiffableUnicastConfig {
                    nexthop: None,
                    import_policy: allowed_import.into(),
                    export_policy: allowed_export.into(),
                }),
                deterministic_collision_resolution: false,
                idle_hold_jitter: None,
                connect_retry_jitter: Some(DiffableJitterRange {
                    max: 1.0.into(),
                    min: 0.75.into(),
                }),
                src_addr: None,
                src_port: None,
            };

            match addr {
                RouterPeerType::Unnumbered { router_lifetime } => {
                    let interface = format!("tfport{port_name}_0");
                    if let Some(_prev) = unnumbered_peers.insert(
                        interface.clone(),
                        DiffableBgpUnnumberedPeerConfig {
                            router_lifetime: router_lifetime.as_u16(),
                            common,
                        },
                    ) {
                        bail!(
                            "invalid config: multiple unnumbered peers \
                             for interface {interface}"
                        );
                    }
                }
                RouterPeerType::Numbered { ip } => {
                    let addr = SocketAddr::new((*ip).into(), BGP_PORT);
                    if let Some(_prev) = numbered_peers.insert(addr, common) {
                        bail!(
                            "invalid config: multiple numbered peers \
                             for address {addr}"
                        );
                    }
                }
            }
        }

        Ok(Self {
            max_paths,
            routers,
            originate4,
            originate6,
            shapers,
            checkers,
            numbered_peers,
            unnumbered_peers,
        })
    }
}

// Given the result of a progenitor request, return `Ok(Some(value))` if the
// request succeeded, `Ok(None)` if the request failed with an HTTP not found,
// or `Err(err)` if the request failed with any other error.
fn notfound_to_none<T>(
    result: Result<MgdResponseValue<T>, MgdClientError>,
) -> Result<Option<T>, MgdClientError> {
    match result {
        Ok(response) => Ok(Some(response.into_inner())),
        Err(err) => {
            if err.status() == Some(http::StatusCode::NOT_FOUND) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests;
