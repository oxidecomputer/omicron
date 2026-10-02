// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DdmReconciler`] is the one-stop-shop for sled-agent communication with the
//! local mg-ddm instance.
//!
//! It wraps a long-running tokio task that is responsible for:
//!
//! * Periodically ensuring mg-ddm is configured to report Oximeter stats
//! * Periodically ensuring that all prefixes we're supposed to advertise are
//!   advertised, and no prefixes we used to advertise are still advertised
//! * Updating either of these immediately when changes are requested

use illumos_utils::opte::PortManager;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX_LENGTH;
use omicron_ddm_admin_client::Client;
use omicron_ddm_admin_client::DdmError;
use omicron_ddm_admin_client::MulticastOrigin;
use omicron_ddm_admin_client::OverlayMulticast;
use omicron_ddm_admin_client::UnderlayMulticastIpv6;
use omicron_ddm_admin_client::Vni;
use omicron_ddm_admin_client::types::EnableStatsRequest;
use oxnet::Ipv6Net;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

// Regardless of any changes, how often should we retry our two primary
// reconciliation actions? Periodic retry handles two cases:
//
// * If we've made changes (e.g., we've started or stopped an internal-dns zone,
//   which changes the prefixes we should be advertising), we'll immediately try
//   to reconcile them with maghemite. If that fails, we don't do anything
//   special: we know we'll retry again shortly anyway.
// * If we've made no changes, both our operations are no-ops from mg-ddm's
//   point of view. But if mg-ddm itself has restarted, it may have lost some
//   state, and our periodic reruns will ensure it gets back in sync. (At the
//   time of this writing, mg-ddm does its own persistence for prefixes to
//   advertise, but will not restart its Oximeter stats production on its own.)
const REENABLE_STATS_INTERVAL: Duration = Duration::from_secs(60);
const RECONCILE_PREFIXES_INTERVAL: Duration = Duration::from_secs(60);
const RECONCILE_MULTICAST_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct DdmReconciler {
    prefixes: watch::Sender<PrefixesToAdvertise>,
    enable_stats: watch::Sender<Option<EnableStatsRequest>>,
    port_manager: watch::Sender<Option<PortManager>>,
    _worker_task: JoinHandle<()>,
}

impl DdmReconciler {
    pub fn new(
        bootstrap_subnet: Ipv6Subnet<SLED_PREFIX_LENGTH>,
        base_log: &Logger,
    ) -> Result<Self, DdmError> {
        let client = Client::localhost(base_log)?;

        let (prefixes, prefixes_rx) =
            watch::channel(PrefixesToAdvertise::new(bootstrap_subnet));
        let (enable_stats, enable_stats_rx) = watch::channel(None);
        let (port_manager, port_manager_rx) = watch::channel(None);

        let worker = Worker::new(
            client,
            prefixes_rx,
            enable_stats_rx,
            port_manager_rx,
            base_log,
        );

        Ok(Self {
            prefixes,
            enable_stats,
            port_manager,
            _worker_task: tokio::spawn(worker.run()),
        })
    }

    /// Late-bind the [`PortManager`] the worker reads OPTE multicast state
    /// from.
    ///
    /// The reconciler is constructed during bootstrap, before the
    /// `PortManager` exists, so the worker starts with no source of multicast
    /// subscriptions. Once the `PortManager` is available, sled-agent calls
    /// this to wire it in. The resulting watch change immediately kicks a
    /// multicast reconcile.
    pub fn set_port_manager(&self, port_manager: PortManager) {
        self.port_manager.send_modify(|pm| {
            *pm = Some(port_manager);
        });
    }

    pub fn enable_stats(&self, request: EnableStatsRequest) {
        self.enable_stats.send_modify(|req| {
            *req = Some(request);
        });
    }

    pub fn set_underlay_subnet(
        &self,
        underlay_subnet: Ipv6Subnet<SLED_PREFIX_LENGTH>,
    ) {
        self.prefixes.send_if_modified(|prefixes| {
            let modified = prefixes.underlay != Some(underlay_subnet);
            prefixes.underlay = Some(underlay_subnet);
            modified
        });
    }

    /// Add an internal DNS subset to the set we should be advertising.
    ///
    /// This method is idempotent.
    pub fn add_internal_dns_subnet(
        &self,
        internal_dns_subnet: Ipv6Subnet<SLED_PREFIX_LENGTH>,
    ) {
        self.prefixes.send_if_modified(|prefixes| {
            prefixes.internal_dns.insert(internal_dns_subnet)
        });
    }

    /// Remove an internal DNS subnet from the set we should be advertising.
    ///
    /// This method is idempotent.
    pub fn remove_internal_dns_subnet(
        &self,
        internal_dns_subnet: Ipv6Subnet<SLED_PREFIX_LENGTH>,
    ) {
        self.prefixes.send_if_modified(|prefixes| {
            prefixes.internal_dns.remove(&internal_dns_subnet)
        });
    }
}

#[derive(Debug)]
struct PrefixesToAdvertise {
    bootstrap: Ipv6Subnet<SLED_PREFIX_LENGTH>,
    underlay: Option<Ipv6Subnet<SLED_PREFIX_LENGTH>>,
    internal_dns: BTreeSet<Ipv6Subnet<SLED_PREFIX_LENGTH>>,
}

impl PrefixesToAdvertise {
    fn new(bootstrap: Ipv6Subnet<SLED_PREFIX_LENGTH>) -> Self {
        Self { bootstrap, underlay: None, internal_dns: BTreeSet::new() }
    }

    fn current(&self) -> BTreeSet<Ipv6Net> {
        let mut prefixes: BTreeSet<Ipv6Net> =
            self.internal_dns.iter().map(|subnet| subnet.net()).collect();

        prefixes.insert(self.bootstrap.net());
        if let Some(underlay) = self.underlay {
            prefixes.insert(underlay.net());
        }

        prefixes
    }
}

#[derive(Debug)]
struct Worker {
    client: Client,
    prefixes: watch::Receiver<PrefixesToAdvertise>,
    enable_stats: watch::Receiver<Option<EnableStatsRequest>>,
    port_manager: watch::Receiver<Option<PortManager>>,
    log: Logger,
}

impl Worker {
    fn new(
        client: Client,
        prefixes: watch::Receiver<PrefixesToAdvertise>,
        enable_stats: watch::Receiver<Option<EnableStatsRequest>>,
        port_manager: watch::Receiver<Option<PortManager>>,
        base_log: &Logger,
    ) -> Self {
        let log = base_log.new(o!("component" => "DdmReconciler"));
        Self { client, prefixes, enable_stats, port_manager, log }
    }

    async fn run(mut self) {
        let mut reenable_stats_ticker =
            tokio::time::interval(REENABLE_STATS_INTERVAL);
        let mut reconcile_prefixes_ticker =
            tokio::time::interval(RECONCILE_PREFIXES_INTERVAL);
        let mut reconcile_multicast_ticker =
            tokio::time::interval(RECONCILE_MULTICAST_INTERVAL);

        // Missed ticks probably mean ddm is not responsive, so just skip them
        // instead of trying to burst and catch up.
        reenable_stats_ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);
        reconcile_prefixes_ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);
        reconcile_multicast_ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            // The OPTE change notify lives inside the (late-bound) PortManager.
            // Clone the handle out of the watch so we can await it without
            // holding the watch borrow across the select. When no PortManager
            // is wired up yet, this arm parks forever and the periodic ticker
            // plus the `port_manager.changed()` arm cover reconciliation.
            let mcast_changed = self
                .port_manager
                .borrow()
                .as_ref()
                .map(PortManager::multicast_changed);

            tokio::select! {
                // cancel-safe per docs on Interval::tick()
                _ = reenable_stats_ticker.tick() => {
                    self.try_enable_stats().await;
                }

                // cancel-safe per docs on Interval::tick()
                _ = reconcile_prefixes_ticker.tick() => {
                    self.try_reconcile_prefixes().await;
                }

                // cancel-safe per docs on Interval::tick()
                _ = reconcile_multicast_ticker.tick() => {
                    self.try_reconcile_multicast().await;
                }

                // cancel-safe per docs on Receiver::changed()
                _ = self.enable_stats.changed() => {
                    self.try_enable_stats().await;
                }

                // cancel-safe per docs on Receiver::changed()
                _ = self.prefixes.changed() => {
                    self.try_reconcile_prefixes().await;
                }

                // cancel-safe per docs on Receiver::changed(). Kicks an initial
                // multicast reconcile as soon as the PortManager is wired up.
                _ = self.port_manager.changed() => {
                    self.try_reconcile_multicast().await;
                }

                // cancel-safe: `Notify::notified` is cancel-safe, and the
                // pending future never resolves, so dropping it is a no-op.
                _ = await_mcast_change(&mcast_changed) => {
                    self.try_reconcile_multicast().await;
                }
            }
        }
    }

    async fn try_enable_stats(&mut self) {
        // Clone the request to avoid keeping the watch channel locked for the
        // duration of the request to DDM below.
        let Some(request) = self.enable_stats.borrow_and_update().clone()
        else {
            return;
        };

        info!(self.log, "attempting to ensure stats are enabled");
        if let Err(err) = self.client.enable_stats(&request).await {
            warn!(
                self.log, "failed to enable stats";
                InlineErrorChain::new(&err),
            );
            return;
        }
        info!(self.log, "successfully enabled stats");
    }

    async fn try_reconcile_prefixes(&mut self) {
        // Get the current set of prefixes we should be advertising. This only
        // holds the watch lock long enough to call `current()`. We release it
        // before moving on to sending a request to DDM.
        let prefixes = self.prefixes.borrow_and_update().current();

        // Ask DDM what prefixes we've originated.
        info!(self.log, "attempting to list currently-originated prefixes");
        let originated = match self.client.get_originated().await {
            Ok(originated) => BTreeSet::from_iter(originated),
            Err(err) => {
                warn!(
                    self.log, "failed to get originated prefixes";
                    InlineErrorChain::new(&err),
                );
                return;
            }
        };

        // Common case: DDM is already originating all the prefixes we expect,
        // so there's nothing to do.
        if originated == prefixes {
            info!(
                self.log,
                "prefix advertisement reconciliation complete \
                 (nothing to advertise or withdraw)"
            );
            return;
        }

        // Uncommon case: build the sets of prefixes we need to withdraw and/or
        // advertise, then pass those on to DDM.
        let to_withdraw: Vec<_> =
            originated.difference(&prefixes).copied().collect();
        let to_advertise: Vec<_> =
            prefixes.difference(&originated).copied().collect();

        if !to_advertise.is_empty() {
            info!(
                self.log, "attempting to add new prefix advertisements";
                "to_advertise" => ?to_advertise,
            );
            if let Err(err) =
                self.client.advertise_prefixes(&to_advertise).await
            {
                warn!(
                    self.log, "failed to add new prefix advertisements";
                    "to_advertise" => ?to_advertise,
                    InlineErrorChain::new(&err),
                );
                return;
            }
        }

        if !to_withdraw.is_empty() {
            info!(
                self.log, "attempting to withdraw prefix advertisements";
                "to_withdraw" => ?to_withdraw,
            );
            if let Err(err) = self.client.withdraw_prefixes(&to_withdraw).await
            {
                warn!(
                    self.log, "failed to withdraw prefix advertisements";
                    "to_withdraw" => ?to_withdraw,
                    InlineErrorChain::new(&err),
                );
                return;
            }
        }

        info!(
            self.log,
            "completed prefix advertisement reconcilation";
            "prefixes" => ?prefixes,
            "added" => ?to_advertise,
            "withdrawn" => ?to_withdraw,
        );
    }

    /// Reconcile the multicast group subscriptions this sled originates to its
    /// local ddmd against the live OPTE state.
    ///
    /// The desired set is the union of overlay groups subscribed across all
    /// OPTE ports (covering both guest VMMs and probes), each mapped to its
    /// underlay group via the M2P table. Originating a `(*,G)` subscription is
    /// what makes the switch's ddmd import the route and program this sled's
    /// switch port as a replication target. Without it the group's underlay
    /// member set stays empty and no traffic is replicated to this sled.
    ///
    /// Each pass reads the full OPTE state rather than tracking changes, so
    /// the loop self-heals after a ddmd restart and cannot wedge on a lost
    /// withdraw.
    async fn try_reconcile_multicast(&mut self) {
        // The PortManager is late-bound during startup. Until it is wired up
        // there is no OPTE state to read and nothing to reconcile.
        let Some(port_manager) = self.port_manager.borrow_and_update().clone()
        else {
            return;
        };

        // Map each overlay group to its underlay address. A subscription with
        // no M2P entry is skipped below: the entry is installed on a separate
        // path and a later pass (kicked by the OPTE change notify) picks it up.
        let m2p = match port_manager.list_mcast_m2p() {
            Ok(m2p) => m2p,
            Err(err) => {
                warn!(
                    self.log, "failed to list multicast M2P mappings";
                    InlineErrorChain::new(&err),
                );
                return;
            }
        };
        let underlay_by_overlay: HashMap<IpAddr, _> =
            m2p.into_iter().map(|m| (m.group, m.underlay)).collect();

        // The subscription set is the source of truth for what this sled
        // should originate. The M2P table only supplies the underlay address
        // needed to advertise, so a missing M2P entry gates advertisement but
        // never withdrawal (see the withdraw computation below).
        let subscribed: HashSet<IpAddr> =
            port_manager.list_mcast_subscriptions().into_iter().collect();

        let desired = desired_multicast_origins(
            &subscribed,
            &underlay_by_overlay,
            &self.log,
        );

        // Ownership invariant: this reconciler treats the *entire* originated
        // multicast set on its local ddmd as its own, withdrawing any origin
        // not in `desired`. That is only correct because the global-zone sled
        // ddmd this client targets (`Client::localhost`) is not shared with any
        // other origin writer. In particular it must never be the ddmd that a
        // switch-zone mgd's mg-lower MRIB loop originates into: that loop runs
        // the same full-set reconcile against its own desired set, so a shared
        // ddmd would make the two withdraw each other's origins every pass. In
        // production the two run in separate zones (switch-zone ddmd vs GZ
        // ddmd) and never collide. The only way to violate this is a test
        // harness pointing mgd at this ddmd via `--ddm-addr`, which must use a
        // distinct ddmd for mgd (or skip the MRIB loop) instead.
        let originated =
            match self.client.get_originated_multicast_groups().await {
                Ok(originated) => {
                    HashSet::<MulticastOrigin>::from_iter(originated)
                }
                Err(err) => {
                    warn!(
                        self.log, "failed to get originated multicast groups";
                        InlineErrorChain::new(&err),
                    );
                    return;
                }
            };

        let MulticastOriginDelta { to_advertise, to_withdraw } =
            multicast_origin_delta(
                &desired,
                &originated,
                &subscribed,
                &underlay_by_overlay,
            );

        if to_advertise.is_empty() && to_withdraw.is_empty() {
            info!(
                self.log,
                "multicast origin reconciliation complete \
                 (nothing to advertise or withdraw)"
            );
            return;
        }

        if !to_advertise.is_empty() {
            info!(
                self.log, "attempting to advertise multicast groups";
                "to_advertise" => ?to_advertise,
            );
            if let Err(err) =
                self.client.advertise_multicast_groups(&to_advertise).await
            {
                warn!(
                    self.log, "failed to advertise multicast groups";
                    "to_advertise" => ?to_advertise,
                    InlineErrorChain::new(&err),
                );
                return;
            }
        }

        if !to_withdraw.is_empty() {
            info!(
                self.log, "attempting to withdraw multicast groups";
                "to_withdraw" => ?to_withdraw,
            );
            if let Err(err) =
                self.client.withdraw_multicast_groups(&to_withdraw).await
            {
                warn!(
                    self.log, "failed to withdraw multicast groups";
                    "to_withdraw" => ?to_withdraw,
                    InlineErrorChain::new(&err),
                );
                return;
            }
        }

        info!(
            self.log,
            "completed multicast origin reconciliation";
            "added" => ?to_advertise,
            "withdrawn" => ?to_withdraw,
        );
    }
}

/// Await the next OPTE multicast change notification.
///
/// Parks forever when the `PortManager` is not yet wired up, leaving the
/// periodic ticker and the `port_manager.changed()` arm to drive
/// reconciliation until then.
async fn await_mcast_change(notify: &Option<Arc<Notify>>) {
    match notify {
        Some(notify) => notify.notified().await,
        None => std::future::pending().await,
    }
}

/// Build the set of multicast origins this sled should advertise: each
/// subscribed overlay group paired with its underlay address.
///
/// A subscribed group with no M2P mapping is skipped. The underlay address is
/// Nexus-assigned (it carries a collision-avoidance salt) and is not
/// recomputable here, so without the mapping the group cannot be originated. A
/// later pass picks it up once the mapping lands. Groups that fail validation
/// (a non-multicast overlay address, or an underlay outside ff04::/64) are
/// likewise skipped.
///
/// Each origin is a `(*,G)` subscription (`source: None`). Source-specific
/// filtering is enforced at the OPTE port, so the underlay binding loses no SSM
/// semantics.
fn desired_multicast_origins(
    subscribed: &HashSet<IpAddr>,
    underlay_by_overlay: &HashMap<IpAddr, Ipv6Addr>,
    log: &Logger,
) -> HashSet<MulticastOrigin> {
    let mut desired = HashSet::new();
    for &group in subscribed {
        let Some(&underlay) = underlay_by_overlay.get(&group) else {
            debug!(
                log,
                "skipping multicast subscription with no M2P mapping";
                "group" => %group,
            );
            continue;
        };

        let overlay_group = match OverlayMulticast::new(group) {
            Ok(g) => g,
            Err(err) => {
                warn!(
                    log, "skipping non-multicast subscription group";
                    "group" => %group,
                    InlineErrorChain::new(&err),
                );
                continue;
            }
        };
        let underlay_group = match UnderlayMulticastIpv6::new(underlay) {
            Ok(g) => g,
            Err(err) => {
                warn!(
                    log, "skipping M2P mapping with invalid underlay";
                    "group" => %group,
                    "underlay" => %underlay,
                    InlineErrorChain::new(&err),
                );
                continue;
            }
        };

        desired.insert(MulticastOrigin {
            overlay_group,
            underlay_group,
            vni: Vni::DEFAULT_MULTICAST,
            metric: 0,
            source: None,
        });
    }
    desired
}

/// The set difference between desired and currently-originated multicast
/// groups: groups to advertise (newly desired) and groups to withdraw (no
/// longer desired and safe to drop).
struct MulticastOriginDelta {
    to_advertise: Vec<MulticastOrigin>,
    to_withdraw: Vec<MulticastOrigin>,
}

/// Compute the advertise/withdraw delta for multicast origins.
///
/// `to_advertise` is the desired groups not yet originated. `to_withdraw` is
/// the originated groups no longer desired, minus any group still preserved by
/// a pending M2P mapping.
///
/// An originated group is preserved (neither advertised nor withdrawn) while
/// its subscription is still present but its M2P entry is transiently absent:
/// the underlay address is Nexus-assigned and not recomputable here, so
/// flapping it off and back on would only churn origination (and the switch's
/// replication members) until the M2P entry reappears. Withdrawal therefore
/// keys on the subscription set, not on M2P presence.
fn multicast_origin_delta(
    desired: &HashSet<MulticastOrigin>,
    originated: &HashSet<MulticastOrigin>,
    subscribed: &HashSet<IpAddr>,
    underlay_by_overlay: &HashMap<IpAddr, Ipv6Addr>,
) -> MulticastOriginDelta {
    let m2p_pending = |origin: &MulticastOrigin| {
        let overlay = origin.overlay_group.ip();
        subscribed.contains(&overlay)
            && !underlay_by_overlay.contains_key(&overlay)
    };

    let to_advertise = desired.difference(originated).cloned().collect();
    let to_withdraw = originated
        .difference(desired)
        .filter(|origin| !m2p_pending(origin))
        .cloned()
        .collect();

    MulticastOriginDelta { to_advertise, to_withdraw }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;

    fn overlay(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn underlay(s: &str) -> Ipv6Addr {
        s.parse().unwrap()
    }

    // A subscribed group with an M2P mapping becomes a (*,G) origin. The
    // overlay and underlay addresses round-trip and the source is None.
    #[test]
    fn desired_origins_pairs_subscription_with_underlay() {
        let logctx = test_setup_log("desired_origins_pairs_subscription");
        let group = overlay("239.1.1.1");
        let under = underlay("ff04::1");

        let subscribed = HashSet::from([group]);
        let m2p = HashMap::from([(group, under)]);

        let origins = desired_multicast_origins(&subscribed, &m2p, &logctx.log);

        assert_eq!(origins.len(), 1);
        let origin = origins.iter().next().unwrap();
        assert_eq!(origin.overlay_group.ip(), group);
        assert_eq!(origin.underlay_group.ip(), under);
        assert_eq!(origin.source, None);
        assert_eq!(origin.vni, Vni::DEFAULT_MULTICAST);

        logctx.cleanup_successful();
    }

    // A subscription with no M2P mapping is skipped: the underlay address is
    // unknown, so it cannot be advertised until the mapping lands. Subscriptions
    // that do have mappings are still emitted.
    #[test]
    fn desired_origins_skips_subscription_without_m2p() {
        let logctx = test_setup_log("desired_origins_skips_without_m2p");
        let mapped = overlay("239.1.1.1");
        let unmapped = overlay("239.1.1.2");

        let subscribed = HashSet::from([mapped, unmapped]);
        let m2p = HashMap::from([(mapped, underlay("ff04::1"))]);

        let origins = desired_multicast_origins(&subscribed, &m2p, &logctx.log);

        assert_eq!(origins.len(), 1);
        assert_eq!(origins.iter().next().unwrap().overlay_group.ip(), mapped,);

        logctx.cleanup_successful();
    }

    // An M2P entry whose underlay falls outside ff04::/64 fails validation and
    // is skipped rather than producing an invalid origin.
    #[test]
    fn desired_origins_skips_invalid_underlay() {
        let logctx = test_setup_log("desired_origins_skips_invalid_underlay");
        let group = overlay("239.1.1.1");
        // ff0e:: is global scope, not the admin-local ff04::/64 underlay subnet.
        let bad_underlay = underlay("ff0e::1");

        let subscribed = HashSet::from([group]);
        let m2p = HashMap::from([(group, bad_underlay)]);

        let origins = desired_multicast_origins(&subscribed, &m2p, &logctx.log);

        assert!(origins.is_empty());

        logctx.cleanup_successful();
    }

    // No subscriptions yields no origins even when M2P mappings exist. The
    // subscription set, not the M2P table, is the source of truth.
    #[test]
    fn desired_origins_empty_when_no_subscriptions() {
        let logctx = test_setup_log("desired_origins_empty_no_subscriptions");
        let subscribed = HashSet::new();
        let m2p = HashMap::from([(overlay("239.1.1.1"), underlay("ff04::1"))]);

        let origins = desired_multicast_origins(&subscribed, &m2p, &logctx.log);

        assert!(origins.is_empty());

        logctx.cleanup_successful();
    }

    fn origin(group: &str, under: &str) -> MulticastOrigin {
        MulticastOrigin {
            overlay_group: OverlayMulticast::new(overlay(group)).unwrap(),
            underlay_group: UnderlayMulticastIpv6::new(underlay(under))
                .unwrap(),
            vni: Vni::DEFAULT_MULTICAST,
            metric: 0,
            source: None,
        }
    }

    // A desired group that is not yet originated is advertised.
    #[test]
    fn delta_advertises_new_desired_group() {
        let group = overlay("239.1.1.1");
        let desired = HashSet::from([origin("239.1.1.1", "ff04::1")]);
        let originated = HashSet::new();
        let subscribed = HashSet::from([group]);
        let m2p = HashMap::from([(group, underlay("ff04::1"))]);

        let delta =
            multicast_origin_delta(&desired, &originated, &subscribed, &m2p);

        assert_eq!(delta.to_advertise, vec![origin("239.1.1.1", "ff04::1")]);
        assert!(delta.to_withdraw.is_empty());
    }

    // A group no longer subscribed (gone from the desired set, no subscription)
    // is withdrawn.
    #[test]
    fn delta_withdraws_unsubscribed_group() {
        let desired = HashSet::new();
        let originated = HashSet::from([origin("239.1.1.1", "ff04::1")]);
        // No subscriptions and no M2P: the group is genuinely gone.
        let subscribed = HashSet::new();
        let m2p = HashMap::new();

        let delta =
            multicast_origin_delta(&desired, &originated, &subscribed, &m2p);

        assert!(delta.to_advertise.is_empty());
        assert_eq!(delta.to_withdraw, vec![origin("239.1.1.1", "ff04::1")]);
    }

    // The #10640-class regression guard: an originated group whose subscription
    // is still present but whose M2P entry transiently vanished must be
    // preserved, not withdrawn. Without the m2p_pending filter the missing M2P
    // entry drops the group from `desired`, and the difference would withdraw a
    // still-wanted group, flapping origination until the entry reappears.
    #[test]
    fn delta_preserves_subscribed_group_with_pending_m2p() {
        let group = overlay("239.1.1.1");
        // M2P entry is absent, so the group is not in `desired`...
        let desired = HashSet::new();
        let originated = HashSet::from([origin("239.1.1.1", "ff04::1")]);
        // ...but the subscription is still present.
        let subscribed = HashSet::from([group]);
        let m2p = HashMap::new();

        let delta =
            multicast_origin_delta(&desired, &originated, &subscribed, &m2p);

        assert!(delta.to_advertise.is_empty());
        assert!(
            delta.to_withdraw.is_empty(),
            "a subscribed group with a pending M2P entry must not be \
             withdrawn, got {:?}",
            delta.to_withdraw,
        );
    }

    // A group already originated and still desired produces no change.
    #[test]
    fn delta_noop_when_converged() {
        let group = overlay("239.1.1.1");
        let desired = HashSet::from([origin("239.1.1.1", "ff04::1")]);
        let originated = HashSet::from([origin("239.1.1.1", "ff04::1")]);
        let subscribed = HashSet::from([group]);
        let m2p = HashMap::from([(group, underlay("ff04::1"))]);

        let delta =
            multicast_origin_delta(&desired, &originated, &subscribed, &m2p);

        assert!(delta.to_advertise.is_empty());
        assert!(delta.to_withdraw.is_empty());
    }
}
