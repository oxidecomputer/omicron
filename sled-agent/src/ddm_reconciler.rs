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
//!
//! In both cases, changes from sled-agent trigger reconcilation without waiting
//! for the typical period tick.

use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_ddm_admin_client::types::EnableStatsRequest;
use omicron_ddm_admin_client::Client;
use omicron_ddm_admin_client::DdmError;
use oxnet::Ipv6Net;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::time::Duration;
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

#[derive(Debug)]
pub(crate) struct DdmReconciler {
    prefixes: watch::Sender<PrefixesToAdvertise>,
    enable_stats: watch::Sender<Option<EnableStatsRequest>>,
    _worker_task: JoinHandle<()>,
}

impl DdmReconciler {
    pub fn new(
        bootstrap_subnet: Ipv6Subnet<SLED_PREFIX>,
        base_log: &Logger,
    ) -> Result<Self, DdmError> {
        let client = Client::localhost(base_log)?;

        let (prefixes, prefixes_rx) =
            watch::channel(PrefixesToAdvertise::new(bootstrap_subnet));
        let (enable_stats, enable_stats_rx) = watch::channel(None);

        let worker =
            Worker::new(client, prefixes_rx, enable_stats_rx, base_log);

        Ok(Self {
            prefixes,
            enable_stats,
            _worker_task: tokio::spawn(worker.run()),
        })
    }

    pub fn enable_stats(&self, request: EnableStatsRequest) {
        self.enable_stats.send_modify(|req| {
            *req = Some(request);
        });
    }

    pub fn set_underlay_subnet(
        &self,
        underlay_subnet: Ipv6Subnet<SLED_PREFIX>,
    ) {
        self.prefixes.send_if_modified(|prefixes| {
            let modified = prefixes.underlay != Some(underlay_subnet);
            prefixes.underlay = Some(underlay_subnet);
            modified
        });
    }

    pub fn set_internal_dns_subnets(
        &self,
        internal_dns_subnets: BTreeSet<Ipv6Subnet<SLED_PREFIX>>,
    ) {
        self.prefixes.send_if_modified(|prefixes| {
            let modified = prefixes.internal_dns != internal_dns_subnets;
            prefixes.internal_dns = internal_dns_subnets;
            modified
        });
    }
}

#[derive(Debug)]
struct PrefixesToAdvertise {
    bootstrap: Ipv6Subnet<SLED_PREFIX>,
    underlay: Option<Ipv6Subnet<SLED_PREFIX>>,
    internal_dns: BTreeSet<Ipv6Subnet<SLED_PREFIX>>,
}

impl PrefixesToAdvertise {
    fn new(bootstrap: Ipv6Subnet<SLED_PREFIX>) -> Self {
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
    log: Logger,
}

impl Worker {
    fn new(
        client: Client,
        prefixes: watch::Receiver<PrefixesToAdvertise>,
        enable_stats: watch::Receiver<Option<EnableStatsRequest>>,
        base_log: &Logger,
    ) -> Self {
        let log = base_log.new(o!("component" => "DdmReconciler"));
        Self { client, prefixes, enable_stats, log }
    }

    async fn run(mut self) {
        let mut reenable_stats_ticker =
            tokio::time::interval(REENABLE_STATS_INTERVAL);
        let mut reconcile_prefixes_ticker =
            tokio::time::interval(RECONCILE_PREFIXES_INTERVAL);

        // Missed ticks probably mean ddm is not responsive; just skip them
        // instead of trying to burst and catch up.
        reenable_stats_ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);
        reconcile_prefixes_ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // cancel-safe per docs on Interval::tick()
                _ = reenable_stats_ticker.tick() => {
                    self.try_enable_stats().await;
                }

                // cancel-safe per docs on Interval::tick()
                _ = reconcile_prefixes_ticker.tick() => {
                    self.try_reconcile_prefixes().await;
                }

                // cancel-safe per docs on Receiver::changed()
                _ = self.enable_stats.changed() => {
                    self.try_enable_stats().await;
                }

                // cancel-safe per docs on Receiver::changed()
                _ = self.prefixes.changed() => {
                    self.try_reconcile_prefixes().await;
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
        // Get the current set of prefixes we should be advertising.
        let prefixes = self.prefixes.borrow_and_update().current();

        // Ask DDM what prefixes we've originated.
        info!(self.log, "attempting to list currently-originated prefixes");
        let originated = match self.client.get_originated().await {
            Ok(originated) => BTreeSet::from_iter(originated.into_iter()),
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
}
