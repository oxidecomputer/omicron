// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciliation of BFD peer configuration within mgd.

use crate::switch_zone_slot::ThisSledSwitchSlot;
use daft::BTreeSetDiff;
use daft::Diffable;
use mg_admin_client::Client;
use mg_admin_client::types::BfdPeerConfig as MgdBfdPeerConfig;
use mg_admin_client::types::SessionMode as MgdSessionMode;
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::RackNetworkConfig;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;

type MgdClientError = mg_admin_client::Error<mg_admin_client::types::Error>;

#[derive(Debug, Clone)]
pub struct MgdBfdOperationFailure {
    pub peer: IpAddr,
    pub error: String,
}

#[derive(Debug, Clone)]
pub enum MgdBfdReconcilerStatus {
    /// Reconciliation was skipped because we couldn't fetch the current set of
    /// BFD peers from mgd.
    FailedReadingBfdPeers(String),
    /// Reconciliation completed.
    Reconciled {
        unchanged: BTreeSet<IpAddr>,
        remove_success: Vec<IpAddr>,
        remove_failure: Vec<MgdBfdOperationFailure>,
        add_success: Vec<IpAddr>,
        add_failure: Vec<MgdBfdOperationFailure>,
    },
}

impl slog::KV for MgdBfdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let skipped_key = "bfd-reconciler-skipped";
        match self {
            Self::FailedReadingBfdPeers(reason) => {
                serializer.emit_str(skipped_key.into(), reason)
            }
            Self::Reconciled {
                unchanged,
                remove_success,
                remove_failure,
                add_success,
                add_failure,
            } => {
                for (key, val) in [
                    ("bfd-unchanged", unchanged.len()),
                    ("bfd-successfully-removed", remove_success.len()),
                    ("bfd-failed-to-remove", remove_failure.len()),
                    ("bfd-successfully-added", add_success.len()),
                    ("bfd-failed-to-add", add_failure.len()),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
        }
    }
}

pub(super) async fn reconcile(
    client: &Client,
    desired_config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> MgdBfdReconcilerStatus {
    let current_peers = match mgd_get_current_bfd_peers(client).await {
        Ok(peers) => peers,
        Err(err) => {
            return MgdBfdReconcilerStatus::FailedReadingBfdPeers(format!(
                "failed to read current BFD peers from mgd: {}",
                InlineErrorChain::new(&err)
            ));
        }
    };

    let plan = ReconciliationPlan::new(
        current_peers,
        desired_config,
        our_switch_slot,
        log,
    );

    apply_plan(client, plan, log).await
}

async fn mgd_get_current_bfd_peers(
    client: &Client,
) -> Result<Vec<MgdBfdPeerConfig>, MgdClientError> {
    let peers = client.get_bfd_peers().await?.into_inner();
    Ok(peers.into_iter().map(|info| info.config).collect())
}

/// Apply the contents of `plan` to mgd via `client`.
///
/// This requires `plan.to_remove.len() + plan.to_add.len()` independent
/// calls to `mgd`. We do not short circuit on failure: we'll always attempt to
/// make every call required. This may not be the right choice, but some
/// arguments in favor:
///
/// * In practice we expect the number of calls here to be small.
/// * We always want to report the status of every step described by `plan`, and
///   implementing stop-on-first-failure means we'd need to record a "didn't
///   attempt because of an earlier failure" status for some steps. That's
///   doable but annoying.
async fn apply_plan(
    client: &Client,
    plan: ReconciliationPlan,
    log: &Logger,
) -> MgdBfdReconcilerStatus {
    let ReconciliationPlan { unchanged, to_remove, to_add } = plan;

    // Remove before adding in case a peer's parameters changed (which shows
    // up as a remove + add of the same peer IP).
    let mut remove_success = Vec::new();
    let mut remove_failure = Vec::new();
    for peer_ip in to_remove {
        match client.remove_bfd_peer(&peer_ip).await {
            Ok(_) => {
                info!(
                    log, "successfully removed BFD peer";
                    "peer" => %peer_ip,
                );
                remove_success.push(peer_ip);
            }
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log, "failed to remove BFD peer";
                    "peer" => %peer_ip,
                    &err,
                );
                remove_failure.push(MgdBfdOperationFailure {
                    peer: peer_ip,
                    error: err.to_string(),
                });
            }
        }
    }

    let mut add_success = Vec::new();
    let mut add_failure = Vec::new();
    for diffable_peer in to_add {
        let mgd_config = MgdBfdPeerConfig::from(diffable_peer);
        let peer_ip = mgd_config.peer;
        match client.add_bfd_peer(&mgd_config).await {
            Ok(_) => {
                info!(
                    log, "successfully added BFD peer";
                    "peer" => %peer_ip,
                );
                add_success.push(peer_ip);
            }
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log, "failed to add BFD peer";
                    "peer" => %peer_ip,
                    &err,
                );
                add_failure.push(MgdBfdOperationFailure {
                    peer: peer_ip,
                    error: err.to_string(),
                });
            }
        }
    }

    MgdBfdReconcilerStatus::Reconciled {
        unchanged,
        remove_success,
        remove_failure,
        add_success,
        add_failure,
    }
}

#[derive(Debug, PartialEq)]
struct ReconciliationPlan {
    // Set of BFD peers that remained unchanged in this reconciliation.
    unchanged: BTreeSet<IpAddr>,

    // Peer IPs to remove (only the IP is needed for removal).
    to_remove: BTreeSet<IpAddr>,

    // Full peer configs to add.
    to_add: BTreeSet<DiffableBfdPeer>,
}

impl ReconciliationPlan {
    fn new(
        mgd_current_peers: Vec<MgdBfdPeerConfig>,
        config: &RackNetworkConfig,
        our_switch_slot: ThisSledSwitchSlot,
        log: &Logger,
    ) -> Self {
        // Convert current mgd peers into diffable form.
        let current: BTreeSet<DiffableBfdPeer> =
            mgd_current_peers.into_iter().map(DiffableBfdPeer::from).collect();

        // Convert desired config into diffable form.
        let desired: BTreeSet<DiffableBfdPeer> = config
            .bfd
            .iter()
            .filter(|peer| peer.switch == our_switch_slot)
            .map(DiffableBfdPeer::from)
            .collect();

        let BTreeSetDiff { common, added, removed } = current.diff(&desired);

        let unchanged =
            common.into_iter().map(|p| p.peer).collect::<BTreeSet<_>>();
        let to_remove =
            removed.into_iter().map(|p| p.peer).collect::<BTreeSet<_>>();
        let to_add = added.into_iter().copied().collect::<BTreeSet<_>>();

        info!(
            log,
            "generated mgd BFD reconciliation plan";
            "bfd_peers_unchanged" => unchanged.len(),
            "bfd_peers_to_remove" => to_remove.len(),
            "bfd_peers_to_add" => to_add.len(),
        );

        Self { unchanged, to_remove, to_add }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, daft::Diffable,
)]
struct DiffableBfdPeer {
    peer: IpAddr,
    listen: IpAddr,
    detection_threshold: u8,
    required_rx: u64,
    mode: DiffableSessionMode,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, daft::Diffable,
)]
enum DiffableSessionMode {
    SingleHop,
    MultiHop,
}

impl From<MgdBfdPeerConfig> for DiffableBfdPeer {
    fn from(config: MgdBfdPeerConfig) -> Self {
        Self {
            peer: config.peer,
            listen: config.listen,
            detection_threshold: config.detection_threshold,
            required_rx: config.required_rx,
            mode: match config.mode {
                MgdSessionMode::SingleHop => DiffableSessionMode::SingleHop,
                MgdSessionMode::MultiHop => DiffableSessionMode::MultiHop,
            },
        }
    }
}

impl From<&'_ sled_agent_types::early_networking::BfdPeerConfig>
    for DiffableBfdPeer
{
    fn from(
        config: &'_ sled_agent_types::early_networking::BfdPeerConfig,
    ) -> Self {
        Self {
            peer: config.remote,
            // TODO-cleanup We should use stronger types for BFD addresses,
            // similar to the work done for BGP in
            // <https://github.com/oxidecomputer/omicron/issues/9832>.
            listen: config.local.unwrap_or(Ipv4Addr::UNSPECIFIED.into()),
            detection_threshold: config.detection_threshold,
            required_rx: config.required_rx,
            mode: match config.mode {
                BfdMode::SingleHop => DiffableSessionMode::SingleHop,
                BfdMode::MultiHop => DiffableSessionMode::MultiHop,
            },
        }
    }
}

impl From<DiffableBfdPeer> for MgdBfdPeerConfig {
    fn from(peer: DiffableBfdPeer) -> Self {
        Self {
            peer: peer.peer,
            listen: peer.listen,
            detection_threshold: peer.detection_threshold,
            required_rx: peer.required_rx,
            mode: match peer.mode {
                DiffableSessionMode::SingleHop => MgdSessionMode::SingleHop,
                DiffableSessionMode::MultiHop => MgdSessionMode::MultiHop,
            },
        }
    }
}

#[cfg(test)]
mod tests;
