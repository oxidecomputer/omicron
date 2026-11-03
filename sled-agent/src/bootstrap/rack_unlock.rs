// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use bootstrap_agent_lockstep_types::SledAgentStartStatus;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use trust_quorum::NodeApiError;
use trust_quorum::NodeTaskHandle;
use trust_quorum_protocol::LoadRackSecretError;
use trust_quorum_types::persistent_state::ExpungedMetadata;
use trust_quorum_types::status::NodeStatus;

const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub(crate) enum RackUnlockOutcome {
    Unlocked,
    Expunged(ExpungedMetadata),
}

#[derive(Debug, PartialEq)]
enum GateDecision {
    Expunged(ExpungedMetadata),
    Wait(SledAgentStartStatus),
}

fn evaluate_status(status: NodeStatus) -> GateDecision {
    if let Some(expunged) = status.persistent_state.expunged {
        return GateDecision::Expunged(expunged);
    }

    let Some(latest_committed_epoch) =
        status.persistent_state.commits.last().copied()
    else {
        return GateDecision::Wait(
            SledAgentStartStatus::WaitingForTrustQuorumConfig {
                connected_peers: status.connected_peers,
            },
        );
    };

    GateDecision::Wait(SledAgentStartStatus::WaitingForRackUnlock {
        latest_committed_epoch,
        connected_peers: status.connected_peers,
    })
}

pub(crate) async fn wait_for_rack_unlock(
    trust_quorum: &NodeTaskHandle,
    start_status_tx: &watch::Sender<SledAgentStartStatus>,
    log: &Logger,
) -> RackUnlockOutcome {
    loop {
        match trust_quorum.status().await {
            Ok(status) => match evaluate_status(status) {
                GateDecision::Expunged(expunged) => {
                    warn!(
                        log,
                        "this sled has been expunged from the trust quorum; \
                         refusing to start sled-agent";
                        "epoch" => %expunged.epoch,
                        "learned_from" => %expunged.from,
                    );
                    return RackUnlockOutcome::Expunged(expunged);
                }
                GateDecision::Wait(status) => {
                    start_status_tx.send_replace(status);
                }
            },
            Err(err) => {
                warn!(
                    log,
                    "failed to read trust quorum status while waiting for \
                     rack unlock";
                    "error" => InlineErrorChain::new(&err),
                );
                sleep(POLL_INTERVAL).await;
                continue;
            }
        }

        match trust_quorum.load_latest_rack_secret().await {
            Ok(Some(_)) => {
                info!(log, "rack unlocked");
                return RackUnlockOutcome::Unlocked;
            }
            Ok(None) => (),
            Err(NodeApiError::LoadRackSecret(
                LoadRackSecretError::NoCommittedConfigurations,
            )) => (),
            Err(err) => {
                warn!(
                    log,
                    "failed to load rack secret while waiting for rack unlock";
                    "error" => InlineErrorChain::new(&err),
                );
            }
        }

        sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trust_quorum_types::configuration::BaseboardId;
    use trust_quorum_types::status::NodeStatus;
    use trust_quorum_types::types::Epoch;

    fn baseboard_id(serial: &str) -> BaseboardId {
        BaseboardId {
            part_number: "test-part".to_string(),
            serial_number: serial.to_string(),
        }
    }

    #[test]
    fn no_commits_waits_for_trust_quorum_config() {
        let peer = baseboard_id("peer");
        let mut status = NodeStatus::default();
        status.connected_peers.insert(peer.clone());

        assert_eq!(
            evaluate_status(status),
            GateDecision::Wait(
                SledAgentStartStatus::WaitingForTrustQuorumConfig {
                    connected_peers: [peer].into_iter().collect(),
                }
            )
        );
    }

    #[test]
    fn expunged_takes_precedence_over_commits() {
        let expunged =
            ExpungedMetadata { epoch: Epoch(3), from: baseboard_id("peer") };
        let mut status = NodeStatus::default();
        status.persistent_state.commits.insert(Epoch(1));
        status.persistent_state.commits.insert(Epoch(2));
        status.persistent_state.expunged = Some(expunged.clone());

        assert_eq!(evaluate_status(status), GateDecision::Expunged(expunged));
    }

    #[test]
    fn committed_and_not_expunged_waits_for_unlock() {
        let peer = baseboard_id("peer");
        let mut status = NodeStatus::default();
        status.persistent_state.commits.insert(Epoch(1));
        status.persistent_state.commits.insert(Epoch(4));
        status.connected_peers.insert(peer.clone());

        assert_eq!(
            evaluate_status(status),
            GateDecision::Wait(SledAgentStartStatus::WaitingForRackUnlock {
                latest_committed_epoch: Epoch(4),
                connected_peers: [peer].into_iter().collect(),
            })
        );
    }
}
