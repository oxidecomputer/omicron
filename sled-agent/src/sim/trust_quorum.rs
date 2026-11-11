// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: fake trust quorum node for the sim; just enough for Nexus (the reconfigure send in tq_add_sleds and tq_remove_sled, then the trust quorum background task) to drive a config from Preparing to Committed.
// TODO-RAINCLAUDE: fidelity limit: each sim sled keeps only its own view, in memory only; nothing is shared between sim sleds or survives a restart.
// TODO-RAINCLAUDE: fidelity limit: no Prepare messages, key shares, rack secrets, or expungement; the coordinator reports every member's prepare as acked at once.
// TODO-RAINCLAUDE: fidelity limit: reconfigure doesn't check last_committed_epoch: sim sleds never learn about configs Nexus seeds directly (e.g. the RSS config), and proxied commits never reach the next coordinator.
// TODO-RAINCLAUDE: fidelity limit: reconfigure doesn't validate membership size or threshold.
// TODO-RAINCLAUDE: fidelity limit: commit and prepare_and_commit accept non-members and unprepared epochs; prepare_and_commit never returns Pending.
// TODO-RAINCLAUDE: fidelity limit: proxy requests aren't validated and succeed without reaching the destination (handled in http_entrypoints.rs).
// TODO-RAINCLAUDE: fidelity limit: LRTQ upgrade, node status, and network config endpoints stay unimplemented (405) even with the fake enabled.

use omicron_uuid_kinds::RackUuid;
use sled_agent_types::trust_quorum::TrustQuorumCoordinatorStatusResponse;
use sled_hardware_types::BaseboardId;
use trust_quorum_types::configuration::Configuration;
use trust_quorum_types::crypto::Sha3_256Digest;
use trust_quorum_types::messages::{CommitRequest, ReconfigureMsg};
use trust_quorum_types::status::{CommitStatus, CoordinatorStatus};
use trust_quorum_types::types::Epoch;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FakeTrustQuorumError {
    #[error(
        "trust quorum request is for rack {got}, but this sled belongs to \
         rack {expected}"
    )]
    RackIdMismatch { expected: RackUuid, got: RackUuid },
    #[error(
        "sled {coordinator} cannot coordinate trust quorum epoch {epoch}: \
         it is not a member of the proposed configuration"
    )]
    CoordinatorNotAMember { coordinator: BaseboardId, epoch: Epoch },
    #[error(
        "cannot coordinate trust quorum epoch {epoch}: this sled is already \
         coordinating a different reconfiguration at the same epoch"
    )]
    ConflictingReconfigure { epoch: Epoch },
    #[error(
        "cannot coordinate trust quorum epoch {proposed}: this sled has \
         already seen epoch {latest}"
    )]
    StaleEpoch { latest: Epoch, proposed: Epoch },
}

pub(crate) struct FakeTrustQuorum {
    own_id: BaseboardId,
    rack_id: RackUuid,
    history: Option<FakeNodeHistory>,
}

enum FakeNodeHistory {
    Coordinating(ReconfigureMsg),
    Committed { epoch: Epoch },
}

impl FakeNodeHistory {
    fn latest_config_epoch(&self) -> Epoch {
        match self {
            FakeNodeHistory::Coordinating(msg) => msg.epoch,
            FakeNodeHistory::Committed { epoch } => *epoch,
        }
    }
}

impl FakeTrustQuorum {
    pub(crate) fn new(own_id: BaseboardId, rack_id: RackUuid) -> Self {
        Self { own_id, rack_id, history: None }
    }

    fn check_rack_id(&self, got: RackUuid) -> Result<(), FakeTrustQuorumError> {
        if self.rack_id == got {
            Ok(())
        } else {
            Err(FakeTrustQuorumError::RackIdMismatch {
                expected: self.rack_id,
                got,
            })
        }
    }

    pub(crate) fn reconfigure(
        &mut self,
        msg: ReconfigureMsg,
    ) -> Result<(), FakeTrustQuorumError> {
        self.check_rack_id(msg.rack_id)?;
        if !msg.members.contains(&self.own_id) {
            return Err(FakeTrustQuorumError::CoordinatorNotAMember {
                coordinator: self.own_id.clone(),
                epoch: msg.epoch,
            });
        }
        if let Some(history) = &self.history {
            match history {
                FakeNodeHistory::Coordinating(current) if *current == msg => {
                    return Ok(());
                }
                FakeNodeHistory::Coordinating(current)
                    if current.epoch == msg.epoch =>
                {
                    return Err(FakeTrustQuorumError::ConflictingReconfigure {
                        epoch: msg.epoch,
                    });
                }
                FakeNodeHistory::Coordinating(_)
                | FakeNodeHistory::Committed { .. } => {}
            }
            let latest = history.latest_config_epoch();
            if msg.epoch <= latest {
                return Err(FakeTrustQuorumError::StaleEpoch {
                    latest,
                    proposed: msg.epoch,
                });
            }
        }
        self.history = Some(FakeNodeHistory::Coordinating(msg));
        Ok(())
    }

    pub(crate) fn coordinator_status(
        &self,
    ) -> TrustQuorumCoordinatorStatusResponse {
        let msg = match &self.history {
            None => {
                return TrustQuorumCoordinatorStatusResponse::NotCoordinating {
                    latest_config_epoch: None,
                };
            }
            Some(FakeNodeHistory::Committed { epoch }) => {
                return TrustQuorumCoordinatorStatusResponse::NotCoordinating {
                    latest_config_epoch: Some(*epoch),
                };
            }
            Some(FakeNodeHistory::Coordinating(msg)) => msg,
        };
        TrustQuorumCoordinatorStatusResponse::Coordinating {
            status: CoordinatorStatus {
                config: placeholder_config(msg, self.own_id.clone()),
                acked_prepares: msg.members.clone(),
            },
        }
    }

    pub(crate) fn commit(
        &mut self,
        request: CommitRequest,
    ) -> Result<(), FakeTrustQuorumError> {
        self.record_commit(request.rack_id, request.epoch)
    }

    pub(crate) fn prepare_and_commit(
        &mut self,
        config: Configuration,
    ) -> Result<CommitStatus, FakeTrustQuorumError> {
        self.record_commit(config.rack_id, config.epoch)?;
        Ok(CommitStatus::Committed)
    }

    fn record_commit(
        &mut self,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<(), FakeTrustQuorumError> {
        self.check_rack_id(rack_id)?;
        let latest =
            self.history.as_ref().map(FakeNodeHistory::latest_config_epoch);
        match latest {
            Some(latest) if epoch < latest => {}
            Some(_) | None => {
                self.history = Some(FakeNodeHistory::Committed { epoch });
            }
        }
        Ok(())
    }
}

// TODO-RAINCLAUDE: placeholder digests; no shares exist, and Nexus only needs one digest per member to build a Configuration.
fn placeholder_config(
    msg: &ReconfigureMsg,
    coordinator: BaseboardId,
) -> Configuration {
    Configuration {
        rack_id: msg.rack_id,
        epoch: msg.epoch,
        coordinator,
        members: msg
            .members
            .iter()
            .map(|id| (id.clone(), Sha3_256Digest::default()))
            .collect(),
        threshold: msg.threshold,
        encrypted_rack_secrets: None,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::BTreeSet;
    use trust_quorum_types::types::Threshold;

    fn baseboard(serial: &str) -> BaseboardId {
        BaseboardId {
            part_number: "sim-gimlet".to_string(),
            serial_number: serial.to_string(),
        }
    }

    fn members() -> BTreeSet<BaseboardId> {
        ["s0", "s1", "s2", "s3"].into_iter().map(baseboard).collect()
    }

    fn reconfigure_msg(rack_id: RackUuid, epoch: u64) -> ReconfigureMsg {
        ReconfigureMsg {
            rack_id,
            epoch: Epoch(epoch),
            last_committed_epoch: Some(Epoch(epoch - 1)),
            members: members(),
            threshold: Threshold(3),
        }
    }

    fn coordinating(
        msg: &ReconfigureMsg,
        coordinator: &BaseboardId,
    ) -> TrustQuorumCoordinatorStatusResponse {
        TrustQuorumCoordinatorStatusResponse::Coordinating {
            status: CoordinatorStatus {
                config: placeholder_config(msg, coordinator.clone()),
                acked_prepares: msg.members.clone(),
            },
        }
    }

    #[test]
    fn reconfigure_rejections() {
        let rack_id = RackUuid::new_v4();
        let own_id = baseboard("s0");
        let mut tq = FakeTrustQuorum::new(own_id.clone(), rack_id);
        let msg = reconfigure_msg(rack_id, 3);
        tq.reconfigure(msg.clone()).expect("reconfigure accepted");
        assert_eq!(tq.reconfigure(msg.clone()), Ok(()));
        let status = coordinating(&msg, &own_id);
        assert_eq!(tq.coordinator_status(), status);

        assert_eq!(
            tq.reconfigure(reconfigure_msg(rack_id, 2)),
            Err(FakeTrustQuorumError::StaleEpoch {
                latest: Epoch(3),
                proposed: Epoch(2),
            })
        );
        let mut different_threshold = msg;
        different_threshold.threshold = Threshold(2);
        assert_eq!(
            tq.reconfigure(different_threshold),
            Err(FakeTrustQuorumError::ConflictingReconfigure {
                epoch: Epoch(3)
            })
        );
        assert_eq!(tq.coordinator_status(), status);
    }

    #[test]
    fn newer_reconfigure_replaces_uncommitted_one() {
        let rack_id = RackUuid::new_v4();
        let own_id = baseboard("s2");
        let mut tq = FakeTrustQuorum::new(own_id.clone(), rack_id);
        tq.reconfigure(reconfigure_msg(rack_id, 2))
            .expect("reconfigure accepted");
        let msg = reconfigure_msg(rack_id, 3);
        tq.reconfigure(msg.clone()).expect("newer reconfigure accepted");
        assert_eq!(tq.coordinator_status(), coordinating(&msg, &own_id));

        tq.commit(CommitRequest { rack_id, epoch: Epoch(2) })
            .expect("older commit accepted");
        assert_eq!(tq.coordinator_status(), coordinating(&msg, &own_id));

        let newer = placeholder_config(&reconfigure_msg(rack_id, 4), own_id);
        assert_eq!(tq.prepare_and_commit(newer), Ok(CommitStatus::Committed));
        assert_eq!(
            tq.coordinator_status(),
            TrustQuorumCoordinatorStatusResponse::NotCoordinating {
                latest_config_epoch: Some(Epoch(4)),
            }
        );
        assert_eq!(
            tq.reconfigure(reconfigure_msg(rack_id, 4)),
            Err(FakeTrustQuorumError::StaleEpoch {
                latest: Epoch(4),
                proposed: Epoch(4),
            })
        );
    }
}
