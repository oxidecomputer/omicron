// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types describing the state of trust quorum in Nexus

use std::collections::{BTreeMap, BTreeSet};

use omicron_uuid_kinds::RackUuid;
use sled_agent_types::sled::BaseboardId;
use trust_quorum_protocol::{
    EncryptedRackSecrets, Epoch, Sha3_256Digest, Threshold,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustQuorumConfigState {
    Preparing,
    PreparingLrtqUpgrade,
    Committing,
    Committed,
    CommittedPartially,
    Aborted,
}

impl TrustQuorumConfigState {
    pub fn is_preparing(&self) -> bool {
        *self == Self::Preparing || *self == Self::PreparingLrtqUpgrade
    }

    pub fn is_committed(&self) -> bool {
        *self == Self::Committed || *self == Self::CommittedPartially
    }

    pub fn is_aborted(&self) -> bool {
        *self == Self::Aborted
    }

    pub fn is_committing(&self) -> bool {
        *self == Self::Committing
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustQuorumMemberState {
    Unacked,
    Prepared,
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustQuorumMemberData {
    pub state: TrustQuorumMemberState,

    // Only filled in once the coordinator state is succesfully polled by nexus
    // after it has created the configuration.
    pub digest: Option<Sha3_256Digest>,
}

impl TrustQuorumMemberData {
    pub fn new() -> Self {
        TrustQuorumMemberData {
            state: TrustQuorumMemberState::Unacked,
            digest: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustQuorumConfig {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub state: TrustQuorumConfigState,
    pub threshold: Threshold,
    pub commit_crash_tolerance: u8,
    pub coordinator: BaseboardId,
    pub encrypted_rack_secrets: Option<EncryptedRackSecrets>,
    pub members: BTreeMap<BaseboardId, TrustQuorumMemberData>,
}

impl TrustQuorumConfig {
    pub fn new(
        proposed: ProposedTrustQuorumConfig,
        coordinator: BaseboardId,
    ) -> Self {
        let num_members = u8::try_from(proposed.members.len()).unwrap();
        assert!(num_members >= 3);
        assert!(num_members <= 32);
        TrustQuorumConfig {
            rack_id: proposed.rack_id,
            epoch: proposed.epoch,
            last_committed_epoch: proposed.last_committed_epoch(),
            state: TrustQuorumConfigState::Preparing,
            threshold: Self::threshold(num_members),
            commit_crash_tolerance: Self::commit_crash_tolerance(num_members),
            coordinator,
            encrypted_rack_secrets: None,
            members: proposed
                .members
                .into_iter()
                .map(|id| {
                    (
                        id,
                        TrustQuorumMemberData {
                            state: TrustQuorumMemberState::Unacked,
                            digest: None,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn acked_commits(&self) -> usize {
        self.members
            .values()
            .filter(|m| m.state == TrustQuorumMemberState::Committed)
            .count()
    }

    fn threshold(num_members: u8) -> Threshold {
        Threshold(num_members / 2 + 1)
    }

    fn commit_crash_tolerance(num_members: u8) -> u8 {
        match num_members {
            0..=3 => 0,
            4..=8 => 1,
            9..=16 => 2,
            _ => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsLrtqUpgrade {
    Yes,
    No { last_committed_epoch: Option<Epoch> },
}

// A trust quorum configuration proposed by a user that will be converted to a
// [`TrustQuorumConfig`] inside a database transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedTrustQuorumConfig {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub is_lrtq_upgrade: IsLrtqUpgrade,
    pub members: BTreeSet<BaseboardId>,
}

impl ProposedTrustQuorumConfig {
    pub fn last_committed_epoch(&self) -> Option<Epoch> {
        match self.is_lrtq_upgrade {
            IsLrtqUpgrade::Yes => None,
            IsLrtqUpgrade::No { last_committed_epoch } => last_committed_epoch,
        }
    }
}
