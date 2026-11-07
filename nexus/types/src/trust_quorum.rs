// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types describing the state of trust quorum in Nexus

use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sled_hardware_types::BaseboardId;
use trust_quorum_types::{
    crypto::EncryptedRackSecrets, crypto::Sha3_256Digest, types::Epoch,
    types::Threshold,
};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
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

    pub fn is_active(&self) -> bool {
        self.is_preparing() || self.is_committing()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TrustQuorumMemberState {
    Unacked,
    Prepared,
    Committed,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct TrustQuorumMemberData {
    pub state: TrustQuorumMemberState,

    // Only filled in once the coordinator state is succesfully polled by nexus
    // after it has created the configuration.
    pub share_digest: Option<Sha3_256Digest>,

    pub time_prepared: Option<DateTime<Utc>>,
    pub time_committed: Option<DateTime<Utc>>,
}

impl TrustQuorumMemberData {
    pub fn new() -> Self {
        TrustQuorumMemberData {
            state: TrustQuorumMemberState::Unacked,
            share_digest: None,
            time_prepared: None,
            time_committed: None,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumConfig {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub state: TrustQuorumConfigState,
    pub threshold: Threshold,
    pub commit_crash_tolerance: u8,
    pub coordinator: BaseboardId,
    pub encrypted_rack_secrets: Option<EncryptedRackSecrets>,
    #[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
    pub members: BTreeMap<BaseboardId, TrustQuorumMemberData>,
    pub time_created: DateTime<Utc>,
    pub time_committing: Option<DateTime<Utc>>,
    pub time_committed: Option<DateTime<Utc>>,
    pub time_aborted: Option<DateTime<Utc>>,
    pub abort_reason: Option<String>,
}

impl TryFrom<&TrustQuorumConfig>
    for trust_quorum_types::configuration::Configuration
{
    type Error = anyhow::Error;
    fn try_from(nexus_config: &TrustQuorumConfig) -> Result<Self, Self::Error> {
        let mut members: BTreeMap<_, _> = BTreeMap::new();
        for (id, data) in &nexus_config.members {
            let Some(digest) = data.share_digest else {
                bail!(format!(
                    "Missing sled digests. \
                    Trust Quorum Configuration state = {:?}",
                    nexus_config.state
                ));
            };
            members.insert(id.clone(), digest);
        }
        Ok(trust_quorum_types::configuration::Configuration {
            rack_id: nexus_config.rack_id,
            coordinator: nexus_config.coordinator.clone(),
            members,
            epoch: nexus_config.epoch,
            threshold: nexus_config.threshold,
            encrypted_rack_secrets: nexus_config.encrypted_rack_secrets.clone(),
        })
    }
}

impl TrustQuorumConfig {
    pub fn new(
        proposed: ProposedTrustQuorumConfig,
        coordinator: BaseboardId,
    ) -> Self {
        let num_members = u8::try_from(proposed.members.len()).unwrap();
        let state = if proposed.is_lrtq_upgrade == IsLrtqUpgrade::Yes {
            TrustQuorumConfigState::PreparingLrtqUpgrade
        } else {
            TrustQuorumConfigState::Preparing
        };
        assert!(num_members >= 3);
        assert!(num_members <= 32);
        TrustQuorumConfig {
            rack_id: proposed.rack_id,
            epoch: proposed.epoch,
            last_committed_epoch: proposed.last_committed_epoch(),
            state,
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
                            share_digest: None,
                            time_prepared: None,
                            time_committed: None,
                        },
                    )
                })
                .collect(),
            time_created: Utc::now(),
            time_committing: None,
            time_committed: None,
            time_aborted: None,
            abort_reason: None,
        }
    }

    pub fn new_rss_committed_config(
        rack_id: RackUuid,
        initial_members: BTreeSet<BaseboardId>,
        coordinator: BaseboardId,
    ) -> TrustQuorumConfig {
        let num_members = u8::try_from(initial_members.len()).unwrap();
        assert!(num_members >= 3);
        assert!(num_members <= 32);
        let now = Utc::now();
        TrustQuorumConfig {
            rack_id,
            epoch: Epoch(1),
            last_committed_epoch: None,
            state: TrustQuorumConfigState::Committed,
            threshold: Self::threshold(num_members),
            commit_crash_tolerance: Self::commit_crash_tolerance(num_members),
            coordinator,
            encrypted_rack_secrets: None,
            members: initial_members
                .into_iter()
                .map(|id| {
                    (
                        id,
                        TrustQuorumMemberData {
                            state: TrustQuorumMemberState::Committed,
                            share_digest: None,
                            time_prepared: Some(now),
                            time_committed: Some(now),
                        },
                    )
                })
                .collect(),
            time_created: now,
            time_committing: Some(now),
            time_committed: Some(now),
            time_aborted: None,
            abort_reason: None,
        }
    }

    pub fn acked_commits(&self) -> usize {
        self.members
            .values()
            .filter(|m| m.state == TrustQuorumMemberState::Committed)
            .count()
    }

    pub fn threshold(num_members: u8) -> Threshold {
        Threshold(num_members / 2 + 1)
    }

    pub fn commit_crash_tolerance(num_members: u8) -> u8 {
        match num_members {
            0..=4 => 0,
            5..=8 => 1,
            9..=15 => 2,
            16..=23 => 3,
            _ => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsLrtqUpgrade {
    Yes,
    // We only use this variant when reconfiguring, and not during an
    // initial configuration via RSS, and therefore there must always be a
    // `last_committed_epoch`.
    No { last_committed_epoch: Epoch },
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
            IsLrtqUpgrade::No { last_committed_epoch } => {
                Some(last_committed_epoch)
            }
        }
    }
}

/// Given the current trust quorum config returned by
/// `tq_load_latest_possible_committed_config`, determine whether the provided
/// sled can be expunged.
pub fn check_trust_quorum_allows_expunge(
    sled_id: SledUuid,
    baseboard_id: &BaseboardId,
    config: &TrustQuorumConfig,
) -> Result<(), Error> {
    let rack_id = config.rack_id;
    let epoch = config.epoch;
    let state = config.state;

    if config.members.contains_key(baseboard_id) {
        return Err(Error::conflict(format!(
            "Cannot expunge sled {sled_id}, as its baseboard {baseboard_id} \
            is still a member of the latest trust quorum configuration for \
            rack {rack_id} (epoch {epoch}, state {state:?})"
        )));
    }

    match state {
        TrustQuorumConfigState::Preparing
        | TrustQuorumConfigState::PreparingLrtqUpgrade => {
            // Trust quorum reconfiguration can still be aborted at this point,
            // so it's not safe to expunge the sled at this point.
            Err(Error::conflict(format!(
                "Cannot expunge sled {sled_id}: the latest trust quorum \
                configuration for rack {rack_id} (epoch {epoch}, state \
                {state:?}) does not include baseboard {baseboard_id}, but it \
                has not started committing and can still be aborted. Wait for \
                the configuration at epoch {epoch} to start committing, then \
                retry. If it is stuck, abort the rack membership change and \
                propose a new configuration that removes this sled."
            )))
        }
        TrustQuorumConfigState::Committing
        | TrustQuorumConfigState::Committed
        | TrustQuorumConfigState::CommittedPartially => Ok(()),
        TrustQuorumConfigState::Aborted => {
            Err(Error::internal_error(&format!(
                "Cannot expunge sled {sled_id}: the trust quorum \
                configuration for rack {rack_id} at epoch {epoch}, loaded as \
                the latest possibly committed configuration, is aborted \
                (this is a programming error because we expect the provided config \
                to be non-aborted)"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Membership {
        Member,
        NonMember,
    }

    #[derive(Debug)]
    enum Expected {
        Allowed,
        MemberConflict,
        PreparingConflict,
        AbortedInternalError,
    }

    #[test]
    fn test_check_trust_quorum_allows_expunge() {
        use Expected::*;
        use Membership::*;
        use TrustQuorumConfigState as S;

        let sled_id = SledUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let epoch = Epoch(7);
        let sled_baseboard = BaseboardId {
            part_number: "test-part".to_string(),
            serial_number: "expunged".to_string(),
        };
        let other_members: Vec<BaseboardId> = (0..3)
            .map(|i| BaseboardId {
                part_number: "test-part".to_string(),
                serial_number: format!("other-{i}"),
            })
            .collect();

        let cases = [
            (S::Preparing, Member, MemberConflict),
            (S::Preparing, NonMember, PreparingConflict),
            (S::PreparingLrtqUpgrade, Member, MemberConflict),
            (S::PreparingLrtqUpgrade, NonMember, PreparingConflict),
            (S::Committing, Member, MemberConflict),
            (S::Committing, NonMember, Allowed),
            (S::Committed, Member, MemberConflict),
            (S::Committed, NonMember, Allowed),
            (S::CommittedPartially, Member, MemberConflict),
            (S::CommittedPartially, NonMember, Allowed),
            (S::Aborted, Member, MemberConflict),
            (S::Aborted, NonMember, AbortedInternalError),
        ];

        for (state, membership, expected) in cases {
            let description = format!("{state:?}, {membership:?}");
            let mut members: BTreeSet<BaseboardId> =
                other_members.iter().cloned().collect();
            match membership {
                Member => {
                    members.insert(sled_baseboard.clone());
                }
                NonMember => {}
            }
            let mut config = TrustQuorumConfig::new_rss_committed_config(
                rack_id,
                members,
                other_members[0].clone(),
            );
            config.epoch = epoch;
            config.state = state;

            let result = check_trust_quorum_allows_expunge(
                sled_id,
                &sled_baseboard,
                &config,
            );

            let (error, expected_parts) = match expected {
                Allowed => {
                    assert_eq!(result, Ok(()), "{description}");
                    continue;
                }
                MemberConflict => (
                    result.expect_err(&description),
                    vec![format!(
                        "Cannot expunge sled {sled_id}, as its baseboard \
                         {sled_baseboard} is still a member of the latest \
                         trust quorum configuration for rack {rack_id} \
                         (epoch 7, state {state:?})"
                    )],
                ),
                PreparingConflict => (
                    result.expect_err(&description),
                    vec![
                        format!(
                            "for rack {rack_id} (epoch 7, state {state:?}) \
                             does not include baseboard {sled_baseboard}"
                        ),
                        "has not started committing and can still be aborted"
                            .to_string(),
                        "Wait for the configuration at epoch 7 to start \
                         committing, then retry."
                            .to_string(),
                        "If it is stuck, abort the rack membership change and \
                         propose a new configuration that removes this sled."
                            .to_string(),
                    ],
                ),
                AbortedInternalError => (
                    result.expect_err(&description),
                    vec![format!(
                        "for rack {rack_id} at epoch 7, loaded as the latest \
                         possibly committed configuration, is aborted"
                    )],
                ),
            };

            let message = match (&expected, &error) {
                (
                    MemberConflict | PreparingConflict,
                    Error::Conflict { message },
                ) => message.external_message().to_string(),
                (
                    AbortedInternalError,
                    Error::InternalError { internal_message },
                ) => internal_message.clone(),
                (expected, error) => panic!(
                    "{description}: expected {expected:?}, got {error:?}"
                ),
            };
            for part in expected_parts {
                assert!(
                    message.contains(&part),
                    "{description}: message {message:?} contains {part:?}"
                );
            }
        }
    }
}
