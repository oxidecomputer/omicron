// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types describing the state of trust quorum in Nexus

use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use omicron_common::now_db_precision;
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
        assert!(usize::from(num_members) >= TRUST_QUORUM_MIN_MEMBERS);
        assert!(usize::from(num_members) <= TRUST_QUORUM_MAX_MEMBERS);
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
            time_created: now_db_precision(),
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
        assert!(usize::from(num_members) >= TRUST_QUORUM_MIN_MEMBERS);
        assert!(usize::from(num_members) <= TRUST_QUORUM_MAX_MEMBERS);
        let now = now_db_precision();
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

    // TODO-RAINCLAUDE: propose latest_committed_config's membership minus sled_to_remove; refuses with RemoveSledError::NotMember if the sled isn't a member, or RemoveSledError::TooFewMembers if fewer than TRUST_QUORUM_MIN_MEMBERS would remain.
    pub fn remove_sled(
        latest_committed_config: &TrustQuorumConfig,
        new_epoch: Epoch,
        sled_to_remove: &BaseboardId,
    ) -> Result<Self, RemoveSledError> {
        let rack_id = latest_committed_config.rack_id;
        let mut members: BTreeSet<_> =
            latest_committed_config.members.keys().cloned().collect();

        if !members.remove(sled_to_remove) {
            return Err(RemoveSledError::NotMember {
                sled: sled_to_remove.clone(),
            });
        }
        if members.len() < TRUST_QUORUM_MIN_MEMBERS {
            return Err(RemoveSledError::TooFewMembers {
                rack_id,
                sled: sled_to_remove.clone(),
                remaining: members.len(),
            });
        }

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members,
        })
    }
}

// TODO-RAINCLAUDE: why ProposedTrustQuorumConfig::remove_sled refused; deliberately not convertible to Error, since each caller (lockstep remove, sled expunge) words the refusal for its own audience and remedy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoveSledError {
    NotMember { sled: BaseboardId },
    TooFewMembers { rack_id: RackUuid, sled: BaseboardId, remaining: usize },
}

/// The minimum number of members in a trust quorum.
pub const TRUST_QUORUM_MIN_MEMBERS: usize = 3;

/// The maximum number of members in a trust quorum.
pub const TRUST_QUORUM_MAX_MEMBERS: usize = 32;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeTaskGate {
    SledRemoved,
    RemovalPreparing,
    RemovalAborted,
    ReAddPreparing { epoch: Epoch },
    ReAdded { epoch: Epoch, state: TrustQuorumConfigState },
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SledExpungeTaskGateError {
    message: String,
}

impl SledExpungeTaskGateError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

// TODO-RAINCLAUDE: the epoch of the base config check_sled_expunge_task_gate needs (the caller passes the removal config itself when this is the removal epoch): the latest config is Preparing and may abort, after which its base becomes the latest non-aborted config. None when the removal is still Preparing or is Aborted (the gate decides on the removal alone), or when the latest config is past Preparing or Aborted (a config past Preparing can't be aborted, so its base never becomes the latest again). Errors when the Preparing latest has no last committed epoch or builds on an epoch older than the removal.
pub fn sled_expunge_task_gate_base_epoch(
    removal_config: &TrustQuorumConfig,
    latest_non_aborted_config: &TrustQuorumConfig,
) -> Result<Option<Epoch>, SledExpungeTaskGateError> {
    match removal_config.state {
        TrustQuorumConfigState::Preparing
        | TrustQuorumConfigState::PreparingLrtqUpgrade
        | TrustQuorumConfigState::Aborted => return Ok(None),
        TrustQuorumConfigState::Committing
        | TrustQuorumConfigState::Committed
        | TrustQuorumConfigState::CommittedPartially => {}
    }
    match latest_non_aborted_config.state {
        TrustQuorumConfigState::Preparing
        | TrustQuorumConfigState::PreparingLrtqUpgrade => {
            preparing_latest_base_epoch(
                removal_config.epoch,
                latest_non_aborted_config,
            )
            .map(Some)
        }
        TrustQuorumConfigState::Committing
        | TrustQuorumConfigState::Committed
        | TrustQuorumConfigState::CommittedPartially
        | TrustQuorumConfigState::Aborted => Ok(None),
    }
}

fn preparing_latest_base_epoch(
    removal_epoch: Epoch,
    latest: &TrustQuorumConfig,
) -> Result<Epoch, SledExpungeTaskGateError> {
    let Some(base_epoch) = latest.last_committed_epoch else {
        return Err(SledExpungeTaskGateError::new(format!(
            "the latest non-aborted configuration (epoch {}, state {:?}) is \
             preparing but has no last committed epoch",
            latest.epoch, latest.state,
        )));
    };
    if base_epoch < removal_epoch {
        return Err(SledExpungeTaskGateError::new(format!(
            "the latest non-aborted configuration (epoch {}) is preparing on \
             top of epoch {base_epoch}, which is older than the removal \
             configuration (epoch {removal_epoch})",
            latest.epoch,
        )));
    }
    Ok(base_epoch)
}

// TODO-RAINCLAUDE: the sled_expunge task's gate (RFD 747). latest_non_aborted_config is what tq_load_latest_possible_committed_config returns, which does NOT skip Preparing configs; latest_base_config is the config at sled_expunge_task_gate_base_epoch (the removal config itself when that is the removal epoch), required when the latest is Preparing and ignored otherwise. A removal past Preparing can never be aborted and must exclude the sled; the sled is removed only if it is in neither the latest config nor, when that is Preparing and could abort, its base. So, for the snapshot of configs the gate is given, SledRemoved does not rely on check_no_new_member_being_expunged_conn refusing re-adds: a re-add is reported, not missed. Unrelated Preparing configs that exclude the sled are ignored. Inconsistent inputs are errors; the rack checks run first, then the removal state, then the rest.
pub fn check_sled_expunge_task_gate(
    removal_config: &TrustQuorumConfig,
    latest_non_aborted_config: &TrustQuorumConfig,
    latest_base_config: Option<&TrustQuorumConfig>,
    baseboard_id: &BaseboardId,
) -> Result<SledExpungeTaskGate, SledExpungeTaskGateError> {
    let rack_id = removal_config.rack_id;
    let removal_epoch = removal_config.epoch;
    let latest = latest_non_aborted_config;
    let gate_error = |message: String| SledExpungeTaskGateError::new(message);

    if latest.rack_id != rack_id {
        return Err(gate_error(format!(
            "the latest non-aborted configuration (epoch {}) is for rack {}, \
             not the removal's rack {rack_id}",
            latest.epoch, latest.rack_id,
        )));
    }
    if let Some(base) = latest_base_config {
        if base.rack_id != rack_id {
            return Err(gate_error(format!(
                "the base configuration (epoch {}) is for rack {}, not the \
                 removal's rack {rack_id}",
                base.epoch, base.rack_id,
            )));
        }
    }

    match removal_config.state {
        TrustQuorumConfigState::Preparing
        | TrustQuorumConfigState::PreparingLrtqUpgrade => {
            return Ok(SledExpungeTaskGate::RemovalPreparing);
        }
        TrustQuorumConfigState::Aborted => {
            return Ok(SledExpungeTaskGate::RemovalAborted);
        }
        TrustQuorumConfigState::Committing
        | TrustQuorumConfigState::Committed
        | TrustQuorumConfigState::CommittedPartially => {}
    }

    if removal_config.members.contains_key(baseboard_id) {
        return Err(gate_error(format!(
            "the removal configuration (epoch {removal_epoch}, state {:?}) \
             still includes the sled",
            removal_config.state,
        )));
    }
    if latest.epoch < removal_epoch {
        return Err(gate_error(format!(
            "the latest non-aborted configuration (epoch {}) is older than \
             the removal configuration (epoch {removal_epoch}, state {:?}), \
             which can no longer be aborted",
            latest.epoch, removal_config.state,
        )));
    }

    let latest_has_sled = latest.members.contains_key(baseboard_id);
    match latest.state {
        TrustQuorumConfigState::Aborted => Err(gate_error(format!(
            "the latest non-aborted configuration (epoch {}) is aborted",
            latest.epoch,
        ))),
        TrustQuorumConfigState::Committing
        | TrustQuorumConfigState::Committed
        | TrustQuorumConfigState::CommittedPartially => {
            if latest_has_sled {
                Ok(SledExpungeTaskGate::ReAdded {
                    epoch: latest.epoch,
                    state: latest.state,
                })
            } else {
                Ok(SledExpungeTaskGate::SledRemoved)
            }
        }
        TrustQuorumConfigState::Preparing
        | TrustQuorumConfigState::PreparingLrtqUpgrade => {
            let base_epoch =
                preparing_latest_base_epoch(removal_epoch, latest)?;
            let base = match latest_base_config {
                Some(base) if base.epoch == base_epoch => base,
                supplied => {
                    let supplied = match supplied {
                        Some(base) => format!("epoch {}", base.epoch),
                        None => "none".to_string(),
                    };
                    return Err(gate_error(format!(
                        "the latest non-aborted configuration (epoch {}) is \
                         preparing on top of epoch {base_epoch}, but the base \
                         configuration supplied is {supplied}",
                        latest.epoch,
                    )));
                }
            };
            match base.state {
                TrustQuorumConfigState::Preparing
                | TrustQuorumConfigState::PreparingLrtqUpgrade
                | TrustQuorumConfigState::Aborted => {
                    return Err(gate_error(format!(
                        "the base configuration (epoch {}, state {:?}) is not \
                         past Preparing",
                        base.epoch, base.state,
                    )));
                }
                TrustQuorumConfigState::Committing
                | TrustQuorumConfigState::Committed
                | TrustQuorumConfigState::CommittedPartially => {}
            }
            if base.members.contains_key(baseboard_id) {
                Ok(SledExpungeTaskGate::ReAdded {
                    epoch: base.epoch,
                    state: base.state,
                })
            } else if latest_has_sled {
                Ok(SledExpungeTaskGate::ReAddPreparing { epoch: latest.epoch })
            } else {
                Ok(SledExpungeTaskGate::SledRemoved)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use test_strategy::proptest;

    #[derive(Clone, Copy, Debug, test_strategy::Arbitrary)]
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

    fn test_baseboard(serial: &str) -> BaseboardId {
        BaseboardId {
            part_number: "test-part".to_string(),
            serial_number: serial.to_string(),
        }
    }

    fn test_config(
        num_members: usize,
        epoch: Epoch,
        state: TrustQuorumConfigState,
    ) -> TrustQuorumConfig {
        let members: BTreeSet<BaseboardId> = (0..num_members)
            .map(|i| test_baseboard(&format!("member-{i}")))
            .collect();
        let coordinator = members.first().expect("at least one member").clone();
        let mut config = TrustQuorumConfig::new_rss_committed_config(
            RackUuid::new_v4(),
            members,
            coordinator,
        );
        config.epoch = epoch;
        config.state = state;
        config
    }

    #[test]
    fn test_proposed_config_remove_sled() {
        let config =
            test_config(4, Epoch(5), TrustQuorumConfigState::Committed);
        let sled_to_remove = test_baseboard("member-2");

        let proposed = ProposedTrustQuorumConfig::remove_sled(
            &config,
            Epoch(6),
            &sled_to_remove,
        )
        .expect("member sled is removed");
        assert_eq!(
            proposed,
            ProposedTrustQuorumConfig {
                rack_id: config.rack_id,
                epoch: Epoch(6),
                is_lrtq_upgrade: IsLrtqUpgrade::No {
                    last_committed_epoch: Epoch(5),
                },
                members: [
                    test_baseboard("member-0"),
                    test_baseboard("member-1"),
                    test_baseboard("member-3"),
                ]
                .into_iter()
                .collect(),
            }
        );

        let non_member = test_baseboard("not-a-member");
        let error = ProposedTrustQuorumConfig::remove_sled(
            &config,
            Epoch(6),
            &non_member,
        )
        .expect_err("removing a non-member sled is refused");
        assert_eq!(error, RemoveSledError::NotMember { sled: non_member });

        let minimum =
            test_config(3, Epoch(5), TrustQuorumConfigState::Committed);
        let error = ProposedTrustQuorumConfig::remove_sled(
            &minimum,
            Epoch(6),
            &sled_to_remove,
        )
        .expect_err("removing from a minimum-size trust quorum is refused");
        assert_eq!(
            error,
            RemoveSledError::TooFewMembers {
                rack_id: minimum.rack_id,
                sled: sled_to_remove,
                remaining: 2,
            },
        );
    }

    const REMOVAL_EPOCH: Epoch = Epoch(5);
    const BASE_EPOCH: Epoch = Epoch(7);
    const LATER_EPOCH: Epoch = Epoch(8);
    const OLDER_EPOCH: Epoch = Epoch(4);

    struct GateFixture {
        rack_id: RackUuid,
        sled: BaseboardId,
    }

    impl GateFixture {
        fn new() -> Self {
            Self {
                rack_id: RackUuid::new_v4(),
                sled: test_baseboard("expunged"),
            }
        }

        // TODO-RAINCLAUDE: every config has a member with the sled's serial but another part number, so membership must compare the full baseboard id.
        fn config(
            &self,
            epoch: Epoch,
            state: TrustQuorumConfigState,
            membership: Membership,
            last_committed_epoch: Option<Epoch>,
        ) -> TrustQuorumConfig {
            let mut config = test_config(4, epoch, state);
            config.rack_id = self.rack_id;
            config.last_committed_epoch = last_committed_epoch;
            config.members.insert(
                BaseboardId {
                    part_number: "other-part".to_string(),
                    serial_number: self.sled.serial_number.clone(),
                },
                TrustQuorumMemberData::new(),
            );
            match membership {
                Membership::Member => {
                    config.members.insert(
                        self.sled.clone(),
                        TrustQuorumMemberData::new(),
                    );
                }
                Membership::NonMember => {}
            }
            config
        }

        fn removal(
            &self,
            state: TrustQuorumConfigState,
            membership: Membership,
        ) -> TrustQuorumConfig {
            self.config(REMOVAL_EPOCH, state, membership, Some(OLDER_EPOCH))
        }

        fn error<T>(
            &self,
            message: &str,
        ) -> Result<T, SledExpungeTaskGateError> {
            Err(SledExpungeTaskGateError::new(message.to_string()))
        }

        fn check(
            &self,
            removal: &TrustQuorumConfig,
            latest: &TrustQuorumConfig,
            base: Option<&TrustQuorumConfig>,
        ) -> Result<SledExpungeTaskGate, SledExpungeTaskGateError> {
            check_sled_expunge_task_gate(removal, latest, base, &self.sled)
        }

        fn has_sled(&self, config: &TrustQuorumConfig) -> bool {
            config.members.contains_key(&self.sled)
        }
    }

    const ALL_STATES: [TrustQuorumConfigState; 6] = [
        TrustQuorumConfigState::Preparing,
        TrustQuorumConfigState::PreparingLrtqUpgrade,
        TrustQuorumConfigState::Committing,
        TrustQuorumConfigState::Committed,
        TrustQuorumConfigState::CommittedPartially,
        TrustQuorumConfigState::Aborted,
    ];

    #[derive(Debug, test_strategy::Arbitrary)]
    enum GateLatest {
        RemovalItself,
        Later {
            #[strategy(proptest::sample::select(ALL_STATES.to_vec()))]
            state: TrustQuorumConfigState,
            membership: Membership,
            built_on: GateBase,
        },
    }

    #[derive(Debug, test_strategy::Arbitrary)]
    enum GateBase {
        Removal,
        Later {
            #[strategy(proptest::sample::select(ALL_STATES.to_vec()))]
            state: TrustQuorumConfigState,
            membership: Membership,
        },
    }

    // TODO-RAINCLAUDE: base is what the loader supplies: the config at sled_expunge_task_gate_base_epoch, or else whatever base config exists (which the gate must ignore); exactly the consistent inputs give Ok, and the properties below then pin the exact outcome.
    // TODO-RAINCLAUDE: the default 256 cases reach a Preparing latest re-adding the sled on a committed base only about half the time.
    #[proptest(cases = 4096)]
    fn proptest_check_sled_expunge_task_gate(
        #[strategy(proptest::sample::select(ALL_STATES.to_vec()))]
        removal_state: TrustQuorumConfigState,
        removal_membership: Membership,
        latest_input: GateLatest,
    ) {
        use TrustQuorumConfigState as S;

        let fixture = GateFixture::new();
        let removal = fixture.removal(removal_state, removal_membership);
        let (latest, base_config) = match latest_input {
            GateLatest::RemovalItself => (removal.clone(), None),
            GateLatest::Later { state, membership, built_on } => match built_on
            {
                GateBase::Removal => {
                    let latest = fixture.config(
                        LATER_EPOCH,
                        state,
                        membership,
                        Some(REMOVAL_EPOCH),
                    );
                    (latest, None)
                }
                GateBase::Later { state: base_state, membership: base } => {
                    let latest = fixture.config(
                        LATER_EPOCH,
                        state,
                        membership,
                        Some(BASE_EPOCH),
                    );
                    let base = fixture.config(
                        BASE_EPOCH,
                        base_state,
                        base,
                        Some(REMOVAL_EPOCH),
                    );
                    (latest, Some(base))
                }
            },
        };
        let base_epoch = sled_expunge_task_gate_base_epoch(&removal, &latest);
        let base = match base_epoch {
            Ok(Some(REMOVAL_EPOCH)) => Some(&removal),
            Ok(_) | Err(_) => base_config.as_ref(),
        };
        let gate = fixture.check(&removal, &latest, base);

        match removal_state {
            S::Preparing | S::PreparingLrtqUpgrade => {
                prop_assert_eq!(
                    gate,
                    Ok(SledExpungeTaskGate::RemovalPreparing)
                );
                prop_assert_eq!(base_epoch, Ok(None));
                return Ok(());
            }
            S::Aborted => {
                prop_assert_eq!(gate, Ok(SledExpungeTaskGate::RemovalAborted));
                prop_assert_eq!(base_epoch, Ok(None));
                return Ok(());
            }
            S::Committing | S::Committed | S::CommittedPartially => {}
        }

        let latest_preparing = match latest.state {
            S::Preparing | S::PreparingLrtqUpgrade => true,
            S::Committing
            | S::Committed
            | S::CommittedPartially
            | S::Aborted => false,
        };
        prop_assert_eq!(
            base_epoch,
            Ok(latest.last_committed_epoch.filter(|_| latest_preparing)),
        );
        let effective_base = base.filter(|_| latest_preparing);
        let base_past_preparing = match effective_base.map(|base| base.state) {
            None
            | Some(S::Committing | S::Committed | S::CommittedPartially) => {
                true
            }
            Some(S::Preparing | S::PreparingLrtqUpgrade | S::Aborted) => false,
        };
        let consistent = !fixture.has_sled(&removal)
            && latest.state != S::Aborted
            && base_past_preparing;
        let base_has_sled = effective_base.is_some_and(|b| fixture.has_sled(b));

        prop_assert_eq!(gate.is_err(), !consistent, "{:?}", gate);
        match gate {
            Ok(SledExpungeTaskGate::SledRemoved) => {
                prop_assert!(!fixture.has_sled(&removal));
                prop_assert!(!fixture.has_sled(&latest));
                prop_assert!(!base_has_sled);
            }
            Ok(SledExpungeTaskGate::ReAdded { epoch, state }) => {
                let readded_in = if latest_preparing {
                    effective_base
                } else {
                    Some(&latest)
                };
                let readded_in =
                    readded_in.expect("a Preparing latest has a base");
                prop_assert_eq!(
                    (epoch, state),
                    (readded_in.epoch, readded_in.state)
                );
                prop_assert!(fixture.has_sled(readded_in));
            }
            Ok(SledExpungeTaskGate::ReAddPreparing { epoch }) => {
                prop_assert!(latest_preparing);
                prop_assert_eq!(epoch, latest.epoch);
                prop_assert!(fixture.has_sled(&latest));
                prop_assert!(!base_has_sled);
            }
            Ok(
                outcome @ (SledExpungeTaskGate::RemovalPreparing
                | SledExpungeTaskGate::RemovalAborted),
            ) => {
                prop_assert!(false, "{outcome:?} for a removal past Preparing");
            }
            Err(_) => {}
        }
    }

    #[test]
    fn test_check_sled_expunge_task_gate_inconsistent_inputs() {
        use Membership::*;
        use TrustQuorumConfigState as S;

        let fixture = GateFixture::new();
        let rack_id = fixture.rack_id;
        let removal = fixture.removal(S::Committed, NonMember);
        let other_rack = RackUuid::new_v4();
        let preparing_on_removal = fixture.config(
            LATER_EPOCH,
            S::Preparing,
            NonMember,
            Some(REMOVAL_EPOCH),
        );
        let preparing_on_base = fixture.config(
            LATER_EPOCH,
            S::Preparing,
            NonMember,
            Some(BASE_EPOCH),
        );

        // TODO-RAINCLAUDE: the rack checks run before the removal state, so they win even over a removal that is still Preparing.
        let preparing_removal = fixture.removal(S::Preparing, NonMember);
        let mut latest_other_rack = preparing_on_removal.clone();
        latest_other_rack.rack_id = other_rack;
        assert_eq!(
            fixture.check(&preparing_removal, &latest_other_rack, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
                 for rack {other_rack}, not the removal's rack {rack_id}"
            )),
        );
        let mut base_other_rack =
            fixture.config(BASE_EPOCH, S::Committed, NonMember, None);
        base_other_rack.rack_id = other_rack;
        assert_eq!(
            fixture.check(
                &preparing_removal,
                &preparing_on_base,
                Some(&base_other_rack)
            ),
            fixture.error(&format!(
                "the base configuration (epoch {BASE_EPOCH}) is for rack \
                 {other_rack}, not the removal's rack {rack_id}"
            )),
        );

        let removal_with_sled = fixture.removal(S::Committed, Member);
        assert_eq!(
            fixture.check(&removal_with_sled, &removal_with_sled, None),
            fixture.error(&format!(
                "the removal configuration (epoch {REMOVAL_EPOCH}, state \
                 Committed) still includes the sled"
            )),
        );

        let older = fixture.config(OLDER_EPOCH, S::Committed, NonMember, None);
        assert_eq!(
            fixture.check(&removal, &older, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {OLDER_EPOCH}) is \
                 older than the removal configuration (epoch {REMOVAL_EPOCH}, \
                 state Committed), which can no longer be aborted"
            )),
        );

        let aborted_latest =
            fixture.config(LATER_EPOCH, S::Aborted, NonMember, None);
        assert_eq!(
            fixture.check(&removal, &aborted_latest, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
                 aborted"
            )),
        );

        let wrong_base =
            fixture.config(Epoch(6), S::Committed, NonMember, None);
        for (latest, base_epoch, supplied, description) in [
            (&preparing_on_base, BASE_EPOCH, None, "none"),
            (
                &preparing_on_removal,
                REMOVAL_EPOCH,
                Some(&wrong_base),
                "epoch 6",
            ),
        ] {
            assert_eq!(
                fixture.check(&removal, latest, supplied),
                fixture.error(&format!(
                    "the latest non-aborted configuration (epoch \
                     {LATER_EPOCH}) is preparing on top of epoch \
                     {base_epoch}, but the base configuration supplied is \
                     {description}"
                )),
            );
        }

        let no_base =
            fixture.config(LATER_EPOCH, S::Preparing, NonMember, None);
        let message = format!(
            "the latest non-aborted configuration (epoch {LATER_EPOCH}, state \
             Preparing) is preparing but has no last committed epoch"
        );
        assert_eq!(
            sled_expunge_task_gate_base_epoch(&removal, &no_base),
            fixture.error(&message),
        );
        assert_eq!(
            fixture.check(&removal, &no_base, None),
            fixture.error(&message)
        );

        let on_older_base = fixture.config(
            LATER_EPOCH,
            S::Preparing,
            NonMember,
            Some(OLDER_EPOCH),
        );
        let message = format!(
            "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
             preparing on top of epoch {OLDER_EPOCH}, which is older than the \
             removal configuration (epoch {REMOVAL_EPOCH})"
        );
        assert_eq!(
            sled_expunge_task_gate_base_epoch(&removal, &on_older_base),
            fixture.error(&message),
        );
        assert_eq!(
            fixture.check(&removal, &on_older_base, Some(&older)),
            fixture.error(&message),
        );

        let preparing_base = fixture.config(
            BASE_EPOCH,
            S::Preparing,
            Member,
            Some(REMOVAL_EPOCH),
        );
        assert_eq!(
            fixture.check(&removal, &preparing_on_base, Some(&preparing_base)),
            fixture.error(&format!(
                "the base configuration (epoch {BASE_EPOCH}, state Preparing) \
                 is not past Preparing"
            )),
        );
    }
}
