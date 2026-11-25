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
use trust_quorum_types::membership::TrustQuorumMembers;
use trust_quorum_types::membership::TrustQuorumMembersError;
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

    pub fn phase(&self) -> TrustQuorumConfigPhase {
        match self {
            Self::Preparing | Self::PreparingLrtqUpgrade => {
                TrustQuorumConfigPhase::Preparing
            }
            Self::Committing => TrustQuorumConfigPhase::PastPreparing(
                TrustQuorumConfigPastPreparing::Committing,
            ),
            Self::Committed => TrustQuorumConfigPhase::PastPreparing(
                TrustQuorumConfigPastPreparing::Committed,
            ),
            Self::CommittedPartially => TrustQuorumConfigPhase::PastPreparing(
                TrustQuorumConfigPastPreparing::CommittedPartially,
            ),
            Self::Aborted => TrustQuorumConfigPhase::Aborted,
        }
    }
}

// TODO-RAINCLAUDE: The coarse phase of a `TrustQuorumConfigState`.
//
// The phase says whether the configuration can still abort. A `Preparing`
// configuration can, and a `PastPreparing` one never can. `PastPreparing`
// carries a `TrustQuorumConfigPastPreparing`, so code that holds one can't see
// a `Preparing` or `Aborted` state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustQuorumConfigPhase {
    Preparing,
    PastPreparing(TrustQuorumConfigPastPreparing),
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustQuorumConfigPastPreparing {
    Committing,
    Committed,
    CommittedPartially,
}

impl From<TrustQuorumConfigPastPreparing> for TrustQuorumConfigState {
    fn from(state: TrustQuorumConfigPastPreparing) -> Self {
        match state {
            TrustQuorumConfigPastPreparing::Committing => Self::Committing,
            TrustQuorumConfigPastPreparing::Committed => Self::Committed,
            TrustQuorumConfigPastPreparing::CommittedPartially => {
                Self::CommittedPartially
            }
        }
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
        let num_members = proposed.members.num_members();
        let state = if proposed.is_lrtq_upgrade == IsLrtqUpgrade::Yes {
            TrustQuorumConfigState::PreparingLrtqUpgrade
        } else {
            TrustQuorumConfigState::Preparing
        };
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
                .into_set()
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
        initial_members: TrustQuorumMembers,
        coordinator: BaseboardId,
    ) -> TrustQuorumConfig {
        let num_members = initial_members.num_members();
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
                .into_set()
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
    pub members: TrustQuorumMembers,
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
        let members = TrustQuorumMembers::new(members).map_err(|error| {
            RemoveSledError::InvalidMembership {
                rack_id,
                sled: sled_to_remove.clone(),
                error,
            }
        })?;

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members,
        })
    }

    /// Create a new `ProposedTrustQuorumConfig` including `new_sleds` in the
    /// membership. Return an error if any of the new sleds exist in the
    /// `latest_committed_config` membership already.
    pub fn add_sleds(
        latest_committed_config: &TrustQuorumConfig,
        new_epoch: Epoch,
        new_sleds: BTreeSet<BaseboardId>,
    ) -> Result<Self, AddSledsError> {
        let rack_id = latest_committed_config.rack_id;
        let existing: BTreeSet<_> =
            latest_committed_config.members.keys().cloned().collect();

        let already_members: BTreeSet<_> =
            existing.intersection(&new_sleds).cloned().collect();
        if !already_members.is_empty() {
            return Err(AddSledsError::AlreadyMembers {
                sleds: already_members,
            });
        }

        let members = TrustQuorumMembers::new(
            new_sleds.union(&existing).cloned().collect(),
        )
        .map_err(|error| AddSledsError::InvalidMembership { rack_id, error })?;

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members,
        })
    }

    /// Create a new `ProposedTrustQuorumConfig` that upgrades a rack from LRTQ,
    /// with the rack's commissioned sleds as its members.
    pub fn upgrade_from_lrtq(
        rack_id: RackUuid,
        new_epoch: Epoch,
        commissioned_sleds: BTreeSet<BaseboardId>,
    ) -> Result<Self, LrtqUpgradeError> {
        let members =
            TrustQuorumMembers::new(commissioned_sleds).map_err(|error| {
                LrtqUpgradeError::InvalidMembership { rack_id, error }
            })?;

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::Yes,
            members,
        })
    }
}

/// Why [`ProposedTrustQuorumConfig::add_sleds`] refused to add sleds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddSledsError {
    /// Some of the sleds are already members of the trust quorum.
    AlreadyMembers { sleds: BTreeSet<BaseboardId> },
    /// The members after adding the sleds are outside the trust quorum size
    /// bounds.
    InvalidMembership { rack_id: RackUuid, error: TrustQuorumMembersError },
}

/// Why [`ProposedTrustQuorumConfig::upgrade_from_lrtq`] refused to upgrade a
/// rack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LrtqUpgradeError {
    /// The commissioned sleds are outside the trust quorum size bounds.
    InvalidMembership { rack_id: RackUuid, error: TrustQuorumMembersError },
}

/// Why [`ProposedTrustQuorumConfig::remove_sled`] refused to remove a sled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoveSledError {
    /// The sled is not a member of the trust quorum.
    NotMember { sled: BaseboardId },
    /// The members left after removing `sled` are outside the trust quorum size
    /// bounds.
    InvalidMembership {
        rack_id: RackUuid,
        sled: BaseboardId,
        error: TrustQuorumMembersError,
    },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum SledExpungeTaskGate {
    SledRemoved,
    RemovalPreparing,
    RemovalAborted,
    ReAddPreparing { epoch: Epoch },
    ReAdded { epoch: Epoch, state: TrustQuorumConfigPastPreparing },
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

// TODO-RAINCLAUDE: A summary of which base configuration a
// `SledExpungeTaskGatePlan` needs. Unlike the plan, it can be compared with
// `==`, so tests use it to check the plan.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SledExpungeTaskGateBase {
    NotNeeded,
    RemovalConfig,
    Load(Epoch),
}

// TODO-RAINCLAUDE: The plan for checking the `sled_expunge` task's gate,
// returned by `sled_expunge_task_gate_plan`.
//
// Each variant carries the only way to finish the check:
// * `Ready` needs nothing more, so the caller calls `check` right away.
// * `Load` needs the base configuration at `epoch()`. The caller loads it and
//   passes it to `check`, which verifies that it is the right configuration.
//
// This means that a caller loads a base configuration exactly when the plan
// asks for one.
#[derive(Clone, Copy, Debug)]
#[must_use]
pub enum SledExpungeTaskGatePlan<'a> {
    Ready(SledExpungeTaskGateReady<'a>),
    Load(SledExpungeTaskGateLoad<'a>),
}

impl SledExpungeTaskGatePlan<'_> {
    #[cfg(test)]
    pub fn base(&self) -> SledExpungeTaskGateBase {
        match self {
            Self::Ready(ready) => match ready.case {
                SledExpungeTaskGateCase::RemovalPreparing
                | SledExpungeTaskGateCase::RemovalAborted
                | SledExpungeTaskGateCase::RemovalPast(
                    SledExpungeTaskGateLatest::PastPreparing { state: _ },
                ) => SledExpungeTaskGateBase::NotNeeded,
                SledExpungeTaskGateCase::RemovalPast(
                    SledExpungeTaskGateLatest::Preparing { base: _ },
                ) => SledExpungeTaskGateBase::RemovalConfig,
            },
            Self::Load(load) => SledExpungeTaskGateBase::Load(load.epoch),
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct SledExpungeTaskGateReady<'a> {
    removal_config: &'a TrustQuorumConfig,
    latest: &'a TrustQuorumConfig,
    case: SledExpungeTaskGateCase<'a>,
}

impl SledExpungeTaskGateReady<'_> {
    pub fn check(
        self,
        baseboard_id: &BaseboardId,
    ) -> Result<SledExpungeTaskGate, SledExpungeTaskGateError> {
        check_sled_expunge_task_gate_case(
            self.removal_config,
            self.latest,
            self.case,
            baseboard_id,
        )
    }
}

#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct SledExpungeTaskGateLoad<'a> {
    removal_config: &'a TrustQuorumConfig,
    latest: &'a TrustQuorumConfig,
    epoch: Epoch,
}

impl SledExpungeTaskGateLoad<'_> {
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn check(
        self,
        base: &TrustQuorumConfig,
        baseboard_id: &BaseboardId,
    ) -> Result<SledExpungeTaskGate, SledExpungeTaskGateError> {
        let rack_id = self.removal_config.rack_id;
        if base.rack_id != rack_id {
            return Err(SledExpungeTaskGateError::new(format!(
                "the base configuration (epoch {}) is for rack {}, not the \
                 removal's rack {rack_id}",
                base.epoch, base.rack_id,
            )));
        }
        if base.epoch != self.epoch {
            return Err(SledExpungeTaskGateError::new(format!(
                "the latest non-aborted configuration (epoch {}) is preparing \
                 on top of epoch {}, but the base configuration loaded is at \
                 epoch {}",
                self.latest.epoch, self.epoch, base.epoch,
            )));
        }
        check_sled_expunge_task_gate_case(
            self.removal_config,
            self.latest,
            SledExpungeTaskGateCase::RemovalPast(
                SledExpungeTaskGateLatest::Preparing { base },
            ),
            baseboard_id,
        )
    }
}

#[derive(Clone, Copy, Debug)]
enum SledExpungeTaskGateCase<'a> {
    RemovalPreparing,
    RemovalAborted,
    RemovalPast(SledExpungeTaskGateLatest<'a>),
}

#[derive(Clone, Copy, Debug)]
enum SledExpungeTaskGateLatest<'a> {
    PastPreparing { state: TrustQuorumConfigPastPreparing },
    Preparing { base: &'a TrustQuorumConfig },
}

// TODO-RAINCLAUDE: Plans the trust quorum check that the `sled_expunge` task
// runs before it acts on a request (RFD 747).
//
// `latest_non_aborted_config` comes from
// `tq_load_latest_possible_committed_config`, which does not skip `Preparing`
// configurations. The latest configuration might still abort and leave its base
// as the latest, so the sled counts as removed only if it is in neither the
// latest configuration nor, when the latest is `Preparing`, its base.
//
// This only covers the configurations that exist when the gate runs. A later
// re-add is covered in two ways. `check_no_new_member_being_expunged_conn`
// refuses it, and `sled_expunge_request_complete` runs the gate again in its
// own transaction.
pub fn sled_expunge_task_gate_plan<'a>(
    removal_config: &'a TrustQuorumConfig,
    latest_non_aborted_config: &'a TrustQuorumConfig,
) -> Result<SledExpungeTaskGatePlan<'a>, SledExpungeTaskGateError> {
    let rack_id = removal_config.rack_id;
    let latest = latest_non_aborted_config;
    if latest.rack_id != rack_id {
        return Err(SledExpungeTaskGateError::new(format!(
            "the latest non-aborted configuration (epoch {}) is for rack {}, \
             not the removal's rack {rack_id}",
            latest.epoch, latest.rack_id,
        )));
    }

    let ready = |case| {
        SledExpungeTaskGatePlan::Ready(SledExpungeTaskGateReady {
            removal_config,
            latest,
            case,
        })
    };
    match removal_config.state.phase() {
        TrustQuorumConfigPhase::Preparing => {
            // TODO-RAINCLAUDE: This is deliberately lenient. While the removal
            // is `Preparing`, we check the rack above but don't compare the
            // removal against the latest configuration, so inconsistent inputs
            // are reported only once the removal leaves `Preparing`. The
            // proptest below depends on this behavior.
            return Ok(ready(SledExpungeTaskGateCase::RemovalPreparing));
        }
        TrustQuorumConfigPhase::Aborted => {
            return Ok(ready(SledExpungeTaskGateCase::RemovalAborted));
        }
        TrustQuorumConfigPhase::PastPreparing(_) => {}
    }
    match latest.state.phase() {
        TrustQuorumConfigPhase::Aborted => {
            Err(SledExpungeTaskGateError::new(format!(
                "the latest non-aborted configuration (epoch {}) is aborted",
                latest.epoch,
            )))
        }
        TrustQuorumConfigPhase::PastPreparing(state) => {
            Ok(ready(SledExpungeTaskGateCase::RemovalPast(
                SledExpungeTaskGateLatest::PastPreparing { state },
            )))
        }
        TrustQuorumConfigPhase::Preparing => {
            let base_epoch =
                preparing_latest_base_epoch(removal_config.epoch, latest)?;
            if base_epoch == removal_config.epoch {
                Ok(ready(SledExpungeTaskGateCase::RemovalPast(
                    SledExpungeTaskGateLatest::Preparing {
                        base: removal_config,
                    },
                )))
            } else {
                Ok(SledExpungeTaskGatePlan::Load(SledExpungeTaskGateLoad {
                    removal_config,
                    latest,
                    epoch: base_epoch,
                }))
            }
        }
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
    if base_epoch >= latest.epoch {
        return Err(SledExpungeTaskGateError::new(format!(
            "the latest non-aborted configuration (epoch {}) is preparing on \
             top of epoch {base_epoch}, which is not older than itself",
            latest.epoch,
        )));
    }
    Ok(base_epoch)
}

fn check_sled_expunge_task_gate_case(
    removal_config: &TrustQuorumConfig,
    latest: &TrustQuorumConfig,
    case: SledExpungeTaskGateCase<'_>,
    baseboard_id: &BaseboardId,
) -> Result<SledExpungeTaskGate, SledExpungeTaskGateError> {
    let removal_epoch = removal_config.epoch;
    let gate_error = SledExpungeTaskGateError::new;

    let latest_case = match case {
        SledExpungeTaskGateCase::RemovalPreparing => {
            return Ok(SledExpungeTaskGate::RemovalPreparing);
        }
        SledExpungeTaskGateCase::RemovalAborted => {
            return Ok(SledExpungeTaskGate::RemovalAborted);
        }
        SledExpungeTaskGateCase::RemovalPast(latest_case) => latest_case,
    };

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
    match latest_case {
        SledExpungeTaskGateLatest::PastPreparing { state } => {
            if latest_has_sled {
                Ok(SledExpungeTaskGate::ReAdded { epoch: latest.epoch, state })
            } else {
                Ok(SledExpungeTaskGate::SledRemoved)
            }
        }
        SledExpungeTaskGateLatest::Preparing { base } => {
            let base_state = match base.state.phase() {
                TrustQuorumConfigPhase::Preparing
                | TrustQuorumConfigPhase::Aborted => {
                    return Err(gate_error(format!(
                        "the base configuration (epoch {}, state {:?}) is not \
                         past Preparing",
                        base.epoch, base.state,
                    )));
                }
                TrustQuorumConfigPhase::PastPreparing(state) => state,
            };
            if base.members.contains_key(baseboard_id) {
                Ok(SledExpungeTaskGate::ReAdded {
                    epoch: base.epoch,
                    state: base_state,
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
    use trust_quorum_types::membership::TRUST_QUORUM_MAX_MEMBERS;

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
                TrustQuorumMembers::new(members)
                    .expect("test members are within the size bounds"),
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
            TrustQuorumMembers::new(members)
                .expect("test members are within the size bounds"),
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
                members: TrustQuorumMembers::new(
                    [
                        test_baseboard("member-0"),
                        test_baseboard("member-1"),
                        test_baseboard("member-3"),
                    ]
                    .into_iter()
                    .collect(),
                )
                .expect("three members are within the size bounds"),
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
            RemoveSledError::InvalidMembership {
                rack_id: minimum.rack_id,
                sled: sled_to_remove,
                error: TrustQuorumMembersError::TooFew { num_members: 2 },
            },
        );
    }

    #[test]
    fn test_proposed_config_add_sleds() {
        let config =
            test_config(3, Epoch(5), TrustQuorumConfigState::Committed);
        let new_sled = test_baseboard("new");

        let proposed = ProposedTrustQuorumConfig::add_sleds(
            &config,
            Epoch(6),
            BTreeSet::from([new_sled.clone()]),
        )
        .expect("a new sled is added");
        assert_eq!(
            proposed,
            ProposedTrustQuorumConfig {
                rack_id: config.rack_id,
                epoch: Epoch(6),
                is_lrtq_upgrade: IsLrtqUpgrade::No {
                    last_committed_epoch: Epoch(5),
                },
                members: TrustQuorumMembers::new(
                    config
                        .members
                        .keys()
                        .cloned()
                        .chain([new_sled.clone()])
                        .collect(),
                )
                .expect("four members are within the size bounds"),
            }
        );

        let member = test_baseboard("member-1");
        let error = ProposedTrustQuorumConfig::add_sleds(
            &config,
            Epoch(6),
            BTreeSet::from([member.clone(), new_sled]),
        )
        .expect_err("adding a member sled is refused");
        assert_eq!(
            error,
            AddSledsError::AlreadyMembers { sleds: BTreeSet::from([member]) }
        );

        let maximum = test_config(
            TRUST_QUORUM_MAX_MEMBERS,
            Epoch(5),
            TrustQuorumConfigState::Committed,
        );
        let error = ProposedTrustQuorumConfig::add_sleds(
            &maximum,
            Epoch(6),
            BTreeSet::from([test_baseboard("new")]),
        )
        .expect_err("adding to a maximum-size trust quorum is refused");
        assert_eq!(
            error,
            AddSledsError::InvalidMembership {
                rack_id: maximum.rack_id,
                error: TrustQuorumMembersError::TooMany {
                    num_members: TRUST_QUORUM_MAX_MEMBERS + 1,
                },
            },
        );
    }

    #[test]
    fn test_proposed_config_upgrade_from_lrtq() {
        let rack_id = RackUuid::new_v4();
        let sleds: BTreeSet<_> =
            (0..3).map(|i| test_baseboard(&format!("member-{i}"))).collect();

        let proposed = ProposedTrustQuorumConfig::upgrade_from_lrtq(
            rack_id,
            Epoch(2),
            sleds.clone(),
        )
        .expect("three commissioned sleds are upgraded");
        assert_eq!(
            proposed,
            ProposedTrustQuorumConfig {
                rack_id,
                epoch: Epoch(2),
                is_lrtq_upgrade: IsLrtqUpgrade::Yes,
                members: TrustQuorumMembers::new(sleds.clone())
                    .expect("three members are within the size bounds"),
            }
        );

        let too_few: BTreeSet<_> = sleds.into_iter().take(2).collect();
        let error = ProposedTrustQuorumConfig::upgrade_from_lrtq(
            rack_id,
            Epoch(2),
            too_few,
        )
        .expect_err("upgrading with too few sleds is refused");
        assert_eq!(
            error,
            LrtqUpgradeError::InvalidMembership {
                rack_id,
                error: TrustQuorumMembersError::TooFew { num_members: 2 },
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

        // TODO-RAINCLAUDE: Every config has a member with the sled's serial
        // number but a different part number. A membership check that compared
        // only serial numbers would wrongly find the sled, so this shows that
        // membership compares the full `BaseboardId`.
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
            match sled_expunge_task_gate_plan(removal, latest)? {
                SledExpungeTaskGatePlan::Ready(ready) => {
                    assert!(base.is_none(), "no base is given to a Ready plan");
                    ready.check(&self.sled)
                }
                SledExpungeTaskGatePlan::Load(load) => load.check(
                    base.expect("a base is given to a Load plan"),
                    &self.sled,
                ),
            }
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

    // TODO-RAINCLAUDE: The expected plan comes from the generator rather than
    // from `sled_expunge_task_gate_plan`, so that the test doesn't rely on the
    // code it is testing.
    // TODO-RAINCLAUDE: With the default of 256 cases, only about half of the
    // runs reach a `Preparing` latest configuration that re-adds the sled on
    // top of a committed base. We run more cases so that a run is very likely
    // to cover it.
    #[proptest(cases = 4096)]
    fn proptest_check_sled_expunge_task_gate(
        #[strategy(proptest::sample::select(ALL_STATES.to_vec()))]
        removal_state: TrustQuorumConfigState,
        removal_membership: Membership,
        latest_input: GateLatest,
    ) {
        use TrustQuorumConfigState as S;

        let is_preparing = |state: TrustQuorumConfigState| match state {
            S::Preparing | S::PreparingLrtqUpgrade => true,
            S::Committing
            | S::Committed
            | S::CommittedPartially
            | S::Aborted => false,
        };
        let fixture = GateFixture::new();
        let removal = fixture.removal(removal_state, removal_membership);
        let removal_past_preparing = match removal_state {
            S::Preparing | S::PreparingLrtqUpgrade | S::Aborted => false,
            S::Committing | S::Committed | S::CommittedPartially => true,
        };
        let (latest, base_config, base_if_needed) = match latest_input {
            GateLatest::RemovalItself => {
                (removal.clone(), None, SledExpungeTaskGateBase::NotNeeded)
            }
            GateLatest::Later { state, membership, built_on } => match built_on
            {
                GateBase::Removal => {
                    let latest = fixture.config(
                        LATER_EPOCH,
                        state,
                        membership,
                        Some(REMOVAL_EPOCH),
                    );
                    (latest, None, SledExpungeTaskGateBase::RemovalConfig)
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
                    (
                        latest,
                        Some(base),
                        SledExpungeTaskGateBase::Load(BASE_EPOCH),
                    )
                }
            },
        };
        let needs_base = removal_past_preparing && is_preparing(latest.state);
        let expected_base = if needs_base {
            base_if_needed
        } else {
            SledExpungeTaskGateBase::NotNeeded
        };
        let latest_aborted_after_removal =
            removal_past_preparing && latest.state == S::Aborted;
        let plan = sled_expunge_task_gate_plan(&removal, &latest);
        if latest_aborted_after_removal {
            prop_assert_eq!(
                plan.map(|plan| plan.base()),
                fixture.error(&format!(
                    "the latest non-aborted configuration (epoch {}) is \
                     aborted",
                    latest.epoch,
                )),
            );
            return Ok(());
        }
        prop_assert_eq!(
            plan.as_ref().map(SledExpungeTaskGatePlan::base),
            Ok(expected_base)
        );
        let plan = plan.expect("the plan is Ok");
        let effective_base = match expected_base {
            SledExpungeTaskGateBase::NotNeeded => None,
            SledExpungeTaskGateBase::RemovalConfig => Some(&removal),
            SledExpungeTaskGateBase::Load(_) => Some(
                base_config
                    .as_ref()
                    .expect("a Load plan is only expected with a later base"),
            ),
        };
        let gate = match plan {
            SledExpungeTaskGatePlan::Ready(ready) => ready.check(&fixture.sled),
            SledExpungeTaskGatePlan::Load(load) => load.check(
                effective_base.expect("a Load plan has a loaded base"),
                &fixture.sled,
            ),
        };

        match removal_state {
            S::Preparing | S::PreparingLrtqUpgrade => {
                prop_assert_eq!(
                    gate,
                    Ok(SledExpungeTaskGate::RemovalPreparing)
                );
                return Ok(());
            }
            S::Aborted => {
                prop_assert_eq!(gate, Ok(SledExpungeTaskGate::RemovalAborted));
                return Ok(());
            }
            S::Committing | S::Committed | S::CommittedPartially => {}
        }

        let latest_preparing = is_preparing(latest.state);
        let base_has_sled = effective_base.is_some_and(|b| fixture.has_sled(b));

        // TODO-RAINCLAUDE: By this point, the test has already returned for an
        // `Aborted` latest configuration, and the generator never builds a
        // configuration with the wrong rack or epoch. That leaves two possible
        // errors, which are checked in this order: the removal still includes
        // the sled, or the base is not past `Preparing`.
        let base_not_past_preparing =
            effective_base.filter(|base| match base.state {
                S::Committing | S::Committed | S::CommittedPartially => false,
                S::Preparing | S::PreparingLrtqUpgrade | S::Aborted => true,
            });
        let expected_error = if fixture.has_sled(&removal) {
            Some(format!(
                "the removal configuration (epoch {REMOVAL_EPOCH}, state \
                 {removal_state:?}) still includes the sled"
            ))
        } else if let Some(base) = base_not_past_preparing {
            Some(format!(
                "the base configuration (epoch {}, state {:?}) is not past \
                 Preparing",
                base.epoch, base.state,
            ))
        } else {
            None
        };
        if let Some(message) = expected_error {
            prop_assert_eq!(gate, fixture.error(&message));
            return Ok(());
        }

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
                    (epoch, TrustQuorumConfigState::from(state)),
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
            Err(error) => {
                prop_assert!(
                    false,
                    "unexpected gate error for consistent inputs: {error}"
                );
            }
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

        // TODO-RAINCLAUDE: Some cases in this test are there to show which
        // check runs first:
        // * The latest configuration's rack is checked before the removal's
        //   state, so a wrong rack is reported even when the removal is still
        //   `Preparing`.
        // * The loaded base's rack is checked before whether the removal still
        //   includes the sled.
        // * An `Aborted` latest configuration is refused by
        //   `sled_expunge_task_gate_plan`, which also comes before the check
        //   for whether the removal still includes the sled.
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
        let removal_with_sled = fixture.removal(S::Committed, Member);
        let mut base_other_rack =
            fixture.config(BASE_EPOCH, S::Committed, NonMember, None);
        base_other_rack.rack_id = other_rack;
        assert_eq!(
            fixture.check(
                &removal_with_sled,
                &preparing_on_base,
                Some(&base_other_rack)
            ),
            fixture.error(&format!(
                "the base configuration (epoch {BASE_EPOCH}) is for rack \
                 {other_rack}, not the removal's rack {rack_id}"
            )),
        );

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
            fixture.check(&removal_with_sled, &aborted_latest, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
                 aborted"
            )),
        );

        let wrong_base =
            fixture.config(Epoch(6), S::Committed, NonMember, None);
        assert_eq!(
            fixture.check(&removal, &preparing_on_base, Some(&wrong_base)),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
                 preparing on top of epoch {BASE_EPOCH}, but the base \
                 configuration loaded is at epoch 6"
            )),
        );

        let no_base =
            fixture.config(LATER_EPOCH, S::Preparing, NonMember, None);
        assert_eq!(
            fixture.check(&removal, &no_base, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}, \
                 state Preparing) is preparing but has no last committed epoch"
            )),
        );

        let on_older_base = fixture.config(
            LATER_EPOCH,
            S::Preparing,
            NonMember,
            Some(OLDER_EPOCH),
        );
        assert_eq!(
            fixture.check(&removal, &on_older_base, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
                 preparing on top of epoch {OLDER_EPOCH}, which is older than \
                 the removal configuration (epoch {REMOVAL_EPOCH})"
            )),
        );

        let on_itself = fixture.config(
            LATER_EPOCH,
            S::Preparing,
            NonMember,
            Some(LATER_EPOCH),
        );
        assert_eq!(
            fixture.check(&removal, &on_itself, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {LATER_EPOCH}) is \
                 preparing on top of epoch {LATER_EPOCH}, which is not older \
                 than itself"
            )),
        );
        let on_newer = fixture.config(
            BASE_EPOCH,
            S::Preparing,
            NonMember,
            Some(LATER_EPOCH),
        );
        assert_eq!(
            fixture.check(&removal, &on_newer, None),
            fixture.error(&format!(
                "the latest non-aborted configuration (epoch {BASE_EPOCH}) is \
                 preparing on top of epoch {LATER_EPOCH}, which is not older \
                 than itself"
            )),
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
