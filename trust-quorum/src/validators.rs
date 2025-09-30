// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various validation functions to be used by a [`crate::Node`]

use crate::configuration::{ConfigurationError, NewConfigParams};
use crate::messages::ReconfigureMsg;
use crate::{
    Epoch, LrtqUpgradeMsg, NodeHandlerCtx, PersistentStateSummary, BaseboardId,
    Threshold,
};
use daft::{BTreeSetDiff, Diffable, Leaf};
use omicron_uuid_kinds::RackUuid;
use slog::{Logger, error, info, warn};
use std::collections::BTreeSet;

/// Rack IDs must remain the same over the lifetime of a trust quorum instance
pub fn check_rack_id(
    msg_rack_id: RackUuid,
    persistent_state: &PersistentStateSummary,
) -> Result<(), MismatchedRackIdError> {
    if let Some(rack_id) = persistent_state.rack_id {
        if rack_id != msg_rack_id {
            return Err(MismatchedRackIdError {
                expected: rack_id,
                got: msg_rack_id,
            });
        }
    }

    Ok(())
}

/// Verify that the node is not decommissioned
fn check_in_service(
    persistent_state: &PersistentStateSummary,
) -> Result<(), SledExpungedError> {
    if let Some(expunged) = &persistent_state.expunged {
        return Err(SledExpungedError {
            from: expunged.from.clone(),
            epoch: expunged.epoch,
            last_prepared_epoch: persistent_state.latest_committed_config,
        });
    }

    Ok(())
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error(
    "sled was decommissioned on msg from {from:?} at epoch {epoch:?}: last prepared epoch = {last_prepared_epoch:?}"
)]
pub struct SledExpungedError {
    from: BaseboardId,
    epoch: Epoch,
    last_prepared_epoch: Option<Epoch>,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("mismatched rack id: expected {expected:?}, got {got:?}")]
pub struct MismatchedRackIdError {
    pub expected: RackUuid,
    pub got: RackUuid,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ReconfigurationError {
    #[error("reconfiguration coordinator must be a member of the new group")]
    CoordinatorMustBeAMemberOfNewGroup,

    #[error("upgrade from LRTQ required")]
    UpgradeFromLrtqRequired,

    #[error("upgrade from LRTQ in progress")]
    UpgradeFromLrtqInProgress,

    #[error(
        "number of members: {num_members:?} must be greater than threshold: \
        {threshold:?}"
    )]
    ThresholdMismatch { num_members: usize, threshold: Threshold },

    #[error(
        "invalid membership size: {0:?}: must be between 3 and 32 inclusive"
    )]
    InvalidMembershipSize(usize),

    #[error(
        "invalid threshold: {0:?}: threshold must be between 2 and 31 inclusive"
    )]
    InvalidThreshold(Threshold),

    #[error(
        "Node has last committed epoch of {node_epoch:?}, \
        message contains {msg_epoch:?}"
    )]
    LastCommittedEpochMismatch {
        node_epoch: Option<Epoch>,
        msg_epoch: Option<Epoch>,
    },

    #[error(
        "sled has already prepared a request at epoch {existing:?}, \
        and cannot prepare another at a smaller or equivalent epoch {new:?}"
    )]
    PreparedEpochMismatch { existing: Epoch, new: Epoch },

    #[error("invalid rack id in reconfigure msg")]
    InvalidRackId(
        #[from]
        #[source]
        MismatchedRackIdError,
    ),

    #[error("cannot reconfigure a decommissioned sled")]
    DecommissionedSled(
        #[from]
        #[source]
        SledExpungedError,
    ),
    #[error(
        "reconfiguration in progress at epoch {current_epoch:?}: cannot \
        reconfigure for older epoch {msg_epoch:?}"
    )]
    ReconfigurationInProgress { current_epoch: Epoch, msg_epoch: Epoch },

    #[error("mismatched reconfiguration requests for epoch {0:?}")]
    MismatchedReconfigurationForSameEpoch(Epoch),

    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum LrtqUpgradeError {
    #[error("invalid rack id")]
    InvalidRackId(
        #[from]
        #[source]
        MismatchedRackIdError,
    ),

    #[error("cannot commit: expunged at epoch {epoch} by {from}")]
    Expunged { epoch: Epoch, from: BaseboardId },

    #[error("not an lrtq node - no lrtq key share")]
    NoLrtqShare,

    #[error("already upgraded from lrtq: committed epoch {0}")]
    AlreadyUpgraded(Epoch),

    #[error("reconfiguration coordinator must be a member of the new group")]
    CoordinatorMustBeAMemberOfNewGroup,

    #[error(
        "number of members: {num_members:?} must be greater than threshold: \
        {threshold:?}"
    )]
    ThresholdMismatch { num_members: usize, threshold: Threshold },

    #[error(
        "invalid membership size: {0:?}: must be between 3 and 32 inclusive"
    )]
    InvalidMembershipSize(usize),

    #[error(
        "invalid threshold: {0:?}: threshold must be between 2 and 31 inclusive"
    )]
    InvalidThreshold(Threshold),

    #[error(
        "sled has already prepared a request at epoch {existing:?}, \
        and cannot prepare another at a smaller or equivalent epoch {new:?}"
    )]
    PreparedEpochMismatch { existing: Epoch, new: Epoch },

    #[error("epoch must be at least 2 as the LRTQ epoch is 1. got {0}")]
    EpochMustBeAtLeast2(Epoch),

    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
}

impl<'a> From<&'a ValidatedReconfigureMsg> for NewConfigParams<'a> {
    fn from(value: &'a ValidatedReconfigureMsg) -> Self {
        Self {
            rack_id: value.rack_id,
            epoch: value.epoch,
            members: &value.members,
            threshold: value.threshold,
            coordinator_id: &value.coordinator_id,
        }
    }
}

/// A `ReconfigureMsg` that has been determined to be valid for the remainder
/// of code paths. We encode this check into a type in a "parse, don't validate"
/// manner.
#[derive(Debug, Clone, PartialEq, Eq, Diffable)]
pub struct ValidatedReconfigureMsg {
    rack_id: RackUuid,
    epoch: Epoch,
    last_committed_epoch: Option<Epoch>,
    members: BTreeSet<BaseboardId>,
    threshold: Threshold,

    // This is not included in the original `ReconfigureMsg`. It's implicit
    // in the node that Nexus sends the request to.
    coordinator_id: BaseboardId,
}

// For diffs we want to allow access to all fields, but not make them public in
// the `ValidatedReconfigureMsg` type itself.
impl<'daft> ValidatedReconfigureMsgDiff<'daft> {
    pub fn rack_id(&self) -> Leaf<&RackUuid> {
        self.rack_id
    }

    pub fn epoch(&self) -> Leaf<&Epoch> {
        self.epoch
    }

    pub fn last_committed_epoch(&self) -> Leaf<Option<&Epoch>> {
        self.last_committed_epoch
    }

    pub fn members(&self) -> &BTreeSetDiff<'daft, BaseboardId> {
        &self.members
    }

    pub fn threshold(&self) -> Leaf<&Threshold> {
        self.threshold
    }

    pub fn coordinator_id(&self) -> Leaf<&BaseboardId> {
        self.coordinator_id
    }
}

impl PartialEq<ValidatedReconfigureMsg> for ReconfigureMsg {
    fn eq(&self, other: &ValidatedReconfigureMsg) -> bool {
        let ReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
        } = self;

        let ValidatedReconfigureMsg {
            rack_id: other_rack_id,
            epoch: other_epoch,
            last_committed_epoch: other_last_committed_epoch,
            members: other_members,
            threshold: other_threshold,
            // This field doesn't exist in `ReconfigureMsg` and is not relevant
            // for comparisons.
            coordinator_id: _,
        } = other;

        rack_id == other_rack_id
            && epoch == other_epoch
            && last_committed_epoch == other_last_committed_epoch
            && members == other_members
            && threshold == other_threshold
    }
}

impl PartialEq<ReconfigureMsg> for ValidatedReconfigureMsg {
    fn eq(&self, other: &ReconfigureMsg) -> bool {
        other.eq(self)
    }
}

impl ValidatedReconfigureMsg {
    /// Ensure that the `ReconfigurationMsg` is valid and return a
    /// `ValidatedReconfigureMsg` if it is.
    ///
    /// Returns `Ok(None)` if this is an idempotent request, indicating
    /// that the ongoing coordination can continue.
    pub fn new(
        log: &Logger,
        coordinator_id: &BaseboardId,
        msg: ReconfigureMsg,
        persistent_state: PersistentStateSummary,
        last_reconfig_msg: Option<&ValidatedReconfigureMsg>,
    ) -> Result<Option<Self>, ReconfigurationError> {
        if persistent_state.is_lrtq_only {
            return Err(ReconfigurationError::UpgradeFromLrtqRequired);
        }

        if !msg.members.contains(coordinator_id) {
            return Err(
                ReconfigurationError::CoordinatorMustBeAMemberOfNewGroup,
            );
        }

        Self::check_membership_sizes(&msg)?;
        check_rack_id(msg.rack_id, &persistent_state)?;
        check_in_service(&persistent_state)?;
        Self::check_epoch(&msg, &persistent_state)?;
        let is_idempotent_request =
            Self::check_existing_coordination(log, &msg, last_reconfig_msg)?;

        if is_idempotent_request {
            return Ok(None);
        }

        let ReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
        } = msg;

        Ok(Some(ValidatedReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
            coordinator_id: coordinator_id.clone(),
        }))
    }

    pub fn rack_id(&self) -> RackUuid {
        self.rack_id
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn last_committed_epoch(&self) -> Option<Epoch> {
        self.last_committed_epoch
    }

    pub fn members(&self) -> &BTreeSet<BaseboardId> {
        &self.members
    }

    pub fn threshold(&self) -> Threshold {
        self.threshold
    }

    pub fn coordinator_id(&self) -> &BaseboardId {
        &self.coordinator_id
    }

    /// Verify that the cluster membership and threshold sizes are within
    /// constraints.
    fn check_membership_sizes(
        msg: &ReconfigureMsg,
    ) -> Result<(), ReconfigurationError> {
        let num_members = msg.members.len();
        if num_members <= msg.threshold.0 as usize {
            return Err(ReconfigurationError::ThresholdMismatch {
                num_members,
                threshold: msg.threshold,
            });
        }

        if num_members < 3 || num_members > 32 {
            return Err(ReconfigurationError::InvalidMembershipSize(
                num_members,
            ));
        }

        if msg.threshold.0 < 2 || msg.threshold.0 > 31 {
            return Err(ReconfigurationError::InvalidThreshold(msg.threshold));
        }

        Ok(())
    }

    // Ensure that the epoch for this reconfiguration is valid
    fn check_epoch(
        msg: &ReconfigureMsg,
        persistent_state: &PersistentStateSummary,
    ) -> Result<(), ReconfigurationError> {
        // Ensure we are strictly ordering committed configurations
        if msg.last_committed_epoch != persistent_state.latest_committed_config
        {
            return Err(ReconfigurationError::LastCommittedEpochMismatch {
                node_epoch: persistent_state.latest_committed_config,
                msg_epoch: msg.last_committed_epoch,
            });
        }

        // Ensure that we haven't seen a newer configuration
        if let Some(latest_epoch) = persistent_state.latest_config {
            if msg.epoch <= latest_epoch {
                return Err(ReconfigurationError::PreparedEpochMismatch {
                    existing: latest_epoch,
                    new: msg.epoch,
                });
            }
        }

        Ok(())
    }

    /// Ensure that if this node is currently coordinating a reconfiguration,
    /// that this request is at least as new as the last one.
    ///
    /// Return `Ok(false)` if the configuration is new, and `Ok(true)` if it
    /// is idempotent.
    fn check_existing_coordination(
        log: &Logger,
        new_msg: &ReconfigureMsg,
        last_reconfig_msg: Option<&ValidatedReconfigureMsg>,
    ) -> Result<bool, ReconfigurationError> {
        let Some(existing_msg) = last_reconfig_msg else {
            return Ok(false);
        };
        let current_epoch = existing_msg.epoch;
        if current_epoch > new_msg.epoch {
            warn!(
                log,
                "Reconfiguration in progress: rejecting stale attempt";
                "current_epoch" => current_epoch.to_string(),
                "msg_epoch" => new_msg.epoch.to_string()
            );
            return Err(ReconfigurationError::ReconfigurationInProgress {
                current_epoch: existing_msg.epoch,
                msg_epoch: new_msg.epoch,
            });
        }

        if current_epoch == new_msg.epoch {
            if existing_msg != new_msg {
                error!(
                    log,
                    concat!(
                        "Reconfiguration in progress for same epoch, ",
                        "but messages differ");
                    "epoch" => new_msg.epoch.to_string(),
                );
                return Err(
                    ReconfigurationError::MismatchedReconfigurationForSameEpoch(
                        new_msg.epoch,
                    ),
                );
            }

            // Idempotent request
            return Ok(true);
        }

        info!(
            log,
            "Configuration being coordinated changed";
            "previous_epoch" => current_epoch.to_string(),
            "new_epoch" => new_msg.epoch.to_string()
        );

        // Valid new request
        Ok(false)
    }
}

impl<'a> From<&'a ValidatedLrtqUpgradeMsg> for NewConfigParams<'a> {
    fn from(value: &'a ValidatedLrtqUpgradeMsg) -> Self {
        Self {
            rack_id: value.rack_id,
            epoch: value.epoch,
            members: &value.members,
            threshold: value.threshold,
            coordinator_id: &value.coordinator_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Diffable)]
pub struct ValidatedLrtqUpgradeMsg {
    rack_id: RackUuid,
    epoch: Epoch,
    members: BTreeSet<BaseboardId>,
    threshold: Threshold,

    // This is not included in the original `LrtqUpgradeMsg`. It's implicit in
    // the node that Nexus sends the request to.
    coordinator_id: BaseboardId,
}

impl ValidatedLrtqUpgradeMsg {
    /// Ensure that the `LrtqUpgradeMsg` is valid and return a
    /// `ValidatedLrtqUpgradeMsg` if it is.
    ///
    /// LRTQ upgrade does not accept idempotent requests. If a configuration has
    /// been seen for a given epoch, then an error is returned. TODO: This might
    /// be the right behavior for normal reconfigurations as well. Nexus is
    /// not going to send a request more than once for the same epoch. For now
    /// though, we leave things as is.
    pub fn new(
        log: &Logger,
        ctx: &mut impl NodeHandlerCtx,
        msg: LrtqUpgradeMsg,
    ) -> Result<Self, LrtqUpgradeError> {
        let ps = ctx.persistent_state();

        if let Some(expunged) = &ps.expunged {
            error!(
                log,
                "LRTQ upgrade attempted on expunged node";
                "expunged_epoch" => %expunged.epoch,
                "expunging_node" => %expunged.from
            );
            return Err(LrtqUpgradeError::Expunged {
                epoch: expunged.epoch,
                from: expunged.from.clone(),
            });
        }

        // If we have an LRTQ share, the rack id must match the one from Nexus
        if let Some(ps_rack_id) = ps.rack_id() {
            if msg.rack_id != ps_rack_id {
                error!(
                    log,
                    "LRTQ upgrade attempted with invalid rack_id";
                    "expected" => %ps_rack_id,
                    "got" => %msg.rack_id
                );
                return Err(MismatchedRackIdError {
                    expected: ps_rack_id,
                    got: msg.rack_id,
                }
                .into());
            }
        }

        if ps.lrtq.is_none() {
            error!(log, "LRTQ upgrade attempted on node without LRTQ share");
            return Err(LrtqUpgradeError::NoLrtqShare);
        }

        if let Some(epoch) = ps.latest_committed_epoch() {
            error!(
                log,
                "LRTQ upgrade attempted when already upgraded";
                "committed_epoch" => %epoch
            );
            return Err(LrtqUpgradeError::AlreadyUpgraded(epoch));
        }

        if !msg.members.contains(ctx.platform_id()) {
            return Err(LrtqUpgradeError::CoordinatorMustBeAMemberOfNewGroup);
        }

        Self::check_membership_sizes(&msg)?;
        Self::check_epoch(ctx, &msg)?;

        let LrtqUpgradeMsg { rack_id, epoch, members, threshold } = msg;

        Ok(ValidatedLrtqUpgradeMsg {
            rack_id,
            epoch,
            members,
            threshold,
            coordinator_id: ctx.platform_id().clone(),
        })
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn coordinator_id(&self) -> &BaseboardId {
        &self.coordinator_id
    }

    /// Verify that the cluster membership and threshold sizes are within
    /// constraints.
    ///
    /// This is essentially a copy of the  method for `ValidatedReconfigureMsg`,
    /// but with different types.
    fn check_membership_sizes(
        msg: &LrtqUpgradeMsg,
    ) -> Result<(), LrtqUpgradeError> {
        let num_members = msg.members.len();
        if num_members <= msg.threshold.0 as usize {
            return Err(LrtqUpgradeError::ThresholdMismatch {
                num_members,
                threshold: msg.threshold,
            });
        }

        if num_members < 3 || num_members > 32 {
            return Err(LrtqUpgradeError::InvalidMembershipSize(num_members));
        }

        if msg.threshold.0 < 2 || msg.threshold.0 > 31 {
            return Err(LrtqUpgradeError::InvalidThreshold(msg.threshold));
        }

        Ok(())
    }

    // Ensure that the epoch for this LRTQ upgrade is valid
    fn check_epoch(
        ctx: &mut impl NodeHandlerCtx,
        msg: &LrtqUpgradeMsg,
    ) -> Result<(), LrtqUpgradeError> {
        // Epochs for LRTQ upgrades must start at 2, as the LRTQ epoch is always 1.
        if msg.epoch < Epoch(2) {
            return Err(LrtqUpgradeError::EpochMustBeAtLeast2(msg.epoch));
        }

        // Ensure that we haven't seen a newer configuration
        if let Some(latest_config) = ctx.persistent_state().latest_config() {
            if msg.epoch <= latest_config.epoch {
                return Err(LrtqUpgradeError::PreparedEpochMismatch {
                    existing: latest_config.epoch,
                    new: msg.epoch,
                });
            }
        }

        Ok(())
    }
}

// For diffs we want to allow access to all fields, but not make them public in
// the `ValidatedLrtqUpgradeMsg` type itself.
impl<'daft> ValidatedLrtqUpgradeMsgDiff<'daft> {
    pub fn rack_id(&self) -> Leaf<&RackUuid> {
        self.rack_id
    }

    pub fn epoch(&self) -> Leaf<&Epoch> {
        self.epoch
    }

    pub fn members(&self) -> &BTreeSetDiff<'daft, BaseboardId> {
        &self.members
    }

    pub fn threshold(&self) -> Leaf<&Threshold> {
        self.threshold
    }

    pub fn coordinator_id(&self) -> Leaf<&BaseboardId> {
        self.coordinator_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::{GenericUuid, RackUuid};
    use proptest::prelude::*;
    use test_strategy::{Arbitrary, proptest};
    use uuid::Uuid;

    fn arb_member() -> impl Strategy<Value = BaseboardId> {
        (0..255u8).prop_map(|serial| {
            BaseboardId { part_number: "test".into(), serial_number: serial.to_string() }
        })
    }

    fn arb_members() -> impl Strategy<Value = BTreeSet<BaseboardId>> {
        proptest::collection::btree_set(arb_member(), 3..10)
    }

    // We want to limit the number of unique rack ids to 2
    // so that they match in most cases.
    fn arb_rack_id() -> impl Strategy<Value = RackUuid> {
        (0..10u8).prop_map(|val| {
            let raw = if val == 9 {
                Uuid::nil()
            } else {
                Uuid::from_bytes([
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                ])
            };
            RackUuid::from_untyped_uuid(raw)
        })
    }

    #[derive(Arbitrary, Debug)]
    pub struct TestInput {
        #[strategy(arb_rack_id())]
        rack_id: RackUuid,
        #[strategy(arb_members())]
        members: BTreeSet<BaseboardId>,
        #[strategy((1..10u64).prop_map(|x| Epoch(x)))]
        epoch: Epoch,
        new_config: bool,
    }

    // Generate dependent values such that reconfiguration always succeds
    #[proptest]
    fn test_validate_reconfigure_msg_new_success(input: TestInput) {
        let logctx = test_setup_log("validate_reconfigure_msg_new_success");
        let last_committed_epoch = if input.new_config {
            None
        } else {
            Some(Epoch(input.epoch.0 - 1))
        };
        let msg = ReconfigureMsg {
            rack_id: input.rack_id,
            epoch: input.epoch,
            last_committed_epoch,
            members: input.members.clone(),
            threshold: Threshold(input.members.len() as u8 - 1),
        };

        let platform_id = input.members.first().unwrap().clone();
        let (persistent_state, last_reconfig_msg) = if input.new_config {
            let persistent_state = PersistentStateSummary {
                rack_id: None,
                is_lrtq_only: false,
                is_uninitialized: true,
                latest_config: None,
                latest_committed_config: None,
                latest_share: None,
                expunged: None,
            };
            (persistent_state, None)
        } else {
            let persistent_state = PersistentStateSummary {
                rack_id: Some(msg.rack_id),
                is_lrtq_only: false,
                is_uninitialized: false,
                latest_config: msg.last_committed_epoch,
                latest_committed_config: msg.last_committed_epoch,
                latest_share: msg.last_committed_epoch,
                expunged: None,
            };
            let mut members = input.members.clone();
            members
                .insert(BaseboardId { part_number: "test".into(), serial_number: "removed_node".into() });
            let last_reconfig_msg = ValidatedReconfigureMsg {
                rack_id: input.rack_id,
                epoch: msg.last_committed_epoch.unwrap(),
                last_committed_epoch: None,
                members,
                threshold: msg.threshold,
                coordinator_id: platform_id.clone(),
            };

            (persistent_state, Some(last_reconfig_msg))
        };

        assert!(
            ValidatedReconfigureMsg::new(
                &logctx.log,
                &platform_id,
                msg,
                persistent_state,
                last_reconfig_msg.as_ref()
            )
            .expect("valid msg")
            .is_some()
        );

        logctx.cleanup_successful();
    }

    /// Test some error conditions when validating a `ReconfigureMsg`
    #[proptest]
    fn test_validate_reconfigure_msg_failure(input: TestInput) {
        let logctx = test_setup_log("validate_reconfigure_msg_new_failure");
        let last_committed_epoch = if input.new_config {
            None
        } else {
            Some(Epoch(input.epoch.0 - 1))
        };
        let msg = ReconfigureMsg {
            rack_id: input.rack_id,
            epoch: input.epoch,
            last_committed_epoch,
            members: input.members.clone(),
            threshold: Threshold(input.members.len() as u8 - 1),
        };

        let platform_id = input.members.first().unwrap().clone();
        let (mut persistent_state, mut last_reconfig_msg) = if input.new_config
        {
            let persistent_state = PersistentStateSummary {
                rack_id: None,
                is_lrtq_only: false,
                is_uninitialized: true,
                latest_config: None,
                latest_committed_config: None,
                latest_share: None,
                expunged: None,
            };
            (persistent_state, None)
        } else {
            let persistent_state = PersistentStateSummary {
                rack_id: Some(msg.rack_id),
                is_lrtq_only: false,
                is_uninitialized: false,
                latest_config: msg.last_committed_epoch,
                latest_committed_config: msg.last_committed_epoch,
                latest_share: msg.last_committed_epoch,
                expunged: None,
            };
            let mut members = input.members.clone();
            members
                .insert(BaseboardId { part_number: "test".into(), serial_number: "removed_node".into() });
            let last_reconfig_msg = ValidatedReconfigureMsg {
                rack_id: input.rack_id,
                epoch: msg.last_committed_epoch.unwrap(),
                last_committed_epoch: None,
                members,
                threshold: msg.threshold,
                coordinator_id: platform_id.clone(),
            };

            (persistent_state, Some(last_reconfig_msg))
        };

        // The messages are valid at this point. We tweak them to elicit
        // different failures.

        // Can't upgrade from LRTQ with a normal `ReconfigureMsg`
        let original_persistent_state = persistent_state.clone();
        let original_msg = msg.clone();

        persistent_state.is_lrtq_only = true;
        let err = ValidatedReconfigureMsg::new(
            &logctx.log,
            &platform_id,
            msg,
            persistent_state.clone(),
            last_reconfig_msg.as_ref(),
        )
        .unwrap_err();
        assert_eq!(err, ReconfigurationError::UpgradeFromLrtqRequired);

        let persistent_state = original_persistent_state.clone();

        // Coordinator must be a member of the new group
        let msg = original_msg.clone();
        let bad_platform_id = BaseboardId { part_number: "bad".into(), serial_number: "bad".into() };
        let err = ValidatedReconfigureMsg::new(
            &logctx.log,
            &bad_platform_id,
            msg,
            persistent_state.clone(),
            last_reconfig_msg.as_ref(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ReconfigurationError::CoordinatorMustBeAMemberOfNewGroup
        );

        // Invalid threshold
        let mut msg = original_msg.clone();
        msg.threshold = Threshold(1);
        let err = ValidatedReconfigureMsg::new(
            &logctx.log,
            &platform_id,
            msg,
            persistent_state.clone(),
            last_reconfig_msg.as_ref(),
        )
        .unwrap_err();
        assert_eq!(err, ReconfigurationError::InvalidThreshold(Threshold(1)));

        if original_persistent_state.rack_id.is_some() {
            let mut msg = original_msg.clone();
            // Rack IDs must match
            msg.rack_id = RackUuid::from_untyped_uuid(Uuid::from_bytes([
                2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            ]));
            let err = ValidatedReconfigureMsg::new(
                &logctx.log,
                &platform_id,
                msg,
                persistent_state,
                last_reconfig_msg.as_ref(),
            )
            .unwrap_err();
            assert_matches!(err, ReconfigurationError::InvalidRackId(_));

            // last_committed epoch must be valid
            let mut msg = original_msg.clone();
            let persistent_state = original_persistent_state.clone();
            msg.last_committed_epoch = Some(Epoch(999));
            let err = ValidatedReconfigureMsg::new(
                &logctx.log,
                &platform_id,
                msg,
                persistent_state.clone(),
                last_reconfig_msg.as_ref(),
            )
            .unwrap_err();
            assert_matches!(
                err,
                ReconfigurationError::LastCommittedEpochMismatch { .. }
            );

            // Make the existing coordination invalid
            let msg = original_msg.clone();
            last_reconfig_msg.as_mut().unwrap().epoch = Epoch(999);
            let err = ValidatedReconfigureMsg::new(
                &logctx.log,
                &platform_id,
                msg,
                persistent_state.clone(),
                last_reconfig_msg.as_ref(),
            )
            .unwrap_err();
            assert_matches!(
                err,
                ReconfigurationError::ReconfigurationInProgress { .. }
            );
        }

        logctx.cleanup_successful();
    }
}
