// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various validation functions to be used by a [`crate::Node`]

use crate::configuration::ConfigurationError;
use crate::messages::ReconfigureMsg;
use crate::{CoordinatorState, Epoch, PersistentState, PlatformId, Threshold};
use omicron_uuid_kinds::RackUuid;
use slog::{Logger, error, info, warn};
use std::collections::BTreeSet;
use std::time::Duration;

/// RackId's must remain the same over the lifetime of a trust quorum instance
pub fn check_rack_id(
    msg_rack_id: RackUuid,
    persistent_state: &PersistentState,
) -> Result<(), MismatchedRackIdError> {
    if let Some(rack_id) = persistent_state.rack_id() {
        if rack_id != msg_rack_id {
            return Err(MismatchedRackIdError {
                expected: rack_id,
                got: msg_rack_id,
            }
            .into());
        }
    }

    Ok(())
}

/// Verify that the node is not decommissioned
fn check_in_service(
    persistent_state: &PersistentState,
) -> Result<(), SledDecommissionedError> {
    if let Some(decommissioned) = &persistent_state.decommissioned {
        return Err(SledDecommissionedError {
            from: decommissioned.from.clone(),
            epoch: decommissioned.epoch,
            last_prepared_epoch: persistent_state.last_prepared_epoch(),
        });
    }

    Ok(())
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error(
    "sled was decommissioned on msg from {from:?} at epoch {epoch:?}: last prepared epoch = {last_prepared_epoch:?}"
)]
pub struct SledDecommissionedError {
    from: PlatformId,
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

    #[error(
        "number of members: {num_members:?} must be greater than threshold: {threshold:?}"
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
        "Node has last committed epoch of {node_epoch:?}, message contains {msg_epoch:?}"
    )]
    LastCommittedEpochMismatch {
        node_epoch: Option<Epoch>,
        msg_epoch: Option<Epoch>,
    },

    #[error(
        "sled has already prepared a request at epoch {existing:?}, and cannot prepare another at a smaller or equivalent epoch {new:?}"
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
        SledDecommissionedError,
    ),
    #[error(
        "reconfiguration in progress at epoch {current_epoch:?}: cannot reconfigure for older epoch {msg_epoch:?}"
    )]
    ReconfigurationInProgress { current_epoch: Epoch, msg_epoch: Epoch },

    #[error("mismatched reconfiguration requests for epoch {0:?}")]
    MismatchedReconfigurationForSameEpoch(Epoch),

    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
}

/// A `ReconfigureMsg` that has been determined to be valid for the remainder
/// of code paths. We encode this check into a type in a "parse, don't validate"
/// manner.
pub struct ValidatedReconfigureMsg {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<PlatformId>,
    pub threshold: Threshold,

    // The timeout before we send a follow up request to a peer
    pub retry_timeout: Duration,
}

impl PartialEq<ValidatedReconfigureMsg> for ReconfigureMsg {
    fn eq(&self, other: &ValidatedReconfigureMsg) -> bool {
        let ReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
            retry_timeout,
        } = self;

        let ValidatedReconfigureMsg {
            rack_id: other_rack_id,
            epoch: other_epoch,
            last_committed_epoch: other_last_committed_epoch,
            members: other_members,
            threshold: other_threshold,
            retry_timeout: other_retry_timeout,
        } = other;

        rack_id == other_rack_id
            && epoch == other_epoch
            && last_committed_epoch == other_last_committed_epoch
            && members == other_members
            && threshold == other_threshold
            && retry_timeout == other_retry_timeout
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
        platform_id: &PlatformId,
        msg: ReconfigureMsg,
        persistent_state: &PersistentState,
        coordinator_state: &Option<CoordinatorState>,
    ) -> Result<Option<Self>, ReconfigurationError> {
        if persistent_state.is_lrtq_only() {
            return Err(ReconfigurationError::UpgradeFromLrtqRequired);
        }

        if !msg.members.contains(platform_id) {
            return Err(
                ReconfigurationError::CoordinatorMustBeAMemberOfNewGroup,
            );
        }

        Self::check_membership_sizes(&msg)?;
        check_rack_id(msg.rack_id, persistent_state)?;
        check_in_service(persistent_state)?;
        Self::check_epoch(&msg, persistent_state)?;
        let is_idempotent_request =
            Self::check_existing_coordination(log, &msg, coordinator_state)?;

        if is_idempotent_request {
            return Ok(None);
        }

        let ReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
            retry_timeout,
        } = msg;

        Ok(Some(ValidatedReconfigureMsg {
            rack_id,
            epoch,
            last_committed_epoch,
            members,
            threshold,
            retry_timeout,
        }))
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
        persistent_state: &PersistentState,
    ) -> Result<(), ReconfigurationError> {
        // Ensure we are strictly ordering committed configurations
        if msg.last_committed_epoch != persistent_state.last_committed_epoch() {
            return Err(ReconfigurationError::LastCommittedEpochMismatch {
                node_epoch: persistent_state.last_committed_epoch(),
                msg_epoch: msg.last_committed_epoch,
            });
        }

        // Ensure that we haven't seen a prepare message for a newer
        // configuration.
        if let Some(last_prepared_epoch) =
            persistent_state.last_prepared_epoch()
        {
            if msg.epoch <= last_prepared_epoch {
                return Err(ReconfigurationError::PreparedEpochMismatch {
                    existing: last_prepared_epoch,
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
        coordinator_state: &Option<CoordinatorState>,
    ) -> Result<bool, ReconfigurationError> {
        let Some(coordinator_state) = coordinator_state else {
            return Ok(false);
        };
        let existing_msg = &coordinator_state.reconfigure_msg;
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
            }
            .into());
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
                return Err(ReconfigurationError::MismatchedReconfigurationForSameEpoch(
                    new_msg.epoch,
                )
                .into());
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
