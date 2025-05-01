// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Various errors for the trust quorum APIs

use crate::configuration::ConfigurationError;
use crate::{Epoch, PlatformId, Threshold};
use omicron_uuid_kinds::RackUuid;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error(
    "sled was decommissioned on msg from {from:?} at epoch {epoch:?}: last prepared epoch = {last_prepared_epoch:?}"
)]
pub struct SledDecommissionedError {
    pub from: PlatformId,
    pub epoch: Epoch,
    pub last_prepared_epoch: Option<Epoch>,
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
