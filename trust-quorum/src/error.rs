// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Errors returned from the Node API
use crate::{Epoch, PlatformId, Threshold};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    thiserror::Error,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub enum Error {
    #[error(
        "sled was decommissioned on msg from {from:?} at epoch {epoch:?}: last prepared epoch = {last_prepared_epoch:?}"
    )]
    SledDecommissioned {
        from: PlatformId,
        epoch: Epoch,
        last_prepared_epoch: Option<Epoch>,
    },

    #[error("sled has already committed a request at epoch {0:?}")]
    AlreadyCommitted(Epoch),

    #[error("sled has already prepared a request at epoch {0:?}")]
    AlreadyPrepared(Epoch),

    #[error(
        "number of members: {num_members:?} must be greater than threshold: {threshold:?}"
    )]
    MembershipThresholdMismatch { num_members: usize, threshold: Threshold },

    #[error(
        "invalid membership size: {0:?}: must be between 3 and 32 inclusive"
    )]
    InvalidMembershipSize(usize),

    #[error(
        "invalid threshold: {0:?}: threshold must be between 2 and 31 inclusive"
    )]
    InvalidThreshold(Threshold),

    #[error("This node is not an LRTQ member")]
    NotAnLrtqMember,

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

    #[error(
        "reconfiguration in progress at epoch {current_epoch:?}: cannot reconfigure for older epoch {msg_epoch:?}"
    )]
    ReconfigurationInProgress { current_epoch: Epoch, msg_epoch: Epoch },

    #[error("Mismatched reconfiguration requests for epoch {0:?}")]
    MismatchedReconfigurationForSameEpoch(Epoch),
}
