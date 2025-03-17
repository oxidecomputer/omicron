// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messsages for the trust quorum protocol

use crate::{Configuration, Epoch, PlatformId, RackId, Share, Threshold};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

/// A request from nexus informing a node to start coordinating a
/// reconfiguration
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Reconfigure {
    pub rack_id: RackId,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<PlatformId>,
    pub threshold: Threshold,

    // The timeout before we send a follow up request to a peer
    pub retry_timeout: Duration,
}

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
    #[error("sled was decommissioned on msg from {from:?} at epoch {epoch:?}: last prepared epoch = {last_prepared_epoch:?}")]
    SledDecommissioned {
        from: PlatformId,
        epoch: Epoch,
        last_prepared_epoch: Option<Epoch>,
    },

    #[error("sled has already committed a request at epoch {0:?}")]
    AlreadyCommitted(Epoch),

    #[error("sled has already prepared a request at epoch {0:?}")]
    AlreadyPrepared(Epoch),

    #[error("number of members: {num_members:?} must be greater than threshold: {threshold:?}")]
    MembershipThresholdMismatch { num_members: usize, threshold: Threshold },

    #[error(
        "invalid membership size: {0:?}: must be between 3 and 32 inclusive"
    )]
    InvalidMembershipSize(usize),

    #[error("invalid threshold: {0:?}: threshold must be between 2 and 31 inclusive")]
    InvalidThreshold(Threshold),

    #[error("This node is not an LRTQ member")]
    NotAnLrtqMember,

    #[error("Node has last committed epoch of {node_epoch:?}, message contains {msg_epoch:?}")]
    LastCommittedEpochMismatch {
        node_epoch: Option<Epoch>,
        msg_epoch: Option<Epoch>,
    },

    #[error("sled has already prepared a request at epoch {existing:?}, and cannot prepare another at a smaller or equivalent epoch {new:?}")]
    PreparedEpochMismatch { existing: Epoch, new: Epoch },
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PrepareMsg {
    pub config: Configuration,
    pub share: Share,
}

/// A message that is sent between peers until all healthy peers have seen it
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CommittedMsg {
    pub epoch: Epoch,

    /// A bitmap of which nodes have so far committed the configuration
    /// for `epoch`. This order, and number, of bits matches that of the
    /// configuration for `epoch`. Members fill in their own
    /// bit after they have committed.
    pub committed_bitmap: u32,
}

/// Messages sent between trust quorum members over a sprockets channel
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum PeerMsg {
    Prepare(PrepareMsg),
    PrepareAck(Epoch),
    Commit(CommitMsg),
    Committed(CommittedMsg),

    GetShare(Epoch),
    Share { epoch: Epoch, share: Share },
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CommitMsg {
    epoch: Epoch,
}
