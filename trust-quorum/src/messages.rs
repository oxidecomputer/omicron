// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messsages for the trust quorum protocol

use crate::{
    BaseboardId, Configuration, Epoch, RackId, Share, ShareDigest, Threshold,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// An API request to a node to start coordinating a reconfiguration
///
/// This is a message sent by Nexus in a real deployment
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Reconfigure {
    pub epoch: Epoch,
    pub last_committed_epoch: Epoch,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: Threshold,
}

/// Requests received from Nexus
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NexusReq {
    pub id: Uuid,
    pub kind: NexusReqKind,
}

/// Data for a message sent from Nexus and proxied via the sled-agent
///
/// As this is a "no io" implementation, appropriate ledger data is
/// read and loaded in by sled-agent.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NexusReqKind {
    /// Nexus seeds a few nodes with commits and then they get gossiped around
    Commit(CommitMsg),

    /// Get the bitmap of which members have seen a commit for a given epoch
    GetCommitted(Epoch),

    /// Retrieve the hash of a share for an LRTQ node
    ///
    /// This is necessary when coordinating upgrades
    GetLrtqShareHash,

    /// Inform a member to upgrade from LRTQ by creating a new PrepareMsg for
    /// epoch 0 and persisting it
    UpgradeFromLrtq(UpgradeFromLrtqMsg),

    /// If the upgrade has not yet been activated, then it can be cancelled
    /// and tried again
    CancelUpgradeFromLrtq(CancelUpgradeFromLrtqMsg),
}

/// Responses to Nexus Requests
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NexusRsp {
    pub request_id: Uuid,
    pub from: BaseboardId,
    pub kind: NexusRspKind,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NexusRspKind {
    CommitAck(Epoch),
    LrtqShareDigest(ShareDigest),
    UpgradeFromLrtqAck { upgrade_id: Uuid },
    CancelUpgradeFromLrtqAck { upgrade_id: Uuid },
    Committed(CommittedMsg),
    Error(NexusRspError),
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
pub enum NexusRspError {
    #[error("sled was decommissioned on msg from {from:?} at epoch {epoch:?}: last prepared epoch = {last_prepared_epoch:?}")]
    SledDecommissioned {
        from: BaseboardId,
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
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PrepareMsg {
    pub config: Configuration,
    pub share: Share,
}

/// A message that is sent between peers until all healthy peers have seen it
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommittedMsg {
    pub epoch: Epoch,

    /// A bitmap of which nodes have so far committed the configuration
    /// for `epoch`. This order, and number, of bits matches that of the
    /// configuration for `epoch`. Members fill in their own
    /// bit after they have committed.
    pub committed_bitmap: u32,
}

/// Data loaded from the ledger by sled-agent on instruction from Nexus
///
/// The epoch is always 0, because LRTQ does not allow key-rotation
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LrtqLedgerData {
    pub rack_uuid: RackId,
    pub threshold: Threshold,
    pub share: Share,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UpgradeFromLrtqMsg {
    pub upgrade_id: Uuid,
    pub members: BTreeMap<BaseboardId, ShareDigest>,
    pub lrtq_ledger_data: LrtqLedgerData,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CancelUpgradeFromLrtqMsg {
    pub upgrade_id: Uuid,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Msg {
    Prepare(PrepareMsg),
    Commit(CommitMsg),
    Committed(CommittedMsg),

    GetShare(Epoch),
    Share { epoch: Epoch, share: Share },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitMsg {
    epoch: Epoch,
}
