// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! All messages sent and received by bootstore nodes and coordinators

use derive_more::From;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vsss_rs::Share;

use crate::trust_quorum::SerializableShareDistribution;

/// A request sent to a [`Node`] from another [`Node`] or a [`Coordinator`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRequest {
    pub version: u32,
    /// A message correlation id to match requests to responses
    pub id: u64,
    pub op: NodeOp,
}

/// A specific operation for a Node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeOp {
    /// Retrieve a key share for the given epoch
    ///
    /// A [`Node`] will only respond if the epoch is still valid
    /// and the sending [`Node`] is a member of the trust quorum.
    GetShare { epoch: i32 },

    /// A request sent by RSS with the trust quorum membership and key share
    /// for epoch 0.
    ///
    /// Epoch 0 is the only epoch in which prepared data can be overwritten.
    /// This is because there is no global datastore (CockroachDB) with which
    /// to persist information that needs to be distributed to all sleds in the
    /// rack. If RSS dies, or more importantly, the scrimlet it is running on
    /// dies, we want to enable re-running on a different scrimlet. Since we
    /// don't know what information was already transferred to sleds, due to
    /// lack of global datastore, we must re-issue the request.
    ///
    /// This request generates a `KeySharePrepare` for epoch 0. Once all sleds
    /// have prepared, RSS trigger the start of CockroachDB replicas. The trust
    /// quorum membership and prepare status will be written into CockroachDB
    /// as the epoch 0 trust quorum configuration. Nexus will then proceed to
    /// commit the trust quorum information, by first writing the Commit to
    /// CockroachDb and then sending a `KeyShareCommit` for epoch 0.
    ///
    /// TODO: The rack plan should also be sent here with similar storage
    /// strategy as the key share/trust quorum membership.
    Initialize {
        rack_uuid: Uuid,
        share_distribution: SerializableShareDistribution,
    },

    /// A request from a [`Coordinator`] for the Prepare phase
    /// of a rekey or reconfiguration
    KeySharePrepare {
        rack_uuid: Uuid,
        epoch: i32,
        share_distribution: SerializableShareDistribution,
    },

    /// A request from a [`Coordinator`] for the Commit phase of a
    /// rekey or reconfiguration
    KeyShareCommit { rack_uuid: Uuid, epoch: i32 },
}

/// A response from a  [`Node`] to another [`Node`] or a [`Coordinator`]
#[derive(Debug, Clone, PartialEq, From, Serialize, Deserialize)]
pub struct NodeResponse {
    pub version: u32,
    /// A message correlation id to match requests to responses
    pub id: u64,
    pub op: NodeOpResult,
}

#[derive(Debug, Clone, PartialEq, From, Serialize, Deserialize)]
// The result of an operation from a [`Node`]
pub enum NodeOpResult {
    /// A key share for a given epoch as requested by [`PeerRequest::GetShare`]
    Share { epoch: i32, share: Share },

    /// An ack for the most recent coordinator message
    CoordinatorAck,

    /// Error responses
    Error(NodeError),
}

/// Errors returned inside a [`NodeOpResult`]
#[derive(
    Debug, Clone, PartialEq, From, Serialize, Deserialize, thiserror::Error,
)]
pub enum NodeError {
    #[error("Version {0} messages are unsupported.")]
    UnsupportedVersion(u32),

    #[error("Key share for epoch {epoch} does not exist.")]
    KeyShareDoesNotExist { epoch: i32 },

    #[error(
        "Received unexpected rack UUID. Expected: {expected}, Actual: {actual}"
    )]
    RackUuidMismatch { expected: Uuid, actual: Uuid },

    #[error("A commit has already occurred for rack {rack_uuid}")]
    AlreadyInitialized { rack_uuid: Uuid },

    #[error(
        "No corresponding key share prepare for this commit: rack UUID:
{rack_uuid}, epoch: {epoch}"
    )]
    MissingKeySharePrepare { rack_uuid: Uuid, epoch: i32 },
}
