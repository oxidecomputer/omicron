// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum types for the Sled Agent API.
//!
//! Core types are re-exported from `trust-quorum-types-versions` to ensure
//! consistency with the trust quorum protocol implementation.

use std::collections::BTreeSet;

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::super::v1::sled::BaseboardId;

// Re-export core types from trust-quorum-types-versions.
// These are the canonical type definitions used by the trust quorum protocol.
pub use trust_quorum_types_versions::v1::alarm::Alarm;
pub use trust_quorum_types_versions::v1::configuration::Configuration;
pub use trust_quorum_types_versions::v1::crypto::EncryptedRackSecrets;
pub use trust_quorum_types_versions::v1::persistent_state::ExpungedMetadata;
pub use trust_quorum_types_versions::v1::status::{
    CommitStatus, CoordinatorStatus, NodePersistentStateSummary, NodeStatus,
};
pub use trust_quorum_types_versions::v1::types::{Epoch, Threshold};

/// Reconfigure message for trust quorum changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumReconfigureRequest {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: Threshold,
}

/// Request to upgrade from LRTQ (Legacy Rack Trust Quorum).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumLrtqUpgradeRequest {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: Threshold,
}

/// Request to commit a trust quorum configuration at a given epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumCommitRequest {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
}

/// Request to prepare and commit a trust quorum configuration.
///
/// This is the `Configuration` sent to a node that missed the `Prepare` phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumPrepareAndCommitRequest {
    pub config: Configuration,
}

/// Request to proxy a commit operation to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumProxyCommitRequest {
    /// The target node to proxy the request to.
    pub destination: BaseboardId,
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// The epoch to commit.
    pub epoch: Epoch,
}

/// Request to proxy a prepare-and-commit operation to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumProxyPrepareAndCommitRequest {
    /// The target node to proxy the request to.
    pub destination: BaseboardId,
    /// The configuration to prepare and commit.
    pub config: Configuration,
}

/// Request to proxy a status request to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumProxyStatusRequest {
    /// The target node to get the status from.
    pub destination: BaseboardId,
}
