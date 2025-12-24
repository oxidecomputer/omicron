// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum types for the Sled Agent API.
//!
//! Core types are re-exported from `trust-quorum-types-versions` to ensure
//! consistency with the trust quorum protocol implementation.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::super::v1::sled::BaseboardId;

// Re-export core types from trust-quorum-types-versions.
// These are the canonical type definitions used by the trust quorum protocol.
pub use trust_quorum_types_versions::v1::alarm::Alarm;
pub use trust_quorum_types_versions::v1::configuration::Configuration;
pub use trust_quorum_types_versions::v1::crypto::EncryptedRackSecrets;
pub use trust_quorum_types_versions::v1::messages::{
    CommitRequest, LrtqUpgradeMsg, PrepareAndCommitRequest, ReconfigureMsg,
};
pub use trust_quorum_types_versions::v1::persistent_state::ExpungedMetadata;
pub use trust_quorum_types_versions::v1::status::{
    CommitStatus, CoordinatorStatus, NodePersistentStateSummary, NodeStatus,
};
pub use trust_quorum_types_versions::v1::types::{Epoch, Threshold};

/// Request to proxy a commit operation to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProxyCommitRequest {
    /// The target node to proxy the request to.
    pub destination: BaseboardId,
    /// The commit request to proxy.
    pub request: CommitRequest,
}

/// Request to proxy a prepare-and-commit operation to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProxyPrepareAndCommitRequest {
    /// The target node to proxy the request to.
    pub destination: BaseboardId,
    /// The prepare-and-commit request to proxy.
    pub request: PrepareAndCommitRequest,
}
