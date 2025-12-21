// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum types for the Sled Agent API.

use std::collections::{BTreeMap, BTreeSet};

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::super::v1::sled::BaseboardId;

/// Reconfigure message for trust quorum changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumReconfigureRequest {
    pub rack_id: RackUuid,
    pub epoch: u64,
    pub last_committed_epoch: Option<u64>,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: u8,
}

/// Request to upgrade from LRTQ (Legacy Rack Trust Quorum).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumLrtqUpgradeRequest {
    pub rack_id: RackUuid,
    pub epoch: u64,
    pub members: BTreeSet<BaseboardId>,
    pub threshold: u8,
}

/// Request to commit a trust quorum configuration at a given epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumCommitRequest {
    pub rack_id: RackUuid,
    pub epoch: u64,
}

/// Response indicating the commit status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrustQuorumCommitResponse {
    /// The configuration has been committed.
    Committed,
    /// The commit is still pending.
    Pending,
}

/// Status of a node coordinating a trust quorum reconfiguration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumCoordinatorStatus {
    /// The configuration being prepared.
    pub config: TrustQuorumConfiguration,
    /// The set of nodes that have acknowledged the prepare.
    pub acked_prepares: BTreeSet<BaseboardId>,
}

/// A trust quorum configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumConfiguration {
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// Unique, monotonically increasing identifier for a configuration.
    pub epoch: u64,
    /// The coordinator of this reconfiguration.
    pub coordinator: BaseboardId,
    /// All members of the configuration and the hex-encoded SHA3-256 hash of
    /// their key shares.
    pub members: BTreeMap<BaseboardId, String>,
    /// The number of sleds required to reconstruct the rack secret.
    pub threshold: u8,
}

/// Request to prepare and commit a trust quorum configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumPrepareAndCommitRequest {
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// Unique, monotonically increasing identifier for a configuration.
    pub epoch: u64,
    /// The coordinator of this reconfiguration.
    pub coordinator: BaseboardId,
    /// All members of the configuration and the hex-encoded SHA3-256 hash of
    /// their key shares.
    pub members: BTreeMap<BaseboardId, String>,
    /// The number of sleds required to reconstruct the rack secret.
    pub threshold: u8,
    /// Encrypted rack secrets from prior configurations, if any.
    pub encrypted_rack_secrets: Option<TrustQuorumEncryptedRackSecrets>,
}

/// Encrypted rack secrets for prior configurations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumEncryptedRackSecrets {
    /// Hex-encoded 32-byte salt used to derive the encryption key.
    pub salt: String,
    /// Hex-encoded encrypted data.
    pub data: String,
}
