// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum types for the Sled Agent API.

use std::collections::{BTreeMap, BTreeSet};

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};

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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumConfiguration {
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// Unique, monotonically increasing identifier for a configuration.
    pub epoch: u64,
    /// The coordinator of this reconfiguration.
    pub coordinator: BaseboardId,
    /// All members of the configuration and the SHA3-256 hash of their key shares.
    #[serde_as(as = "BTreeMap<_, Hex>")]
    #[schemars(with = "BTreeMap<BaseboardId, String>")]
    pub members: BTreeMap<BaseboardId, [u8; 32]>,
    /// The number of sleds required to reconstruct the rack secret.
    pub threshold: u8,
}

/// Request to prepare and commit a trust quorum configuration.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumPrepareAndCommitRequest {
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// Unique, monotonically increasing identifier for a configuration.
    pub epoch: u64,
    /// The coordinator of this reconfiguration.
    pub coordinator: BaseboardId,
    /// All members of the configuration and the SHA3-256 hash of their key shares.
    #[serde_as(as = "BTreeMap<_, Hex>")]
    #[schemars(with = "BTreeMap<BaseboardId, String>")]
    pub members: BTreeMap<BaseboardId, [u8; 32]>,
    /// The number of sleds required to reconstruct the rack secret.
    pub threshold: u8,
    /// Encrypted rack secrets from prior configurations, if any.
    pub encrypted_rack_secrets: Option<TrustQuorumEncryptedRackSecrets>,
}

/// Encrypted rack secrets for prior configurations.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumEncryptedRackSecrets {
    /// 32-byte salt used to derive the encryption key.
    #[serde_as(as = "Hex")]
    #[schemars(with = "String")]
    pub salt: [u8; 32],
    /// Encrypted data.
    #[serde_as(as = "Hex")]
    #[schemars(with = "String")]
    pub data: Vec<u8>,
}

/// Request to proxy a commit operation to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumProxyCommitRequest {
    /// The target node to proxy the request to.
    pub destination: BaseboardId,
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// The epoch to commit.
    pub epoch: u64,
}

/// Request to proxy a prepare-and-commit operation to another trust quorum node.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumProxyPrepareAndCommitRequest {
    /// The target node to proxy the request to.
    pub destination: BaseboardId,
    /// Unique ID of the rack.
    pub rack_id: RackUuid,
    /// Unique, monotonically increasing identifier for a configuration.
    pub epoch: u64,
    /// The coordinator of this reconfiguration.
    pub coordinator: BaseboardId,
    /// All members of the configuration and the SHA3-256 hash of their key shares.
    #[serde_as(as = "BTreeMap<_, Hex>")]
    #[schemars(with = "BTreeMap<BaseboardId, String>")]
    pub members: BTreeMap<BaseboardId, [u8; 32]>,
    /// The number of sleds required to reconstruct the rack secret.
    pub threshold: u8,
    /// Encrypted rack secrets from prior configurations, if any.
    pub encrypted_rack_secrets: Option<TrustQuorumEncryptedRackSecrets>,
}

/// Request to proxy a status request to another trust quorum node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumProxyStatusRequest {
    /// The target node to get the status from.
    pub destination: BaseboardId,
}

/// Status of a trust quorum node, returned from a proxied status request.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumNodeStatus {
    /// The peers this node is connected to.
    pub connected_peers: BTreeSet<BaseboardId>,
    /// Any alarms raised by this node.
    pub alarms: Vec<TrustQuorumAlarm>,
    /// Summary of the node's persistent state.
    pub persistent_state: TrustQuorumPersistentStateSummary,
    /// Number of proxied requests currently in flight.
    pub proxied_requests: u64,
}

/// An alarm indicating a protocol invariant violation in trust quorum.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrustQuorumAlarm {
    /// Different configurations found for the same epoch.
    MismatchedConfigurations {
        /// The first configuration.
        config1: TrustQuorumConfiguration,
        /// The second (mismatched) configuration.
        config2: TrustQuorumConfiguration,
        /// The source of the mismatch (either a baseboard ID or "Nexus").
        from: String,
    },

    /// The key share computer could not compute this node's share.
    ShareComputationFailed {
        /// The epoch for which share computation failed.
        epoch: u64,
        /// The error message.
        error: String,
    },

    /// We started collecting shares for a committed configuration,
    /// but we no longer have that configuration in our persistent state.
    CommittedConfigurationLost {
        /// The latest committed epoch.
        latest_committed_epoch: u64,
        /// The epoch we were collecting shares for.
        collecting_epoch: u64,
    },

    /// Decrypting the encrypted rack secrets failed when presented with a
    /// valid rack secret.
    RackSecretDecryptionFailed {
        /// The epoch for which decryption failed.
        epoch: u64,
        /// The error message.
        error: String,
    },

    /// Reconstructing the rack secret failed when presented with valid shares.
    RackSecretReconstructionFailed {
        /// The epoch for which reconstruction failed.
        epoch: u64,
        /// The error message.
        error: String,
    },
}

/// Summary of a trust quorum node's persistent state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrustQuorumPersistentStateSummary {
    /// Whether the node has an LRTQ (legacy) share.
    pub has_lrtq_share: bool,
    /// Epochs for which configurations have been prepared.
    pub configs: BTreeSet<u64>,
    /// Epochs for which key shares exist.
    pub shares: BTreeSet<u64>,
    /// Epochs that have been committed.
    pub commits: BTreeSet<u64>,
    /// Whether this node has been expunged from the quorum.
    pub expunged: bool,
}
