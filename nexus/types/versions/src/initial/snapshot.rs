// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Snapshot types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    ByteCount, IdentityMetadata, IdentityMetadataCreateParams, NameOrId,
    ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// SELECTORS

#[derive(Deserialize, JsonSchema)]
pub struct SnapshotSelector {
    /// Name or ID of the project, only required if `snapshot` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the snapshot
    pub snapshot: NameOrId,
}

// VIEWS

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    Creating,
    Ready,
    Faulted,
    Destroyed,
}

/// View of a Snapshot
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Snapshot {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub project_id: Uuid,
    pub disk_id: Uuid,

    pub state: SnapshotState,

    pub size: ByteCount,
}

// PARAMS

/// Create-time parameters for a `Snapshot`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SnapshotCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The disk to be snapshotted
    pub disk: NameOrId,
}
