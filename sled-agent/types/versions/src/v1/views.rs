// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Response types for Sled Agent API v1.
//!
//! This module contains response-only types used by the Sled Agent API.
//!
//! Per RFD 619, high-level response types (views) are defined in the earliest
//! version they appear in. These types are used directly by the API crate with
//! fixed identifiers.

use std::collections::BTreeMap;

use omicron_common::api::external::Generation;
use omicron_common::disk::DiskVariant;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

/// Response for listing artifacts.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactListResponse {
    pub generation: Generation,
    pub list: BTreeMap<ArtifactHash, usize>,
}

/// Response for copying artifacts from a depot.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactCopyFromDepotResponse {}

/// Response for putting an artifact.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactPutResponse {
    /// The number of valid M.2 artifact datasets we found on the sled. There is
    /// typically one of these datasets for each functional M.2.
    pub datasets: usize,

    /// The number of valid writes to the M.2 artifact datasets. This should be
    /// less than or equal to the number of artifact datasets.
    pub successful_writes: usize,
}

/// Response for VMM disk snapshot requests.
#[derive(Serialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestResponse {
    pub snapshot_id: Uuid,
}

/// Information about a zpool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Zpool {
    pub id: ZpoolUuid,
    pub disk_type: DiskType,
}

/// Disk type classification.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum DiskType {
    U2,
    M2,
}

impl From<DiskVariant> for DiskType {
    fn from(v: DiskVariant) -> Self {
        match v {
            DiskVariant::U2 => Self::U2,
            DiskVariant::M2 => Self::M2,
        }
    }
}
