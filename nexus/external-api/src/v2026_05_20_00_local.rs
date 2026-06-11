// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026_05_20_00 that cannot live in `nexus-types-versions`
//! because they convert to/from `omicron-common` types (orphan rule).
//!
//! This version pre-dates `DISK_BLOCK_SIZE_TYPE`, which changed the `Disk`
//! response field `block_size` from `ByteCount` to the constrained `BlockSize`
//! newtype. Older API versions in the range
//! `VERSION_READ_ONLY_DISKS_NULLABLE..VERSION_DISK_BLOCK_SIZE_TYPE` still
//! report `block_size` as a plain `ByteCount`.

use api_identity::ObjectIdentity;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::DiskType;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::ObjectIdentity;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// View of a Disk
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Disk {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    /// ID of snapshot from which disk was created, if any
    pub snapshot_id: Option<Uuid>,
    /// ID of image from which disk was created, if any
    pub image_id: Option<Uuid>,
    pub size: ByteCount,
    pub block_size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
    pub disk_type: DiskType,
    /// Whether or not this disk is read-only.
    pub read_only: bool,
}

impl From<external::Disk> for Disk {
    fn from(new: external::Disk) -> Self {
        let external::Disk {
            identity,
            project_id,
            snapshot_id,
            image_id,
            size,
            block_size,
            state,
            device_path,
            disk_type,
            read_only,
        } = new;
        Self {
            identity,
            project_id,
            snapshot_id,
            image_id,
            size,
            block_size: block_size.into(),
            state,
            device_path,
            disk_type,
            read_only,
        }
    }
}
