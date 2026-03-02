// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for version INSTANCES_EXTERNAL_SUBNETS.
//!
//! The `Disk` view type for this version range does not include a `read_only`
//! field and uses the new `DiskType` from `omicron_common` (no old `Crucible`
//! variant).

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    ByteCount, DiskState, DiskType, IdentityMetadata, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of a Disk.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Disk {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    /// ID of snapshot from which disk was created, if any.
    pub snapshot_id: Option<Uuid>,
    /// ID of image from which disk was created, if any.
    pub image_id: Option<Uuid>,
    pub size: ByteCount,
    pub block_size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
    pub disk_type: DiskType,
}

// -- View type conversions --

impl From<omicron_common::api::external::Disk> for Disk {
    fn from(new: omicron_common::api::external::Disk) -> Self {
        let omicron_common::api::external::Disk {
            identity,
            project_id,
            snapshot_id,
            image_id,
            size,
            block_size,
            state,
            device_path,
            disk_type,
            read_only: _, // read_only does not exist in this version.
        } = new;
        Self {
            identity,
            project_id,
            snapshot_id,
            image_id,
            size,
            block_size,
            state,
            device_path,
            disk_type,
        }
    }
}

impl TryFrom<crate::v2025_11_20_00::disk::Disk> for Disk {
    type Error = dropshot::HttpError;

    fn try_from(
        old: crate::v2025_11_20_00::disk::Disk,
    ) -> Result<Self, Self::Error> {
        Ok(Disk {
            identity: old.identity,
            project_id: old.project_id,
            snapshot_id: old.snapshot_id,
            image_id: old.image_id,
            size: old.size,
            block_size: old.block_size,
            state: old.state,
            device_path: old.device_path,
            disk_type: match old.disk_type {
                crate::v2025_11_20_00::disk::DiskType::Crucible => {
                    DiskType::Distributed
                }
            },
        })
    }
}
