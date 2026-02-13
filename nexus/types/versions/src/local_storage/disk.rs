// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for version LOCAL_STORAGE.

use omicron_common::api::external::{ByteCount, IdentityMetadataCreateParams};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025112000;
use crate::v2025112000::disk::BlockSize;

/// Different sources for a Distributed Disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskSource {
    /// Create a blank disk
    Blank {
        /// size of blocks for this Disk. valid values are: 512, 2048, or 4096
        block_size: BlockSize,
    },

    /// Create a disk from a disk snapshot
    Snapshot { snapshot_id: Uuid },

    /// Create a disk from an image
    Image { image_id: Uuid },

    /// Create a blank disk that will accept bulk writes or pull blocks from an
    /// external source.
    ImportingBlocks { block_size: BlockSize },
}

impl From<v2025112000::disk::DiskSource> for DiskSource {
    fn from(old: v2025112000::disk::DiskSource) -> Self {
        match old {
            v2025112000::disk::DiskSource::Blank { block_size } => {
                DiskSource::Blank { block_size }
            }
            v2025112000::disk::DiskSource::Snapshot { snapshot_id } => {
                DiskSource::Snapshot { snapshot_id }
            }
            v2025112000::disk::DiskSource::Image { image_id } => {
                DiskSource::Image { image_id }
            }
            v2025112000::disk::DiskSource::ImportingBlocks { block_size } => {
                DiskSource::ImportingBlocks { block_size }
            }
        }
    }
}

/// The source of a `Disk`'s blocks
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskBackend {
    Local {},

    Distributed {
        /// The initial source for this disk
        disk_source: DiskSource,
    },
}

/// Create-time parameters for a `Disk`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskCreate {
    /// The common identifying metadata for the disk
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The source for this `Disk`'s blocks
    pub disk_backend: DiskBackend,

    /// The total size of the Disk (in bytes)
    pub size: ByteCount,
}

impl From<v2025112000::disk::DiskCreate> for DiskCreate {
    fn from(old: v2025112000::disk::DiskCreate) -> Self {
        DiskCreate {
            identity: old.identity,
            disk_backend: DiskBackend::Distributed {
                disk_source: old.disk_source.into(),
            },
            size: old.size,
        }
    }
}

// Response type conversion: new Disk (from omicron-common) to old v2025112000 Disk
impl TryFrom<omicron_common::api::external::Disk> for v2025112000::disk::Disk {
    type Error = dropshot::HttpError;

    fn try_from(
        new: omicron_common::api::external::Disk,
    ) -> Result<Self, Self::Error> {
        Ok(v2025112000::disk::Disk {
            identity: new.identity,
            project_id: new.project_id,
            snapshot_id: new.snapshot_id,
            image_id: new.image_id,
            size: new.size,
            block_size: new.block_size,
            state: new.state,
            device_path: new.device_path,
            disk_type: match new.disk_type {
                omicron_common::api::external::DiskType::Distributed => {
                    v2025112000::disk::DiskType::Crucible
                }
                _ => {
                    return Err(dropshot::HttpError::for_client_error(
                        Some(String::from("Not Acceptable")),
                        dropshot::ClientErrorStatusCode::NOT_ACCEPTABLE,
                        String::from(
                            "disk type variant not supported for client version",
                        ),
                    ));
                }
            },
        })
    }
}

// Forward conversion for request types: old DiskType to new DiskType
impl From<v2025112000::disk::DiskType>
    for omicron_common::api::external::DiskType
{
    fn from(old: v2025112000::disk::DiskType) -> Self {
        match old {
            v2025112000::disk::DiskType::Crucible => {
                omicron_common::api::external::DiskType::Distributed
            }
        }
    }
}

// Forward conversion: old Disk to new Disk
impl From<v2025112000::disk::Disk> for omicron_common::api::external::Disk {
    fn from(old: v2025112000::disk::Disk) -> Self {
        omicron_common::api::external::Disk {
            identity: old.identity,
            project_id: old.project_id,
            snapshot_id: old.snapshot_id,
            image_id: old.image_id,
            size: old.size,
            block_size: old.block_size,
            state: old.state,
            device_path: old.device_path,
            disk_type: old.disk_type.into(),
            // Old Disk type predates read-only disks; default to false.
            read_only: false,
        }
    }
}
