// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for version READ_ONLY_DISKS.
//!
//! These are identical to the READ_ONLY_DISKS_NULLABLE types except that
//! `read_only` on `DiskSource::Snapshot` and `DiskSource::Image` is required
//! (no `#[serde(default)]`).

use omicron_common::api::external::{ByteCount, IdentityMetadataCreateParams};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025_11_20_00::disk::BlockSize;

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
    Snapshot {
        snapshot_id: Uuid,
        /// If `true`, the disk created from this snapshot will be read-only.
        read_only: bool,
    },
    /// Create a disk from an image
    Image {
        image_id: Uuid,
        /// If `true`, the disk created from this image will be read-only.
        read_only: bool,
    },
    /// Create a blank disk that will accept bulk writes or pull blocks from an
    /// external source.
    ImportingBlocks { block_size: BlockSize },
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
    /// Total size of the Disk in bytes
    pub size: ByteCount,
}

// -- Conversions from the previous version (LOCAL_STORAGE) --
// The prior version's DiskCreate uses v2025_12_03_00 disk types (no read_only).
// We add read_only=false as the default.

impl From<crate::v2025_12_03_00::disk::DiskBackend> for DiskBackend {
    fn from(old: crate::v2025_12_03_00::disk::DiskBackend) -> Self {
        match old {
            crate::v2025_12_03_00::disk::DiskBackend::Local {} => {
                Self::Local {}
            }
            crate::v2025_12_03_00::disk::DiskBackend::Distributed {
                disk_source,
            } => {
                // Convert old DiskSource (no read_only) to new DiskSource
                // (with read_only), defaulting to false.
                let disk_source = match disk_source {
                    crate::v2025_12_03_00::disk::DiskSource::Blank {
                        block_size,
                    } => DiskSource::Blank { block_size },
                    crate::v2025_12_03_00::disk::DiskSource::Snapshot {
                        snapshot_id,
                    } => DiskSource::Snapshot { snapshot_id, read_only: false },
                    crate::v2025_12_03_00::disk::DiskSource::Image {
                        image_id,
                    } => DiskSource::Image { image_id, read_only: false },
                    crate::v2025_12_03_00::disk::DiskSource::ImportingBlocks {
                        block_size,
                    } => DiskSource::ImportingBlocks { block_size },
                };
                Self::Distributed { disk_source }
            }
        }
    }
}

impl From<crate::v2025_12_03_00::disk::DiskCreate> for DiskCreate {
    fn from(old: crate::v2025_12_03_00::disk::DiskCreate) -> Self {
        Self {
            identity: old.identity,
            disk_backend: old.disk_backend.into(),
            size: old.size,
        }
    }
}
