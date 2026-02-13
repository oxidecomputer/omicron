// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for version READ_ONLY_DISKS_NULLABLE.

use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025_11_20_00::disk::BlockSize;
use omicron_common::api::external::ByteCount;

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
        #[serde(default)]
        read_only: bool,
    },
    /// Create a disk from an image
    Image {
        image_id: Uuid,
        /// If `true`, the disk created from this image will be read-only.
        #[serde(default)]
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
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The source for this `Disk`'s blocks
    pub disk_backend: DiskBackend,
    /// Total size of the Disk in bytes
    pub size: ByteCount,
}

// -- Conversions from the previous version (READ_ONLY_DISKS) --

impl From<crate::v2026_01_30_01::disk::DiskSource> for DiskSource {
    fn from(old: crate::v2026_01_30_01::disk::DiskSource) -> Self {
        // The types are structurally identical; only `#[serde(default)]`
        // differs between versions.
        match old {
            crate::v2026_01_30_01::disk::DiskSource::Blank { block_size } => {
                Self::Blank { block_size }
            }
            crate::v2026_01_30_01::disk::DiskSource::Snapshot {
                snapshot_id,
                read_only,
            } => Self::Snapshot { snapshot_id, read_only },
            crate::v2026_01_30_01::disk::DiskSource::Image {
                image_id,
                read_only,
            } => Self::Image { image_id, read_only },
            crate::v2026_01_30_01::disk::DiskSource::ImportingBlocks {
                block_size,
            } => Self::ImportingBlocks { block_size },
        }
    }
}

impl From<crate::v2026_01_30_01::disk::DiskBackend> for DiskBackend {
    fn from(old: crate::v2026_01_30_01::disk::DiskBackend) -> Self {
        match old {
            crate::v2026_01_30_01::disk::DiskBackend::Local {} => {
                Self::Local {}
            }
            crate::v2026_01_30_01::disk::DiskBackend::Distributed {
                disk_source,
            } => Self::Distributed { disk_source: disk_source.into() },
        }
    }
}

impl From<crate::v2026_01_30_01::disk::DiskCreate> for DiskCreate {
    fn from(old: crate::v2026_01_30_01::disk::DiskCreate) -> Self {
        Self {
            identity: old.identity,
            disk_backend: old.disk_backend.into(),
            size: old.size,
        }
    }
}
