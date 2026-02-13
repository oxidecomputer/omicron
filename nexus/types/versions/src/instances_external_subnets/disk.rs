// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for version INSTANCES_EXTERNAL_SUBNETS.
//!
//! At this version, `DiskSource` is the old one from `v2025112000` (no
//! `read_only` field). The `Disk` view uses the new `DiskType` from
//! `omicron_common` but does not have a `read_only` field.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    ByteCount, DiskState, DiskType, IdentityMetadata,
    IdentityMetadataCreateParams, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The source of a `Disk`'s blocks
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskBackend {
    Local {},

    Distributed {
        /// The initial source for this disk
        disk_source: crate::v2025112000::disk::DiskSource,
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
}

// -- Conversions from the previous version (LOCAL_STORAGE) --

impl From<crate::v2025120300::disk::DiskCreate> for DiskCreate {
    fn from(old: crate::v2025120300::disk::DiskCreate) -> Self {
        DiskCreate {
            identity: old.identity,
            disk_backend: old.disk_backend.into(),
            size: old.size,
        }
    }
}

impl From<crate::v2025120300::disk::DiskBackend> for DiskBackend {
    fn from(old: crate::v2025120300::disk::DiskBackend) -> Self {
        match old {
            crate::v2025120300::disk::DiskBackend::Local {} => {
                DiskBackend::Local {}
            }
            crate::v2025120300::disk::DiskBackend::Distributed {
                disk_source,
            } => {
                // v2025120300::DiskSource and v2025112000::DiskSource have the
                // same shape (no read_only). Convert field by field.
                let disk_source = match disk_source {
                    crate::v2025120300::disk::DiskSource::Blank {
                        block_size,
                    } => crate::v2025112000::disk::DiskSource::Blank {
                        block_size,
                    },
                    crate::v2025120300::disk::DiskSource::Snapshot {
                        snapshot_id,
                    } => crate::v2025112000::disk::DiskSource::Snapshot {
                        snapshot_id,
                    },
                    crate::v2025120300::disk::DiskSource::Image {
                        image_id,
                    } => {
                        crate::v2025112000::disk::DiskSource::Image { image_id }
                    }
                    crate::v2025120300::disk::DiskSource::ImportingBlocks {
                        block_size,
                    } => {
                        crate::v2025112000::disk::DiskSource::ImportingBlocks {
                            block_size,
                        }
                    }
                };
                DiskBackend::Distributed { disk_source }
            }
        }
    }
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
            read_only: _, // read_only does not exist in v2026013000.
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

impl TryFrom<crate::v2025112000::disk::Disk> for Disk {
    type Error = dropshot::HttpError;

    fn try_from(
        old: crate::v2025112000::disk::Disk,
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
                crate::v2025112000::disk::DiskType::Crucible => {
                    DiskType::Distributed
                }
            },
        })
    }
}
