// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2025_11_20_00 that cannot live in `nexus-types-versions`
//! because they convert to/from `omicron-common` types (orphan rule).

use crate::v2026_01_30_00_local;
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiskType {
    Crucible,
}

impl From<DiskType> for external::DiskType {
    fn from(old: DiskType) -> external::DiskType {
        match old {
            DiskType::Crucible => external::DiskType::Distributed,
        }
    }
}

/// View of a Disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Disk {
    #[serde(flatten)]
    pub identity: external::IdentityMetadata,
    pub project_id: Uuid,
    /// ID of snapshot from which disk was created, if any
    pub snapshot_id: Option<Uuid>,
    /// ID of image from which disk was created, if any
    pub image_id: Option<Uuid>,
    pub size: external::ByteCount,
    pub block_size: external::ByteCount,
    pub state: external::DiskState,
    pub device_path: String,
    pub disk_type: DiskType,
}

impl From<Disk> for external::Disk {
    fn from(old: Disk) -> external::Disk {
        external::Disk {
            identity: old.identity,
            project_id: old.project_id,
            snapshot_id: old.snapshot_id,
            image_id: old.image_id,
            size: old.size,
            block_size: old.block_size,
            state: old.state,
            device_path: old.device_path,
            disk_type: old.disk_type.into(),
            read_only: false, // read_only defaults to false
        }
    }
}

impl TryFrom<external::Disk> for Disk {
    type Error = dropshot::HttpError;

    fn try_from(new: external::Disk) -> Result<Disk, Self::Error> {
        Ok(Disk {
            identity: new.identity,
            project_id: new.project_id,
            snapshot_id: new.snapshot_id,
            image_id: new.image_id,
            size: new.size,
            block_size: new.block_size,
            state: new.state,
            device_path: new.device_path,
            disk_type: match new.disk_type {
                external::DiskType::Distributed => DiskType::Crucible,

                _ => {
                    // Cannot display any other variant for this old client.
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

impl From<Disk> for v2026_01_30_00_local::Disk {
    fn from(old: Disk) -> v2026_01_30_00_local::Disk {
        v2026_01_30_00_local::Disk {
            identity: old.identity,
            project_id: old.project_id,
            snapshot_id: old.snapshot_id,
            image_id: old.image_id,
            size: old.size,
            block_size: old.block_size,
            state: old.state,
            device_path: old.device_path,
            disk_type: old.disk_type.into(),
        }
    }
}

impl TryFrom<v2026_01_30_00_local::Disk> for Disk {
    type Error = dropshot::HttpError;

    fn try_from(new: v2026_01_30_00_local::Disk) -> Result<Disk, Self::Error> {
        Ok(Disk {
            identity: new.identity,
            project_id: new.project_id,
            snapshot_id: new.snapshot_id,
            image_id: new.image_id,
            size: new.size,
            block_size: new.block_size,
            state: new.state,
            device_path: new.device_path,
            disk_type: match new.disk_type {
                external::DiskType::Distributed => DiskType::Crucible,

                _ => {
                    // Cannot display any other variant for this old client.
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
