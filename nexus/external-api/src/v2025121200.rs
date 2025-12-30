// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025121200 to 2025121800

use omicron_common::api::external;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiskType {
    Distributed,
    Local,
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

impl From<external::Disk> for Disk {
    fn from(new: external::Disk) -> Disk {
        let (snapshot_id, image_id, disk_type) = match new.disk_type {
            external::DiskType::Distributed { snapshot_id, image_id } => {
                (snapshot_id, image_id, DiskType::Distributed)
            }

            external::DiskType::Local { .. } => (None, None, DiskType::Local),
        };

        let device_path = format!("/mnt/{}", new.identity.name.as_str());

        Disk {
            identity: new.identity,
            project_id: new.project_id,
            snapshot_id,
            image_id,
            size: new.size,
            block_size: new.block_size,
            state: new.state,
            device_path,
            disk_type,
        }
    }
}
