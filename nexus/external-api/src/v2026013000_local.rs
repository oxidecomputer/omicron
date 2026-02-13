// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026013000 that cannot live in `nexus-types-versions`
//! because they convert to/from `omicron-common` types (orphan rule).

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
            read_only: _, // read_only doth not exist in v2026013000
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
