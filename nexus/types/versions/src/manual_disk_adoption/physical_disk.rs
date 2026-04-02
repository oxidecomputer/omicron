// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Physical disk types for version MANUAL_DISK_ADOPTION.

use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::physical_disk::PhysicalDiskKind;

/// A physical disk that has not yet been adopted by the control plane
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct UninitializedPhysicalDisk {
    pub sled_id: SledUuid,
    pub slot: u64,
    pub variant: PhysicalDiskKind,
    pub disk_id: PhysicalDiskId,
}

/// The unique identity of a physical disk
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct PhysicalDiskId {
    pub vendor: String,
    pub serial: String,
    pub model: String,
}

impl From<omicron_common::disk::DiskIdentity> for PhysicalDiskId {
    fn from(value: omicron_common::disk::DiskIdentity) -> Self {
        Self { vendor: value.vendor, serial: value.serial, model: value.model }
    }
}

/// A request to adopt a physical disk into the control plane
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct PhysicalDiskAdoptionRequest {
    pub id: uuid::Uuid,
    pub vendor: String,
    pub serial: String,
    pub model: String,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
}
