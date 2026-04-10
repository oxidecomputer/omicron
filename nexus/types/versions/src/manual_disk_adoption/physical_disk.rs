// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Physical disk types for version MANUAL_DISK_ADOPTION.

use omicron_uuid_kinds::{PhysicalDiskAdoptionRequestUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::physical_disk::PhysicalDiskKind;

/// A physical disk that has not yet been adopted by the control plane
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Unadopted {
    pub sled_id: SledUuid,
    pub slot: i64,
    pub variant: PhysicalDiskKind,
    pub disk_id: PhysicalDiskManufacturerIdentity,
}

/// The unique identity of a physical disk provided by the manufacturer
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct PhysicalDiskManufacturerIdentity {
    pub vendor: String,
    pub serial: String,
    pub model: String,
}

impl From<omicron_common::disk::DiskIdentity>
    for PhysicalDiskManufacturerIdentity
{
    fn from(value: omicron_common::disk::DiskIdentity) -> Self {
        Self { vendor: value.vendor, serial: value.serial, model: value.model }
    }
}

/// A request to adopt a physical disk into the control plane
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct PhysicalDiskAdoptionRequest {
    pub id: PhysicalDiskAdoptionRequestUuid,
    pub vendor: String,
    pub serial: String,
    pub model: String,
    pub time_created: chrono::DateTime<chrono::Utc>,
}
