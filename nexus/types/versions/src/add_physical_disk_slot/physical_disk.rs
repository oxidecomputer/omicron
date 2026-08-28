// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::asset::AssetIdentityMetadata;
use crate::v2025_11_20_00::physical_disk::{
    PhysicalDiskKind, PhysicalDiskPolicy, PhysicalDiskState,
};
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of a Physical Disk
///
/// Physical disks reside in a particular sled and are used to store both
/// Instance Disk data as well as internal metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct PhysicalDisk {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,

    /// The operator-defined policy for a physical disk.
    pub policy: PhysicalDiskPolicy,
    /// The current state Nexus believes the disk to be in.
    pub state: PhysicalDiskState,

    /// The sled to which this disk is attached, if any.
    #[schemars(with = "Option<Uuid>")]
    pub sled_id: Option<SledUuid>,
    /// The physical slot in the sled where this disk was last observed to be
    /// located, or null if its location is not known at this time.
    pub slot: Option<i64>,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub form_factor: PhysicalDiskKind,
}

impl From<PhysicalDisk> for v2025_11_20_00::physical_disk::PhysicalDisk {
    fn from(new: PhysicalDisk) -> Self {
        let PhysicalDisk {
            identity,
            policy,
            state,
            sled_id,
            slot: _,
            vendor,
            serial,
            model,
            form_factor,
        } = new;
        Self {
            identity,
            policy,
            state,
            sled_id,
            vendor,
            serial,
            model,
            form_factor,
        }
    }
}
