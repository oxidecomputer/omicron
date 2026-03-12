// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Physical disk types for version INITIAL.

use super::asset::AssetIdentityMetadata;
use daft::Diffable;
use omicron_common::disk::DiskVariant;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::EnumIter;
use uuid::Uuid;

/// Describes the form factor of physical disks.
#[derive(
    Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalDiskKind {
    M2,
    U2,
}

impl From<DiskVariant> for PhysicalDiskKind {
    fn from(dv: DiskVariant) -> Self {
        match dv {
            DiskVariant::M2 => PhysicalDiskKind::M2,
            DiskVariant::U2 => PhysicalDiskKind::U2,
        }
    }
}

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

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub form_factor: PhysicalDiskKind,
}

/// The operator-defined policy of a physical disk.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PhysicalDiskPolicy {
    /// The operator has indicated that the disk is in-service.
    InService,

    /// The operator has indicated that the disk has been permanently removed
    /// from service.
    ///
    /// This is a terminal state: once a particular disk ID is expunged, it
    /// will never return to service. (The actual hardware may be reused, but
    /// it will be treated as a brand-new disk.)
    ///
    /// An expunged disk is always non-provisionable.
    Expunged,
}

/// The current state of the disk, as determined by Nexus.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
    Diffable,
)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalDiskState {
    /// The disk is currently active, and has resources allocated on it.
    Active,

    /// The disk has been permanently removed from service.
    ///
    /// This is a terminal state: once a particular disk ID is decommissioned,
    /// it will never return to service. (The actual hardware may be reused,
    /// but it will be treated as a brand-new disk.)
    Decommissioned,
}
