// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types shared among crates

use std::fmt;

use anyhow::bail;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::external::Generation, ledger::Ledgerable, zpool_name::ZpoolKind,
};

#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct OmicronPhysicalDiskConfig {
    pub identity: DiskIdentity,
    pub id: Uuid,
    pub pool_id: ZpoolUuid,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronPhysicalDisksConfig {
    /// generation number of this configuration
    ///
    /// This generation number is owned by the control plane (i.e., RSS or
    /// Nexus, depending on whether RSS-to-Nexus handoff has happened).  It
    /// should not be bumped within Sled Agent.
    ///
    /// Sled Agent rejects attempts to set the configuration to a generation
    /// older than the one it's currently running.
    pub generation: Generation,

    pub disks: Vec<OmicronPhysicalDiskConfig>,
}

impl Default for OmicronPhysicalDisksConfig {
    fn default() -> Self {
        Self { generation: Generation::new(), disks: vec![] }
    }
}

impl Ledgerable for OmicronPhysicalDisksConfig {
    fn is_newer_than(&self, other: &OmicronPhysicalDisksConfig) -> bool {
        self.generation > other.generation
    }

    // No need to do this, the generation number is provided externally.
    fn generation_bump(&mut self) {}
}

impl OmicronPhysicalDisksConfig {
    pub fn new() -> Self {
        Self { generation: Generation::new(), disks: vec![] }
    }
}

/// Uniquely identifies a disk.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct DiskIdentity {
    pub vendor: String,
    pub model: String,
    pub serial: String,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    Ord,
    PartialOrd,
)]
pub enum DiskVariant {
    U2,
    M2,
}

impl From<ZpoolKind> for DiskVariant {
    fn from(kind: ZpoolKind) -> DiskVariant {
        match kind {
            ZpoolKind::External => DiskVariant::U2,
            ZpoolKind::Internal => DiskVariant::M2,
        }
    }
}

/// Describes an M.2 slot, often in the context of writing a system image to
/// it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub enum M2Slot {
    A,
    B,
}

impl fmt::Display for M2Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::A => f.write_str("A"),
            Self::B => f.write_str("B"),
        }
    }
}

impl TryFrom<i64> for M2Slot {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            // Gimlet should have 2 M.2 drives: drive A is assigned slot 17, and
            // drive B is assigned slot 18.
            17 => Ok(Self::A),
            18 => Ok(Self::B),
            _ => bail!("unexpected M.2 slot {value}"),
        }
    }
}
