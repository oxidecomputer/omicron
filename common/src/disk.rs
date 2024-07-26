// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types shared among crates

use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::external::Generation,
    ledger::Ledgerable,
    zpool_name::{ZpoolKind, ZpoolName},
};

pub use crate::api::internal::shared::DatasetKind;

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

#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Clone,
    JsonSchema,
    PartialOrd,
    Ord,
)]
pub struct DatasetName {
    // A unique identifier for the Zpool on which the dataset is stored.
    pool_name: ZpoolName,
    // A name for the dataset within the Zpool.
    kind: DatasetKind,
}

impl DatasetName {
    pub fn new(pool_name: ZpoolName, kind: DatasetKind) -> Self {
        Self { pool_name, kind }
    }

    pub fn pool(&self) -> &ZpoolName {
        &self.pool_name
    }

    pub fn dataset(&self) -> &DatasetKind {
        &self.kind
    }

    /// Returns the full name of the dataset, as would be returned from
    /// "zfs get" or "zfs list".
    ///
    /// If this dataset should be encrypted, this automatically adds the
    /// "crypt" dataset component.
    pub fn full_name(&self) -> String {
        // Currently, we encrypt all datasets except Crucible.
        //
        // Crucible already performs encryption internally, and we
        // avoid double-encryption.
        if self.kind.dataset_should_be_encrypted() {
            self.full_encrypted_name()
        } else {
            self.full_unencrypted_name()
        }
    }

    fn full_encrypted_name(&self) -> String {
        format!("{}/crypt/{}", self.pool_name, self.kind)
    }

    fn full_unencrypted_name(&self) -> String {
        format!("{}/{}", self.pool_name, self.kind)
    }
}

/// Configuration information necessary to request a single dataset
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
pub struct DatasetConfig {
    /// The UUID of the dataset being requested
    pub id: DatasetUuid,

    /// The dataset's name
    pub name: DatasetName,

    /// The compression mode to be supplied, if any
    pub compression: Option<String>,

    /// The upper bound on the amount of storage used by this dataset
    pub quota: Option<usize>,

    /// The lower bound on the amount of storage usable by this dataset
    pub reservation: Option<usize>,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct DatasetsConfig {
    /// generation number of this configuration
    ///
    /// This generation number is owned by the control plane (i.e., RSS or
    /// Nexus, depending on whether RSS-to-Nexus handoff has happened).  It
    /// should not be bumped within Sled Agent.
    ///
    /// Sled Agent rejects attempts to set the configuration to a generation
    /// older than the one it's currently running.
    ///
    /// Note that "Generation::new()", AKA, the first generation number,
    /// is reserved for "no datasets". This is the default configuration
    /// for a sled before any requests have been made.
    pub generation: Generation,

    pub datasets: Vec<DatasetConfig>,
}

impl Default for DatasetsConfig {
    fn default() -> Self {
        Self { generation: Generation::new(), datasets: vec![] }
    }
}

impl Ledgerable for DatasetsConfig {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    // No need to do this, the generation number is provided externally.
    fn generation_bump(&mut self) {}
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
