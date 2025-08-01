// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types shared among crates

use anyhow::bail;
use camino::{Utf8Path, Utf8PathBuf};
use daft::Diffable;
use id_map::IdMappable;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::api::internal::shared::DatasetKindParseError;
use crate::{
    api::external::{ByteCount, Generation},
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
    pub id: PhysicalDiskUuid,
    pub pool_id: ZpoolUuid,
}

impl IdMappable for OmicronPhysicalDiskConfig {
    type Id = PhysicalDiskUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
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

    pub fn into_parts(self) -> (ZpoolName, DatasetKind) {
        (self.pool_name, self.kind)
    }

    pub fn pool(&self) -> &ZpoolName {
        &self.pool_name
    }

    pub fn kind(&self) -> &DatasetKind {
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

    /// Returns the mountpoint of the dataset.
    ///
    /// If this dataset is delegated to a non-global zone, returns "/data".
    ///
    /// If this dataset is intended for the global zone and should be encrypted,
    /// this automatically adds the "crypt" dataset component.
    pub fn mountpoint(&self, root: &Utf8Path) -> Utf8PathBuf {
        if self.kind.zoned() {
            Utf8PathBuf::from("/data")
        } else {
            self.pool_name.dataset_mountpoint(
                root,
                &if self.kind.dataset_should_be_encrypted() {
                    format!("crypt/{}", self.kind)
                } else {
                    self.kind.to_string()
                },
            )
        }
    }

    fn full_encrypted_name(&self) -> String {
        format!("{}/crypt/{}", self.pool_name, self.kind)
    }

    fn full_unencrypted_name(&self) -> String {
        format!("{}/{}", self.pool_name, self.kind)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetNameParseError {
    #[error("missing '/' separator in dataset name {0}")]
    MissingSlash(String),
    #[error("could not parse zpool name {zpool}: {err}")]
    ParseZpoolName { zpool: String, err: String },
    #[error("could not parse dataset kind {kind}")]
    ParseDatasetKind {
        kind: String,
        #[source]
        err: DatasetKindParseError,
    },
    #[error("expected `crypt/` for kind {kind:?} in dataset name {name}")]
    MissingCryptInName { kind: DatasetKind, name: String },
    #[error("unexpected `crypt/` for kind {kind:?} in dataset name {name}")]
    UnexpectedCryptInName { kind: DatasetKind, name: String },
}

impl FromStr for DatasetName {
    type Err = DatasetNameParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (pool_name, remainder) = s.split_once('/').ok_or_else(|| {
            DatasetNameParseError::MissingSlash(s.to_string())
        })?;

        let pool_name = ZpoolName::from_str(pool_name).map_err(|err| {
            DatasetNameParseError::ParseZpoolName {
                zpool: pool_name.to_string(),
                err,
            }
        })?;

        let (kind_str, name_has_crypt) =
            if let Some(remainder) = remainder.strip_prefix("crypt/") {
                (remainder, true)
            } else {
                (remainder, false)
            };

        let kind = DatasetKind::from_str(kind_str).map_err(|err| {
            DatasetNameParseError::ParseDatasetKind {
                kind: kind_str.to_string(),
                err,
            }
        })?;

        match (kind.dataset_should_be_encrypted(), name_has_crypt) {
            (true, true) | (false, false) => Ok(Self { pool_name, kind }),
            (true, false) => Err(DatasetNameParseError::MissingCryptInName {
                kind,
                name: s.to_string(),
            }),
            (false, true) => {
                Err(DatasetNameParseError::UnexpectedCryptInName {
                    kind,
                    name: s.to_string(),
                })
            }
        }
    }
}

#[derive(
    Copy,
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
    Diffable,
)]
pub struct GzipLevel(u8);

// Fastest compression level
const GZIP_LEVEL_MIN: u8 = 1;

// Best compression ratio
const GZIP_LEVEL_MAX: u8 = 9;

impl GzipLevel {
    pub const fn new<const N: u8>() -> Self {
        assert!(N >= GZIP_LEVEL_MIN, "Compression level too small");
        assert!(N <= GZIP_LEVEL_MAX, "Compression level too large");
        Self(N)
    }
}

impl FromStr for GzipLevel {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let level = s.parse::<u8>()?;
        if level < GZIP_LEVEL_MIN || level > GZIP_LEVEL_MAX {
            bail!("Invalid gzip compression level: {level}");
        }
        Ok(Self(level))
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompressionAlgorithm {
    // Selects a default compression algorithm. This is dependent on both the
    // zpool and OS version.
    On,

    // Disables compression.
    #[default]
    Off,

    // Selects the default Gzip compression level.
    //
    // According to the ZFS docs, this is "gzip-6", but that's a default value,
    // which may change with OS updates.
    Gzip,

    GzipN {
        level: GzipLevel,
    },
    Lz4,
    Lzjb,
    Zle,
}

/// These match the arguments which can be passed to "zfs set compression=..."
impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use CompressionAlgorithm::*;
        let s = match self {
            On => "on",
            Off => "off",
            Gzip => "gzip",
            GzipN { level } => {
                return write!(f, "gzip-{}", level.0);
            }
            Lz4 => "lz4",
            Lzjb => "lzjb",
            Zle => "zle",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use CompressionAlgorithm::*;
        let c = match s {
            "on" => On,
            "" | "off" => Off,
            "gzip" => Gzip,
            "lz4" => Lz4,
            "lzjb" => Lzjb,
            "zle" => Zle,
            _ => {
                let Some(suffix) = s.strip_prefix("gzip-") else {
                    bail!("Unknown compression algorithm {s}");
                };
                GzipN { level: suffix.parse()? }
            }
        };
        Ok(c)
    }
}

/// Shared configuration information to request a dataset.
#[derive(
    Clone,
    Debug,
    Default,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct SharedDatasetConfig {
    /// The compression mode to be used by the dataset
    pub compression: CompressionAlgorithm,

    /// The upper bound on the amount of storage used by this dataset
    pub quota: Option<ByteCount>,

    /// The lower bound on the amount of storage usable by this dataset
    pub reservation: Option<ByteCount>,
}

/// Configuration information necessary to request a single dataset.
///
/// These datasets are tracked directly by Nexus.
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

    #[serde(flatten)]
    pub inner: SharedDatasetConfig,
}

impl IdMappable for DatasetConfig {
    type Id = DatasetUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
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

    pub datasets: BTreeMap<DatasetUuid, DatasetConfig>,
}

impl Default for DatasetsConfig {
    fn default() -> Self {
        Self { generation: Generation::new(), datasets: BTreeMap::new() }
    }
}

impl Ledgerable for DatasetsConfig {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    // No need to do this, the generation number is provided externally.
    fn generation_bump(&mut self) {}
}

/// Identifies how a single dataset management operation may have succeeded or
/// failed.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DatasetManagementStatus {
    pub dataset_name: DatasetName,
    pub err: Option<String>,
}

/// The result from attempting to manage datasets.
#[derive(Default, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[must_use = "this `DatasetManagementResult` may contain errors, which should be handled"]
pub struct DatasetsManagementResult {
    pub status: Vec<DatasetManagementStatus>,
}

impl DatasetsManagementResult {
    pub fn has_error(&self) -> bool {
        for status in &self.status {
            if status.err.is_some() {
                return true;
            }
        }
        false
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
    Diffable,
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

/// Identifies how a single disk management operation may have succeeded or
/// failed.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DiskManagementStatus {
    pub identity: DiskIdentity,
    pub err: Option<DiskManagementError>,
}

/// The result from attempting to manage underlying disks.
///
/// This is more complex than a simple "Error" type because it's possible
/// for some disks to be initialized correctly, while others can fail.
///
/// This structure provides a mechanism for callers to learn about partial
/// failures, and handle them appropriately on a per-disk basis.
#[derive(Default, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[must_use = "this `DiskManagementResult` may contain errors, which should be handled"]
pub struct DisksManagementResult {
    pub status: Vec<DiskManagementStatus>,
}

impl DisksManagementResult {
    pub fn has_error(&self) -> bool {
        for status in &self.status {
            if status.err.is_some() {
                return true;
            }
        }
        false
    }

    pub fn has_retryable_error(&self) -> bool {
        for status in &self.status {
            if let Some(err) = &status.err {
                if err.retryable() {
                    return true;
                }
            }
        }
        false
    }
}

#[derive(
    Clone, Debug, thiserror::Error, JsonSchema, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum DiskManagementError {
    #[error("Disk requested by control plane, but not found on device")]
    NotFound,

    #[error("Disk requested by control plane is an internal disk: {0}")]
    InternalDiskControlPlaneRequest(PhysicalDiskUuid),

    #[error("Expected zpool UUID of {expected}, but saw {observed}")]
    ZpoolUuidMismatch { expected: ZpoolUuid, observed: ZpoolUuid },

    #[error(
        "Failed to access keys necessary to unlock storage. This error may be transient."
    )]
    KeyManager(String),

    #[error("Other error starting disk management: {0}")]
    Other(String),
}

impl DiskManagementError {
    pub fn retryable(&self) -> bool {
        match self {
            DiskManagementError::KeyManager(_) => true,
            _ => false,
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
    Diffable,
    strum::EnumIter,
)]
pub enum M2Slot {
    A,
    B,
}

impl M2Slot {
    /// Flip from `A` to `B` or vice versa.
    pub fn toggled(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    /// Convert this slot to an MGS "firmware slot" index.
    pub fn to_mgs_firmware_slot(self) -> u16 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    /// Convert a putative MGS "firmware slot" index to an `M2Slot`, returning
    /// `None` if `slot` is invalid.
    pub fn from_mgs_firmware_slot(slot: u16) -> Option<Self> {
        match slot {
            0 => Some(Self::A),
            1 => Some(Self::B),
            _ => None,
        }
    }
}

impl fmt::Display for M2Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::A => f.write_str("A"),
            Self::B => f.write_str("B"),
        }
    }
}

impl FromStr for M2Slot {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "a" | "A" => Ok(Self::A),
            "b" | "B" => Ok(Self::B),
            _ => Err(format!(
                "unrecognized value {s} for M2 slot. \
                 Must be one of `a`, `A`, `b`, or `B`",
            )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use test_strategy::proptest;

    #[proptest]
    fn parse_dataset_name(pool_id: [u8; 16], kind: DatasetKind) {
        let pool_id = ZpoolUuid::from_bytes(pool_id);
        for pool in
            [ZpoolName::new_internal(pool_id), ZpoolName::new_external(pool_id)]
        {
            let dataset_name = DatasetName::new(pool, kind.clone());
            let s = dataset_name.full_name();
            match DatasetName::from_str(&s) {
                Ok(d) => assert_eq!(d, dataset_name),
                Err(err) => panic!("failed to parse dataset name {s}: {err}"),
            }
        }
    }
}
