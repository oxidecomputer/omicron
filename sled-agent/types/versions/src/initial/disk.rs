// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for the Sled Agent API.

use iddqd::IdOrdItem;
use iddqd::id_upcast;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Information about a zpool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Zpool {
    pub id: ZpoolUuid,
    pub disk_type: DiskVariant,
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
    daft::Diffable,
    strum::EnumIter,
)]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum M2Slot {
    A,
    B,
}

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

impl IdOrdItem for OmicronPhysicalDiskConfig {
    type Key<'a> = PhysicalDiskUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
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
    daft::Diffable,
)]
pub struct DiskIdentity {
    pub vendor: String,
    pub model: String,
    pub serial: String,
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

impl IdOrdItem for DatasetConfig {
    type Key<'a> = DatasetUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
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
    daft::Diffable,
)]
pub struct GzipLevel(pub(crate) u8);

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
    daft::Diffable,
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
