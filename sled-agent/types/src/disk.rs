// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for sled-agent.

pub use sled_agent_types_versions::latest::disk::*;

use omicron_common::api::external::ByteCount;
use omicron_common::disk::DatasetKind;

/// The kinds of datasets that every in-service U.2 has, regardless of which
/// zones are placed on it.
///
/// These datasets are owned by the sled config: RSS includes them in the
/// initial blueprint, the Reconfigurator planner maintains them afterwards,
/// and the sled-agent config reconciler creates them and applies their
/// properties.
///
/// Use [`strum::IntoEnumIterator::iter`] to visit every kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::EnumIter)]
pub enum PerDiskDatasetKind {
    /// Long-term storage for debug data (crash dumps, core dumps, logs, etc.).
    /// See `DebugCollector`.
    Debug,
    /// Parent of transient zone root filesystems.
    TransientZoneRoot,
    /// Parent of encrypted local storage datasets.
    LocalStorage,
    /// Parent of unencrypted local storage datasets.
    LocalStorageUnencrypted,
}

// TODO-correctness: This value of 100GiB is a pretty wild guess, and should be
// tuned as needed.
const U2_DEBUG_DATASET_QUOTA: ByteCount = ByteCount::from_gibibytes_u32(100);

impl PerDiskDatasetKind {
    /// Returns the properties of this dataset.
    pub fn config(self) -> SharedDatasetConfig {
        match self {
            Self::Debug => SharedDatasetConfig {
                compression: CompressionAlgorithm::GzipN {
                    level: GzipLevel::new::<9>(),
                },
                quota: Some(U2_DEBUG_DATASET_QUOTA),
                reservation: None,
            },
            Self::TransientZoneRoot
            | Self::LocalStorage
            | Self::LocalStorageUnencrypted => SharedDatasetConfig {
                compression: CompressionAlgorithm::Off,
                quota: None,
                reservation: None,
            },
        }
    }
}

impl From<PerDiskDatasetKind> for DatasetKind {
    fn from(kind: PerDiskDatasetKind) -> Self {
        match kind {
            PerDiskDatasetKind::Debug => DatasetKind::Debug,
            PerDiskDatasetKind::TransientZoneRoot => {
                DatasetKind::TransientZoneRoot
            }
            PerDiskDatasetKind::LocalStorage => DatasetKind::LocalStorage,
            PerDiskDatasetKind::LocalStorageUnencrypted => {
                DatasetKind::LocalStorageUnencrypted
            }
        }
    }
}
