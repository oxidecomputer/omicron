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
/// properties. Sled-agent does not create or modify them on its own when it
/// adopts a disk.
///
/// See [`per_disk_dataset_config`] for the properties of each.
pub const PER_DISK_DATASET_KINDS: [DatasetKind; 4] = [
    DatasetKind::Debug,
    DatasetKind::TransientZoneRoot,
    DatasetKind::LocalStorage,
    DatasetKind::LocalStorageUnencrypted,
];

// TODO-correctness: This value of 100GiB is a pretty wild guess, and should be
// tuned as needed.
const DEBUG_DATASET_QUOTA: ByteCount = ByteCount::from_gibibytes_u32(100);

/// Returns the properties for one of the [`PER_DISK_DATASET_KINDS`], or
/// `None` if `kind` is not one of them.
pub fn per_disk_dataset_config(
    kind: &DatasetKind,
) -> Option<SharedDatasetConfig> {
    match kind {
        // For long-term storage of miscellaneous debug data, including kernel
        // crash dumps, process core dumps, log files, etc. See
        // `DebugCollector`.
        DatasetKind::Debug => Some(SharedDatasetConfig {
            compression: CompressionAlgorithm::GzipN {
                level: GzipLevel::new::<9>(),
            },
            quota: Some(DEBUG_DATASET_QUOTA),
            reservation: None,
        }),
        DatasetKind::TransientZoneRoot
        | DatasetKind::LocalStorage
        | DatasetKind::LocalStorageUnencrypted => Some(SharedDatasetConfig {
            compression: CompressionAlgorithm::Off,
            quota: None,
            reservation: None,
        }),
        DatasetKind::Cockroach
        | DatasetKind::Crucible
        | DatasetKind::Clickhouse
        | DatasetKind::ClickhouseKeeper
        | DatasetKind::ClickhouseServer
        | DatasetKind::ExternalDns
        | DatasetKind::InternalDns
        | DatasetKind::TransientZone { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_disk_dataset_kinds_have_configs() {
        for kind in &PER_DISK_DATASET_KINDS {
            assert!(
                per_disk_dataset_config(kind).is_some(),
                "missing config for per-disk dataset kind {kind:?}"
            );
        }
    }
}
