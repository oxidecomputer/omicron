// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::hardware::{DiskError, DiskVariant, Partition};
use std::path::PathBuf;

const M2_EXPECTED_PARTITION_COUNT: usize = 6;
static M2_EXPECTED_PARTITIONS: [Partition; M2_EXPECTED_PARTITION_COUNT] = [
    Partition::BootImage,
    Partition::Reserved,
    Partition::Reserved,
    Partition::Reserved,
    Partition::DumpDevice,
    Partition::ZfsPool,
];

const U2_EXPECTED_PARTITION_COUNT: usize = 1;
static U2_EXPECTED_PARTITIONS: [Partition; U2_EXPECTED_PARTITION_COUNT] = [
    Partition::ZfsPool,
];

fn parse_partition_types<const N: usize>(
    path: &PathBuf,
    partitions: &Vec<libefi_illumos::Partition>,
    expected_partitions: &[Partition; N],
) -> Result<Vec<Partition>, DiskError> {
    if partitions.len() != N {
        return Err(DiskError::BadPartitionLayout { path: path.clone() });
    }
    for i in 0..N {
        if partitions[i].index() != i {
            return Err(DiskError::BadPartitionLayout { path: path.clone() });
        }

        // NOTE: If we wanted to, we could validate additional information about
        // the size, GUID, or name of the partition. At the moment, however,
        // we're relying on the index within the partition table to indicate the
        // "intent" of the partition.
    }

    Ok(expected_partitions.iter().map(|p| p.clone()).collect())
}

pub fn parse_partition_layout(
    devfs_path: &PathBuf,
    variant: DiskVariant,
) -> Result<Vec<Partition>, DiskError> {
    // Open the "Whole Disk" (wd) as a raw device to be parsed by the
    // libefi-illumos library. This lets us peek at the GPT before
    // making too many assumptions about it.
    let path = PathBuf::from(format!("{path}:wd,raw", path = devfs_path.display()));
    let file = std::fs::File::open(&path).map_err(|error| {
        DiskError::IoError {
            path: path.clone(),
            error,
        }
    })?;
    let gpt = libefi_illumos::Gpt::new(file).map_err(|error| {
        DiskError::Gpt {
            path: path.clone(),
            error,
        }
    })?;

    let mut partitions: Vec<_> = gpt.partitions().collect();
    match variant {
        DiskVariant::U2 => {
            partitions.truncate(U2_EXPECTED_PARTITION_COUNT);
            parse_partition_types(&path, &partitions, &U2_EXPECTED_PARTITIONS)
        },
        DiskVariant::M2 => {
            partitions.truncate(M2_EXPECTED_PARTITION_COUNT);
            parse_partition_types(&path, &partitions, &M2_EXPECTED_PARTITIONS)
        },
    }
}
