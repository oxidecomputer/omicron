// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::illumos::fstyp::Fstyp;
use crate::illumos::zpool::Zpool;
use crate::illumos::zpool::ZpoolName;
use slog::Logger;
use std::path::PathBuf;
use uuid::Uuid;

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        mod illumos;
        pub(crate) use illumos::*;
    } else {
        mod non_illumos;
        pub(crate) use non_illumos::*;
    }
}

/// Provides information from the underlying hardware about updates
/// which may require action on behalf of the Sled Agent.
///
/// These updates should generally be "non-opinionated" - the higher
/// layers of the sled agent can make the call to ignore these updates
/// or not.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum HardwareUpdate {
    TofinoDeviceChange,
    TofinoLoaded,
    TofinoUnloaded,
    DiskAdded(UnparsedDisk),
    DiskRemoved(UnparsedDisk),
}

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("Cannot open {path} due to {error}")]
    IoError { path: PathBuf, error: std::io::Error },
    #[error("Failed to open partition at {path} due to {error}")]
    Gpt { path: PathBuf, error: anyhow::Error },
    #[error("Unexpected partition layout at {path}: {why}")]
    BadPartitionLayout { path: PathBuf, why: String },
    #[error("Requested partition {partition:?} not found on device {path}")]
    NotFound { path: PathBuf, partition: Partition },
    #[error(transparent)]
    ZpoolCreate(#[from] crate::illumos::zpool::CreateError),
    #[error("Cannot format {path}: missing a '/dev' path")]
    CannotFormatMissingDevPath { path: PathBuf },
    #[error("Formatting M.2 devices is not yet implemented")]
    CannotFormatM2NotImplemented,
}

/// A partition (or 'slice') of a disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum Partition {
    /// The partition may be used to boot an OS image.
    BootImage,
    /// Reserved for future use.
    Reserved,
    /// The partition may be used as a dump device.
    DumpDevice,
    /// The partition may contain a ZFS pool.
    ZfsPool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskPaths {
    // Full path to the disk under "/devices".
    // Should NOT end with a ":partition_letter".
    pub devfs_path: PathBuf,
    // Optional path to the disk under "/dev/dsk".
    pub dev_path: Option<PathBuf>,
}

/// Uniquely identifies a disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskIdentity {
    pub vendor: String,
    pub serial: String,
    pub model: String,
}

impl DiskPaths {
    // Returns the "illumos letter-indexed path" for a device.
    fn partition_path(&self, index: usize) -> Option<PathBuf> {
        let index = u8::try_from(index).ok()?;

        let path = self.devfs_path.display();
        let character = match index {
            0..=5 => (b'a' + index) as char,
            _ => return None,
        };
        Some(PathBuf::from(format!("{path}:{character}")))
    }

    /// Returns the path to the whole disk
    #[allow(dead_code)]
    pub(crate) fn whole_disk(&self, raw: bool) -> PathBuf {
        PathBuf::from(format!(
            "{path}:wd{raw}",
            path = self.devfs_path.display(),
            raw = if raw { ",raw" } else { "" },
        ))
    }

    // Finds the first 'variant' partition, and returns the path to it.
    fn partition_device_path(
        &self,
        partitions: &[Partition],
        expected_partition: Partition,
    ) -> Result<PathBuf, DiskError> {
        for (index, partition) in partitions.iter().enumerate() {
            if &expected_partition == partition {
                let path = self.partition_path(index).ok_or_else(|| {
                    DiskError::NotFound {
                        path: self.devfs_path.clone(),
                        partition: expected_partition,
                    }
                })?;
                return Ok(path);
            }
        }
        return Err(DiskError::NotFound {
            path: self.devfs_path.clone(),
            partition: expected_partition,
        });
    }
}

/// A disk which has been observed by monitoring hardware.
///
/// No guarantees are made about the partitions which exist within this disk.
/// This exists as a distinct entity from [Disk] because it may be desirable to
/// monitor for hardware in one context, and conform disks to partition layouts
/// in a different context.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnparsedDisk {
    paths: DiskPaths,
    slot: i64,
    variant: DiskVariant,
    identity: DiskIdentity,
}

impl UnparsedDisk {
    #[allow(dead_code)]
    pub fn new(
        devfs_path: PathBuf,
        dev_path: Option<PathBuf>,
        slot: i64,
        variant: DiskVariant,
        identity: DiskIdentity,
    ) -> Self {
        Self {
            paths: DiskPaths { devfs_path, dev_path },
            slot,
            variant,
            identity,
        }
    }

    pub fn devfs_path(&self) -> &PathBuf {
        &self.paths.devfs_path
    }
}

/// A physical disk conforming to the expected partition layout.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Disk {
    paths: DiskPaths,
    slot: i64,
    variant: DiskVariant,
    identity: DiskIdentity,
    partitions: Vec<Partition>,

    // This embeds the assumtion that there is exactly one parsed zpool per
    // disk.
    zpool_name: ZpoolName,
}

impl Disk {
    #[allow(dead_code)]
    pub fn new(
        log: &Logger,
        unparsed_disk: UnparsedDisk,
    ) -> Result<Self, DiskError> {
        let paths = &unparsed_disk.paths;
        // First, ensure the GPT has the right format. This does not necessarily
        // mean that the partitions are populated with the data we need.
        let partitions =
            ensure_partition_layout(&log, &paths, unparsed_disk.variant)?;

        // Find the path to the zpool which exists on this disk.
        //
        // NOTE: At the moment, we're hard-coding the assumption that at least
        // one zpool should exist on every disk.
        let zpool_path =
            paths.partition_device_path(&partitions, Partition::ZfsPool)?;

        let zpool_name = match Fstyp::get_zpool(&zpool_path) {
            Ok(zpool_name) => zpool_name,
            Err(_) => {
                // What happened here?
                // - We saw that a GPT exists for this Disk (or we didn't, and
                // made our own).
                // - However, this particular partition does not appear to have
                // a zpool.
                //
                // This can happen in situations where "zpool create"
                // initialized a zpool, and "zpool destroy" removes the zpool
                // but still leaves the partition table untouched.
                //
                // To remedy: Let's enforce that the partition exists.
                info!(
                    log,
                    "GPT exists without Zpool: formatting zpool on disk {}",
                    paths.devfs_path.display()
                );
                // If a zpool does not already exist, create one.
                let zpool_name = ZpoolName::new(Uuid::new_v4());
                Zpool::create(zpool_name.clone(), &zpool_path)?;
                zpool_name
            }
        };

        Ok(Self {
            paths: unparsed_disk.paths,
            slot: unparsed_disk.slot,
            variant: unparsed_disk.variant,
            identity: unparsed_disk.identity,
            partitions,
            zpool_name,
        })
    }

    pub fn identity(&self) -> &DiskIdentity {
        &self.identity
    }

    pub fn variant(&self) -> DiskVariant {
        self.variant
    }

    pub fn devfs_path(&self) -> &PathBuf {
        &self.paths.devfs_path
    }

    pub fn zpool_name(&self) -> &ZpoolName {
        &self.zpool_name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum DiskVariant {
    U2,
    M2,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_disk_paths() {
        const DEVFS_PATH: &'static str = "/devices/my/disk";
        let paths =
            DiskPaths { devfs_path: PathBuf::from(DEVFS_PATH), dev_path: None };
        assert_eq!(
            paths.whole_disk(false),
            PathBuf::from(format!("{DEVFS_PATH}:wd"))
        );
        assert_eq!(
            paths.whole_disk(true),
            PathBuf::from(format!("{DEVFS_PATH}:wd,raw"))
        );
        assert_eq!(
            paths.partition_path(0),
            Some(PathBuf::from(format!("{DEVFS_PATH}:a")))
        );
        assert_eq!(
            paths.partition_path(1),
            Some(PathBuf::from(format!("{DEVFS_PATH}:b")))
        );
        assert_eq!(
            paths.partition_path(2),
            Some(PathBuf::from(format!("{DEVFS_PATH}:c")))
        );
        assert_eq!(
            paths.partition_path(3),
            Some(PathBuf::from(format!("{DEVFS_PATH}:d")))
        );
        assert_eq!(
            paths.partition_path(4),
            Some(PathBuf::from(format!("{DEVFS_PATH}:e")))
        );
        assert_eq!(
            paths.partition_path(5),
            Some(PathBuf::from(format!("{DEVFS_PATH}:f")))
        );
        assert_eq!(paths.partition_path(6), None);
    }

    #[test]
    fn test_partition_device_paths() {
        const DEVFS_PATH: &'static str = "/devices/my/disk";
        let paths =
            DiskPaths { devfs_path: PathBuf::from(DEVFS_PATH), dev_path: None };

        assert_eq!(
            paths
                .partition_device_path(
                    &[Partition::ZfsPool,],
                    Partition::ZfsPool,
                )
                .expect("Should have found partition"),
            paths.partition_path(0).unwrap(),
        );

        assert_eq!(
            paths
                .partition_device_path(
                    &[
                        Partition::BootImage,
                        Partition::Reserved,
                        Partition::ZfsPool,
                        Partition::DumpDevice,
                    ],
                    Partition::ZfsPool,
                )
                .expect("Should have found partition"),
            paths.partition_path(2).unwrap(),
        );

        assert!(matches!(
            paths
                .partition_device_path(&[], Partition::ZfsPool,)
                .expect_err("Should not have found partition"),
            DiskError::NotFound { .. },
        ));
    }
}
