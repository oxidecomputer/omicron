// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types

use camino::{Utf8Path, Utf8PathBuf};
use derive_more::From;
use illumos_utils::zpool::{Zpool, ZpoolKind, ZpoolName};
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use sled_hardware::{
    DiskVariant, Partition, PooledDisk, PooledDiskError, UnparsedDisk,
};
use slog::Logger;
use std::fs::File;

use crate::dataset;

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error(transparent)]
    Dataset(#[from] crate::dataset::DatasetError),
    #[error(transparent)]
    PooledDisk(#[from] sled_hardware::PooledDiskError),
}

// A synthetic disk that acts as one "found" by the hardware and that is backed
// by a zpool
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SyntheticDisk {
    pub identity: DiskIdentity,
    pub zpool_name: ZpoolName,
    pub slot: i64,
}

// By adding slots at an "offset", this acts as a barrier against synthetic
// disks overlapping with "real disk" in a mixed-disk deployment.
//
// This shouldn't happen in prod, and is an unlikely test-only scenario, but
// we'd still like to protect against it, since it could confuse the inventory
// system.
const SYNTHETIC_SLOT_OFFSET: i64 = 1024;

impl SyntheticDisk {
    // Create a zpool and import it for the synthetic disk
    // Zpools willl be set to the min size of 64Mib
    pub fn create_zpool(
        dir: &Utf8Path,
        zpool_name: &ZpoolName,
        slot: i64,
    ) -> SyntheticDisk {
        // 64 MiB (min size of zpool)
        const DISK_SIZE: u64 = 64 * 1024 * 1024;
        let path = dir.join(zpool_name.to_string());
        let file = File::create(&path).unwrap();
        file.set_len(DISK_SIZE).unwrap();
        drop(file);
        Zpool::create(zpool_name, &path).unwrap();
        Zpool::import(zpool_name).unwrap();
        Zpool::set_failmode_continue(zpool_name).unwrap();
        Self::new(zpool_name.clone(), slot)
    }

    pub fn new(zpool_name: ZpoolName, slot: i64) -> SyntheticDisk {
        let id = zpool_name.id();
        let identity = DiskIdentity {
            vendor: "synthetic-vendor".to_string(),
            serial: format!("synthetic-serial-{id}"),
            model: "synthetic-model".to_string(),
        };
        SyntheticDisk {
            identity,
            zpool_name,
            slot: slot + SYNTHETIC_SLOT_OFFSET,
        }
    }
}

// An [`UnparsedDisk`] disk learned about from the hardware or a wrapped zpool
#[derive(Debug, Clone, PartialEq, Eq, Hash, From)]
pub enum RawDisk {
    Real(UnparsedDisk),
    Synthetic(SyntheticDisk),
}

impl RawDisk {
    pub fn is_boot_disk(&self) -> bool {
        match self {
            Self::Real(disk) => disk.is_boot_disk(),
            Self::Synthetic(disk) => {
                // Just label any M.2 the boot disk.
                disk.zpool_name.kind() == ZpoolKind::Internal
            }
        }
    }

    pub fn identity(&self) -> &DiskIdentity {
        match self {
            Self::Real(disk) => &disk.identity(),
            Self::Synthetic(disk) => &disk.identity,
        }
    }

    pub fn variant(&self) -> DiskVariant {
        match self {
            Self::Real(disk) => disk.variant(),
            Self::Synthetic(disk) => match disk.zpool_name.kind() {
                ZpoolKind::External => DiskVariant::U2,
                ZpoolKind::Internal => DiskVariant::M2,
            },
        }
    }

    #[cfg(test)]
    pub fn zpool_name(&self) -> &ZpoolName {
        match self {
            Self::Real(_) => unreachable!(),
            Self::Synthetic(disk) => &disk.zpool_name,
        }
    }

    pub fn is_synthetic(&self) -> bool {
        match self {
            Self::Real(_) => false,
            Self::Synthetic(_) => true,
        }
    }

    pub fn is_real(&self) -> bool {
        !self.is_synthetic()
    }

    pub fn devfs_path(&self) -> &Utf8PathBuf {
        match self {
            Self::Real(disk) => disk.devfs_path(),
            Self::Synthetic(_) => unreachable!(),
        }
    }
}

/// A physical [`PooledDisk`] or a [`SyntheticDisk`] that contains or is backed
/// by a single zpool and that has provisioned datasets. This disk is ready for
/// usage by higher level software.
#[derive(Debug, Clone, PartialEq, Eq, Hash, From)]
pub enum Disk {
    Real(PooledDisk),
    Synthetic(SyntheticDisk),
}

impl Disk {
    pub async fn new(
        log: &Logger,
        raw_disk: RawDisk,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<Self, DiskError> {
        let disk = match raw_disk {
            RawDisk::Real(disk) => PooledDisk::new(log, disk)?.into(),
            RawDisk::Synthetic(disk) => Disk::Synthetic(disk),
        };
        dataset::ensure_zpool_has_datasets(
            log,
            disk.zpool_name(),
            disk.identity(),
            key_requester,
        )
        .await?;

        if matches!(disk.variant(), DiskVariant::U2) {
            dataset::ensure_zpool_datasets_are_encrypted(
                log,
                disk.zpool_name(),
            )
            .await
            .map_err(|err| crate::dataset::DatasetError::from(err))?;
        }

        Ok(disk)
    }

    pub fn is_synthetic(&self) -> bool {
        match self {
            Self::Real(_) => false,
            Self::Synthetic(_) => true,
        }
    }

    pub fn is_real(&self) -> bool {
        !self.is_synthetic()
    }

    pub fn is_boot_disk(&self) -> bool {
        match self {
            Self::Real(disk) => disk.is_boot_disk,
            Self::Synthetic(disk) => {
                // Just label any M.2 the boot disk.
                disk.zpool_name.kind() == ZpoolKind::Internal
            }
        }
    }

    pub fn identity(&self) -> &DiskIdentity {
        match self {
            Self::Real(disk) => &disk.identity,
            Self::Synthetic(disk) => &disk.identity,
        }
    }

    pub fn variant(&self) -> DiskVariant {
        match self {
            Self::Real(disk) => disk.variant,
            Self::Synthetic(disk) => match disk.zpool_name.kind() {
                ZpoolKind::External => DiskVariant::U2,
                ZpoolKind::Internal => DiskVariant::M2,
            },
        }
    }

    pub fn devfs_path(&self) -> &Utf8PathBuf {
        match self {
            Self::Real(disk) => &disk.paths.devfs_path,
            Self::Synthetic(_) => unreachable!(),
        }
    }

    pub fn zpool_name(&self) -> &ZpoolName {
        match self {
            Self::Real(disk) => &disk.zpool_name,
            Self::Synthetic(disk) => &disk.zpool_name,
        }
    }

    pub fn boot_image_devfs_path(
        &self,
        raw: bool,
    ) -> Result<Utf8PathBuf, PooledDiskError> {
        match self {
            Self::Real(disk) => disk.paths.partition_device_path(
                &disk.partitions,
                Partition::BootImage,
                raw,
            ),
            Self::Synthetic(_) => unreachable!(),
        }
    }

    pub fn dump_device_devfs_path(
        &self,
        raw: bool,
    ) -> Result<Utf8PathBuf, PooledDiskError> {
        match self {
            Self::Real(disk) => disk.paths.partition_device_path(
                &disk.partitions,
                Partition::DumpDevice,
                raw,
            ),
            Self::Synthetic(_) => unreachable!(),
        }
    }

    pub fn slot(&self) -> i64 {
        match self {
            Self::Real(disk) => disk.slot,
            Self::Synthetic(disk) => disk.slot,
        }
    }
}
