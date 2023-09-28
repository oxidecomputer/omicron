// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types

use camino::Utf8PathBuf;
use illumos_utils::zpool::{ZpoolKind, ZpoolName};
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use sled_hardware::{
    DiskPaths, DiskVariant, Partition, PooledDisk, PooledDiskError,
    UnparsedDisk,
};
use slog::Logger;

use crate::dataset;

/// A wrapper around real disks or synthetic disks backed by a file
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DiskWrapper {
    Real { disk: Disk, devfs_path: Utf8PathBuf },
    Synthetic { zpool_name: ZpoolName },
}

impl From<Disk> for DiskWrapper {
    fn from(disk: Disk) -> Self {
        let devfs_path = disk.devfs_path().clone();
        Self::Real { disk, devfs_path }
    }
}

impl DiskWrapper {
    pub fn identity(&self) -> DiskIdentity {
        match self {
            DiskWrapper::Real { disk, .. } => disk.identity().clone(),
            DiskWrapper::Synthetic { zpool_name } => {
                let id = zpool_name.id();
                DiskIdentity {
                    vendor: "synthetic-vendor".to_string(),
                    serial: format!("synthetic-serial-{id}"),
                    model: "synthetic-model".to_string(),
                }
            }
        }
    }

    pub fn variant(&self) -> DiskVariant {
        match self {
            DiskWrapper::Real { disk, .. } => disk.variant(),
            DiskWrapper::Synthetic { zpool_name } => match zpool_name.kind() {
                ZpoolKind::External => DiskVariant::U2,
                ZpoolKind::Internal => DiskVariant::M2,
            },
        }
    }

    pub fn zpool_name(&self) -> &ZpoolName {
        match self {
            DiskWrapper::Real { disk, .. } => disk.zpool_name(),
            DiskWrapper::Synthetic { zpool_name } => zpool_name,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error(transparent)]
    Dataset(#[from] crate::dataset::DatasetError),
    #[error(transparent)]
    PooledDisk(#[from] sled_hardware::PooledDiskError),
}

/// A physical disk conforming to the expected partition layout
/// and which contains provisioned zpools and datasets. This disk
/// is ready for usage by higher level software.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Disk {
    paths: DiskPaths,
    slot: i64,
    variant: DiskVariant,
    identity: DiskIdentity,
    is_boot_disk: bool,
    partitions: Vec<Partition>,

    // This embeds the assumtion that there is exactly one parsed zpool per
    // disk.
    zpool_name: ZpoolName,
}

impl Disk {
    pub async fn new(
        log: &Logger,
        unparsed_disk: UnparsedDisk,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<Self, DiskError> {
        let disk = PooledDisk::new(log, unparsed_disk)?;
        dataset::ensure_zpool_has_datasets(
            log,
            &disk.zpool_name,
            &disk.identity,
            key_requester,
        )
        .await?;
        Ok(disk.into())
    }
    pub fn is_boot_disk(&self) -> bool {
        self.is_boot_disk
    }

    pub fn identity(&self) -> &DiskIdentity {
        &self.identity
    }

    pub fn variant(&self) -> DiskVariant {
        self.variant
    }

    pub fn devfs_path(&self) -> &Utf8PathBuf {
        &self.paths.devfs_path
    }

    pub fn zpool_name(&self) -> &ZpoolName {
        &self.zpool_name
    }

    pub fn boot_image_devfs_path(
        &self,
        raw: bool,
    ) -> Result<Utf8PathBuf, PooledDiskError> {
        self.paths.partition_device_path(
            &self.partitions,
            Partition::BootImage,
            raw,
        )
    }

    pub fn dump_device_devfs_path(
        &self,
        raw: bool,
    ) -> Result<Utf8PathBuf, PooledDiskError> {
        self.paths.partition_device_path(
            &self.partitions,
            Partition::DumpDevice,
            raw,
        )
    }

    pub fn slot(&self) -> i64 {
        self.slot
    }
}

impl From<PooledDisk> for Disk {
    fn from(pd: PooledDisk) -> Self {
        Self {
            paths: pd.paths,
            slot: pd.slot,
            variant: pd.variant,
            identity: pd.identity,
            is_boot_disk: pd.is_boot_disk,
            partitions: pd.partitions,
            zpool_name: pd.zpool_name,
        }
    }
}
