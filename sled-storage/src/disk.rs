// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types

use anyhow::bail;
use camino::{Utf8Path, Utf8PathBuf};
use derive_more::From;
use iddqd::{IdOrdItem, id_upcast};
use key_manager::StorageKeyRequester;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::ZpoolUuid;
use sled_agent_types::disk::DiskIdentity;
use sled_agent_types::disk::DiskVariant;
use sled_hardware::{
    DiskFirmware, Partition, PooledDisk, PooledDiskError, UnparsedDisk,
};
use slog::{Logger, info};

use crate::config::MountConfig;
use crate::dataset;

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error(transparent)]
    Dataset(#[from] crate::dataset::DatasetError),
    #[error(transparent)]
    PooledDisk(#[from] sled_hardware::PooledDiskError),
}

/// A synthetic disk which has been formatted with a zpool.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SyntheticDisk {
    raw: RawSyntheticDisk,
    zpool_name: ZpoolName,
}

// By adding slots at an "offset", this acts as a barrier against synthetic
// disks overlapping with "real disk" in a mixed-disk deployment.
//
// This shouldn't happen in prod, and is an unlikely test-only scenario, but
// we'd still like to protect against it, since it could confuse the inventory
// system.
const SYNTHETIC_PCIE_SLOT_OFFSET: i64 = 1024;

// A generic name for the firmware in slot1 of an NVMe device.
//
// bhyve for example uses "1.0" and marks slot1 as read-only (must be within 8chars).
const SYNTHETIC_FIRMWARE_SLOT1: &str = "SYNTH1";

impl SyntheticDisk {
    // "Manages" a SyntheticDisk by ensuring that it has a Zpool and importing
    // it. If the zpool already exists, it is imported, but not re-created.
    pub async fn new(
        log: &Logger,
        mount_config: &MountConfig,
        raw: RawSyntheticDisk,
        zpool_id: Option<ZpoolUuid>,
    ) -> Self {
        let path = if raw.path.is_absolute() {
            raw.path.clone()
        } else {
            mount_config.synthetic_disk_root.join(&raw.path)
        };

        info!(
            log,
            "Invoking SyntheticDisk::new";
            "identity" => ?raw.identity,
            "path" => %path,
        );

        let zpool_name = sled_hardware::disk::ensure_zpool_exists(
            log,
            raw.variant,
            &path,
            zpool_id,
        )
        .await
        .unwrap();
        sled_hardware::disk::ensure_zpool_imported(log, &zpool_name)
            .await
            .unwrap();
        sled_hardware::disk::ensure_zpool_failmode_is_continue(
            log,
            &zpool_name,
        )
        .await
        .unwrap();

        Self { raw, zpool_name }
    }
}

// A synthetic disk that acts as one "found" by the hardware and that is backed
// by a vdev.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RawSyntheticDisk {
    pub path: Utf8PathBuf,
    pub identity: DiskIdentity,
    pub variant: DiskVariant,
    /// A fake PCIe physical slot number, offset so it cannot collide with a
    /// real disk. See [`UnparsedDisk::pcie_slot`].
    pub pcie_slot: i64,
    pub firmware: DiskFirmware,
    /// A chassis location spelled the way real hardware spells it, such as
    /// "N3" or "M.2 East", so that anything keyed on those labels treats a
    /// synthetic disk like a real one. See [`UnparsedDisk::location`].
    pub location: String,
}

impl RawSyntheticDisk {
    /// Creates the file with a specified length, and also parses it as
    /// a [RawSyntheticDisk].
    pub fn new_with_length<P: AsRef<Utf8Path>>(
        vdev: P,
        length: u64,
        pcie_slot: i64,
    ) -> Result<Self, anyhow::Error> {
        let file = std::fs::File::create(vdev.as_ref())?;
        file.set_len(length)?;
        Self::load(vdev, pcie_slot)
    }

    /// Treats a file at path `vdev` as a synthetic disk. The file
    /// should already exist, and have the desired length.
    ///
    /// The file must be named `m2_<serial>.vdev` or `u2_<serial>.vdev`. When
    /// `<serial>` is an integer it also picks the disk's chassis location:
    /// `u2_3.vdev` sits in bay N3, and `m2_0.vdev` and `m2_1.vdev` are M.2
    /// East and M.2 West. Otherwise `pcie_slot` picks the location.
    pub fn load<P: AsRef<Utf8Path>>(
        vdev: P,
        pcie_slot: i64,
    ) -> Result<Self, anyhow::Error> {
        let path = vdev.as_ref();
        let Some(file) = path.file_name() else {
            bail!("Missing file name for synthetic disk");
        };

        let Some(file) = file.strip_suffix(".vdev") else {
            bail!("Missing '.vdev' suffix for synthetic disk");
        };

        let (serial, variant) = if let Some(serial) = file.strip_prefix("m2_") {
            (serial, DiskVariant::M2)
        } else if let Some(serial) = file.strip_prefix("u2_") {
            (serial, DiskVariant::U2)
        } else {
            bail!("Unknown file prefix: {file}. Try one of {{m2_,u2_}}");
        };

        let identity = DiskIdentity {
            vendor: "synthetic-vendor".to_string(),
            serial: format!("synthetic-serial-{serial}"),
            model: format!("synthetic-model-{variant:?}"),
        };

        let ordinal = serial.parse::<i64>().unwrap_or(pcie_slot);
        let location = synthetic_disk_location(variant, ordinal);

        let firmware = DiskFirmware::new(
            1,
            None,
            true,
            1,
            vec![Some(SYNTHETIC_FIRMWARE_SLOT1.to_string())],
        );

        Ok(Self {
            path: path.into(),
            identity,
            variant,
            pcie_slot: pcie_slot + SYNTHETIC_PCIE_SLOT_OFFSET,
            firmware,
            location,
        })
    }
}

/// The chassis location a fake disk reports, spelled the way real hardware
/// spells it so that anything keyed on those labels treats a fake disk like a
/// real one: the `ordinal`th U.2 sits in bay `N<ordinal>`, and M.2s 0 and 1
/// are "M.2 East" and "M.2 West".
///
/// Real sleds have two M.2 sockets, but a test may number its disks with one
/// counter across both variants, so a later M.2 is labelled `M.2 <ordinal>`.
pub fn synthetic_disk_location(variant: DiskVariant, ordinal: i64) -> String {
    match variant {
        DiskVariant::U2 => format!("N{ordinal}"),
        DiskVariant::M2 => match ordinal {
            0 => "M.2 East".to_string(),
            1 => "M.2 West".to_string(),
            n => format!("M.2 {n}"),
        },
    }
}

// An [`UnparsedDisk`] disk learned about from the hardware or a wrapped zpool
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, From)]
pub enum RawDisk {
    Real(UnparsedDisk),
    Synthetic(RawSyntheticDisk),
}

impl RawDisk {
    pub fn is_boot_disk(&self) -> bool {
        match self {
            Self::Real(disk) => disk.is_boot_disk(),
            Self::Synthetic(disk) => {
                // Just label any M.2 the boot disk.
                disk.variant == DiskVariant::M2
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
            Self::Synthetic(disk) => disk.variant,
        }
    }

    pub fn is_synthetic(&self) -> bool {
        match self {
            Self::Real(_) => false,
            Self::Synthetic(_) => true,
        }
    }

    pub fn u2_zpool_path(&self) -> Result<Utf8PathBuf, PooledDiskError> {
        if !matches!(self.variant(), DiskVariant::U2) {
            return Err(PooledDiskError::UnexpectedVariant);
        }
        match self {
            Self::Real(disk) => {
                let paths = disk.paths();
                // This is hard-coded to be "0", but that's because we aren't
                // really parsing the whole partition table before considering
                // where this would be see.
                paths
                    .partition_path(0, false)
                    .ok_or_else(|| PooledDiskError::ZpoolDoesNotExist)
            }
            Self::Synthetic(raw) => Ok(raw.path.clone()),
        }
    }

    pub fn devfs_path(&self) -> &Utf8PathBuf {
        match self {
            Self::Real(disk) => disk.devfs_path(),
            Self::Synthetic(_) => unreachable!(),
        }
    }

    pub fn pcie_slot(&self) -> i64 {
        match self {
            Self::Real(disk) => disk.pcie_slot(),
            Self::Synthetic(disk) => disk.pcie_slot,
        }
    }

    /// See [`UnparsedDisk::location`].
    pub fn location(&self) -> &str {
        match self {
            Self::Real(disk) => disk.location(),
            Self::Synthetic(disk) => &disk.location,
        }
    }

    pub fn firmware(&self) -> &DiskFirmware {
        match self {
            RawDisk::Real(unparsed) => unparsed.firmware(),
            RawDisk::Synthetic(synthetic) => &synthetic.firmware,
        }
    }

    #[cfg(feature = "testing")]
    pub fn firmware_mut(&mut self) -> &mut DiskFirmware {
        match self {
            RawDisk::Real(unparsed) => unparsed.firmware_mut(),
            RawDisk::Synthetic(synthetic) => &mut synthetic.firmware,
        }
    }
}

impl IdOrdItem for RawDisk {
    type Key<'a> = &'a DiskIdentity;

    fn key(&self) -> Self::Key<'_> {
        self.identity()
    }

    id_upcast!();
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
        mount_config: &MountConfig,
        raw_disk: RawDisk,
        pool_id: Option<ZpoolUuid>,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<Self, DiskError> {
        let disk: Disk = match raw_disk {
            RawDisk::Real(disk) => {
                PooledDisk::new(log, disk, pool_id).await?.into()
            }
            RawDisk::Synthetic(disk) => Disk::Synthetic(
                SyntheticDisk::new(log, mount_config, disk, pool_id).await,
            ),
        };
        dataset::ensure_zpool_has_datasets(
            log,
            mount_config,
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

    pub fn is_boot_disk(&self) -> bool {
        match self {
            Self::Real(disk) => disk.is_boot_disk,
            Self::Synthetic(disk) => {
                // Just label any M.2 the boot disk.
                disk.raw.variant == DiskVariant::M2
            }
        }
    }

    pub fn identity(&self) -> &DiskIdentity {
        match self {
            Self::Real(disk) => &disk.identity,
            Self::Synthetic(disk) => &disk.raw.identity,
        }
    }

    pub fn variant(&self) -> DiskVariant {
        self.zpool_name().kind().into()
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
            Self::Synthetic(_) => {
                Err(PooledDiskError::SyntheticDiskNoDevfsPath)
            }
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
            Self::Synthetic(_) => {
                Err(PooledDiskError::SyntheticDiskNoDevfsPath)
            }
        }
    }

    pub fn pcie_slot(&self) -> i64 {
        match self {
            Self::Real(disk) => disk.pcie_slot,
            Self::Synthetic(disk) => disk.raw.pcie_slot,
        }
    }

    /// See [`UnparsedDisk::location`].
    pub fn location(&self) -> &str {
        match self {
            Self::Real(disk) => &disk.location,
            Self::Synthetic(disk) => &disk.raw.location,
        }
    }

    /// Copies the properties that may change while a disk stays in place
    /// from `raw_disk`: its firmware metadata, which a firmware update
    /// changes, and its chassis location, which is re-read from the hardware
    /// topology on every poll.
    pub fn update_mutable_properties(&mut self, raw_disk: &RawDisk) {
        match self {
            Disk::Real(pooled_disk) => {
                pooled_disk.firmware = raw_disk.firmware().clone();
                pooled_disk.location = raw_disk.location().to_string();
            }
            Disk::Synthetic(synthetic_disk) => {
                synthetic_disk.raw.firmware = raw_disk.firmware().clone();
            }
        }
    }

    pub fn firmware(&self) -> &DiskFirmware {
        match self {
            Disk::Real(disk) => &disk.firmware,
            Disk::Synthetic(disk) => &disk.raw.firmware,
        }
    }
}

impl From<Disk> for RawDisk {
    fn from(disk: Disk) -> RawDisk {
        match disk {
            Disk::Real(pooled_disk) => RawDisk::Real(UnparsedDisk::new(
                pooled_disk.paths.devfs_path,
                pooled_disk.paths.dev_path,
                pooled_disk.pcie_slot,
                pooled_disk.zpool_name.kind().into(),
                pooled_disk.identity,
                pooled_disk.is_boot_disk,
                pooled_disk.firmware,
                pooled_disk.location,
            )),
            Disk::Synthetic(synthetic_disk) => {
                RawDisk::Synthetic(synthetic_disk.raw)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location_of(vdev: &str, slot: i64) -> String {
        RawSyntheticDisk::load(Utf8Path::new(vdev), slot)
            .expect("vdev name parses")
            .location
    }

    #[test]
    fn synthetic_location_matches_real_labels() {
        assert_eq!(location_of("u2_0.vdev", 7), "N0");
        assert_eq!(location_of("/some/dir/u2_9.vdev", 7), "N9");
        assert_eq!(location_of("m2_0.vdev", 7), "M.2 East");
        assert_eq!(location_of("m2_1.vdev", 7), "M.2 West");
    }

    #[test]
    fn synthetic_location_falls_back_to_slot() {
        // A non-numeric serial, such as a zpool UUID, cannot pick a bay, so
        // the caller's slot index does instead.
        assert_eq!(location_of("u2_deadbeef.vdev", 4), "N4");
        assert_eq!(location_of("m2_deadbeef.vdev", 0), "M.2 East");
        assert_eq!(location_of("m2_deadbeef.vdev", 5), "M.2 5");
    }

    #[test]
    fn synthetic_location_is_reported() {
        let disk = RawDisk::Synthetic(
            RawSyntheticDisk::load(Utf8Path::new("u2_3.vdev"), 0).unwrap(),
        );
        assert_eq!(disk.location(), "N3");
    }
}
