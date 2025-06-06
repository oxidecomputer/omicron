// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::{Utf8Path, Utf8PathBuf};
use illumos_devinfo::DevInfo;
use illumos_devinfo::DevLinkType;
use illumos_devinfo::DevLinks;
use illumos_devinfo::Node;
use illumos_utils::fstyp::Fstyp;
use illumos_utils::zpool::Api;
use illumos_utils::zpool::Zpool;
use omicron_common::disk::{DiskIdentity, DiskVariant};
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::ZpoolUuid;
use serde::{Deserialize, Serialize};
use slog::Logger;
use slog::{info, warn};

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        use crate::illumos::*;
    } else {
        use crate::non_illumos::*;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PooledDiskError {
    #[error("Cannot open {path} due to {error}")]
    IoError { path: Utf8PathBuf, error: std::io::Error },
    #[error("Failed to open partition at {path} due to {error}")]
    Gpt { path: Utf8PathBuf, error: anyhow::Error },
    #[error("Unexpected partition layout at {path}: {why}")]
    BadPartitionLayout { path: Utf8PathBuf, why: String },
    #[error("Requested partition {partition:?} not found on device {path}")]
    NotFound { path: Utf8PathBuf, partition: Partition },
    #[error("Zpool UUID required to format this disk")]
    MissingZpoolUuid,
    #[error(
        "Observed Zpool with unexpected UUID (saw: {observed}, expected: {expected})"
    )]
    UnexpectedUuid { expected: ZpoolUuid, observed: ZpoolUuid },
    #[error("Unexpected disk variant")]
    UnexpectedVariant,
    #[error("Zpool does not exist")]
    ZpoolDoesNotExist,
    #[error(transparent)]
    ZpoolCreate(#[from] illumos_utils::zpool::CreateError),
    #[error("Cannot import zpool: {0}")]
    ZpoolImport(illumos_utils::zpool::Error),
    #[error("Cannot format {path}: missing a '/dev' path")]
    CannotFormatMissingDevPath { path: Utf8PathBuf },
    #[error("Formatting M.2 devices is not yet implemented")]
    CannotFormatM2NotImplemented,
    #[error(transparent)]
    NvmeFormatAndResize(#[from] NvmeFormattingError),
    #[error("Invalid Utf8 path: {0}")]
    FromPathBuf(#[from] camino::FromPathBufError),
    #[error("Failed to access devinfo: {0}")]
    DevInfo(anyhow::Error),
    #[error("Could not translate {0} to '/dev' path: no links")]
    NoDevLinks(Utf8PathBuf),
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

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Deserialize, Serialize,
)]
pub struct DiskPaths {
    // Full path to the disk under "/devices".
    // Should NOT end with a ":partition_letter".
    pub devfs_path: Utf8PathBuf,
    // Optional path to the disk under "/dev/dsk".
    pub dev_path: Option<Utf8PathBuf>,
}

impl DiskPaths {
    // Returns the "illumos letter-indexed path" for a device.
    pub fn partition_path(
        &self,
        index: usize,
        raw: bool,
    ) -> Option<Utf8PathBuf> {
        let index = u8::try_from(index).ok()?;

        let path = &self.devfs_path;
        let character = match index {
            0..=5 => (b'a' + index) as char,
            _ => return None,
        };
        Some(Utf8PathBuf::from(format!(
            "{path}:{character}{suffix}",
            suffix = if raw { ",raw" } else { "" }
        )))
    }

    /// Returns the path to the whole disk
    #[allow(dead_code)]
    pub(crate) fn whole_disk(&self, raw: bool) -> Utf8PathBuf {
        let path = &self.devfs_path;
        Utf8PathBuf::from(format!(
            "{path}:wd{raw}",
            raw = if raw { ",raw" } else { "" },
        ))
    }

    // Finds the first 'variant' partition, and returns the path to it.
    pub fn partition_device_path(
        &self,
        partitions: &[Partition],
        expected_partition: Partition,
        raw: bool,
    ) -> Result<Utf8PathBuf, PooledDiskError> {
        for (index, partition) in partitions.iter().enumerate() {
            if &expected_partition == partition {
                let path =
                    self.partition_path(index, raw).ok_or_else(|| {
                        PooledDiskError::NotFound {
                            path: self.devfs_path.clone(),
                            partition: expected_partition,
                        }
                    })?;
                return Ok(path);
            }
        }
        Err(PooledDiskError::NotFound {
            path: self.devfs_path.clone(),
            partition: expected_partition,
        })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Deserialize, Serialize,
)]
pub struct DiskFirmware {
    active_slot: u8,
    next_active_slot: Option<u8>,
    slot1_read_only: bool,
    number_of_slots: u8,
    // NB: This vec is 0 indexed while active_slot and next_active_slot are
    // referring to "slots" in terms of the NVMe spec which defines slots 1-7.
    // If the active_slot is 1, then it will be slot_firmware_versions[0] in the
    // vector.
    slot_firmware_versions: Vec<Option<String>>,
}

impl DiskFirmware {
    pub fn active_slot(&self) -> u8 {
        self.active_slot
    }

    pub fn next_active_slot(&self) -> Option<u8> {
        self.next_active_slot
    }

    pub fn slot1_read_only(&self) -> bool {
        self.slot1_read_only
    }

    pub fn number_of_slots(&self) -> u8 {
        self.number_of_slots
    }

    pub fn slots(&self) -> &[Option<String>] {
        self.slot_firmware_versions.as_slice()
    }
}

impl DiskFirmware {
    pub fn new(
        active_slot: u8,
        next_active_slot: Option<u8>,
        slot1_read_only: bool,
        number_of_slots: u8,
        slots: Vec<Option<String>>,
    ) -> Self {
        Self {
            active_slot,
            next_active_slot,
            slot1_read_only,
            number_of_slots,
            slot_firmware_versions: slots,
        }
    }
}

/// A disk which has been observed by monitoring hardware.
///
/// No guarantees are made about the partitions which exist within this disk.
/// This exists as a distinct entity from `Disk` in `sled-storage` because it
/// may be desirable to monitor for hardware in one context, and conform disks
/// to partition layouts in a different context.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Deserialize, Serialize,
)]
pub struct UnparsedDisk {
    nvme_instance: i32,
    slot: i64,
    variant: DiskVariant,
    identity: DiskIdentity,
    is_boot_disk: bool,
    firmware: DiskFirmware,
}

impl UnparsedDisk {
    pub fn new(
        nvme_instance: i32,
        slot: i64,
        variant: DiskVariant,
        identity: DiskIdentity,
        is_boot_disk: bool,
        firmware: DiskFirmware,
    ) -> Self {
        Self { nvme_instance, slot, variant, identity, is_boot_disk, firmware }
    }

    pub fn variant(&self) -> DiskVariant {
        self.variant
    }

    pub fn identity(&self) -> &DiskIdentity {
        &self.identity
    }

    pub fn is_boot_disk(&self) -> bool {
        self.is_boot_disk
    }

    pub fn slot(&self) -> i64 {
        self.slot
    }

    pub fn firmware(&self) -> &DiskFirmware {
        &self.firmware
    }

    #[cfg(feature = "testing")]
    pub fn firmware_mut(&mut self) -> &mut DiskFirmware {
        &mut self.firmware
    }
}

/// A physical disk that is partitioned to contain exactly one zpool
///
/// A PooledDisk relies on hardware specific information to be constructed
/// and is the highest level disk structure in the `sled-hardware` package.
/// The `sled-storage` package contains `Disk`s whose zpool and datasets can be
/// manipulated. This separation exists to remove the hardware dependent logic
/// from the ZFS related logic which can also operate on file backed zpools.
/// Doing things this way allows us to not put higher level concepts like
/// storage keys into this hardware related package.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PooledDisk {
    pub paths: DiskPaths,
    pub slot: i64,
    pub nvme_instance: i32,
    pub variant: DiskVariant,
    pub identity: DiskIdentity,
    pub is_boot_disk: bool,
    pub partitions: Vec<Partition>,
    // This embeds the assumtion that there is exactly one parsed zpool per
    // disk.
    pub zpool_name: ZpoolName,
    pub firmware: DiskFirmware,
}

impl PooledDisk {
    /// Create a new PooledDisk
    pub async fn new(
        log: &Logger,
        unparsed_disk: UnparsedDisk,
        zpool_id: Option<ZpoolUuid>,
    ) -> Result<Self, PooledDiskError> {
        let paths = find_disk_paths(&unparsed_disk)?;
        let variant = unparsed_disk.variant;
        let identity = &unparsed_disk.identity;
        // Ensure the GPT has the right format. This does not necessarily
        // mean that the partitions are populated with the data we need.
        let partitions =
            ensure_partition_layout(&log, &paths, variant, identity, zpool_id)
                .await?;

        // Find the path to the zpool which exists on this disk.
        //
        // NOTE: At the moment, we're hard-coding the assumption that at least
        // one zpool should exist on every disk.
        let zpool_path = paths.partition_device_path(
            &partitions,
            Partition::ZfsPool,
            false,
        )?;

        let zpool_name =
            ensure_zpool_exists(log, variant, &zpool_path, zpool_id).await?;
        ensure_zpool_imported(log, &zpool_name).await?;
        ensure_zpool_failmode_is_continue(log, &zpool_name).await?;

        Ok(Self {
            paths,
            slot: unparsed_disk.slot,
            nvme_instance: unparsed_disk.nvme_instance,
            variant: unparsed_disk.variant,
            identity: unparsed_disk.identity,
            is_boot_disk: unparsed_disk.is_boot_disk,
            partitions,
            zpool_name,
            firmware: unparsed_disk.firmware,
        })
    }
}

fn find_disk_paths(
    unparsed_disk: &UnparsedDisk,
) -> Result<DiskPaths, PooledDiskError> {
    let mut devinfo = DevInfo::new().map_err(PooledDiskError::DevInfo)?;
    let blkdev = devinfo.walk_driver("blkdev");
    let found: Vec<_> = blkdev
        .filter_map(|res| {
            let node = res.ok()?;
            let parent = node.parent().ok()??;
            if parent.instance() == Some(unparsed_disk.nvme_instance) {
                Some(node)
            } else {
                None
            }
        })
        .collect();
    let node = found
        .first()
        // XXX we can attempt to reset the state of the world by
        // looking at namespaces and attempting to attach blkdev to a
        // nvme namespace, we also want to verify that only a single
        // namespace is present.
        .expect("blkdev not attached");
    let devfs_path = node.devfs_path().map_err(PooledDiskError::DevInfo)?;
    let dev_path = get_dev_path_of_whole_disk(&node)?;
    assert!(devfs_path.starts_with('/'));
    let devfs_path = format!("/devices{devfs_path}");

    Ok(DiskPaths { devfs_path: Utf8PathBuf::from(&devfs_path), dev_path })
}

fn get_dev_path_of_whole_disk(
    node: &Node<'_>,
) -> Result<Option<Utf8PathBuf>, PooledDiskError> {
    let mut wm = node.minors();
    while let Some(m) =
        wm.next().transpose().map_err(PooledDiskError::DevInfo)?
    {
        // "wd" stands for "whole disk"
        if m.name() != "wd" {
            continue;
        }
        let links = {
            match DevLinks::new(true) {
                Ok(links) => links,
                Err(_) => {
                    DevLinks::new(false).map_err(PooledDiskError::DevInfo)?
                }
            }
        };
        let devfs_path = m.devfs_path().map_err(PooledDiskError::DevInfo)?;

        let paths = links
            .links_for_path(&devfs_path)
            .map_err(PooledDiskError::DevInfo)?
            .into_iter()
            .filter(|l| {
                // Devices in "/dev/dsk" have names that denote their purpose,
                // of the form "controller, disk, slice" or "controller, disk,
                // partition".
                //
                // The suffix of "d0" is typical of an individual disk, and is
                // the expected device to correspond with the "wd" device in
                // the "/devices" hierarchy.
                l.linktype() == DevLinkType::Primary
                    && l.path()
                        .file_name()
                        .map(|f| f.to_string_lossy().ends_with("d0"))
                        .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        if paths.is_empty() {
            return Err(PooledDiskError::NoDevLinks(Utf8PathBuf::from(
                devfs_path,
            )));
        }
        return Ok(Some(paths[0].path().to_path_buf().try_into()?));
    }
    Ok(None)
}

/// Checks if the zpool exists, but makes no modifications,
/// and does not attempt to import the zpool.
pub async fn check_if_zpool_exists(
    zpool_path: &Utf8Path,
) -> Result<ZpoolName, PooledDiskError> {
    let zpool_name = match Fstyp::get_zpool(&zpool_path).await {
        Ok(zpool_name) => zpool_name,
        Err(_) => return Err(PooledDiskError::ZpoolDoesNotExist),
    };
    Ok(zpool_name)
}

pub async fn ensure_zpool_exists(
    log: &Logger,
    variant: DiskVariant,
    zpool_path: &Utf8Path,
    zpool_id: Option<ZpoolUuid>,
) -> Result<ZpoolName, PooledDiskError> {
    let zpool_name = match Fstyp::get_zpool(&zpool_path).await {
        Ok(zpool_name) => {
            if let Some(expected) = zpool_id {
                info!(log, "Checking that UUID in storage matches request"; "expected" => ?expected);
                let observed = zpool_name.id();
                if expected != observed {
                    warn!(log, "Zpool UUID mismatch"; "expected" => ?expected, "observed" => ?observed);
                    return Err(PooledDiskError::UnexpectedUuid {
                        expected,
                        observed,
                    });
                }
            }
            zpool_name
        }
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
                "GPT exists without Zpool: formatting zpool at {}", zpool_path,
            );
            let id = match zpool_id {
                Some(id) => {
                    info!(log, "Formatting zpool with requested ID"; "id" => ?id);
                    id
                }
                None => {
                    let id = ZpoolUuid::new_v4();
                    info!(log, "Formatting zpool with generated ID"; "id" => ?id);
                    id
                }
            };

            // If a zpool does not already exist, create one.
            let zpool_name = match variant {
                DiskVariant::M2 => ZpoolName::new_internal(id),
                DiskVariant::U2 => ZpoolName::new_external(id),
            };
            Zpool::real_api().create(&zpool_name, &zpool_path).await?;
            zpool_name
        }
    };
    Zpool::import(&zpool_name).await.map_err(|e| {
        warn!(log, "Failed to import zpool {zpool_name}: {e}");
        PooledDiskError::ZpoolImport(e)
    })?;

    Ok(zpool_name)
}

pub async fn ensure_zpool_imported(
    log: &Logger,
    zpool_name: &ZpoolName,
) -> Result<(), PooledDiskError> {
    Zpool::import(&zpool_name).await.map_err(|e| {
        warn!(log, "Failed to import zpool {zpool_name}: {e}");
        PooledDiskError::ZpoolImport(e)
    })?;
    Ok(())
}

pub async fn ensure_zpool_failmode_is_continue(
    log: &Logger,
    zpool_name: &ZpoolName,
) -> Result<(), PooledDiskError> {
    // Ensure failmode is set to `continue`. See
    // https://github.com/oxidecomputer/omicron/issues/2766 for details. The
    // short version is, each pool is only backed by one vdev. There is no
    // recovery if one starts breaking, so if connectivity to one dies it's
    // actively harmful to try to wait for it to come back; we'll be waiting
    // forever and get stuck. We'd rather get the errors so we can deal with
    // them ourselves.
    Zpool::set_failmode_continue(&zpool_name).await.map_err(|e| {
        warn!(
            log,
            "Failed to set failmode=continue on zpool {zpool_name}: {e}"
        );
        PooledDiskError::ZpoolImport(e)
    })?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_disk_paths() {
        const DEVFS_PATH: &'static str = "/devices/my/disk";
        let paths = DiskPaths {
            devfs_path: Utf8PathBuf::from(DEVFS_PATH),
            dev_path: None,
        };
        assert_eq!(
            paths.whole_disk(false),
            Utf8PathBuf::from(format!("{DEVFS_PATH}:wd"))
        );
        assert_eq!(
            paths.whole_disk(true),
            Utf8PathBuf::from(format!("{DEVFS_PATH}:wd,raw"))
        );
        assert_eq!(
            paths.partition_path(0, false),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:a")))
        );
        assert_eq!(
            paths.partition_path(1, false),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:b")))
        );
        assert_eq!(
            paths.partition_path(2, false),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:c")))
        );
        assert_eq!(
            paths.partition_path(3, false),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:d")))
        );
        assert_eq!(
            paths.partition_path(4, false),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:e")))
        );
        assert_eq!(
            paths.partition_path(5, false),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:f")))
        );
        assert_eq!(paths.partition_path(6, false), None);

        assert_eq!(
            paths.partition_path(0, true),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:a,raw")))
        );
        assert_eq!(
            paths.partition_path(1, true),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:b,raw")))
        );
        assert_eq!(
            paths.partition_path(2, true),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:c,raw")))
        );
        assert_eq!(
            paths.partition_path(3, true),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:d,raw")))
        );
        assert_eq!(
            paths.partition_path(4, true),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:e,raw")))
        );
        assert_eq!(
            paths.partition_path(5, true),
            Some(Utf8PathBuf::from(format!("{DEVFS_PATH}:f,raw")))
        );
        assert_eq!(paths.partition_path(6, true), None);
    }

    #[test]
    fn test_partition_device_paths() {
        const DEVFS_PATH: &'static str = "/devices/my/disk";
        let paths = DiskPaths {
            devfs_path: Utf8PathBuf::from(DEVFS_PATH),
            dev_path: None,
        };

        assert_eq!(
            paths
                .partition_device_path(
                    &[Partition::ZfsPool],
                    Partition::ZfsPool,
                    false,
                )
                .expect("Should have found partition"),
            paths.partition_path(0, false).unwrap(),
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
                    false,
                )
                .expect("Should have found partition"),
            paths.partition_path(2, false).unwrap(),
        );

        assert!(matches!(
            paths
                .partition_device_path(&[], Partition::ZfsPool, false)
                .expect_err("Should not have found partition"),
            PooledDiskError::NotFound { .. },
        ));
    }
}
