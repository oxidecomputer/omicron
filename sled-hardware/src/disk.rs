// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::{Utf8Path, Utf8PathBuf};
use illumos_utils::fstyp::Fstyp;
use illumos_utils::zfs;
use illumos_utils::zfs::DestroyDatasetErrorVariant;
use illumos_utils::zfs::EncryptionDetails;
use illumos_utils::zfs::Keypath;
use illumos_utils::zfs::Mountpoint;
use illumos_utils::zfs::SizeDetails;
use illumos_utils::zfs::Zfs;
use illumos_utils::zpool::Zpool;
use illumos_utils::zpool::ZpoolKind;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use rand::distributions::{Alphanumeric, DistString};
use slog::Logger;
use slog::{info, warn};
use std::sync::OnceLock;
use tokio::fs::{remove_file, File};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use uuid::Uuid;

/// This path is intentionally on a `tmpfs` to prevent copy-on-write behavior
/// and to ensure it goes away on power off.
///
/// We want minimize the time the key files are in memory, and so we rederive
/// the keys and recreate the files on demand when creating and mounting
/// encrypted filesystems. We then zero them and unlink them.
pub const KEYPATH_ROOT: &str = "/var/run/oxide/";

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        use crate::illumos::*;
    } else {
        use crate::non_illumos::*;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("Cannot open {path} due to {error}")]
    IoError { path: Utf8PathBuf, error: std::io::Error },
    #[error("Failed to open partition at {path} due to {error}")]
    Gpt { path: Utf8PathBuf, error: anyhow::Error },
    #[error("Unexpected partition layout at {path}: {why}")]
    BadPartitionLayout { path: Utf8PathBuf, why: String },
    #[error("Requested partition {partition:?} not found on device {path}")]
    NotFound { path: Utf8PathBuf, partition: Partition },
    #[error(transparent)]
    DestroyFilesystem(#[from] illumos_utils::zfs::DestroyDatasetError),
    #[error(transparent)]
    EnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),
    #[error(transparent)]
    ZpoolCreate(#[from] illumos_utils::zpool::CreateError),
    #[error("Cannot import zpool: {0}")]
    ZpoolImport(illumos_utils::zpool::Error),
    #[error("Cannot format {path}: missing a '/dev' path")]
    CannotFormatMissingDevPath { path: Utf8PathBuf },
    #[error("Formatting M.2 devices is not yet implemented")]
    CannotFormatM2NotImplemented,
    #[error("KeyManager error: {0}")]
    KeyManager(#[from] key_manager::Error),
    #[error("Missing StorageKeyRequester when creating U.2 disk")]
    MissingStorageKeyRequester,
    #[error("Encrypted filesystem '{0}' missing 'oxide:epoch' property")]
    CannotParseEpochProperty(String),
    #[error("Encrypted dataset '{dataset}' cannot set 'oxide:agent' property: {err}")]
    CannotSetAgentProperty {
        dataset: String,
        #[source]
        err: Box<zfs::SetValueError>,
    },
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
    pub devfs_path: Utf8PathBuf,
    // Optional path to the disk under "/dev/dsk".
    pub dev_path: Option<Utf8PathBuf>,
}

impl DiskPaths {
    // Returns the "illumos letter-indexed path" for a device.
    fn partition_path(&self, index: usize, raw: bool) -> Option<Utf8PathBuf> {
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
    fn partition_device_path(
        &self,
        partitions: &[Partition],
        expected_partition: Partition,
        raw: bool,
    ) -> Result<Utf8PathBuf, DiskError> {
        for (index, partition) in partitions.iter().enumerate() {
            if &expected_partition == partition {
                let path =
                    self.partition_path(index, raw).ok_or_else(|| {
                        DiskError::NotFound {
                            path: self.devfs_path.clone(),
                            partition: expected_partition,
                        }
                    })?;
                return Ok(path);
            }
        }
        Err(DiskError::NotFound {
            path: self.devfs_path.clone(),
            partition: expected_partition,
        })
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
    is_boot_disk: bool,
}

impl UnparsedDisk {
    #[allow(dead_code)]
    pub fn new(
        devfs_path: Utf8PathBuf,
        dev_path: Option<Utf8PathBuf>,
        slot: i64,
        variant: DiskVariant,
        identity: DiskIdentity,
        is_boot_disk: bool,
    ) -> Self {
        Self {
            paths: DiskPaths { devfs_path, dev_path },
            slot,
            variant,
            identity,
            is_boot_disk,
        }
    }

    pub fn devfs_path(&self) -> &Utf8PathBuf {
        &self.paths.devfs_path
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
}

/// A physical disk conforming to the expected partition layout.
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

// Helper type for describing expected datasets and their optional quota.
#[derive(Clone, Copy, Debug)]
struct ExpectedDataset {
    // Name for the dataset
    name: &'static str,
    // Optional quota, in _bytes_
    quota: Option<usize>,
    // Identifies if the dataset should be deleted on boot
    wipe: bool,
    // Optional compression mode
    compression: Option<&'static str>,
}

impl ExpectedDataset {
    const fn new(name: &'static str) -> Self {
        ExpectedDataset { name, quota: None, wipe: false, compression: None }
    }

    const fn quota(mut self, quota: usize) -> Self {
        self.quota = Some(quota);
        self
    }

    const fn wipe(mut self) -> Self {
        self.wipe = true;
        self
    }

    const fn compression(mut self, compression: &'static str) -> Self {
        self.compression = Some(compression);
        self
    }
}

pub const INSTALL_DATASET: &'static str = "install";
pub const CRASH_DATASET: &'static str = "crash";
pub const CLUSTER_DATASET: &'static str = "cluster";
pub const CONFIG_DATASET: &'static str = "config";
pub const M2_DEBUG_DATASET: &'static str = "debug";
pub const M2_BACKING_DATASET: &'static str = "backing";
// TODO-correctness: This value of 100GiB is a pretty wild guess, and should be
// tuned as needed.
pub const DEBUG_DATASET_QUOTA: usize = 100 * (1 << 30);
// ditto.
pub const DUMP_DATASET_QUOTA: usize = 100 * (1 << 30);
// passed to zfs create -o compression=
pub const DUMP_DATASET_COMPRESSION: &'static str = "gzip-9";

// U.2 datasets live under the encrypted dataset and inherit encryption
pub const ZONE_DATASET: &'static str = "crypt/zone";
pub const DUMP_DATASET: &'static str = "crypt/debug";
pub const U2_DEBUG_DATASET: &'static str = "crypt/debug";

// This is the root dataset for all U.2 drives. Encryption is inherited.
pub const CRYPT_DATASET: &'static str = "crypt";

const U2_EXPECTED_DATASET_COUNT: usize = 2;
static U2_EXPECTED_DATASETS: [ExpectedDataset; U2_EXPECTED_DATASET_COUNT] = [
    // Stores filesystems for zones
    ExpectedDataset::new(ZONE_DATASET).wipe(),
    // For storing full kernel RAM dumps
    ExpectedDataset::new(DUMP_DATASET)
        .quota(DUMP_DATASET_QUOTA)
        .compression(DUMP_DATASET_COMPRESSION),
];

const M2_EXPECTED_DATASET_COUNT: usize = 6;
static M2_EXPECTED_DATASETS: [ExpectedDataset; M2_EXPECTED_DATASET_COUNT] = [
    // Stores software images.
    //
    // Should be duplicated to both M.2s.
    ExpectedDataset::new(INSTALL_DATASET),
    // Stores crash dumps.
    ExpectedDataset::new(CRASH_DATASET),
    // Backing store for OS data that should be persisted across reboots.
    // Its children are selectively overlay mounted onto parts of the ramdisk
    // root.
    ExpectedDataset::new(M2_BACKING_DATASET),
    // Stores cluster configuration information.
    //
    // Should be duplicated to both M.2s.
    ExpectedDataset::new(CLUSTER_DATASET),
    // Stores configuration data, including:
    // - What services should be launched on this sled
    // - Information about how to initialize the Sled Agent
    // - (For scrimlets) RSS setup information
    //
    // Should be duplicated to both M.2s.
    ExpectedDataset::new(CONFIG_DATASET),
    // Store debugging data, such as service bundles.
    ExpectedDataset::new(M2_DEBUG_DATASET).quota(DEBUG_DATASET_QUOTA),
];

impl Disk {
    /// Create a new Disk
    ///
    /// WARNING: In all cases where a U.2 is a possible `DiskVariant`, a
    /// `StorageKeyRequester` must be passed so that disk encryption can
    /// be used. The `StorageManager` for the sled-agent  always has a
    /// `StorageKeyRequester` available, and so the only place we should pass
    /// `None` is for the M.2s touched by the Installinator.
    pub async fn new(
        log: &Logger,
        unparsed_disk: UnparsedDisk,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<Self, DiskError> {
        let paths = &unparsed_disk.paths;
        let variant = unparsed_disk.variant;
        // Ensure the GPT has the right format. This does not necessarily
        // mean that the partitions are populated with the data we need.
        let partitions = ensure_partition_layout(&log, &paths, variant)?;

        // Find the path to the zpool which exists on this disk.
        //
        // NOTE: At the moment, we're hard-coding the assumption that at least
        // one zpool should exist on every disk.
        let zpool_path = paths.partition_device_path(
            &partitions,
            Partition::ZfsPool,
            false,
        )?;

        let zpool_name = Self::ensure_zpool_exists(log, variant, &zpool_path)?;
        Self::ensure_zpool_ready(
            log,
            &zpool_name,
            &unparsed_disk.identity,
            key_requester,
        )
        .await?;

        Ok(Self {
            paths: unparsed_disk.paths,
            slot: unparsed_disk.slot,
            variant: unparsed_disk.variant,
            identity: unparsed_disk.identity,
            is_boot_disk: unparsed_disk.is_boot_disk,
            partitions,
            zpool_name,
        })
    }

    pub async fn ensure_zpool_ready(
        log: &Logger,
        zpool_name: &ZpoolName,
        disk_identity: &DiskIdentity,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<(), DiskError> {
        Self::ensure_zpool_imported(log, &zpool_name)?;
        Self::ensure_zpool_failmode_is_continue(log, &zpool_name)?;
        Self::ensure_zpool_has_datasets(
            log,
            &zpool_name,
            disk_identity,
            key_requester,
        )
        .await?;
        Ok(())
    }

    fn ensure_zpool_exists(
        log: &Logger,
        variant: DiskVariant,
        zpool_path: &Utf8Path,
    ) -> Result<ZpoolName, DiskError> {
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
                    "GPT exists without Zpool: formatting zpool at {}",
                    zpool_path,
                );
                // If a zpool does not already exist, create one.
                let zpool_name = match variant {
                    DiskVariant::M2 => ZpoolName::new_internal(Uuid::new_v4()),
                    DiskVariant::U2 => ZpoolName::new_external(Uuid::new_v4()),
                };
                Zpool::create(zpool_name.clone(), &zpool_path)?;
                zpool_name
            }
        };
        Zpool::import(zpool_name.clone()).map_err(|e| {
            warn!(log, "Failed to import zpool {zpool_name}: {e}");
            DiskError::ZpoolImport(e)
        })?;

        Ok(zpool_name)
    }

    fn ensure_zpool_imported(
        log: &Logger,
        zpool_name: &ZpoolName,
    ) -> Result<(), DiskError> {
        Zpool::import(zpool_name.clone()).map_err(|e| {
            warn!(log, "Failed to import zpool {zpool_name}: {e}");
            DiskError::ZpoolImport(e)
        })?;
        Ok(())
    }

    fn ensure_zpool_failmode_is_continue(
        log: &Logger,
        zpool_name: &ZpoolName,
    ) -> Result<(), DiskError> {
        // Ensure failmode is set to `continue`. See
        // https://github.com/oxidecomputer/omicron/issues/2766 for details. The
        // short version is, each pool is only backed by one vdev. There is no
        // recovery if one starts breaking, so if connectivity to one dies it's
        // actively harmful to try to wait for it to come back; we'll be waiting
        // forever and get stuck. We'd rather get the errors so we can deal with
        // them ourselves.
        Zpool::set_failmode_continue(&zpool_name).map_err(|e| {
            warn!(
                log,
                "Failed to set failmode=continue on zpool {zpool_name}: {e}"
            );
            DiskError::ZpoolImport(e)
        })?;
        Ok(())
    }

    // Ensure that the zpool contains all the datasets we would like it to
    // contain.
    async fn ensure_zpool_has_datasets(
        log: &Logger,
        zpool_name: &ZpoolName,
        disk_identity: &DiskIdentity,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<(), DiskError> {
        let (root, datasets) = match zpool_name.kind().into() {
            DiskVariant::M2 => (None, M2_EXPECTED_DATASETS.iter()),
            DiskVariant::U2 => {
                (Some(CRYPT_DATASET), U2_EXPECTED_DATASETS.iter())
            }
        };

        let zoned = false;
        let do_format = true;

        // Ensure the root encrypted filesystem exists
        // Datasets below this in the hierarchy will inherit encryption
        if let Some(dataset) = root {
            let Some(key_requester) = key_requester else {
                return Err(DiskError::MissingStorageKeyRequester);
            };
            let mountpoint = zpool_name.dataset_mountpoint(dataset);
            let keypath: Keypath = disk_identity.into();

            let epoch =
                if let Ok(epoch_str) = Zfs::get_oxide_value(dataset, "epoch") {
                    if let Ok(epoch) = epoch_str.parse::<u64>() {
                        epoch
                    } else {
                        return Err(DiskError::CannotParseEpochProperty(
                            dataset.to_string(),
                        ));
                    }
                } else {
                    // We got an error trying to call `Zfs::get_oxide_value`
                    // which indicates that the dataset doesn't exist or there
                    // was a problem  running the command.
                    //
                    // Note that `Zfs::get_oxide_value` will succeed even if
                    // the epoch is missing. `epoch_str` will show up as a dash
                    // (`-`) and will not parse into a `u64`. So we don't have
                    // to worry about that case here as it is handled above.
                    //
                    // If the error indicated that the command failed for some
                    // other reason, but the dataset actually existed, we will
                    // try to create the dataset below and that will fail. So
                    // there is no harm in just loading the latest secret here.
                    key_requester.load_latest_secret().await?
                };

            let key =
                key_requester.get_key(epoch, disk_identity.clone()).await?;

            let mut keyfile =
                KeyFile::create(keypath.clone(), key.expose_secret(), log)
                    .await
                    .map_err(|error| DiskError::IoError {
                        path: keypath.0.clone(),
                        error,
                    })?;

            let encryption_details = EncryptionDetails { keypath, epoch };

            info!(
                log,
                "Ensuring encrypted filesystem: {} for epoch {}",
                dataset,
                epoch
            );
            let result = Zfs::ensure_filesystem(
                &format!("{}/{}", zpool_name, dataset),
                Mountpoint::Path(mountpoint),
                zoned,
                do_format,
                Some(encryption_details),
                None,
                None,
                None,
            );

            keyfile.zero_and_unlink().await.map_err(|error| {
                DiskError::IoError { path: keyfile.path().0.clone(), error }
            })?;

            result?;
        };

        for dataset in datasets.into_iter() {
            let mountpoint = zpool_name.dataset_mountpoint(dataset.name);
            let name = &format!("{}/{}", zpool_name, dataset.name);

            // Use a value that's alive for the duration of this sled agent
            // to answer the question: should we wipe this disk, or have
            // we seen it before?
            //
            // If this value comes from a prior iteration of the sled agent,
            // we opt to remove the corresponding dataset.
            static AGENT_LOCAL_VALUE: OnceLock<String> = OnceLock::new();
            let agent_local_value = AGENT_LOCAL_VALUE.get_or_init(|| {
                Alphanumeric.sample_string(&mut rand::thread_rng(), 20)
            });

            if dataset.wipe {
                match Zfs::get_oxide_value(name, "agent") {
                    Ok(v) if &v == agent_local_value => {
                        info!(
                            log,
                            "Skipping automatic wipe for dataset: {}", name
                        );
                    }
                    Ok(_) | Err(_) => {
                        info!(
                            log,
                            "Automatically destroying dataset: {}", name
                        );
                        Zfs::destroy_dataset(name).or_else(|err| {
                            // If we can't find the dataset, that's fine -- it
                            // might not have been formatted yet.
                            if let DestroyDatasetErrorVariant::NotFound =
                                err.err
                            {
                                Ok(())
                            } else {
                                Err(err)
                            }
                        })?;
                    }
                }
            }

            let encryption_details = None;
            let size_details = Some(SizeDetails {
                quota: dataset.quota,
                compression: dataset.compression,
            });
            Zfs::ensure_filesystem(
                name,
                Mountpoint::Path(mountpoint),
                zoned,
                do_format,
                encryption_details,
                size_details,
                None,
                None,
            )?;

            if dataset.wipe {
                Zfs::set_oxide_value(name, "agent", agent_local_value)
                    .map_err(|err| DiskError::CannotSetAgentProperty {
                        dataset: name.clone(),
                        err: Box::new(err),
                    })?;
            }
        }
        Ok(())
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
    ) -> Result<Utf8PathBuf, DiskError> {
        self.paths.partition_device_path(
            &self.partitions,
            Partition::BootImage,
            raw,
        )
    }

    pub fn dump_device_devfs_path(
        &self,
        raw: bool,
    ) -> Result<Utf8PathBuf, DiskError> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
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

/// A file that wraps a zfs encryption key.
///
/// We put this in a RAM backed filesystem and zero and delete it when we are
/// done with it. Unfortunately we cannot do this inside `Drop` because there is no
/// equivalent async drop.
pub struct KeyFile {
    path: Keypath,
    file: File,
    log: Logger,
}

impl KeyFile {
    pub async fn create(
        path: Keypath,
        key: &[u8; 32],
        log: &Logger,
    ) -> std::io::Result<KeyFile> {
        // TODO: fix this to not truncate
        // We want to overwrite any existing contents.
        // If we truncate we may leave dirty pages around
        // containing secrets.
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path.0)
            .await?;
        file.write_all(key).await?;
        info!(log, "Created keyfile {}", path);
        Ok(KeyFile { path, file, log: log.clone() })
    }

    /// These keyfiles live on a tmpfs and we zero the file so the data doesn't
    /// linger on the page in memory.
    ///
    /// It'd be nice to `impl Drop for `KeyFile` and then call `zero`
    /// from within the drop handler, but async `Drop` isn't supported.
    pub async fn zero_and_unlink(&mut self) -> std::io::Result<()> {
        let zeroes = [0u8; 32];
        let _ = self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&zeroes).await?;
        info!(self.log, "Zeroed and unlinked keyfile {}", self.path);
        remove_file(&self.path().0).await?;
        Ok(())
    }

    pub fn path(&self) -> &Keypath {
        &self.path
    }
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
            DiskError::NotFound { .. },
        ));
    }
}
