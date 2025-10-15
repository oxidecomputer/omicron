// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for creating ZFS vdevs for tests that need to exercise real ZFS
//! operations (snapshots, dataset property management, etc.).

use anyhow::Context;
use anyhow::bail;
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use illumos_utils::ExecutionError;
use illumos_utils::PFEXEC;
use illumos_utils::zfs::CanMount;
use illumos_utils::zfs::DatasetEnsureArgs;
use illumos_utils::zfs::Mountpoint;
use illumos_utils::zfs::Zfs;
use key_manager::KeyManager;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::SharedDatasetConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalZpoolUuid;
use omicron_uuid_kinds::InternalZpoolUuid;
use sled_storage::config::MountConfig;
use sled_storage::dataset::M2_DEBUG_DATASET;
use sled_storage::disk::Disk;
use sled_storage::disk::RawDisk;
use sled_storage::disk::RawSyntheticDisk;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::io;
use std::io::BufRead;
use std::process::Command;
use std::sync::Arc;

pub struct ZfsTestHarness {
    // Always `Some(_)`, except in `cleanup()` and `drop()`.
    inner: Option<Inner>,
}

impl Drop for ZfsTestHarness {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            // cleanup() already called! we're done.
            return;
        };
        eprintln!(
            "WARNING: ZfsTestHarness dropped without calling 'cleanup()'.\n\
             Attempting automated cleanup of {}",
            inner.vdev_dir.path(),
        );
        if let Err(errs) = inner.cleanup() {
            eprintln!("FAILED");
            eprintln!(
                "The following commands may need to be run to clean up state:"
            );
            eprintln!("---");
            for err in errs {
                eprintln!("{}", InlineErrorChain::new(&err));
            }
            eprintln!("---");
            panic!("Dropped without cleanup. See stderr for cleanup advice");
        } else {
            eprintln!("OK");
        }
    }
}

impl ZfsTestHarness {
    pub const DEFAULT_VDEV_SIZE: u64 = 64 * (1 << 20);

    pub fn new(log: Logger) -> Self {
        let inner = Inner::new(log).expect("successfully set up tempdir");
        Self { inner: Some(inner) }
    }

    pub fn cleanup(&mut self) {
        let Some(inner) = self.inner.take() else {
            panic!("cleanup() called multiple times");
        };

        if let Err(errs) = inner.cleanup() {
            let errs = errs
                .into_iter()
                .map(|err| InlineErrorChain::new(&err).to_string())
                .collect::<Vec<_>>();
            panic!("Failed to clean up ZFS test harness:\n{}", errs.join("\n"));
        }
    }

    /// Add `ndisks` synthetic internal (M.2) disks, backed by vdevs.
    pub async fn add_internal_disks(&mut self, ndisks: usize) {
        let inner =
            self.inner.as_mut().expect("cleanup() should not have been called");
        for _ in 0..ndisks {
            inner.add_new_disk(DiskVariant::M2).await;
        }
    }

    /// Add `ndisks` synthetic external (U.2) disks, backed by vdevs.
    pub async fn add_external_disks(&mut self, ndisks: usize) {
        let inner =
            self.inner.as_mut().expect("cleanup() should not have been called");
        for _ in 0..ndisks {
            inner.add_new_disk(DiskVariant::U2).await;
        }
    }

    pub fn mount_config(&self) -> &Arc<MountConfig> {
        let inner =
            self.inner.as_ref().expect("cleanup() should not have been called");
        &inner.mount_config
    }

    pub fn all_internal_zpools(&self) -> impl Iterator<Item = &ZpoolName> + '_ {
        let inner =
            self.inner.as_ref().expect("cleanup() should not have been called");
        inner.all_internal_zpools()
    }

    pub fn all_internal_zpool_ids(
        &self,
    ) -> impl Iterator<Item = InternalZpoolUuid> + '_ {
        self.all_internal_zpools().map(|zpool| match zpool {
            ZpoolName::Internal(id) => *id,
            ZpoolName::External(_) => {
                unreachable!("all_internal_zpools returned an external zpool");
            }
        })
    }

    pub fn all_external_zpools(&self) -> impl Iterator<Item = &ZpoolName> + '_ {
        let inner =
            self.inner.as_ref().expect("cleanup() should not have been called");
        inner.all_external_zpools()
    }

    pub fn all_external_zpool_ids(
        &self,
    ) -> impl Iterator<Item = ExternalZpoolUuid> + '_ {
        self.all_external_zpools().map(|zpool| match zpool {
            ZpoolName::External(id) => *id,
            ZpoolName::Internal(_) => {
                unreachable!("all_external_zpools returned an internal zpool");
            }
        })
    }

    /// Return the directories for storing zone service bundles.
    pub fn all_zone_bundle_directories(&self) -> Vec<Utf8PathBuf> {
        // The directory within the debug dataset in which bundles are created.
        const BUNDLE_DIRECTORY: &str = "bundle";

        // The directory for zone bundles.
        const ZONE_BUNDLE_DIRECTORY: &str = "zone";

        let inner =
            self.inner.as_ref().expect("cleanup() should not have been called");
        inner
            .all_internal_zpools()
            .map(|zpool| {
                zpool
                    .dataset_mountpoint(
                        &inner.mount_config.root,
                        M2_DEBUG_DATASET,
                    )
                    .join(BUNDLE_DIRECTORY)
                    .join(ZONE_BUNDLE_DIRECTORY)
            })
            .collect()
    }

    pub async fn ensure_datasets(
        &self,
        datasets: impl IntoIterator<Item = DatasetConfig>,
    ) -> anyhow::Result<()> {
        let inner =
            self.inner.as_ref().expect("cleanup() should not have been called");
        for config in datasets {
            let details = DatasetCreationDetails {
                zoned: config.name.kind().zoned(),
                mountpoint: Mountpoint(
                    config.name.mountpoint(&inner.mount_config.root),
                ),
                full_name: config.name.full_name(),
            };
            inner
                .ensure_dataset(
                    config.name.pool(),
                    config.id,
                    &config.inner,
                    &details,
                )
                .await?;
        }
        Ok(())
    }
}

struct Inner {
    log: Logger,
    vdev_dir: Utf8TempDir,
    vdevs: Vec<RawDisk>,
    disks: Vec<Disk>,
    next_disk_index: i64,
    mount_config: Arc<MountConfig>,
    key_requester: StorageKeyRequester,
}

impl Inner {
    fn new(log: Logger) -> Result<Self, io::Error> {
        let vdev_dir = Utf8TempDir::new_in("/var/tmp")?;
        let mount_config =
            MountConfig { root: vdev_dir.path().into(), ..Default::default() };
        let (mut key_manager, key_requester) =
            KeyManager::new(&log, HardcodedSecretRetriever);
        tokio::spawn(async move { key_manager.run().await });

        Ok(Self {
            log,
            vdev_dir,
            vdevs: Vec::new(),
            disks: Vec::new(),
            next_disk_index: 0,
            mount_config: Arc::new(mount_config),
            key_requester,
        })
    }

    fn all_internal_zpools(&self) -> impl Iterator<Item = &ZpoolName> {
        self.disks.iter().filter_map(|d| match d.variant() {
            DiskVariant::U2 => None,
            DiskVariant::M2 => Some(d.zpool_name()),
        })
    }

    fn all_external_zpools(&self) -> impl Iterator<Item = &ZpoolName> {
        self.disks.iter().filter_map(|d| match d.variant() {
            DiskVariant::U2 => Some(d.zpool_name()),
            DiskVariant::M2 => None,
        })
    }

    fn next_disk_index(&mut self) -> i64 {
        self.next_disk_index += 1;
        self.next_disk_index
    }

    async fn add_new_disk(&mut self, variant: DiskVariant) {
        let disk_index = self.next_disk_index();
        let name = match variant {
            DiskVariant::U2 => format!("u2_{disk_index}.vdev"),
            DiskVariant::M2 => format!("m2_{disk_index}.vdev"),
        };
        let path = self.vdev_dir.path().join(name);
        let raw_disk: RawDisk = RawSyntheticDisk::new_with_length(
            &path,
            ZfsTestHarness::DEFAULT_VDEV_SIZE,
            disk_index,
        )
        .expect("created synthetic disk")
        .into();
        self.vdevs.push(raw_disk.clone());

        let disk = Disk::new(
            &self.log,
            &self.mount_config,
            raw_disk,
            None,
            Some(&self.key_requester),
        )
        .await
        .expect("adopted disk for new vdev");
        self.disks.push(disk);
    }

    fn pools(&self) -> Vec<ZpoolName> {
        self.disks.iter().map(|d| d.zpool_name()).cloned().collect()
    }

    async fn ensure_dataset(
        &self,
        zpool: &ZpoolName,
        dataset_id: DatasetUuid,
        config: &SharedDatasetConfig,
        details: &DatasetCreationDetails,
    ) -> anyhow::Result<()> {
        info!(
            self.log,
            "ensure_dataset";
            "config" => ?config,
            "details" => ?details,
        );

        // We can only place datasets within managed disks.
        // If a disk is attached to this sled, but not a part of the Control
        // Plane, it is treated as "not found" for dataset placement.
        if !self.disks.iter().any(|disk| disk.zpool_name() == zpool) {
            warn!(self.log, "Failed to find zpool");
            bail!("Failed to find zpool {zpool}");
        }

        let DatasetCreationDetails { zoned, mountpoint, full_name } = details;
        // The "crypt" dataset needs these details, but should already exist
        // by the time we're creating datasets inside.
        let encryption_details = None;
        let size_details = Some(illumos_utils::zfs::SizeDetails {
            quota: config.quota,
            reservation: config.reservation,
            compression: config.compression,
        });
        Zfs::ensure_dataset(DatasetEnsureArgs {
            name: &full_name,
            mountpoint: mountpoint.clone(),
            can_mount: CanMount::On,
            zoned: *zoned,
            encryption_details,
            size_details,
            id: Some(dataset_id),
            additional_options: None,
        })
        .await
        .with_context(|| {
            format!("failed to ensure dataset {dataset_id} in zpool {zpool}")
        })?;

        Ok(())
    }

    fn cleanup(self) -> Result<(), Vec<ExecutionError>> {
        let mut failed_commands = Vec::new();

        eprintln!("Terminating ZfsTestHarness");
        for pool in self.pools() {
            let pool_name = pool.to_string();
            eprintln!("Destroying pool: {pool_name}");
            if let Err(err) = illumos_utils::execute(
                &mut Command::new(PFEXEC)
                    .args(["zpool", "destroy", &pool_name]),
            ) {
                failed_commands.push(err);
            }
        }

        // Make sure that we're actually able to delete everything within the
        // temporary directory.
        //
        // This is necessary because the act of mounting datasets within this
        // directory may have created directories owned by root, and the test
        // process may not have been started as root.
        //
        // Since we're about to delete all these files anyway, make them
        // accessible to everyone before destroying them.
        match illumos_utils::execute(&mut Command::new("find").args([
            self.vdev_dir.path().as_str(),
            "-type",
            "d",
        ])) {
            Ok(vdev_dirs) => {
                for dir in vdev_dirs.stdout.lines() {
                    let dir = dir.expect("`find` output is valid UTF8");
                    eprintln!("Making {dir} mutable");
                    if let Err(err) = illumos_utils::execute(
                        &mut Command::new(PFEXEC).args(["chmod", "S-ci", &dir]),
                    ) {
                        failed_commands.push(err);
                    }
                }
            }
            Err(err) => {
                failed_commands.push(err);
            }
        }

        if let Err(err) =
            illumos_utils::execute(&mut Command::new(PFEXEC).args([
                "chmod",
                "-R",
                "a+rw",
                self.vdev_dir.path().as_str(),
            ]))
        {
            failed_commands.push(err);
        }

        if let Err(err) = illumos_utils::execute(
            &mut Command::new(illumos_utils::PFEXEC).args([
                "rm",
                "-rf",
                self.vdev_dir.path().as_str(),
            ]),
        ) {
            failed_commands.push(err);
        }

        if failed_commands.is_empty() { Ok(()) } else { Err(failed_commands) }
    }
}

#[derive(Debug)]
struct DatasetCreationDetails {
    zoned: bool,
    mountpoint: Mountpoint,
    full_name: String,
}

/// A [`key-manager::SecretRetriever`] that only returns hardcoded IKM for
/// epoch 0
#[derive(Debug, Default)]
struct HardcodedSecretRetriever;

#[async_trait::async_trait]
impl key_manager::SecretRetriever for HardcodedSecretRetriever {
    async fn get_latest(
        &self,
    ) -> Result<key_manager::VersionedIkm, key_manager::SecretRetrieverError>
    {
        let epoch = 0;
        let salt = [0u8; 32];
        let secret = [0x1d; 32];

        Ok(key_manager::VersionedIkm::new(epoch, salt, &secret))
    }

    /// We don't plan to do any key rotation before trust quorum is ready
    async fn get(
        &self,
        epoch: u64,
    ) -> Result<key_manager::SecretState, key_manager::SecretRetrieverError>
    {
        if epoch != 0 {
            return Err(key_manager::SecretRetrieverError::NoSuchEpoch(epoch));
        }
        Ok(key_manager::SecretState::Current(self.get_latest().await?))
    }
}
