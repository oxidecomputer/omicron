// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for creating a StorageManager under test.

use crate::config::MountConfig;
use crate::disk::RawDisk;
use crate::manager::{StorageHandle, StorageManager};
use camino::Utf8PathBuf;
use key_manager::StorageKeyRequester;
use omicron_common::disk::{
    OmicronPhysicalDiskConfig, OmicronPhysicalDisksConfig,
};
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog::{Logger, info};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use xshell::{Shell, cmd};

/// A [`key-manager::SecretRetriever`] that only returns hardcoded IKM for
/// epoch 0
#[derive(Debug, Default)]
struct HardcodedSecretRetriever {
    inject_error: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl key_manager::SecretRetriever for HardcodedSecretRetriever {
    async fn get_latest(
        &self,
    ) -> Result<key_manager::VersionedIkm, key_manager::SecretRetrieverError>
    {
        if self.inject_error.load(Ordering::SeqCst) {
            return Err(key_manager::SecretRetrieverError::Bootstore(
                "Timeout".to_string(),
            ));
        }

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
        if self.inject_error.load(Ordering::SeqCst) {
            return Err(key_manager::SecretRetrieverError::Bootstore(
                "Timeout".to_string(),
            ));
        }
        if epoch != 0 {
            return Err(key_manager::SecretRetrieverError::NoSuchEpoch(epoch));
        }
        Ok(key_manager::SecretState::Current(self.get_latest().await?))
    }
}

/// Helper utility for tests that want to use a StorageManager.
///
/// Attempts to make it easy to create a set of vdev-based M.2 and U.2
/// devices, which can be formatted with arbitrary zpools.
pub struct StorageManagerTestHarness {
    handle: StorageHandle,
    vdev_dir: Option<camino_tempfile::Utf8TempDir>,
    vdevs: std::collections::BTreeSet<RawDisk>,
    next_slot: i64,
    #[allow(unused)]
    key_requester: StorageKeyRequester,
    key_manager_error_injector: Arc<AtomicBool>,
    key_manager_task: tokio::task::JoinHandle<()>,
    storage_manager_task: tokio::task::JoinHandle<()>,
}

impl Drop for StorageManagerTestHarness {
    fn drop(&mut self) {
        if let Some(vdev_dir) = self.vdev_dir.take() {
            eprint!(
                "WARNING: StorageManagerTestHarness called without 'cleanup()'.\n\
                 Attempting automated cleanup of {}",
                vdev_dir.path(),
            );

            let pools = [
                (
                    omicron_common::zpool_name::ZPOOL_INTERNAL_PREFIX,
                    vdev_dir.path().join("pool/int"),
                ),
                (
                    omicron_common::zpool_name::ZPOOL_EXTERNAL_PREFIX,
                    vdev_dir.path().join("pool/ext"),
                ),
            ];

            let mut failed_commands = vec![];

            for (prefix, pool) in pools {
                let Ok(entries) = pool.read_dir_utf8() else {
                    continue;
                };
                for entry in entries.flatten() {
                    let pool_name = format!("{prefix}{}", entry.file_name());
                    if let Err(_) =
                        std::process::Command::new(illumos_utils::PFEXEC)
                            .args(["zpool", "destroy", &pool_name])
                            .status()
                    {
                        failed_commands
                            .push(format!("pfexec zpool destroy {pool_name}"));
                    }
                }
            }

            let vdev_path = vdev_dir.path();
            let sh = Shell::new().unwrap();
            match cmd!(sh, "find {vdev_path} -type d").read() {
                Err(err) => {
                    failed_commands
                        .push(format!("find {vdev_path} -type d: {err}"));
                }
                Ok(vdev_dirs) => {
                    for dir in vdev_dirs.lines() {
                        if let Err(err) =
                            cmd!(sh, "pfexec chmod S-ci {dir}").quiet().run()
                        {
                            failed_commands.push(format!(
                                "pfexec chmod S-ci {dir}: {err}"
                            ));
                        }
                    }
                }
            }

            if let Err(_) = std::process::Command::new(illumos_utils::PFEXEC)
                .args(["rm", "-rf", vdev_path.as_str()])
                .status()
            {
                failed_commands.push(format!("pfexec rm -rf {vdev_path}"));
            }

            if !failed_commands.is_empty() {
                eprintln!("FAILED");
                eprintln!(
                    "The following commands may need to be run to clean up state:"
                );
                eprintln!("---");
                for cmd in failed_commands {
                    eprintln!("{cmd}");
                }
                eprintln!("---");
                panic!(
                    "Dropped without cleanup. See stderr for cleanup advice"
                );
            } else {
                eprintln!("OK");
            }
        }
    }
}

impl StorageManagerTestHarness {
    /// Creates a new StorageManagerTestHarness with no associated disks.
    pub async fn new(log: &Logger) -> Self {
        let tmp = camino_tempfile::tempdir_in("/var/tmp")
            .expect("Failed to make temporary directory");
        info!(log, "Using tmp: {}", tmp.path());
        Self::new_with_tmp_dir(log, tmp).await
    }

    async fn new_with_tmp_dir(
        log: &Logger,
        tmp: camino_tempfile::Utf8TempDir,
    ) -> Self {
        let mount_config =
            MountConfig { root: tmp.path().into(), ..Default::default() };

        let key_manager_error_injector = Arc::new(AtomicBool::new(false));
        let (mut key_manager, key_requester) = key_manager::KeyManager::new(
            &log,
            HardcodedSecretRetriever {
                inject_error: key_manager_error_injector.clone(),
            },
        );
        let (manager, handle) =
            StorageManager::new(&log, mount_config, key_requester.clone());

        // Spawn the key_manager so that it will respond to requests for encryption keys
        let key_manager_task =
            tokio::spawn(async move { key_manager.run().await });

        // Spawn the storage manager as done by sled-agent
        let storage_manager_task = tokio::spawn(async move {
            manager.run().await;
        });

        Self {
            handle,
            vdev_dir: Some(tmp),
            vdevs: std::collections::BTreeSet::new(),
            next_slot: 0,
            key_requester,
            key_manager_error_injector,
            key_manager_task,
            storage_manager_task,
        }
    }

    /// Emulate a system rebooting.
    ///
    /// - Stops the currently running tasks and restarts them
    /// - Re-inserts all vdevs previously created by [Self::add_vdevs].
    pub async fn reboot(mut self, log: &Logger) -> Self {
        // Abort ongoing tasks, in lieu of a cleaner shutdown mechanism.
        self.key_manager_task.abort();
        self.storage_manager_task.abort();

        // Deconstruct the test harness
        let vdev_dir =
            std::mem::take(&mut self.vdev_dir).expect("Already terminated");
        let vdevs = std::mem::take(&mut self.vdevs);

        // Re-create all the state we created during the constructor, but
        // leave the temporary directory as it was "before reboot".
        let mut slef = Self::new_with_tmp_dir(log, vdev_dir).await;
        slef.next_slot = self.next_slot;

        // Notify ourselves of the new disks, just as the hardware would.
        //
        // NOTE: Technically, if these disks have pools, they're still imported.
        // However, the SledManager doesn't know about them, and wouldn't
        // assume they're being managed right now.
        for raw_disk in vdevs {
            slef.handle
                .detected_raw_disk(raw_disk.clone())
                .await // Notify StorageManager
                .await // Wait for it to finish processing
                .unwrap();
            slef.vdevs.insert(raw_disk.clone());
        }

        slef
    }

    #[allow(unused)]
    pub(crate) fn mount_config(&self) -> MountConfig {
        MountConfig {
            root: self
                .vdev_dir
                .as_ref()
                .expect("Harness destroyed?")
                .path()
                .into(),
            ..Default::default()
        }
    }

    #[allow(unused)]
    pub(crate) fn key_requester(&self) -> &StorageKeyRequester {
        &self.key_requester
    }

    pub const DEFAULT_VDEV_SIZE: u64 = 64 * (1 << 20);

    /// Adds raw devices to the [crate::manager::StorageManager], as if they were detected via
    /// hardware. Can be called several times.
    ///
    /// Each device is [Self::DEFAULT_VDEV_SIZE] in size.
    /// Use [Self::add_vdevs_with_size] if you need more control
    /// over device sizes.
    pub async fn add_vdevs<P: AsRef<str> + ?Sized>(
        &mut self,
        vdevs: &[&P],
    ) -> Vec<RawDisk> {
        self.add_vdevs_with_size(
            &vdevs
                .iter()
                .map(|vdev| (vdev, Self::DEFAULT_VDEV_SIZE))
                .collect::<Vec<_>>(),
        )
        .await
    }

    pub async fn add_vdevs_with_size<P: AsRef<str> + ?Sized>(
        &mut self,
        vdevs: &[(&P, u64)],
    ) -> Vec<RawDisk> {
        let vdev_dir = self
            .vdev_dir
            .as_ref()
            .expect("Cannot add vdevs, test harness terminated");
        let mut added = vec![];
        for (vdev, size) in vdevs
            .iter()
            .map(|(vdev, size)| (Utf8PathBuf::from(vdev.as_ref()), size))
        {
            assert!(vdev.is_relative());
            let vdev_path = vdev_dir.path().join(&vdev);
            let raw_disk: RawDisk =
                crate::disk::RawSyntheticDisk::new_with_length(
                    &vdev_path,
                    *size,
                    self.next_slot,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to create synthetic disk for {vdev}: {err:?}"
                    )
                })
                .into();
            self.next_slot += 1;
            self.handle
                .detected_raw_disk(raw_disk.clone())
                .await // Notify StorageManager
                .await // Wait for it to finish processing
                .unwrap();

            self.vdevs.insert(raw_disk.clone());
            added.push(raw_disk);
        }
        added
    }

    // Removes a vdev from the set of "tracked" devices.
    //
    // This is equivalent to having the hardware monitor unplug a device.
    //
    // If this device has an associated zpool, it must be either re-attached
    // to the harness or manually destroyed before the test completes.
    // Otherwise, removing the temporary directory containing that zpool
    // will likely fail with a "device busy" error.
    pub async fn remove_vdev(&mut self, raw: &RawDisk) {
        assert!(self.vdevs.remove(&raw), "Vdev does not exist");
        self.handle
            .detected_raw_disk_removal(raw.clone())
            .await
            .await
            .expect("Failed to remove vdev");
    }

    // Update a vdev.
    //
    // Note: currently the only portion of a vdev that we update is the firmware
    // metadata.
    pub async fn update_vdev(&mut self, raw: &RawDisk) {
        self.handle
            .detected_raw_disk_update(raw.clone())
            .await
            .await
            .expect("Failed to update vdev");
    }

    // Adds a vdev to the set of "tracked" devices.
    pub async fn add_vdev_as(&mut self, raw_disk: RawDisk) {
        self.handle
            .detected_raw_disk(raw_disk.clone())
            .await // Notify StorageManager
            .await // Wait for it to finish processing
            .unwrap();
        self.vdevs.insert(raw_disk.clone());
    }

    pub fn make_config(
        &self,
        generation: u32,
        disks: &[RawDisk],
    ) -> OmicronPhysicalDisksConfig {
        let disks = disks
            .into_iter()
            .map(|raw| {
                let identity = raw.identity();

                OmicronPhysicalDiskConfig {
                    identity: identity.clone(),
                    id: PhysicalDiskUuid::new_v4(),
                    pool_id: ZpoolUuid::new_v4(),
                }
            })
            .collect();

        OmicronPhysicalDisksConfig {
            generation: omicron_common::api::external::Generation::from(
                generation,
            ),
            disks,
        }
    }

    /// Returns the underlying [crate::manager::StorageHandle].
    pub fn handle_mut(&mut self) -> &mut StorageHandle {
        &mut self.handle
    }

    /// Returns the underlying [crate::manager::StorageHandle].
    pub fn handle(&self) -> &StorageHandle {
        &self.handle
    }

    /// Set to "true" to throw errors, "false" to not inject errors.
    pub fn key_manager_error_injector(&self) -> &Arc<AtomicBool> {
        &self.key_manager_error_injector
    }

    /// Cleanly terminates the test harness
    pub async fn cleanup(&mut self) {
        let Some(vdev_dir) = self.vdev_dir.take() else {
            // Already terminated
            return;
        };

        eprintln!("Terminating StorageManagerTestHarness");
        let disks = self.handle().get_latest_disks().await;
        let pools = disks.get_all_zpools();

        self.key_manager_task.abort();
        self.storage_manager_task.abort();

        for (pool, _) in pools {
            eprintln!("Destroying pool: {pool:?}");
            if let Err(e) = illumos_utils::zpool::Zpool::destroy(&pool).await {
                eprintln!("Failed to destroy {pool:?}: {e:?}");
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
        let mut command = std::process::Command::new("/usr/bin/pfexec");
        let mount = vdev_dir.path();

        let sh = Shell::new().unwrap();
        let dirs = cmd!(sh, "find {mount} -type d")
            .read()
            .expect("Failed to find dirs");
        for dir in dirs.lines() {
            println!("Making {dir} mutable");
            cmd!(sh, "pfexec chmod S-ci {dir}")
                .quiet()
                .run()
                .expect("Failed to make directory mutable");
        }

        let cmd = command.args(["chmod", "-R", "a+rw", mount.as_str()]);
        cmd.output().expect(
            "Failed to change ownership of the temporary directory we're trying to delete"
        );

        // Actually delete everything, and check the result to fail loud if
        // something goes wrong.
        vdev_dir.close().expect("Failed to clean up temporary directory");
    }
}
