// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use std::collections::HashSet;

use crate::dataset::DatasetError;
use crate::disk::{Disk, DiskError, RawDisk};
use crate::error::Error;
use crate::resources::StorageResources;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use slog::{debug, error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, Duration, MissedTickBehavior};

// The size of the mpsc bounded channel used to communicate
// between the `StorageHandle` and `StorageManager`.
const QUEUE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageManagerState {
    WaitingForKeyManager,
    QueuingDisks,
    Normal,
}

enum StorageRequest {
    AddDisk(RawDisk),
    RemoveDisk(RawDisk),
    DisksChanged(HashSet<RawDisk>),
    //    NewFilesystem(NewFilesystemRequest),
    KeyManagerReady,
    /// This will always grab the latest state after any new updates, as it
    /// serializes through the `StorageManager` task.
    /// This serialization is particularly useful for tests.
    GetLatestResources(oneshot::Sender<StorageResources>),
}

/// A mechanism for interacting with the [`StorageManager`]
pub struct StorageHandle {
    tx: mpsc::Sender<StorageRequest>,
    resource_updates: watch::Receiver<StorageResources>,
}

impl StorageHandle {
    /// Adds a disk and associated zpool to the storage manager.
    pub async fn upsert_disk(&self, disk: RawDisk) {
        self.tx.send(StorageRequest::AddDisk(disk)).await.unwrap();
    }

    /// Removes a disk, if it's tracked by the storage manager, as well
    /// as any associated zpools.
    pub async fn delete_disk(&self, disk: RawDisk) {
        self.tx.send(StorageRequest::RemoveDisk(disk)).await.unwrap();
    }

    /// Ensures that the storage manager tracks exactly the provided disks.
    ///
    /// This acts similar to a batch [Self::upsert_disk] for all new disks, and
    /// [Self::delete_disk] for all removed disks.
    ///
    /// If errors occur, an arbitrary "one" of them will be returned, but a
    /// best-effort attempt to add all disks will still be attempted.
    pub async fn ensure_using_exactly_these_disks<I>(&self, raw_disks: I)
    where
        I: IntoIterator<Item = RawDisk>,
    {
        self.tx
            .send(StorageRequest::DisksChanged(raw_disks.into_iter().collect()))
            .await
            .unwrap();
    }

    /// Notify the [`StorageManager`] that the [`key_manager::KeyManager`]
    /// has determined what [`key_manager::SecretRetriever`] to use and
    /// it is now possible to retrieve secrets and construct keys. Note
    /// that in cases of using the trust quorum, it is possible that the
    /// [`key_manager::SecretRetriever`] is ready, but enough key shares cannot
    /// be retrieved from other sleds. In this case, we still will be unable
    /// to add the disks successfully. In the common case this is a transient
    /// error. In other cases it may be fatal. However, that is outside the
    /// scope of the cares of this module.
    pub async fn key_manager_ready(&self) {
        self.tx.send(StorageRequest::KeyManagerReady).await.unwrap();
    }

    /// Wait for a boot disk to be initialized
    pub async fn wait_for_boot_disk(&mut self) -> (DiskIdentity, ZpoolName) {
        loop {
            // We panic if the sender is dropped, as this means
            // the StorageManager has gone away, which it should not do.
            self.resource_updates.changed().await.unwrap();
            // Limit any RWLock related cancellation issues by immediately cloning
            let resources = self.resource_updates.borrow().clone();
            if let Some((disk_id, zpool_name)) = resources.boot_disk() {
                return (disk_id, zpool_name);
            }
        }
    }

    /// Wait for any storage resource changes
    pub async fn wait_for_changes(&mut self) -> StorageResources {
        self.resource_updates.changed().await.unwrap();
        self.resource_updates.borrow().clone()
    }

    /// Retrieve the latest value of `StorageResources` from the
    /// `StorageManager` task.
    pub async fn get_latest_resources(&mut self) -> StorageResources {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StorageRequest::GetLatestResources(tx)).await.unwrap();
        rx.await.unwrap()
    }
}

/// The storage manager responsible for the state of the storage
/// on a sled. The storage manager runs in its own task and is interacted
/// with via the [`StorageHandle`].
pub struct StorageManager {
    log: Logger,
    state: StorageManagerState,
    rx: mpsc::Receiver<StorageRequest>,
    resources: StorageResources,
    queued_u2_drives: HashSet<RawDisk>,
    key_requester: StorageKeyRequester,
    resource_updates: watch::Sender<StorageResources>,
}

impl StorageManager {
    pub fn new(
        log: &Logger,
        key_requester: StorageKeyRequester,
    ) -> (StorageManager, StorageHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let resources = StorageResources::default();
        let (update_tx, update_rx) = watch::channel(resources.clone());
        (
            StorageManager {
                log: log.new(o!("component" => "StorageManager")),
                state: StorageManagerState::WaitingForKeyManager,
                rx,
                resources,
                queued_u2_drives: HashSet::new(),
                key_requester,
                resource_updates: update_tx,
            },
            StorageHandle { tx, resource_updates: update_rx },
        )
    }

    /// Run the main receive loop of the `StorageManager`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(&mut self) {
        loop {
            const QUEUED_DISK_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
            let mut interval = interval(QUEUED_DISK_RETRY_TIMEOUT);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tokio::select! {
                res = self.step() => {
                    if let Err(e) = res {
                        warn!(self.log, "{e}");
                    }
                }
                _ = interval.tick(),
                    if self.state == StorageManagerState::QueuingDisks =>
                {
                    self.add_queued_disks().await;
                }
            }
        }
    }

    /// Process the next event
    ///
    /// This is useful for testing/debugging
    pub async fn step(&mut self) -> Result<(), Error> {
        // The sending side should never disappear
        match self.rx.recv().await.unwrap() {
            StorageRequest::AddDisk(raw_disk) => match raw_disk.variant() {
                DiskVariant::U2 => self.add_u2_disk(raw_disk).await?,
                DiskVariant::M2 => self.add_m2_disk(raw_disk).await?,
            },
            StorageRequest::RemoveDisk(raw_disk) => {
                self.remove_disk(raw_disk).await;
            }
            StorageRequest::DisksChanged(_raw_disks) => todo!(),
            StorageRequest::KeyManagerReady => {
                self.state = StorageManagerState::Normal;
                self.add_queued_disks().await;
            }
            StorageRequest::GetLatestResources(tx) => {
                let _ = tx.send(self.resources.clone());
            }
        }
        Ok(())
    }

    // Loop through all queued disks inserting them into [`StorageResources`]
    // unless we hit a transient error. If we hit a transient error, we return
    // and wait for the next retry window to re-call this method. If we hit a
    // permanent error we log it, but we continue inserting queued disks.
    async fn add_queued_disks(&mut self) {
        self.state = StorageManagerState::Normal;
        // Operate on queued real disks

        // Disks that should be requeued.
        let mut saved = HashSet::new();
        let queued = std::mem::take(&mut self.queued_u2_drives);
        let mut iter = queued.into_iter();
        while let Some(disk) = iter.next() {
            if self.state == StorageManagerState::QueuingDisks {
                // We hit a transient error in a prior iteration.
                saved.insert(disk);
            } else {
                // Try ot add the disk. If there was a transient error the disk will
                // have been requeued. If there was a permanent error, it will have been
                // dropped. If there is an another unexpected error, we will handle it and
                // requeue ourselves.
                if let Err(err) = self.add_u2_disk(disk.clone()).await {
                    warn!(
                    self.log,
                    "Potentially transient error: {err}: - requeing disk {:?}",
                    disk
                );
                    saved.insert(disk);
                }
            }
        }
        // Merge any requeued disks from transient errors with saved disks here
        self.queued_u2_drives.extend(saved);
    }

    // Add a real U.2 disk to [`StorageResources`] or queue it to be added later
    async fn add_u2_disk(&mut self, raw_disk: RawDisk) -> Result<(), Error> {
        if self.state != StorageManagerState::Normal {
            self.queued_u2_drives.insert(raw_disk);
            return Ok(());
        }

        match Disk::new(&self.log, raw_disk.clone(), Some(&self.key_requester))
            .await
        {
            Ok(disk) => {
                if self.resources.insert_disk(disk)? {
                    let _ = self
                        .resource_updates
                        .send_replace(self.resources.clone());
                }
                Ok(())
            }
            Err(err @ DiskError::Dataset(DatasetError::KeyManager(_))) => {
                warn!(
                    self.log,
                    "Transient error: {err} - queuing disk {:?}", raw_disk
                );
                self.queued_u2_drives.insert(raw_disk);
                self.state = StorageManagerState::QueuingDisks;
                Ok(())
            }
            Err(err) => {
                error!(
                    self.log,
                    "Persistent error: {err} - not queueing disk {:?}",
                    raw_disk
                );
                Ok(())
            }
        }
    }

    // Add a real U.2 disk to [`StorageResources`]
    //
    //
    // We never queue M.2 drives, as they don't rely on [`KeyManager`] based
    // encryption
    async fn add_m2_disk(&mut self, raw_disk: RawDisk) -> Result<(), Error> {
        let disk =
            Disk::new(&self.log, raw_disk.clone(), Some(&self.key_requester))
                .await?;
        if self.resources.insert_disk(disk)? {
            let _ = self.resource_updates.send_replace(self.resources.clone());
        }
        Ok(())
    }

    // Delete a real disk
    async fn remove_disk(&mut self, raw_disk: RawDisk) {
        // If the disk is a U.2, we want to first delete it from any queued disks
        let _ = self.queued_u2_drives.remove(&raw_disk);
        if self.resources.remove_disk(raw_disk) {
            let _ = self.resource_updates.send_replace(self.resources.clone());
        }
    }
}

/// All tests only use synthetic disks, but are expected to be run on illumos
/// systems.
#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use crate::disk::SyntheticDisk;

    use super::*;
    use async_trait::async_trait;
    use camino_tempfile::tempdir;
    use illumos_utils::zpool::Zpool;
    use key_manager::{
        KeyManager, SecretRetriever, SecretRetrieverError, SecretState,
        VersionedIkm,
    };
    use omicron_test_utils::dev::test_setup_log;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use uuid::Uuid;

    /// A [`key-manager::SecretRetriever`] that only returns hardcoded IKM for
    /// epoch 0
    #[derive(Debug, Default)]
    struct HardcodedSecretRetriever {
        inject_error: Arc<AtomicBool>,
    }

    #[async_trait]
    impl SecretRetriever for HardcodedSecretRetriever {
        async fn get_latest(
            &self,
        ) -> Result<VersionedIkm, SecretRetrieverError> {
            if self.inject_error.load(Ordering::SeqCst) {
                return Err(SecretRetrieverError::Bootstore(
                    "Timeout".to_string(),
                ));
            }

            let epoch = 0;
            let salt = [0u8; 32];
            let secret = [0x1d; 32];

            Ok(VersionedIkm::new(epoch, salt, &secret))
        }

        /// We don't plan to do any key rotation before trust quorum is ready
        async fn get(
            &self,
            epoch: u64,
        ) -> Result<SecretState, SecretRetrieverError> {
            if self.inject_error.load(Ordering::SeqCst) {
                return Err(SecretRetrieverError::Bootstore(
                    "Timeout".to_string(),
                ));
            }
            if epoch != 0 {
                return Err(SecretRetrieverError::NoSuchEpoch(epoch));
            }
            Ok(SecretState::Current(self.get_latest().await?))
        }
    }

    #[tokio::test]
    async fn add_u2_disk_while_not_in_normal_stage_and_ensure_it_gets_queued() {
        let logctx = test_setup_log(
            "add_u2_disk_while_not_in_normal_stage_and_ensure_it_gets_queued",
        );
        let (mut _key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, _) = StorageManager::new(&logctx.log, key_requester);
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let raw_disk: RawDisk = SyntheticDisk::new(zpool_name).into();
        assert_eq!(StorageManagerState::WaitingForKeyManager, manager.state);
        manager.add_u2_disk(raw_disk.clone()).await.unwrap();
        assert!(manager.resources.all_u2_zpools().is_empty());
        assert_eq!(manager.queued_u2_drives, HashSet::from([raw_disk.clone()]));

        // Check other non-normal stages and enusre disk gets queued
        manager.queued_u2_drives.clear();
        manager.state = StorageManagerState::QueuingDisks;
        manager.add_u2_disk(raw_disk.clone()).await.unwrap();
        assert!(manager.resources.all_u2_zpools().is_empty());
        assert_eq!(manager.queued_u2_drives, HashSet::from([raw_disk]));
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_u2_gets_added_to_resources() {
        let logctx = test_setup_log("ensure_u2_gets_added_to_resources");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, _) = StorageManager::new(&logctx.log, key_requester);
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk = SyntheticDisk::create_zpool(dir.path(), &zpool_name).into();

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Set the stage to pretend we've progressed enough to have a key_manager available.
        manager.state = StorageManagerState::Normal;
        manager.add_u2_disk(disk).await.unwrap();
        assert_eq!(manager.resources.all_u2_zpools().len(), 1);
        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn wait_for_bootdisk() {
        let logctx = test_setup_log("wait_for_bootdisk");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, mut handle) =
            StorageManager::new(&logctx.log, key_requester);
        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Spawn the storage manager as done by sled-agent
        tokio::spawn(async move {
            manager.run().await;
        });

        // Create a synthetic internal disk
        let zpool_name = ZpoolName::new_internal(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk = SyntheticDisk::create_zpool(dir.path(), &zpool_name).into();

        handle.upsert_disk(disk).await;
        handle.wait_for_boot_disk().await;
        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn queued_disks_get_added_as_resources() {
        let logctx = test_setup_log("queued_disks_get_added_as_resources");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, mut handle) =
            StorageManager::new(&logctx.log, key_requester);

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Spawn the storage manager as done by sled-agent
        tokio::spawn(async move {
            manager.run().await;
        });

        // Queue up a disks, as we haven't told the `StorageManager` that
        // the `KeyManager` is ready yet.
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk = SyntheticDisk::create_zpool(dir.path(), &zpool_name).into();
        handle.upsert_disk(disk).await;
        let resources = handle.get_latest_resources().await;
        assert!(resources.all_u2_zpools().is_empty());

        // Now inform the storage manager that the key manager is ready
        // The queued disk should be successfully added
        handle.key_manager_ready().await;
        let resources = handle.get_latest_resources().await;
        assert_eq!(resources.all_u2_zpools().len(), 1);
        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }

    /// For this test, we are going to step through the msg recv loop directly
    /// without running the `StorageManager` in a tokio task.
    /// This allows us to control timing precisely.
    #[tokio::test]
    async fn queued_disks_get_requeued_on_secret_retriever_error() {
        let logctx = test_setup_log(
            "queued_disks_get_requeued_on_secret_retriever_error",
        );
        let inject_error = Arc::new(AtomicBool::new(false));
        let (mut key_manager, key_requester) = KeyManager::new(
            &logctx.log,
            HardcodedSecretRetriever { inject_error: inject_error.clone() },
        );
        let (mut manager, handle) =
            StorageManager::new(&logctx.log, key_requester);

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Queue up a disks, as we haven't told the `StorageManager` that
        // the `KeyManager` is ready yet.
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk = SyntheticDisk::create_zpool(dir.path(), &zpool_name).into();
        handle.upsert_disk(disk).await;
        manager.step().await.unwrap();

        // We can't wait for a reply through the handle as the storage manager task
        // isn't actually running. We just check the resources directly.
        assert!(manager.resources.all_u2_zpools().is_empty());

        // Let's inject an error to the `SecretRetriever` to simulate a trust
        // quorum timeout
        inject_error.store(true, Ordering::SeqCst);

        // Now inform the storage manager that the key manager is ready
        // The queued disk should not be added due to the error
        handle.key_manager_ready().await;
        manager.step().await.unwrap();
        assert!(manager.resources.all_u2_zpools().is_empty());

        // Manually simulating a timer tick to add queued disks should also
        // still hit the error
        manager.add_queued_disks().await;
        assert!(manager.resources.all_u2_zpools().is_empty());

        // Clearing the injected error will cause the disk to get added
        inject_error.store(false, Ordering::SeqCst);
        manager.add_queued_disks().await;
        assert_eq!(1, manager.resources.all_u2_zpools().len());

        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn delete_disk_triggers_notification() {
        let logctx = test_setup_log("delete_disk_triggers_notification");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, mut handle) =
            StorageManager::new(&logctx.log, key_requester);

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Spawn the storage manager as done by sled-agent
        tokio::spawn(async move {
            manager.run().await;
        });

        // Inform the storage manager that the key manager is ready, so disks
        // don't get queued
        handle.key_manager_ready().await;

        // Create and add a disk
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk: RawDisk =
            SyntheticDisk::create_zpool(dir.path(), &zpool_name).into();
        handle.upsert_disk(disk.clone()).await;

        // Wait for the add disk notification
        let resources = handle.wait_for_changes().await;
        assert_eq!(resources.all_u2_zpools().len(), 1);

        // Delete the disk and wait for a notification
        handle.delete_disk(disk).await;
        let resources = handle.wait_for_changes().await;
        assert!(resources.all_u2_zpools().is_empty());

        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }
}
