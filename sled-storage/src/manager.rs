// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use std::collections::HashSet;

use crate::dataset::{DatasetError, DatasetName};
use crate::disk::{Disk, DiskError, RawDisk};
use crate::error::Error;
use crate::resources::{AddDiskResult, StorageResources};
use camino::Utf8PathBuf;
use illumos_utils::zfs::{Mountpoint, Zfs};
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use slog::{error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, Duration, MissedTickBehavior};
use uuid::Uuid;

// The size of the mpsc bounded channel used to communicate
// between the `StorageHandle` and `StorageManager`.
//
// How did we choose this bound, and why?
//
// Picking a bound can be tricky, but in general, you want the channel to act
// unbounded, such that sends never fail. This makes the channels reliable,
// such that we never drop messages inside the process, and the caller doesn't
// have to choose what to do when overloaded. This simplifies things drastically
// for developers. However, you also don't want to make the channel actually
// unbounded, because that can lead to run-away memory growth and pathological
// behaviors, such that requests get slower over time until the system crashes.
//
// Our team's chosen solution, and used elsewhere in the codebase, is is to
// choose a large enough bound such that we should never hit it in practice
// unless we are truly overloaded. If we hit the bound it means that beyond that
// requests will start to build up and we will eventually topple over. So when
// we hit this bound, we just go ahead and panic.
//
// Picking a channel bound is hard to do empirically, but practically, if
// requests are mostly mutating task local state, a bound of 1024 or even 8192
// should be plenty. Tasks that must perform longer running ops can spawn helper
// tasks as necessary or include their own handles for replies rather than
// synchronously waiting. Memory for the queue can be kept small with boxing of
// large messages.
//
// Here we start relatively small so that we can evaluate our choice over time.
const QUEUE_SIZE: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageManagerState {
    WaitingForKeyManager,
    QueueingDisks,
    Normal,
}

#[derive(Debug)]
struct NewFilesystemRequest {
    dataset_id: Uuid,
    dataset_name: DatasetName,
    responder: oneshot::Sender<Result<(), Error>>,
}

#[derive(Debug)]
enum StorageRequest {
    AddDisk(RawDisk),
    RemoveDisk(RawDisk),
    DisksChanged(HashSet<RawDisk>),
    NewFilesystem(NewFilesystemRequest),
    KeyManagerReady,
    /// This will always grab the latest state after any new updates, as it
    /// serializes through the `StorageManager` task after all prior requests.
    /// This serialization is particularly useful for tests.
    GetLatestResources(oneshot::Sender<StorageResources>),

    /// Get the internal task state of the manager
    GetManagerState(oneshot::Sender<StorageManagerData>),
}

/// Data managed internally to the StorageManagerTask that can be useful
/// to clients for debugging purposes, and that isn't exposed in other ways.
#[derive(Debug, Clone)]
pub struct StorageManagerData {
    pub state: StorageManagerState,
    pub queued_u2_drives: HashSet<RawDisk>,
}

/// A mechanism for interacting with the [`StorageManager`]
#[derive(Clone)]
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
            let resources = self.resource_updates.borrow_and_update();
            if let Some((disk_id, zpool_name)) = resources.boot_disk() {
                return (disk_id, zpool_name);
            }
            drop(resources);
            // We panic if the sender is dropped, as this means
            // the StorageManager has gone away, which it should not do.
            self.resource_updates.changed().await.unwrap();
        }
    }

    /// Wait for any storage resource changes
    pub async fn wait_for_changes(&mut self) -> StorageResources {
        self.resource_updates.changed().await.unwrap();
        self.resource_updates.borrow_and_update().clone()
    }

    /// Retrieve the latest value of `StorageResources` from the
    /// `StorageManager` task.
    pub async fn get_latest_resources(&self) -> StorageResources {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StorageRequest::GetLatestResources(tx)).await.unwrap();
        rx.await.unwrap()
    }

    /// Return internal data useful for debugging and testing
    pub async fn get_manager_state(&self) -> StorageManagerData {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StorageRequest::GetManagerState(tx)).await.unwrap();
        rx.await.unwrap()
    }

    pub async fn upsert_filesystem(
        &self,
        dataset_id: Uuid,
        dataset_name: DatasetName,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let request =
            NewFilesystemRequest { dataset_id, dataset_name, responder: tx };
        self.tx.send(StorageRequest::NewFilesystem(request)).await.unwrap();
        rx.await.unwrap()
    }
}

// Some sled-agent tests cannot currently use the real StorageManager
// and want to fake the entire behavior, but still have access to the
// `StorageResources`. We allow this via use of the `FakeStorageManager`
// that will respond to real storage requests from a real `StorageHandle`.
#[cfg(feature = "testing")]
pub struct FakeStorageManager {
    rx: mpsc::Receiver<StorageRequest>,
    resources: StorageResources,
    resource_updates: watch::Sender<StorageResources>,
}

#[cfg(feature = "testing")]
impl FakeStorageManager {
    pub fn new() -> (Self, StorageHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let resources = StorageResources::default();
        let (update_tx, update_rx) = watch::channel(resources.clone());
        (
            Self { rx, resources, resource_updates: update_tx },
            StorageHandle { tx, resource_updates: update_rx },
        )
    }

    /// Run the main receive loop of the `FakeStorageManager`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        loop {
            match self.rx.recv().await {
                Some(StorageRequest::AddDisk(raw_disk)) => {
                    if self.add_disk(raw_disk).disk_inserted() {
                        self.resource_updates
                            .send_replace(self.resources.clone());
                    }
                }
                Some(StorageRequest::GetLatestResources(tx)) => {
                    let _ = tx.send(self.resources.clone());
                }
                Some(_) => {
                    unreachable!();
                }
                None => break,
            }
        }
    }

    // Add a disk to `StorageResources` if it is new and return true if so
    fn add_disk(&mut self, raw_disk: RawDisk) -> AddDiskResult {
        let disk = match raw_disk {
            RawDisk::Real(_) => {
                panic!(
                    "Only synthetic disks can be used with `FakeStorageManager`"
                );
            }
            RawDisk::Synthetic(synthetic_disk) => {
                Disk::Synthetic(synthetic_disk)
            }
        };
        self.resources.insert_fake_disk(disk)
    }
}

/// The storage manager responsible for the state of the storage
/// on a sled. The storage manager runs in its own task and is interacted
/// with via the [`StorageHandle`].
pub struct StorageManager {
    log: Logger,
    state: StorageManagerState,
    // Used to find the capacity of the channel for tracking purposes
    tx: mpsc::Sender<StorageRequest>,
    rx: mpsc::Receiver<StorageRequest>,
    resources: StorageResources,
    queued_u2_drives: HashSet<RawDisk>,
    key_requester: StorageKeyRequester,
    resource_updates: watch::Sender<StorageResources>,
    last_logged_capacity: usize,
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
                tx: tx.clone(),
                rx,
                resources,
                queued_u2_drives: HashSet::new(),
                key_requester,
                resource_updates: update_tx,
                last_logged_capacity: QUEUE_SIZE,
            },
            StorageHandle { tx, resource_updates: update_rx },
        )
    }

    /// Run the main receive loop of the `StorageManager`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
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
                    if self.state == StorageManagerState::QueueingDisks =>
                {
                    if self.add_queued_disks().await {
                        let _ = self.resource_updates.send_replace(self.resources.clone());
                    }
                }
            }
        }
    }

    /// Process the next event
    ///
    /// This is useful for testing/debugging
    pub async fn step(&mut self) -> Result<(), Error> {
        const CAPACITY_LOG_THRESHOLD: usize = 10;
        // We check the capacity and log it every time it changes by at least 10
        // entries in either direction.
        let current = self.tx.capacity();
        if self.last_logged_capacity.saturating_sub(current)
            >= CAPACITY_LOG_THRESHOLD
        {
            info!(
                self.log,
                "Channel capacity decreased";
                "previous" => ?self.last_logged_capacity,
                "current" => ?current
            );
            self.last_logged_capacity = current;
        } else if current.saturating_sub(self.last_logged_capacity)
            >= CAPACITY_LOG_THRESHOLD
        {
            info!(
                self.log,
                "Channel capacity increased";
                "previous" => ?self.last_logged_capacity,
                "current" => ?current
            );
            self.last_logged_capacity = current;
        }
        // The sending side never disappears because we hold a copy
        let req = self.rx.recv().await.unwrap();
        info!(self.log, "Received {:?}", req);
        let should_send_updates = match req {
            StorageRequest::AddDisk(raw_disk) => {
                self.add_disk(raw_disk).await?.disk_inserted()
            }
            StorageRequest::RemoveDisk(raw_disk) => self.remove_disk(raw_disk),
            StorageRequest::DisksChanged(raw_disks) => {
                self.ensure_using_exactly_these_disks(raw_disks).await
            }
            StorageRequest::NewFilesystem(request) => {
                let result = self.add_dataset(&request).await;
                if result.is_err() {
                    warn!(self.log, "{result:?}");
                }
                let _ = request.responder.send(result);
                false
            }
            StorageRequest::KeyManagerReady => {
                self.state = StorageManagerState::Normal;
                self.add_queued_disks().await
            }
            StorageRequest::GetLatestResources(tx) => {
                let _ = tx.send(self.resources.clone());
                false
            }
            StorageRequest::GetManagerState(tx) => {
                let _ = tx.send(StorageManagerData {
                    state: self.state,
                    queued_u2_drives: self.queued_u2_drives.clone(),
                });
                false
            }
        };

        if should_send_updates {
            let _ = self.resource_updates.send_replace(self.resources.clone());
        }

        Ok(())
    }

    // Loop through all queued disks inserting them into [`StorageResources`]
    // unless we hit a transient error. If we hit a transient error, we return
    // and wait for the next retry window to re-call this method. If we hit a
    // permanent error we log it, but we continue inserting queued disks.
    //
    // Return true if updates should be sent to watchers, false otherwise
    async fn add_queued_disks(&mut self) -> bool {
        info!(
            self.log,
            "Attempting to add queued disks";
            "num_disks" => %self.queued_u2_drives.len()
        );
        self.state = StorageManagerState::Normal;

        let mut send_updates = false;

        // Disks that should be requeued.
        let queued = self.queued_u2_drives.clone();
        let mut to_dequeue = HashSet::new();
        for disk in queued.iter() {
            if self.state == StorageManagerState::QueueingDisks {
                // We hit a transient error in a prior iteration.
                break;
            } else {
                match self.add_u2_disk(disk.clone()).await {
                    Err(_) => {
                        // This is an unrecoverable error, so we don't queue the
                        // disk again.
                        to_dequeue.insert(disk);
                    }
                    Ok(AddDiskResult::DiskInserted) => {
                        send_updates = true;
                        to_dequeue.insert(disk);
                    }
                    Ok(AddDiskResult::DiskAlreadyInserted) => {
                        to_dequeue.insert(disk);
                    }
                    Ok(AddDiskResult::DiskQueued) => (),
                }
            }
        }
        // Dequeue any inserted disks
        self.queued_u2_drives.retain(|k| !to_dequeue.contains(k));
        send_updates
    }

    // Add a disk to `StorageResources` if it is new,
    // updated, or its pool has been updated as determined by
    // [`$crate::resources::StorageResources::insert_disk`] and we decide not to
    // queue the disk for later addition.
    async fn add_disk(
        &mut self,
        raw_disk: RawDisk,
    ) -> Result<AddDiskResult, Error> {
        match raw_disk.variant() {
            DiskVariant::U2 => self.add_u2_disk(raw_disk).await,
            DiskVariant::M2 => self.add_m2_disk(raw_disk).await,
        }
    }

    // Add a U.2 disk to [`StorageResources`] or queue it to be added later
    async fn add_u2_disk(
        &mut self,
        raw_disk: RawDisk,
    ) -> Result<AddDiskResult, Error> {
        if self.state != StorageManagerState::Normal {
            self.queued_u2_drives.insert(raw_disk);
            return Ok(AddDiskResult::DiskQueued);
        }

        match Disk::new(&self.log, raw_disk.clone(), Some(&self.key_requester))
            .await
        {
            Ok(disk) => self.resources.insert_disk(disk),
            Err(err @ DiskError::Dataset(DatasetError::KeyManager(_))) => {
                warn!(
                    self.log,
                    "Transient error: {err}: queuing disk";
                    "disk_id" => ?raw_disk.identity()
                );
                self.queued_u2_drives.insert(raw_disk);
                self.state = StorageManagerState::QueueingDisks;
                Ok(AddDiskResult::DiskQueued)
            }
            Err(err) => {
                error!(
                    self.log,
                    "Persistent error:not queueing disk";
                    "err" => ?err,
                    "disk_id" => ?raw_disk.identity()
                );
                Err(err.into())
            }
        }
    }

    // Add a U.2 disk to [`StorageResources`] if new and return `Ok(true)` if so
    //
    //
    // We never queue M.2 drives, as they don't rely on [`KeyManager`] based
    // encryption
    async fn add_m2_disk(
        &mut self,
        raw_disk: RawDisk,
    ) -> Result<AddDiskResult, Error> {
        let disk =
            Disk::new(&self.log, raw_disk.clone(), Some(&self.key_requester))
                .await?;
        self.resources.insert_disk(disk)
    }

    // Delete a real disk and return `true` if the disk was actually removed
    fn remove_disk(&mut self, raw_disk: RawDisk) -> bool {
        // If the disk is a U.2, we want to first delete it from any queued disks
        let _ = self.queued_u2_drives.remove(&raw_disk);
        self.resources.remove_disk(raw_disk.identity())
    }

    // Find all disks to remove that are not in raw_disks and remove them. Then
    // take the remaining disks and try to add them all. `StorageResources` will
    // inform us if anything changed, and if so we return true, otherwise we
    // return false.
    async fn ensure_using_exactly_these_disks(
        &mut self,
        raw_disks: HashSet<RawDisk>,
    ) -> bool {
        let mut should_update = false;

        // Clear out any queued U.2 disks that are real.
        // We keep synthetic disks, as they are only added once.
        self.queued_u2_drives.retain(|d| d.is_synthetic());

        let all_ids: HashSet<_> =
            raw_disks.iter().map(|d| d.identity()).collect();

        // Find all existing disks not in the current set
        let to_remove: Vec<DiskIdentity> = self
            .resources
            .disks()
            .keys()
            .filter_map(|id| {
                if !all_ids.contains(id) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for id in to_remove {
            if self.resources.remove_disk(&id) {
                should_update = true;
            }
        }

        for raw_disk in raw_disks {
            let disk_id = raw_disk.identity().clone();
            match self.add_disk(raw_disk).await {
                Ok(AddDiskResult::DiskInserted) => should_update = true,
                Ok(_) => (),
                Err(err) => {
                    warn!(
                        self.log,
                        "Failed to add disk to storage resources: {err}";
                        "disk_id" => ?disk_id
                    );
                }
            }
        }

        should_update
    }

    // Attempts to add a dataset within a zpool, according to `request`.
    async fn add_dataset(
        &mut self,
        request: &NewFilesystemRequest,
    ) -> Result<(), Error> {
        info!(self.log, "add_dataset: {:?}", request);
        if !self
            .resources
            .disks()
            .values()
            .any(|(_, pool)| &pool.name == request.dataset_name.pool())
        {
            return Err(Error::ZpoolNotFound(format!(
                "{}, looked up while trying to add dataset",
                request.dataset_name.pool(),
            )));
        }

        let zoned = true;
        let fs_name = &request.dataset_name.full_name();
        let do_format = true;
        let encryption_details = None;
        let size_details = None;
        Zfs::ensure_filesystem(
            fs_name,
            Mountpoint::Path(Utf8PathBuf::from("/data")),
            zoned,
            do_format,
            encryption_details,
            size_details,
            None,
        )?;
        // Ensure the dataset has a usable UUID.
        if let Ok(id_str) = Zfs::get_oxide_value(&fs_name, "uuid") {
            if let Ok(id) = id_str.parse::<Uuid>() {
                if id != request.dataset_id {
                    return Err(Error::UuidMismatch {
                        name: Box::new(request.dataset_name.clone()),
                        old: id,
                        new: request.dataset_id,
                    });
                }
                return Ok(());
            }
        }
        Zfs::set_oxide_value(
            &fs_name,
            "uuid",
            &request.dataset_id.to_string(),
        )?;

        Ok(())
    }
}

/// All tests only use synthetic disks, but are expected to be run on illumos
/// systems.
#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use crate::dataset::DatasetKind;
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log(
            "add_u2_disk_while_not_in_normal_stage_and_ensure_it_gets_queued",
        );
        let (mut _key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, _) = StorageManager::new(&logctx.log, key_requester);
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let raw_disk: RawDisk = SyntheticDisk::new(zpool_name, 0).into();
        assert_eq!(StorageManagerState::WaitingForKeyManager, manager.state);
        manager.add_u2_disk(raw_disk.clone()).await.unwrap();
        assert!(manager.resources.all_u2_zpools().is_empty());
        assert_eq!(manager.queued_u2_drives, HashSet::from([raw_disk.clone()]));

        // Check other non-normal stages and ensure disk gets queued
        manager.queued_u2_drives.clear();
        manager.state = StorageManagerState::QueueingDisks;
        manager.add_u2_disk(raw_disk.clone()).await.unwrap();
        assert!(manager.resources.all_u2_zpools().is_empty());
        assert_eq!(manager.queued_u2_drives, HashSet::from([raw_disk]));
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_u2_gets_added_to_resources() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("ensure_u2_gets_added_to_resources");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (mut manager, _) = StorageManager::new(&logctx.log, key_requester);
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk =
            SyntheticDisk::create_zpool(dir.path(), &zpool_name, 0).into();

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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("wait_for_bootdisk");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (manager, mut handle) =
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
        let disk =
            SyntheticDisk::create_zpool(dir.path(), &zpool_name, 0).into();

        handle.upsert_disk(disk).await;
        handle.wait_for_boot_disk().await;
        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn queued_disks_get_added_as_resources() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("queued_disks_get_added_as_resources");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (manager, handle) = StorageManager::new(&logctx.log, key_requester);

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
        let disk =
            SyntheticDisk::create_zpool(dir.path(), &zpool_name, 0).into();
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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
        let disk =
            SyntheticDisk::create_zpool(dir.path(), &zpool_name, 0).into();
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("delete_disk_triggers_notification");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (manager, mut handle) =
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
            SyntheticDisk::create_zpool(dir.path(), &zpool_name, 0).into();
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

    #[tokio::test]
    async fn ensure_using_exactly_these_disks() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("ensure_using_exactly_these_disks");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (manager, mut handle) =
            StorageManager::new(&logctx.log, key_requester);

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Spawn the storage manager as done by sled-agent
        tokio::spawn(async move {
            manager.run().await;
        });

        // Create a bunch of file backed external disks with zpools
        let dir = tempdir().unwrap();
        let zpools: Vec<ZpoolName> =
            (0..10).map(|_| ZpoolName::new_external(Uuid::new_v4())).collect();
        let disks: Vec<RawDisk> = zpools
            .iter()
            .enumerate()
            .map(|(slot, zpool_name)| {
                SyntheticDisk::create_zpool(
                    dir.path(),
                    zpool_name,
                    slot.try_into().unwrap(),
                )
                .into()
            })
            .collect();

        // Add the first 3 disks, and ensure they get queued, as we haven't
        // marked our key manager ready yet
        handle
            .ensure_using_exactly_these_disks(disks.iter().take(3).cloned())
            .await;
        let state = handle.get_manager_state().await;
        assert_eq!(state.queued_u2_drives.len(), 3);
        assert_eq!(state.state, StorageManagerState::WaitingForKeyManager);
        assert!(handle.get_latest_resources().await.all_u2_zpools().is_empty());

        // Mark the key manager ready and wait for the storage update
        handle.key_manager_ready().await;
        let resources = handle.wait_for_changes().await;
        let expected: HashSet<_> =
            disks.iter().take(3).map(|d| d.identity()).collect();
        let actual: HashSet<_> = resources.disks().keys().collect();
        assert_eq!(expected, actual);

        // Add first three disks after the initial one. The returned resources
        // should not contain the first disk.
        handle
            .ensure_using_exactly_these_disks(
                disks.iter().skip(1).take(3).cloned(),
            )
            .await;
        let resources = handle.wait_for_changes().await;
        let expected: HashSet<_> =
            disks.iter().skip(1).take(3).map(|d| d.identity()).collect();
        let actual: HashSet<_> = resources.disks().keys().collect();
        assert_eq!(expected, actual);

        // Ensure the same set of disks and make sure no change occurs
        // Note that we directly request the resources this time so we aren't
        // waiting forever for a change notification.
        handle
            .ensure_using_exactly_these_disks(
                disks.iter().skip(1).take(3).cloned(),
            )
            .await;
        let resources2 = handle.get_latest_resources().await;
        assert_eq!(resources, resources2);

        // Add a disjoint set of disks and see that only they come through
        handle
            .ensure_using_exactly_these_disks(
                disks.iter().skip(4).take(5).cloned(),
            )
            .await;
        let resources = handle.wait_for_changes().await;
        let expected: HashSet<_> =
            disks.iter().skip(4).take(5).map(|d| d.identity()).collect();
        let actual: HashSet<_> = resources.disks().keys().collect();
        assert_eq!(expected, actual);

        // Finally, change the zpool backing of the 5th disk to be that of the 10th
        // and ensure that disk changes. Note that we don't change the identity
        // of the 5th disk.
        let mut modified_disk = disks[4].clone();
        if let RawDisk::Synthetic(disk) = &mut modified_disk {
            disk.zpool_name = disks[9].zpool_name().clone();
        } else {
            panic!();
        }
        let mut expected: HashSet<_> =
            disks.iter().skip(5).take(4).cloned().collect();
        expected.insert(modified_disk);

        handle
            .ensure_using_exactly_these_disks(expected.clone().into_iter())
            .await;
        let resources = handle.wait_for_changes().await;

        // Ensure the one modified disk changed as we expected
        assert_eq!(5, resources.disks().len());
        for raw_disk in expected {
            let (disk, pool) =
                resources.disks().get(raw_disk.identity()).unwrap();
            assert_eq!(disk.zpool_name(), raw_disk.zpool_name());
            assert_eq!(&pool.name, disk.zpool_name());
            assert_eq!(raw_disk.identity(), &pool.parent);
        }

        // Cleanup
        for zpool in zpools {
            Zpool::destroy(&zpool).unwrap();
        }
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_filesystem() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("upsert_filesystem");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever::default());
        let (manager, handle) = StorageManager::new(&logctx.log, key_requester);

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Spawn the storage manager as done by sled-agent
        tokio::spawn(async move {
            manager.run().await;
        });

        handle.key_manager_ready().await;

        // Create and add a disk
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let disk: RawDisk =
            SyntheticDisk::create_zpool(dir.path(), &zpool_name, 0).into();
        handle.upsert_disk(disk.clone()).await;

        // Create a filesystem
        let dataset_id = Uuid::new_v4();
        let dataset_name =
            DatasetName::new(zpool_name.clone(), DatasetKind::Crucible);
        handle.upsert_filesystem(dataset_id, dataset_name).await.unwrap();

        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }
}
