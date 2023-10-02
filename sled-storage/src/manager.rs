// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use std::collections::{BTreeSet, HashSet};

use crate::dataset::{self, DatasetError};
use crate::disk::{Disk, DiskError, DiskWrapper};
use crate::error::Error;
use crate::resources::StorageResources;
use derive_more::From;
use illumos_utils::zpool::{ZpoolKind, ZpoolName};
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use sled_hardware::{DiskVariant, UnparsedDisk};
use slog::{debug, error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot, watch};

// The size of the mpsc bounded channel used to communicate
// between the `StorageHandle` and `StorageManager`.
const QUEUE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageManagerStage {
    WaitingForBootDisk,
    WaitingForKeyManager,
    QueuingDisks,
    Normal,
}

enum StorageRequest {
    AddDisk(UnparsedDisk),
    AddSyntheticDisk(ZpoolName),
    RemoveDisk(UnparsedDisk),
    DisksChanged(Vec<UnparsedDisk>),
    //    NewFilesystem(NewFilesystemRequest),
    KeyManagerReady,
}

/// A mechanism for interacting with the [`StorageManager`]
pub struct StorageHandle {
    tx: mpsc::Sender<StorageRequest>,
    resource_updates: watch::Receiver<StorageResources>,
}

/// The storage manager responsible for the state of the storage
/// on a sled. The storage manager runs in its own task and is interacted
/// with via the [`StorageHandle`].
pub struct StorageManager {
    log: Logger,
    stage: StorageManagerStage,
    rx: mpsc::Receiver<StorageRequest>,
    resources: StorageResources,
    queued_u2_drives: HashSet<UnparsedDisk>,
    queued_synthetic_u2_drives: HashSet<ZpoolName>,
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
                stage: StorageManagerStage::WaitingForBootDisk,
                rx,
                resources,
                queued_u2_drives: HashSet::new(),
                queued_synthetic_u2_drives: HashSet::new(),
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
            if let Err(e) = self.step().await {
                warn!(self.log, "{e}");
                return;
            }
        }
    }

    /// Process the next event
    ///
    /// This is useful for testing/debugging
    pub async fn step(&mut self) -> Result<(), Error> {
        // The sending side should never disappear
        match self.rx.recv().await.unwrap() {
            StorageRequest::AddDisk(unparsed_disk) => {
                match unparsed_disk.variant() {
                    DiskVariant::U2 => self.add_u2_disk(unparsed_disk).await?,
                    DiskVariant::M2 => todo!(),
                }
            }
            StorageRequest::AddSyntheticDisk(zpool_name) => {
                match zpool_name.kind() {
                    ZpoolKind::External => {
                        self.add_synthetic_u2_disk(zpool_name).await?
                    }
                    ZpoolKind::Internal => todo!(),
                }
            }
            StorageRequest::RemoveDisk(_unparsed_disk) => todo!(),
            StorageRequest::DisksChanged(_unparsed_disks) => todo!(),
            StorageRequest::KeyManagerReady => todo!(),
        }
        Ok(())
    }

    /// Add a real U.2 disk to storage resources or queue it to be added later
    async fn add_u2_disk(
        &mut self,
        unparsed_disk: UnparsedDisk,
    ) -> Result<(), Error> {
        if self.stage != StorageManagerStage::Normal {
            self.queued_u2_drives.insert(unparsed_disk);
            return Ok(());
        }

        match Disk::new(
            &self.log,
            unparsed_disk.clone(),
            Some(&self.key_requester),
        )
        .await
        {
            Ok(disk) => self.resources.insert_real_disk(disk),
            Err(err @ DiskError::Dataset(DatasetError::KeyManager(_))) => {
                warn!(
                    self.log,
                    "Transient error: {err} - queuing disk {:?}", unparsed_disk
                );
                self.queued_u2_drives.insert(unparsed_disk);
                self.stage = StorageManagerStage::QueuingDisks;
                Err(err.into())
            }
            Err(err) => {
                error!(
                    self.log,
                    "Persistent error: {err} - not queueing disk {:?}",
                    unparsed_disk
                );
                Err(err.into())
            }
        }
    }

    /// Add a synthetic U.2 disk to storage resources or queue it to be added later
    async fn add_synthetic_u2_disk(
        &mut self,
        zpool_name: ZpoolName,
    ) -> Result<(), Error> {
        if self.stage != StorageManagerStage::Normal {
            info!(self.log, "Queuing synthetic U.2 drive: {zpool_name}");
            self.queued_synthetic_u2_drives.insert(zpool_name);
            return Ok(());
        }

        let synthetic_id = DiskIdentity {
            vendor: "fake_vendor".to_string(),
            serial: "fake_serial".to_string(),
            model: zpool_name.id().to_string(),
        };

        debug!(self.log, "Ensure zpool has datasets: {zpool_name}");
        match dataset::ensure_zpool_has_datasets(
            &self.log,
            &zpool_name,
            &synthetic_id,
            Some(&self.key_requester),
        )
        .await
        {
            Ok(()) => self.resources.insert_synthetic_disk(zpool_name),
            Err(err @ DatasetError::KeyManager(_)) => {
                warn!(
                    self.log,
                    "Transient error: {err} - queuing disk {:?}", synthetic_id
                );
                self.queued_synthetic_u2_drives.insert(zpool_name);
                self.stage = StorageManagerStage::QueuingDisks;
                Ok(())
            }
            Err(err) => {
                error!(
                    self.log,
                    "Persistent error: {err} - not queueing disk {:?}",
                    synthetic_id
                );
                Ok(())
            }
        }
    }
}

/// All tests only use synthetic disks, but are expected to be run on illumos
/// systems.
#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use camino::{Utf8Path, Utf8PathBuf};
    use camino_tempfile::tempdir;
    use illumos_utils::zpool::Zpool;
    use key_manager::{
        KeyManager, SecretRetriever, SecretRetrieverError, SecretState,
        VersionedIkm,
    };
    use omicron_test_utils::dev::test_setup_log;
    use std::fs::File;
    use uuid::Uuid;

    /// A [`key-manager::SecretRetriever`] that only returns hardcoded IKM for
    /// epoch 0
    #[derive(Debug)]
    struct HardcodedSecretRetriever {}

    #[async_trait]
    impl SecretRetriever for HardcodedSecretRetriever {
        async fn get_latest(
            &self,
        ) -> Result<VersionedIkm, SecretRetrieverError> {
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
            if epoch != 0 {
                return Err(SecretRetrieverError::NoSuchEpoch(epoch));
            }
            Ok(SecretState::Current(self.get_latest().await?))
        }
    }

    // 64 MiB (min size of zpool)
    const DISK_SIZE: u64 = 64 * 1024 * 1024;

    // Create a synthetic disk with a zpool backed by a file
    fn new_disk(dir: &Utf8Path, zpool_name: &ZpoolName) -> Utf8PathBuf {
        let path = dir.join(zpool_name.to_string());
        let file = File::create(&path).unwrap();
        file.set_len(DISK_SIZE).unwrap();
        drop(file);
        Zpool::create(zpool_name, &path).unwrap();
        Zpool::import(zpool_name).unwrap();
        Zpool::set_failmode_continue(zpool_name).unwrap();
        path
    }

    #[tokio::test]
    async fn add_u2_disk_while_not_in_normal_stage_and_ensure_it_gets_queued() {
        let logctx = test_setup_log(
            "add_u2_disk_while_not_in_normal_stage_and_ensure_it_gets_queued",
        );
        let (mut _key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever {});
        let (mut manager, _) = StorageManager::new(&logctx.log, key_requester);
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        assert_eq!(StorageManagerStage::WaitingForBootDisk, manager.stage);
        manager.add_synthetic_u2_disk(zpool_name.clone()).await.unwrap();
        assert!(manager.resources.all_u2_zpools().is_empty());
        assert_eq!(
            manager.queued_synthetic_u2_drives,
            HashSet::from([zpool_name.clone()])
        );

        // Walk through other non-normal stages and enusre disk gets queued
        for stage in [
            StorageManagerStage::WaitingForKeyManager,
            StorageManagerStage::QueuingDisks,
        ] {
            manager.queued_synthetic_u2_drives.clear();
            manager.stage = stage;
            manager.add_synthetic_u2_disk(zpool_name.clone()).await.unwrap();
            assert!(manager.resources.all_u2_zpools().is_empty());
            assert_eq!(
                manager.queued_synthetic_u2_drives,
                HashSet::from([zpool_name.clone()])
            );
        }
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_u2_gets_added_to_resources() {
        let logctx = test_setup_log("ensure_u2_gets_added_to_resources");
        let (mut key_manager, key_requester) =
            KeyManager::new(&logctx.log, HardcodedSecretRetriever {});
        let (mut manager, _) = StorageManager::new(&logctx.log, key_requester);
        let zpool_name = ZpoolName::new_external(Uuid::new_v4());
        let dir = tempdir().unwrap();
        let _ = new_disk(dir.path(), &zpool_name);

        // Spawn the key_manager so that it will respond to requests for encryption keys
        tokio::spawn(async move { key_manager.run().await });

        // Set the stage to pretend we've progressed enough to have a key_manager available.
        manager.stage = StorageManagerStage::Normal;
        manager.add_synthetic_u2_disk(zpool_name.clone()).await.unwrap();
        assert_eq!(manager.resources.all_u2_zpools().len(), 1);
        Zpool::destroy(&zpool_name).unwrap();
        logctx.cleanup_successful();
    }
}
