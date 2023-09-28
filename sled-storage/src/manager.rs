// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use std::collections::{BTreeSet, HashSet};

use crate::dataset::DatasetError;
use crate::disk::{Disk, DiskError, DiskWrapper};
use crate::error::Error;
use crate::resources::StorageResources;
use derive_more::From;
use illumos_utils::zpool::{ZpoolKind, ZpoolName};
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use sled_hardware::{DiskVariant, UnparsedDisk};
use slog::{error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot};

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

enum StorageRequest {}

/// A mechanism for interacting with the [`StorageManager`]
pub struct StorageHandle {
    tx: mpsc::Sender<StorageRequest>,
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
    queued_synthetic_u2_drives: BTreeSet<ZpoolName>,
    key_requester: StorageKeyRequester,
}

impl StorageManager {
    pub fn new(
        log: &Logger,
        key_requester: StorageKeyRequester,
    ) -> (StorageManager, StorageHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        (
            StorageManager {
                log: log.new(o!("component" => "StorageManager")),
                stage: StorageManagerStage::WaitingForBootDisk,
                rx,
                resources: StorageResources::default(),
                queued_u2_drives: HashSet::new(),
                queued_synthetic_u2_drives: BTreeSet::new(),
                key_requester,
            },
            StorageHandle { tx },
        )
    }

    /// Add a disk to storage resources or queue it to be added later
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
}
