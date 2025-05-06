// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Many of the ZFS operations sled-agent performs are not atomic, because they
//! involve multiple lower-level ZFS operations. This module implements a tokio
//! task that serializes a set of operations to ensure no two operations could
//! be executing concurrently.
//!
//! It uses the common pattern of "a task with a mpsc channel to send requests,
//! using oneshot channels to send responses".

use camino::Utf8PathBuf;
use id_map::IdMap;
use id_map::IdMappable;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use omicron_common::disk::DatasetConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use sled_storage::config::MountConfig;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use slog::Logger;
use slog::warn;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub enum DatasetTaskError {
    #[error("cannot perform dataset operations: waiting for key manager")]
    WaitingForKeyManager,
    #[error("dataset task busy; cannot service new requests")]
    Busy,
    #[error("internal error: dataset task exited!")]
    Exited,
}

#[derive(Debug)]
pub(crate) struct DatasetEnsureResult(IdMap<SingleDatasetEnsureResult>);

#[derive(Debug, Clone)]
struct SingleDatasetEnsureResult {
    config: DatasetConfig,
    state: DatasetState,
}

impl IdMappable for SingleDatasetEnsureResult {
    type Id = DatasetUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}

#[derive(Debug, Clone)]
enum DatasetState {
    Mounted,
    FailedToMount, // TODO add error
    UuidMismatch(DatasetUuid),
    ZpoolNotFound,
    ParentMissingFromConfig,
    ParentFailedToMount,
}

#[derive(Debug)]
pub(crate) struct DatasetTaskHandle(mpsc::Sender<DatasetTaskRequest>);

impl DatasetTaskHandle {
    pub fn spawn_dataset_task(
        mount_config: Arc<MountConfig>,
        base_log: &Logger,
    ) -> Self {
        // We don't expect too many concurrent requests to this task, and want
        // to detect "the task is wedged" pretty quickly. Common operations:
        //
        // 1. Reconciler wants to ensure datasets (at most 1 at a time)
        // 2. Inventory requests from Nexus (likely at most 3 at a time)
        // 3. Support bundle operations (unlikely to be multiple concurrently)
        //
        // so we'll pick a number that allows all of those plus a little
        // overhead.
        let (request_tx, request_rx) = mpsc::channel(16);

        tokio::spawn(
            DatasetTask {
                mount_config,
                request_rx,
                log: base_log.new(slog::o!("component" => "DatasetTask")),
            }
            .run(),
        );

        Self(request_tx)
    }

    pub async fn datasets_ensure(
        &self,
        _dataset_configs: IdMap<DatasetConfig>,
        _zpools: BTreeSet<ZpoolName>,
    ) -> Result<DatasetEnsureResult, DatasetTaskError> {
        unimplemented!()
    }

    pub async fn inventory(
        &self,
        _zpools: BTreeSet<ZpoolName>,
    ) -> Result<Vec<InventoryDataset>, DatasetTaskError> {
        unimplemented!()
    }

    pub async fn nested_dataset_ensure_mounted(
        &self,
        _dataset: NestedDatasetLocation,
    ) -> Result<Utf8PathBuf, DatasetTaskError> {
        unimplemented!()
    }

    pub async fn nested_dataset_ensure(
        &self,
        _config: NestedDatasetConfig,
    ) -> Result<(), DatasetTaskError> {
        unimplemented!()
    }

    pub async fn nested_dataset_destroy(
        &self,
        _name: NestedDatasetLocation,
    ) -> Result<(), DatasetTaskError> {
        unimplemented!()
    }

    pub async fn nested_dataset_list(
        &self,
        _name: NestedDatasetLocation,
        _options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, DatasetTaskError> {
        unimplemented!()
    }
}

struct DatasetTask {
    mount_config: Arc<MountConfig>,
    request_rx: mpsc::Receiver<DatasetTaskRequest>,
    log: Logger,
}

impl DatasetTask {
    async fn run(mut self) {
        while let Some(req) = self.request_rx.recv().await {
            self.handle_request(req).await;
        }
        warn!(self.log, "all request handles closed; exiting dataset task");
    }

    async fn handle_request(&mut self, _req: DatasetTaskRequest) {
        unimplemented!()
    }
}

enum DatasetTaskRequest {}
