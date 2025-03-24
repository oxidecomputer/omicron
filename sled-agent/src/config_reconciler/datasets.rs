// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use debug_ignore::DebugIgnore;
use omicron_common::disk::DatasetConfig;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;

#[derive(Debug, thiserror::Error)]
pub enum DatasetTaskError {
    #[error("cannot perform dataset operations: waiting for key manager")]
    WaitingForKeyManager,
    #[error("dataset task busy; cannot service new requests")]
    Busy,
    #[error("internal error: dataset task exited!")]
    Exited,
}

pub struct DatasetTaskReconcilerHandle {
    tx: mpsc::Sender<ReconcilerRequest>,
}

impl DatasetTaskReconcilerHandle {
    pub async fn datasets_ensure(
        &self,
        datasets: Vec<DatasetConfig>,
    ) -> Result<(), DatasetTaskError> {
        let (tx, rx) = oneshot::channel();
        let req =
            ReconcilerRequest::DatasetsEnsure { datasets, tx: DebugIgnore(tx) };
        try_send_wrapper(&self.tx, req, rx).await
    }
}

pub struct DatasetTaskSupportBundleHandle {
    tx: mpsc::Sender<SupportBundleRequest>,
}

impl DatasetTaskSupportBundleHandle {
    pub async fn nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<(), DatasetTaskError> {
        let (tx, rx) = oneshot::channel();
        let req = SupportBundleRequest::NestedDatasetEnsure {
            config,
            tx: DebugIgnore(tx),
        };
        try_send_wrapper(&self.tx, req, rx).await
    }

    pub async fn nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<(), DatasetTaskError> {
        let (tx, rx) = oneshot::channel();
        let req = SupportBundleRequest::NestedDatasetDestroy {
            name,
            tx: DebugIgnore(tx),
        };
        try_send_wrapper(&self.tx, req, rx).await
    }

    pub async fn nested_dataset_list(
        &self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
    ) -> Result<(), DatasetTaskError> {
        let (tx, rx) = oneshot::channel();
        let req = SupportBundleRequest::NestedDatasetList {
            name,
            options,
            tx: DebugIgnore(tx),
        };
        try_send_wrapper(&self.tx, req, rx).await
    }
}

async fn try_send_wrapper<Req, Resp>(
    tx: &mpsc::Sender<Req>,
    req: Req,
    rx: oneshot::Receiver<Resp>,
) -> Result<Resp, DatasetTaskError> {
    tx.try_send(req).map_err(|err| match err {
        TrySendError::Full(_) => DatasetTaskError::Busy,
        // We should never see this error in production, as the dataset task
        // never exits, but may see it in tests.
        TrySendError::Closed(_) => DatasetTaskError::Exited,
    })?;
    match rx.await {
        Ok(result) => Ok(result),
        // As above, we should never see this error in production.
        Err(_) => Err(DatasetTaskError::Exited),
    }
}

#[derive(Debug)]
enum ReconcilerRequest {
    DatasetsEnsure {
        datasets: Vec<DatasetConfig>,
        tx: DebugIgnore<oneshot::Sender<()>>,
    },
}

#[derive(Debug)]
enum SupportBundleRequest {
    NestedDatasetEnsure {
        config: NestedDatasetConfig,
        tx: DebugIgnore<oneshot::Sender<()>>,
    },
    NestedDatasetDestroy {
        name: NestedDatasetLocation,
        tx: DebugIgnore<oneshot::Sender<()>>,
    },
    NestedDatasetList {
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
        tx: DebugIgnore<oneshot::Sender<()>>,
    },
}
