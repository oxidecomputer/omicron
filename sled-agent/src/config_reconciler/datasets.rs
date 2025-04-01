// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use debug_ignore::DebugIgnore;
use illumos_utils::zfs::DatasetProperties;
use illumos_utils::zfs::WhichDatasets;
use illumos_utils::zfs::Zfs;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::SharedDatasetConfig;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::future::Future;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;

#[derive(Debug, thiserror::Error)]
pub enum DatasetTaskError {
    #[error("cannot perform dataset operations: waiting for key manager")]
    WaitingForKeyManager,
    #[error("failed to list nested dataset properties")]
    NestedDatasetListProperties(#[source] anyhow::Error),
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
    ) -> Result<Vec<NestedDatasetConfig>, DatasetTaskError> {
        let (tx, rx) = oneshot::channel();
        let req = SupportBundleRequest::NestedDatasetList {
            name,
            options,
            tx: DebugIgnore(tx),
        };
        try_send_wrapper(&self.tx, req, rx).await?
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
        tx: DebugIgnore<
            oneshot::Sender<Result<Vec<NestedDatasetConfig>, DatasetTaskError>>,
        >,
    },
}

pub struct DatasetTask {
    reconciler_rx: mpsc::Receiver<ReconcilerRequest>,
    support_bundle_rx: mpsc::Receiver<SupportBundleRequest>,
    log: Logger,
}

impl DatasetTask {
    pub fn spawn(
        log: Logger,
    ) -> (DatasetTaskReconcilerHandle, DatasetTaskSupportBundleHandle) {
        Self::spawn_impl(log, RealZfs)
    }

    fn spawn_impl<T: ZfsImpl>(
        log: Logger,
        mut zfs_impl: T,
    ) -> (DatasetTaskReconcilerHandle, DatasetTaskSupportBundleHandle) {
        // The Reconciler never sends concurrent requests, so this channel size
        // can be tiny.
        let (reconciler_tx, reconciler_rx) = mpsc::channel(1);

        // Support bundle requests come in from Nexus. We can allow some small
        // number of queued requests, but don't want to allow too many to avoid
        // many blocked HTTP requests if we go out to lunch somehow.
        let (support_bundle_tx, support_bundle_rx) = mpsc::channel(8);

        tokio::spawn(async move {
            Self { reconciler_rx, support_bundle_rx, log }
                .run(&mut zfs_impl)
                .await;
        });

        (
            DatasetTaskReconcilerHandle { tx: reconciler_tx },
            DatasetTaskSupportBundleHandle { tx: support_bundle_tx },
        )
    }

    async fn run<T: ZfsImpl>(mut self, zfs: &mut T) {
        loop {
            tokio::select! {
                // Cancel-safe, per docs on `recv()`.
                req = self.reconciler_rx.recv(),
                    if !self.reconciler_rx.is_closed() =>
                {
                    if let Some(req) = req {
                        self.handle_reconciler_request(req).await;
                    }
                }

                // Cancel-safe, per docs on `recv()`.
                req = self.support_bundle_rx.recv(),
                    if !self.support_bundle_rx.is_closed() =>
                {
                    if let Some(req) = req {
                        self.handle_support_bundle_request(req, zfs).await;
                    }
                }

                else => {
                    // This should never happen in production, but may happen in
                    // tests.
                    warn!(self.log, "all handles closed; exiting dataset task");
                    return;
                }
            }
        }
    }

    async fn handle_reconciler_request(&mut self, _req: ReconcilerRequest) {
        unimplemented!()
    }

    async fn handle_support_bundle_request<T: ZfsImpl>(
        &self,
        req: SupportBundleRequest,
        zfs: &mut T,
    ) {
        match req {
            SupportBundleRequest::NestedDatasetEnsure { .. } => {
                unimplemented!()
            }
            SupportBundleRequest::NestedDatasetDestroy { .. } => {
                unimplemented!()
            }
            SupportBundleRequest::NestedDatasetList { name, options, tx } => {
                _ = tx
                    .0
                    .send(self.nested_dataset_list(name, options, zfs).await);
            }
        }
    }

    async fn nested_dataset_list<T: ZfsImpl>(
        &self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
        zfs: &mut T,
    ) -> Result<Vec<NestedDatasetConfig>, DatasetTaskError> {
        let log = self.log.new(o!("request" => "nested_dataset_list"));
        info!(log, "Listing nested datasets");

        let full_name = name.full_name();
        let get_properties_result = zfs
            .get_dataset_properties(
                &[full_name],
                WhichDatasets::SelfAndChildren,
            )
            .await;

        let properties = match get_properties_result {
            Ok(properties) => properties,
            Err(err) => {
                let err = DatasetTaskError::NestedDatasetListProperties(err);
                warn!(
                    log,
                    "Failed to access nested dataset";
                    "name" => ?name,
                    InlineErrorChain::new(&err),
                );
                return Err(err);
            }
        };

        let root_path = name.root.full_name();
        Ok(properties
            .into_iter()
            .filter_map(|prop| {
                let path = if prop.name == root_path {
                    match options {
                        NestedDatasetListOptions::ChildrenOnly => return None,
                        NestedDatasetListOptions::SelfAndChildren => {
                            String::new()
                        }
                    }
                } else {
                    prop.name
                        .strip_prefix(&root_path)?
                        .strip_prefix("/")?
                        .to_string()
                };

                Some(NestedDatasetConfig {
                    // The output of our "zfs list" command could be nested away
                    // from the root - so we actually copy our input to our
                    // output here, and update the path relative to the input
                    // root.
                    name: NestedDatasetLocation {
                        path,
                        root: name.root.clone(),
                    },
                    inner: SharedDatasetConfig {
                        compression: prop.compression.parse().ok()?,
                        quota: prop.quota,
                        reservation: prop.reservation,
                    },
                })
            })
            .collect())
    }
}

trait ZfsImpl: Send + 'static {
    fn get_dataset_properties(
        &mut self,
        datasets: &[String],
        which: WhichDatasets,
    ) -> impl Future<Output = anyhow::Result<Vec<DatasetProperties>>> + Send;
}

struct RealZfs;

impl ZfsImpl for RealZfs {
    async fn get_dataset_properties(
        &mut self,
        datasets: &[String],
        which: WhichDatasets,
    ) -> anyhow::Result<Vec<DatasetProperties>> {
        let datasets = datasets.to_vec();
        tokio::task::spawn_blocking(move || {
            Zfs::get_dataset_properties(&datasets, which)
        })
        .await
        .expect("blocking closure did not panic")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use illumos_utils::zpool::ZpoolName;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct FakeZfs {
        get_requests: Vec<(Vec<String>, WhichDatasets)>,
        get_responses: Vec<anyhow::Result<Vec<DatasetProperties>>>,
    }

    impl ZfsImpl for Arc<Mutex<FakeZfs>> {
        async fn get_dataset_properties(
            &mut self,
            datasets: &[String],
            which: WhichDatasets,
        ) -> anyhow::Result<Vec<DatasetProperties>> {
            let mut slf = self.lock().unwrap();
            slf.get_requests.push((datasets.to_vec(), which));
            slf.get_responses.remove(0)
        }
    }

    #[tokio::test]
    async fn test_get_dataset_properties_forwards_errors() {
        let logctx =
            dev::test_setup_log("test_get_dataset_properties_forwards_errors");

        let fake_zfs = Arc::new(Mutex::new(FakeZfs::default()));
        let (_, handle) =
            DatasetTask::spawn_impl(logctx.log.clone(), Arc::clone(&fake_zfs));

        let expected_err = "dummy error";
        fake_zfs.lock().unwrap().get_responses.push(Err(anyhow!(expected_err)));

        match handle
            .nested_dataset_list(
                NestedDatasetLocation {
                    path: "test".into(),
                    root: DatasetName::new(
                        ZpoolName::new_external(ZpoolUuid::new_v4()),
                        DatasetKind::Debug,
                    ),
                },
                NestedDatasetListOptions::SelfAndChildren,
            )
            .await
        {
            Err(DatasetTaskError::NestedDatasetListProperties(err))
                if err.to_string() == expected_err => {}
            Err(err) => panic!("unexpected error: {err:#}"),
            Ok(props) => panic!("unexpected success: {props:?}"),
        }

        logctx.cleanup_successful();
    }

    // TODO-john more tests
    // TODO-john address unimplemented!()
}
