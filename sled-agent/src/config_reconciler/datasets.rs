// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use debug_ignore::DebugIgnore;
use futures::StreamExt as _;
use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::zfs::CanMount;
use illumos_utils::zfs::DatasetEnsureArgs;
use illumos_utils::zfs::DatasetProperties;
use illumos_utils::zfs::EnsureDatasetError;
use illumos_utils::zfs::Mountpoint;
use illumos_utils::zfs::WhichDatasets;
use illumos_utils::zfs::Zfs;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_storage::config::MountConfig;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
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

#[derive(Debug, Clone, Default)]
pub struct DatasetMap(IdMap<OmicronDataset>);

impl DatasetMap {
    pub(super) fn has_dataset_with_retryable_error(&self) -> bool {
        self.0.iter().any(|dataset| match &dataset.state {
            // Mounted datasets are not in an error state.
            DatasetState::Mounted => false,
            // These errors are permanent until we get a new config; there's no
            // need to retry.
            DatasetState::UuidMismatch { .. }
            | DatasetState::ParentMissingFromConfig => false,
            // These errors _could_ be ephemeral; they depend on other
            // components. Err on the side of retrying; we could try to be more
            // fine-grained here (e.g., if our parent failed to mount due to a
            // permanent error, we don't need to retry ourselves).
            DatasetState::ZpoolNotFound | DatasetState::ParentFailedToMount => {
                true
            }
            // Treat all errors from `illumos_utils` as retryable. This is
            // probably wrong?
            DatasetState::FailedToMount(_) => true,
        })
    }
}

#[derive(Debug, Clone)]
struct OmicronDataset {
    config: DatasetConfig,
    state: DatasetState,
}

impl IdMappable for OmicronDataset {
    type Id = DatasetUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}

#[derive(Debug, Clone)]
enum DatasetState {
    Mounted,
    FailedToMount(Arc<EnsureDatasetError>),
    UuidMismatch { name: String, old: DatasetUuid, new: DatasetUuid },
    ZpoolNotFound,
    ParentMissingFromConfig,
    ParentFailedToMount,
}

pub struct DatasetTaskReconcilerHandle {
    tx: mpsc::Sender<ReconcilerRequest>,
}

impl DatasetTaskReconcilerHandle {
    pub(super) async fn datasets_ensure(
        &self,
        dataset_configs: IdMap<DatasetConfig>,
        mount_config: Arc<MountConfig>,
        all_managed_u2_pools: BTreeSet<ZpoolUuid>,
    ) -> Result<DatasetMap, DatasetTaskError> {
        let (tx, rx) = oneshot::channel();
        let req = ReconcilerRequest::DatasetsEnsure {
            dataset_configs,
            mount_config,
            all_managed_u2_pools,
            tx: DebugIgnore(tx),
        };
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
        dataset_configs: IdMap<DatasetConfig>,
        mount_config: Arc<MountConfig>,
        all_managed_u2_pools: BTreeSet<ZpoolUuid>,
        tx: DebugIgnore<oneshot::Sender<DatasetMap>>,
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
    datasets: DatasetMap,
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
        zfs_impl: T,
    ) -> (DatasetTaskReconcilerHandle, DatasetTaskSupportBundleHandle) {
        // The Reconciler never sends concurrent requests, so this channel size
        // can be tiny.
        let (reconciler_tx, reconciler_rx) = mpsc::channel(1);

        // Support bundle requests come in from Nexus. We can allow some small
        // number of queued requests, but don't want to allow too many to avoid
        // many blocked HTTP requests if we go out to lunch somehow.
        let (support_bundle_tx, support_bundle_rx) = mpsc::channel(8);

        tokio::spawn(async move {
            Self {
                datasets: DatasetMap::default(),
                reconciler_rx,
                support_bundle_rx,
                log,
            }
            .run(&zfs_impl)
            .await;
        });

        (
            DatasetTaskReconcilerHandle { tx: reconciler_tx },
            DatasetTaskSupportBundleHandle { tx: support_bundle_tx },
        )
    }

    async fn run<T: ZfsImpl>(mut self, zfs: &T) {
        loop {
            tokio::select! {
                // Cancel-safe, per docs on `recv()`.
                req = self.reconciler_rx.recv(),
                    if !self.reconciler_rx.is_closed() =>
                {
                    if let Some(req) = req {
                        self.handle_reconciler_request(zfs, req).await;
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

    async fn handle_reconciler_request<T: ZfsImpl>(
        &mut self,
        zfs: &T,
        req: ReconcilerRequest,
    ) {
        match req {
            ReconcilerRequest::DatasetsEnsure {
                dataset_configs: config,
                mount_config,
                all_managed_u2_pools,
                tx,
            } => {
                self.datasets_ensure(
                    zfs,
                    &mount_config,
                    config,
                    all_managed_u2_pools,
                )
                .await;
                _ = tx.0.send(self.datasets.clone());
            }
        }
    }

    async fn datasets_ensure<T: ZfsImpl>(
        &mut self,
        zfs: &T,
        mount_config: &MountConfig,
        config: IdMap<DatasetConfig>,
        all_managed_u2_pools: BTreeSet<ZpoolUuid>,
    ) {
        let log = &self.log;

        // There's an implicit hierarchy inside the list of `DatasetConfig`s:
        //
        // 1. Each zpool may contain main datasets
        // 2. Any `DatasetKind::TransientZone { .. }` is a child of the
        //    `DatasetKind::TransientZoneRoot` on that same zpool.
        // 3. Most of the other `DatasetKind`s are children of the crypt dataset
        //    on that same zpool; however, the crypt dataset is not currently
        //    one we manage explicitly (it's ensured and mounted implicitly when
        //    we start managing its zpool).
        //
        // We make a pass over the datasets here and form a few buckets:
        //
        // 1. Collect all the `TransientZone`s configs, keyed by zpool
        // 2. Collect all the other configs in a `Vec`, but keep a map of
        //    zpool-to-`TransientZoneRoot` IDs
        //
        // We can ensure all the datasets in group 2 concurrently. After that
        // group is done, we can ensure all the datasets in group 1 concurrently
        // (but must filter out any whose parent `TransientZoneRoot` failed to
        // mount).
        let mut transient_zone_root_by_zpool = BTreeMap::new();
        let mut transient_zone_configs_by_zpool: BTreeMap<_, Vec<_>> =
            BTreeMap::new();
        let mut non_transient_zone_configs = Vec::new();

        // Also collect a list of all the dataset names we want to ensure; we'll
        // check whether they already exist, are mounted, and have the expected
        // properties to avoid doing unnecessary work.
        let mut dataset_names = Vec::new();

        for dataset in config {
            let zpool_id = dataset.name.pool().id();
            if !all_managed_u2_pools.contains(&zpool_id) {
                warn!(
                    log,
                    "configured dataset on zpool we're not managing";
                    "dataset" => ?dataset,
                );
                self.datasets.0.insert(OmicronDataset {
                    config: dataset,
                    state: DatasetState::ZpoolNotFound,
                });
                continue;
            }

            dataset_names.push(dataset.name.full_name());

            match dataset.name.kind() {
                DatasetKind::TransientZone { .. } => {
                    transient_zone_configs_by_zpool
                        .entry(zpool_id)
                        .or_default()
                        .push(dataset);
                }
                DatasetKind::TransientZoneRoot => {
                    // Record the dataset ID of the transient zone root for this
                    // pool, and log a warning if there are multiple. (This
                    // should never happen: we should reject such a sled config
                    // at ledgering time.)
                    if let Some(prev) = transient_zone_root_by_zpool
                        .insert(zpool_id, dataset.id)
                    {
                        warn!(
                            log,
                            "multiple transient zone root datasets on zpool";
                            "zpool_id" => %zpool_id,
                            "ignoring_root" => %prev,
                        );
                    }
                    non_transient_zone_configs.push(dataset);
                }
                DatasetKind::Cockroach
                | DatasetKind::Crucible
                | DatasetKind::Clickhouse
                | DatasetKind::ClickhouseKeeper
                | DatasetKind::ClickhouseServer
                | DatasetKind::ExternalDns
                | DatasetKind::InternalDns
                | DatasetKind::Debug
                | DatasetKind::Update => {
                    non_transient_zone_configs.push(dataset);
                }
            }
        }

        // Gather properties about all the datasets we want to ensure, if they
        // exist.
        //
        // This pre-fetching lets us avoid individually querying them later.
        let old_datasets = zfs
            .get_dataset_properties(&dataset_names, WhichDatasets::SelfOnly)
            .await
            .inspect_err(|err| {
                warn!(
                    log,
                    "failed to fetch ZFS dataset properties; \
                     will attempt to ensure all datasets";
                    InlineErrorChain::new(err.as_ref()),
                );
            })
            .unwrap_or_default()
            .into_iter()
            .map(|props| (props.name.clone(), props))
            .collect::<BTreeMap<_, _>>();
        let old_datasets = &old_datasets;

        // Ensure all the "group 2" datasets (i.e., everyting except
        // `TransientZone` datasets) concurrently.
        const DATASET_ENSURE_CONCURRENCY_LIMIT: usize = 16;
        let mut non_transient_zones = futures::stream::iter(
            non_transient_zone_configs.into_iter().map(|dataset| async move {
                let state = Self::dataset_ensure_one(
                    zfs,
                    mount_config,
                    &dataset,
                    old_datasets.get(&dataset.name.full_name()),
                    log,
                )
                .await;
                (dataset, state)
            }),
        )
        .buffer_unordered(DATASET_ENSURE_CONCURRENCY_LIMIT);

        while let Some((dataset, state)) = non_transient_zones.next().await {
            self.datasets.0.insert(OmicronDataset { config: dataset, state });
        }

        // For each transient zone dataset: either ensure it or mark down why we
        // don't try.
        let mut transient_zone_futures = Vec::new();
        for (zpool_id, datasets) in transient_zone_configs_by_zpool {
            for dataset in datasets {
                // Did we have the parent `TransientZoneRoot` for this zpool?
                let Some(zpool_transient_zone_root_dataset_id) =
                    transient_zone_root_by_zpool.get(&zpool_id)
                else {
                    self.datasets.0.insert(OmicronDataset {
                        config: dataset,
                        state: DatasetState::ParentMissingFromConfig,
                    });
                    continue;
                };

                // Did we successfully ensure that parent dataset?
                if !matches!(
                    self.datasets
                        .0
                        .get(zpool_transient_zone_root_dataset_id)
                        .map(|d| &d.state),
                    Some(DatasetState::Mounted)
                ) {
                    self.datasets.0.insert(OmicronDataset {
                        config: dataset,
                        state: DatasetState::ParentFailedToMount,
                    });
                    continue;
                }

                transient_zone_futures.push(async move {
                    let state = Self::dataset_ensure_one(
                        zfs,
                        mount_config,
                        &dataset,
                        old_datasets.get(&dataset.name.full_name()),
                        log,
                    )
                    .await;
                    (dataset, state)
                });
            }
        }

        let mut transient_zones = futures::stream::iter(transient_zone_futures)
            .buffer_unordered(DATASET_ENSURE_CONCURRENCY_LIMIT);
        while let Some((dataset, state)) = transient_zones.next().await {
            self.datasets.0.insert(OmicronDataset { config: dataset, state });
        }
    }

    async fn dataset_ensure_one<T: ZfsImpl>(
        zfs: &T,
        mount_config: &MountConfig,
        dataset: &DatasetConfig,
        old_dataset: Option<&DatasetProperties>,
        log: &Logger,
    ) -> DatasetState {
        let log = log.new(slog::o!("name" => dataset.name.full_name()));
        info!(log, "Ensuring dataset");

        if let Some(state) =
            Self::should_skip_dataset_ensure(dataset, old_dataset, &log)
        {
            return state;
        };

        // All the dataset kinds we currently allow in `DatasetConfig` are not
        // _themselves_ encrypted. Many of them are children of the encrypted
        // `crypt` dataset, which would require non-`None` encryption details to
        // ensure, but that's currently handled by `Disk::new()` when we start
        // managing external disks. This uses an explicit match so that if we
        // ever add `DatasetKind::Crypt`, we are forced to update this path
        // (which currently doesn't know what the encryption details would be).
        let encryption_details = match dataset.name.kind() {
            DatasetKind::Cockroach
            | DatasetKind::Crucible
            | DatasetKind::Clickhouse
            | DatasetKind::ClickhouseKeeper
            | DatasetKind::ClickhouseServer
            | DatasetKind::ExternalDns
            | DatasetKind::InternalDns
            | DatasetKind::TransientZoneRoot
            | DatasetKind::TransientZone { .. }
            | DatasetKind::Debug
            | DatasetKind::Update => None,
        };

        let size_details = Some(illumos_utils::zfs::SizeDetails {
            quota: dataset.inner.quota,
            reservation: dataset.inner.reservation,
            compression: dataset.inner.compression,
        });

        match zfs
            .ensure_dataset(DatasetEnsureArgs {
                name: dataset.name.full_name(),
                mountpoint: Mountpoint::Path(mount_config.root.clone()),
                can_mount: CanMount::On,
                zoned: dataset.name.kind().zoned(),
                encryption_details,
                size_details,
                id: Some(dataset.id),
                additional_options: None,
            })
            .await
        {
            Ok(()) => {
                info!(log, "Ensured dataset");
                DatasetState::Mounted
            }
            Err(err) => {
                warn!(
                    log, "Failed to ensure dataset";
                    InlineErrorChain::new(&err),
                );
                DatasetState::FailedToMount(Arc::new(err))
            }
        }
    }

    fn should_skip_dataset_ensure(
        dataset: &DatasetConfig,
        old_dataset: Option<&DatasetProperties>,
        log: &Logger,
    ) -> Option<DatasetState> {
        let Some(old_dataset) = old_dataset else {
            info!(log, "This dataset did not exist");
            return None;
        };

        let Some(old_id) = old_dataset.id else {
            info!(log, "Old properties missing UUID");
            return None;
        };

        if old_id != dataset.id {
            return Some(DatasetState::UuidMismatch {
                name: dataset.name.full_name(),
                old: old_id,
                new: dataset.id,
            });
        }

        let old_props = match SharedDatasetConfig::try_from(old_dataset) {
            Ok(old_props) => old_props,
            Err(err) => {
                warn!(
                    log, "Failed to parse old properties";
                    InlineErrorChain::new(err.as_ref()),
                );
                return None;
            }
        };

        info!(log, "Parsed old dataset properties"; "props" => ?old_props);
        if old_props != dataset.inner {
            info!(
                log,
                "Dataset properties changed";
                "old_props" => ?old_props,
                "requested_props" => ?dataset.inner,
            );
            return None;
        }

        if !dataset.name.kind().zoned() && !old_dataset.mounted {
            info!(
                log,
                "Dataset might need to be mounted";
                "old_dataset" => ?old_dataset,
                "requested_props" => ?dataset.inner,
            );
            return None;
        }

        info!(log, "No changes necessary, returning early");
        return Some(DatasetState::Mounted);
    }

    async fn handle_support_bundle_request<T: ZfsImpl>(
        &self,
        req: SupportBundleRequest,
        zfs: &T,
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
        zfs: &T,
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

trait ZfsImpl: Send + Sync + 'static {
    fn get_dataset_properties(
        &self,
        datasets: &[String],
        which: WhichDatasets,
    ) -> impl Future<Output = anyhow::Result<Vec<DatasetProperties>>> + Send;

    fn ensure_dataset(
        &self,
        args: DatasetEnsureArgs,
    ) -> impl Future<Output = Result<(), EnsureDatasetError>> + Send;
}

struct RealZfs;

impl ZfsImpl for RealZfs {
    async fn get_dataset_properties(
        &self,
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

    async fn ensure_dataset(
        &self,
        args: DatasetEnsureArgs,
    ) -> Result<(), EnsureDatasetError> {
        tokio::task::spawn_blocking(move || Zfs::ensure_dataset(args))
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
            &self,
            datasets: &[String],
            which: WhichDatasets,
        ) -> anyhow::Result<Vec<DatasetProperties>> {
            let mut slf = self.lock().unwrap();
            slf.get_requests.push((datasets.to_vec(), which));
            slf.get_responses.remove(0)
        }

        async fn ensure_dataset(
            &self,
            _args: DatasetEnsureArgs,
        ) -> Result<(), EnsureDatasetError> {
            unimplemented!()
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
