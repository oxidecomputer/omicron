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

use crate::CurrentlyManagedZpoolsReceiver;
use camino::Utf8PathBuf;
use debug_ignore::DebugIgnore;
use futures::StreamExt;
use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::zfs;
use illumos_utils::zfs::CanMount;
use illumos_utils::zfs::DatasetEnsureArgs;
use illumos_utils::zfs::DatasetProperties;
use illumos_utils::zfs::DestroyDatasetError;
use illumos_utils::zfs::Mountpoint;
use illumos_utils::zfs::WhichDatasets;
use illumos_utils::zfs::Zfs;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::SharedDatasetConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use sled_storage::config::MountConfig;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use slog::Logger;
use slog::info;
use slog::warn;
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
    #[error("dataset task busy; cannot service new requests")]
    Busy,
    #[error("internal error: dataset task exited!")]
    Exited,
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetEnsureError {
    #[error("could not find matching zpool: {0}")]
    ZpoolNotFound(ZpoolName),
    #[error("failed to ensure dataset")]
    EnsureFailed {
        name: String,
        #[source]
        err: zfs::EnsureDatasetError,
    },
    #[cfg(test)]
    #[error("test error: {0}")]
    TestError(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum NestedDatasetMountError {
    #[error("could not mount dataset {}", .name)]
    MountFailed {
        name: String,
        #[source]
        err: zfs::EnsureDatasetError,
    },
    #[cfg(test)]
    #[error("test error: {0}")]
    TestError(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum NestedDatasetEnsureError {
    #[error("parent dataset has not been mounted: {}", .0.full_name())]
    ParentDatasetNotMounted(DatasetName),
    #[error("could not ensure dataset {}", .name.full_name())]
    EnsureFailed {
        name: NestedDatasetLocation,
        #[source]
        err: DatasetEnsureError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum NestedDatasetDestroyError {
    #[error("cannot destroy dataset with empty name")]
    EmptyName,
    #[error(transparent)]
    DestroyFailed(#[from] DestroyDatasetError),
}

#[derive(Debug, thiserror::Error)]
pub enum NestedDatasetListError {
    #[error("failed to list properties of dataset {}", .name.full_name())]
    DatasetListProperties {
        name: DatasetName,
        #[source]
        err: anyhow::Error,
    },
}

#[derive(Debug, Clone, Default)]
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
    FailedToEnsure(Arc<DatasetEnsureError>),
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
        currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
        base_log: &Logger,
    ) -> Self {
        Self::spawn_with_zfs_impl(
            mount_config,
            currently_managed_zpools_rx,
            base_log,
            RealZfs,
        )
    }

    fn spawn_with_zfs_impl<T: ZfsImpl>(
        mount_config: Arc<MountConfig>,
        currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
        base_log: &Logger,
        zfs: T,
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
                currently_managed_zpools_rx,
                request_rx,
                datasets: DatasetEnsureResult::default(),
                log: base_log.new(slog::o!("component" => "DatasetTask")),
            }
            .run(zfs),
        );

        Self(request_tx)
    }

    pub async fn inventory(
        &self,
        _zpools: BTreeSet<ZpoolName>,
    ) -> Result<Vec<InventoryDataset>, DatasetTaskError> {
        unimplemented!()
    }

    pub async fn datasets_ensure(
        &self,
        datasets: IdMap<DatasetConfig>,
    ) -> Result<DatasetEnsureResult, DatasetTaskError> {
        self.try_send_request(|tx| DatasetTaskRequest::DatasetsEnsure {
            datasets,
            tx,
        })
        .await
    }

    pub async fn nested_dataset_ensure_mounted(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<Result<Utf8PathBuf, NestedDatasetMountError>, DatasetTaskError>
    {
        self.try_send_request(|tx| DatasetTaskRequest::NestedDatasetMount {
            name: name.clone(),
            tx,
        })
        .await
    }

    pub async fn nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<Result<(), NestedDatasetEnsureError>, DatasetTaskError> {
        self.try_send_request(|tx| DatasetTaskRequest::NestedDatasetEnsure {
            config,
            tx,
        })
        .await
    }

    pub async fn nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<Result<(), NestedDatasetDestroyError>, DatasetTaskError> {
        self.try_send_request(|tx| DatasetTaskRequest::NestedDatasetDestroy {
            name,
            tx,
        })
        .await
    }

    pub async fn nested_dataset_list(
        &self,
        dataset: DatasetName,
        options: NestedDatasetListOptions,
    ) -> Result<
        Result<Vec<NestedDatasetConfig>, NestedDatasetListError>,
        DatasetTaskError,
    > {
        self.try_send_request(|tx| DatasetTaskRequest::NestedDatasetList {
            dataset,
            options,
            tx,
        })
        .await
    }

    async fn try_send_request<T, F>(
        &self,
        make_request: F,
    ) -> Result<T, DatasetTaskError>
    where
        F: FnOnce(DebugIgnore<oneshot::Sender<T>>) -> DatasetTaskRequest,
    {
        let (tx, rx) = oneshot::channel();
        let request = make_request(DebugIgnore(tx));
        self.0.try_send(request).map_err(|err| match err {
            // We should only see this error if the ledger task has gotten badly
            // behind updating ledgers on M.2s.
            TrySendError::Full(_) => DatasetTaskError::Busy,
            // We should never see this error in production, as the ledger task
            // never exits, but may see it in tests.
            TrySendError::Closed(_) => DatasetTaskError::Exited,
        })?;
        rx.await.map_err(|_| {
            // As above, we should never see this error in production.
            DatasetTaskError::Exited
        })
    }
}

struct DatasetTask {
    mount_config: Arc<MountConfig>,
    request_rx: mpsc::Receiver<DatasetTaskRequest>,
    currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    datasets: DatasetEnsureResult,
    log: Logger,
}

impl DatasetTask {
    async fn run<T: ZfsImpl>(mut self, zfs: T) {
        while let Some(req) = self.request_rx.recv().await {
            self.handle_request(req, &zfs).await;
        }
        warn!(self.log, "all request handles closed; exiting dataset task");
    }

    async fn handle_request<T: ZfsImpl>(
        &mut self,
        request: DatasetTaskRequest,
        zfs: &T,
    ) {
        // In all cases, we don't care if the receiver is gone.
        match request {
            DatasetTaskRequest::DatasetsEnsure { datasets, tx } => {
                self.datasets_ensure(datasets, zfs).await;
                _ = tx.0.send(self.datasets.clone());
            }
            DatasetTaskRequest::NestedDatasetMount { name, tx } => {
                _ = tx.0.send(self.nested_dataset_mount(name, zfs).await);
            }
            DatasetTaskRequest::NestedDatasetEnsure { config, tx } => {
                _ = tx.0.send(self.nested_dataset_ensure(config, zfs).await);
            }
            DatasetTaskRequest::NestedDatasetDestroy { name, tx } => {
                _ = tx.0.send(self.nested_dataset_destroy(name, zfs).await);
            }
            DatasetTaskRequest::NestedDatasetList { dataset, options, tx } => {
                _ = tx.0.send(
                    self.nested_dataset_list(dataset, options, zfs).await,
                );
            }
        }
    }

    async fn datasets_ensure<T: ZfsImpl>(
        &mut self,
        config: IdMap<DatasetConfig>,
        zfs: &T,
    ) {
        // There's an implicit hierarchy inside the list of `DatasetConfig`s:
        //
        // 1. Each zpool may contain many datasets
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

        // Grab a snapshot of the currently-managed zpools. We'll refuse to
        // create any datasets on zpools that aren't in this set.
        //
        // This looks like a TOCTOU problem: what if the set of managed zpools
        // changed in between this snapshot and when we actually create datasets
        // below? In practice it isn't, although this feels a little fragile:
        // only the reconciler task calls this method, and only the reconciler
        // task changes the set of managed disks. It will not change the set of
        // managed disks while waiting for us to ensure datasets on its behalf.
        let currently_managed_zpools =
            self.currently_managed_zpools_rx.current();

        for dataset in config {
            let zpool = dataset.name.pool();

            if !currently_managed_zpools.contains(&zpool) {
                warn!(
                    self.log,
                    "configured dataset on zpool we're not managing";
                    "dataset" => ?dataset,
                );
                self.datasets.0.insert(SingleDatasetEnsureResult {
                    config: dataset,
                    state: DatasetState::ZpoolNotFound,
                });
                continue;
            }

            dataset_names.push(dataset.name.full_name());

            match dataset.name.kind() {
                DatasetKind::TransientZone { .. } => {
                    transient_zone_configs_by_zpool
                        .entry(zpool.id())
                        .or_default()
                        .push(dataset);
                }
                DatasetKind::TransientZoneRoot => {
                    // Record the dataset ID of the transient zone root for this
                    // pool, and log a warning if there are multiple. (This
                    // should never happen: we should reject such a sled config
                    // at ledgering time.)
                    if let Some(prev) = transient_zone_root_by_zpool
                        .insert(zpool.id(), dataset.id)
                    {
                        warn!(
                            self.log,
                            "multiple transient zone root datasets on zpool";
                            "zpool" => %zpool,
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
                    self.log,
                    "failed to fetch ZFS dataset properties; \
                     will attempt to ensure all datasets";
                    InlineErrorChain::new(err.as_ref()),
                );
            })
            .unwrap_or_default()
            .into_iter()
            .map(|props| (props.name.clone(), props))
            .collect::<BTreeMap<_, _>>();

        // Capture references to appease borrow checking on the closures and
        // async blocks below.
        let old_datasets = &old_datasets;
        let mount_config = &self.mount_config;
        let log = &self.log;

        // Ensure all the datasets except those with kind `TransientZone { .. }`
        // concurrently.
        const DATASET_ENSURE_CONCURRENCY_LIMIT: usize = 16;
        let mut non_transient_zones = futures::stream::iter(
            non_transient_zone_configs.into_iter().map(|dataset| async move {
                let state = match Self::ensure_one_dataset(
                    DatasetCreationDetails::Config(
                        &dataset,
                        old_datasets.get(&dataset.name.full_name()),
                    ),
                    &mount_config,
                    &log,
                    zfs,
                )
                .await
                {
                    Ok(state) => state,
                    Err(err) => DatasetState::FailedToEnsure(Arc::new(err)),
                };
                (dataset, state)
            }),
        )
        .buffer_unordered(DATASET_ENSURE_CONCURRENCY_LIMIT);

        while let Some((config, state)) = non_transient_zones.next().await {
            self.datasets.0.insert(SingleDatasetEnsureResult { config, state });
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
                    self.datasets.0.insert(SingleDatasetEnsureResult {
                        config: dataset,
                        state: DatasetState::ParentMissingFromConfig,
                    });
                    continue;
                };

                // Have we successfully ensured that parent dataset?
                if !matches!(
                    self.datasets
                        .0
                        .get(zpool_transient_zone_root_dataset_id)
                        .map(|d| &d.state),
                    Some(DatasetState::Mounted)
                ) {
                    self.datasets.0.insert(SingleDatasetEnsureResult {
                        config: dataset,
                        state: DatasetState::ParentFailedToMount,
                    });
                    continue;
                }

                transient_zone_futures.push(async move {
                    let state = match Self::ensure_one_dataset(
                        DatasetCreationDetails::Config(
                            &dataset,
                            old_datasets.get(&dataset.name.full_name()),
                        ),
                        &mount_config,
                        &log,
                        zfs,
                    )
                    .await
                    {
                        Ok(state) => state,
                        Err(err) => DatasetState::FailedToEnsure(Arc::new(err)),
                    };
                    (dataset, state)
                });
            }
        }

        let mut transient_zones = futures::stream::iter(transient_zone_futures)
            .buffer_unordered(DATASET_ENSURE_CONCURRENCY_LIMIT);
        while let Some((config, state)) = transient_zones.next().await {
            self.datasets.0.insert(SingleDatasetEnsureResult { config, state });
        }
    }

    /// Compare `dataset`'s properties against `old_dataset` (an set of
    /// recently-retrieved properties from ZFS). If we already know
    /// the state of `dataset` based on those properties, return `Some(state)`;
    /// otherwise, return `None`.
    fn is_dataset_state_known(
        dataset: &DatasetConfig,
        old_dataset: Option<&DatasetProperties>,
        log: &Logger,
    ) -> Option<DatasetState> {
        let log = log.new(slog::o!("dataset" => dataset.name.full_name()));

        let Some(old_dataset) = old_dataset else {
            info!(log, "This dataset did not exist");
            return None;
        };

        let Some(old_id) = old_dataset.id else {
            info!(log, "Old properties missing UUID");
            return None;
        };

        if old_id != dataset.id {
            return Some(DatasetState::UuidMismatch(old_id));
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

    // Ensures a dataset exists within a zpool.
    //
    // The caller is expected to verify that the parent entity of this new
    // dataset is valid (for root datasets, that the zpool they're on is
    // currently managed by us; for nested datasets, that the parent dataset
    // exists and is mounted).
    async fn ensure_one_dataset<T: ZfsImpl>(
        details: DatasetCreationDetails<'_>,
        mount_config: &MountConfig,
        log: &Logger,
        zfs: &T,
    ) -> Result<DatasetState, DatasetEnsureError> {
        info!(log, "ensure_dataset"; "details" => ?details);

        // Unpack the particulars of the kind of dataset we're creating.
        let (dataset_id, zoned, mountpoint, full_name, size_details) =
            match details {
                DatasetCreationDetails::Nested(config) => {
                    let dataset_id = None;
                    let zoned = false;
                    let mountpoint =
                        Mountpoint(config.name.mountpoint(&mount_config.root));
                    let full_name = config.name.full_name();

                    (dataset_id, zoned, mountpoint, full_name, &config.inner)
                }
                DatasetCreationDetails::Config(config, old_props) => {
                    // Do we alread know the state of this dataset based on
                    // `old_props`?
                    if let Some(state) =
                        Self::is_dataset_state_known(config, old_props, log)
                    {
                        return Ok(state);
                    }

                    let dataset_id = Some(config.id);
                    let zoned = config.name.kind().zoned();
                    let mountpoint =
                        Mountpoint(config.name.mountpoint(&mount_config.root));
                    let full_name = config.name.full_name();

                    (dataset_id, zoned, mountpoint, full_name, &config.inner)
                }
            };

        // All the datasets we currently ensure from within this task are not
        // _themselves_ encrypted. Many of them are children of the encrypted
        // `crypt` dataset. Ensuring that dataset would require non-`None`
        // encryption details, but that's currently handled by `Disk::new()`
        // when we start managing external disks.
        let encryption_details = None;

        let size_details = Some(illumos_utils::zfs::SizeDetails {
            quota: size_details.quota,
            reservation: size_details.reservation,
            compression: size_details.compression,
        });
        zfs.ensure_dataset(DatasetEnsureArgs {
            name: &full_name,
            mountpoint,
            can_mount: CanMount::On,
            zoned,
            encryption_details,
            size_details,
            id: dataset_id,
            additional_options: None,
        })
        .await?;
        Ok(DatasetState::Mounted)
    }

    async fn nested_dataset_mount<T: ZfsImpl>(
        &self,
        name: NestedDatasetLocation,
        zfs: &T,
    ) -> Result<Utf8PathBuf, NestedDatasetMountError> {
        let mountpoint = name.mountpoint(&self.mount_config.root);

        zfs.ensure_nested_dataset_mounted(
            name.full_name(),
            Mountpoint(mountpoint.clone()),
        )
        .await?;

        Ok(mountpoint)
    }

    async fn nested_dataset_ensure<T: ZfsImpl>(
        &self,
        config: NestedDatasetConfig,
        zfs: &T,
    ) -> Result<(), NestedDatasetEnsureError> {
        let log = self.log.new(slog::o!("request" => "nested_dataset_ensure"));

        // Has our parent dataset been mounted?
        //
        // TODO-cleanup Could we get the parent dataset ID instead of its name?
        // Then we could do a lookup instead of a scan.
        if !self.datasets.0.iter().any(|result| {
            result.config.name == config.name.root
                && matches!(result.state, DatasetState::Mounted)
        }) {
            return Err(NestedDatasetEnsureError::ParentDatasetNotMounted(
                config.name.root,
            ));
        }

        let root = &self.mount_config.root;
        let mountpoint_path = config.name.mountpoint(root);

        info!(
            log,
            "Ensuring nested dataset";
            "mountpoint" => ?mountpoint_path.as_path()
        );

        match Self::ensure_one_dataset(
            DatasetCreationDetails::Nested(&config),
            &self.mount_config,
            &self.log,
            zfs,
        )
        .await
        {
            Ok(_state) => {
                // We don't keep track of the resulting state of nested
                // datasets; the support bundle system manages that on its own.
                Ok(())
            }
            Err(err) => Err(NestedDatasetEnsureError::EnsureFailed {
                name: config.name,
                err,
            }),
        }
    }

    async fn nested_dataset_destroy<T: ZfsImpl>(
        &self,
        name: NestedDatasetLocation,
        zfs: &T,
    ) -> Result<(), NestedDatasetDestroyError> {
        let full_name = name.full_name();
        let log = self.log.new(slog::o!(
            "request" => "nested_dataset_destroy",
            "dataset" => full_name.clone(),
        ));
        info!(log, "Destroying nested dataset");

        if name.path.is_empty() {
            let err = NestedDatasetDestroyError::EmptyName;
            warn!(log, "{}", InlineErrorChain::new(&err));
            return Err(err);
        }

        zfs.destroy_dataset(full_name).await.map_err(From::from)
    }

    async fn nested_dataset_list<T: ZfsImpl>(
        &self,
        dataset: DatasetName,
        options: NestedDatasetListOptions,
        zfs: &T,
    ) -> Result<Vec<NestedDatasetConfig>, NestedDatasetListError> {
        let log = self.log.new(slog::o!("request" => "nested_dataset_list"));
        info!(log, "Listing nested datasets");

        let root_path = dataset.full_name();
        let get_properties_result = zfs
            .get_dataset_properties(
                &[root_path.clone()],
                WhichDatasets::SelfAndChildren,
            )
            .await;

        let properties = match get_properties_result {
            Ok(properties) => properties,
            Err(err) => {
                let err = NestedDatasetListError::DatasetListProperties {
                    name: dataset,
                    err,
                };
                warn!(
                    log, "Failed to access nested dataset";
                    InlineErrorChain::new(&err),
                );
                return Err(err);
            }
        };

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
                    name: NestedDatasetLocation { path, root: dataset.clone() },
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

#[derive(Debug)]
enum DatasetTaskRequest {
    DatasetsEnsure {
        datasets: IdMap<DatasetConfig>,
        tx: DebugIgnore<oneshot::Sender<DatasetEnsureResult>>,
    },
    NestedDatasetMount {
        name: NestedDatasetLocation,
        tx: DebugIgnore<
            oneshot::Sender<Result<Utf8PathBuf, NestedDatasetMountError>>,
        >,
    },
    NestedDatasetEnsure {
        config: NestedDatasetConfig,
        tx: DebugIgnore<oneshot::Sender<Result<(), NestedDatasetEnsureError>>>,
    },
    NestedDatasetDestroy {
        name: NestedDatasetLocation,
        tx: DebugIgnore<oneshot::Sender<Result<(), NestedDatasetDestroyError>>>,
    },
    NestedDatasetList {
        dataset: DatasetName,
        options: NestedDatasetListOptions,
        tx: DebugIgnore<
            oneshot::Sender<
                Result<Vec<NestedDatasetConfig>, NestedDatasetListError>,
            >,
        >,
    },
}

#[derive(Debug)]
enum DatasetCreationDetails<'a> {
    Nested(&'a NestedDatasetConfig),
    Config(&'a DatasetConfig, Option<&'a DatasetProperties>),
}

// Trait allowing us to test `DatasetTask` without real ZFS interactions (and on
// any platform). Production code has only one implementor (`RealZfs` below),
// but our tests provide their own.
trait ZfsImpl: Send + Sync + 'static {
    fn ensure_dataset(
        &self,
        args: DatasetEnsureArgs,
    ) -> impl Future<Output = Result<(), DatasetEnsureError>> + Send;

    fn ensure_nested_dataset_mounted(
        &self,
        name: String,
        mountpoint: Mountpoint,
    ) -> impl Future<Output = Result<(), NestedDatasetMountError>> + Send;

    fn destroy_dataset(
        &self,
        name: String,
    ) -> impl Future<Output = Result<(), DestroyDatasetError>> + Send;

    fn get_dataset_properties(
        &self,
        datasets: &[String],
        which: WhichDatasets,
    ) -> impl Future<Output = anyhow::Result<Vec<DatasetProperties>>> + Send;
}

struct RealZfs;

impl ZfsImpl for RealZfs {
    async fn ensure_dataset(
        &self,
        args: DatasetEnsureArgs<'_>,
    ) -> Result<(), DatasetEnsureError> {
        // Unpack `args` so we can clone `name` so we can move it into the
        // closure below so that we can safely use `spawn_blocking`. We could
        // also consider changing `DatasetEnsureArgs` to hold a `String` instead
        // of a `&str`...
        let DatasetEnsureArgs {
            name,
            mountpoint,
            can_mount,
            zoned,
            encryption_details,
            size_details,
            id,
            additional_options,
        } = args;
        let name = name.to_owned();
        let full_name = name.clone();

        tokio::task::spawn_blocking(move || {
            let args = DatasetEnsureArgs {
                name: &name,
                mountpoint,
                can_mount,
                zoned,
                encryption_details,
                size_details,
                id,
                additional_options,
            };
            Zfs::ensure_dataset(args)
        })
        .await
        .expect("blocking closure did not panic")
        .map_err(|err| DatasetEnsureError::EnsureFailed {
            name: full_name,
            err,
        })
    }

    async fn ensure_nested_dataset_mounted(
        &self,
        name: String,
        mountpoint: Mountpoint,
    ) -> Result<(), NestedDatasetMountError> {
        let name_cloned = name.clone();
        tokio::task::spawn_blocking(move || {
            Zfs::ensure_dataset_mounted_and_exists(&name_cloned, &mountpoint)
        })
        .await
        .expect("blocking closure did not panic")
        .map_err(|err| NestedDatasetMountError::MountFailed { name, err })
    }

    async fn destroy_dataset(
        &self,
        name: String,
    ) -> Result<(), DestroyDatasetError> {
        tokio::task::spawn_blocking(move || Zfs::destroy_dataset(&name))
            .await
            .expect("blocking closure did not panic")
    }

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use illumos_utils::zfs::DestroyDatasetErrorVariant;
    use omicron_common::api::external::ByteCount;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use proptest::collection::btree_map;
    use proptest::prelude::*;
    use proptest::sample::size_range;
    use std::sync::Mutex;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;

    #[derive(Debug, Clone, Default)]
    struct InMemoryZfs {
        inner: Arc<Mutex<InMemoryZfsState>>,
    }

    impl InMemoryZfs {
        fn ensure_should_fail(&self, name: String, reason: &'static str) {
            self.inner.lock().unwrap().ensure_should_fail.insert(name, reason);
        }
    }

    #[derive(Debug, Default)]
    struct InMemoryZfsState {
        // Datasets that have been ensured, keyed by name.
        datasets: BTreeMap<String, DatasetProperties>,
        // Number of times a given dataset name has been passed to
        // `ensure_dataset()`.
        ensure_call_counts: BTreeMap<String, usize>,
        // Map of reasons to return an error for any attempts to
        // `ensure_dataset()` for the given dataset name.
        ensure_should_fail: BTreeMap<String, &'static str>,
    }

    impl ZfsImpl for InMemoryZfs {
        async fn ensure_dataset(
            &self,
            args: DatasetEnsureArgs<'_>,
        ) -> Result<(), DatasetEnsureError> {
            let mut state = self.inner.lock().unwrap();

            *state
                .ensure_call_counts
                .entry(args.name.to_string())
                .or_default() += 1;

            if let Some(err) = state.ensure_should_fail.get(args.name) {
                return Err(DatasetEnsureError::TestError(err));
            }

            // Refuse to ensure `TransientZone` datasets before their associated
            // `TransientZoneRoot` has been ensured. We don't have access to the
            // `DatasetKind` here, so we have to pick apart the names. :(
            if args.name.contains("/crypt/zone/") {
                let parent = args.name.rsplit_once('/').unwrap().0;
                if !state.datasets.contains_key(parent) {
                    return Err(DatasetEnsureError::TestError(
                        "parent transient zone root does not exist",
                    ));
                }
            }

            let props = DatasetProperties {
                id: args.id,
                name: args.name.to_string(),
                mounted: true,
                avail: ByteCount::from_kibibytes_u32(1024),
                used: ByteCount::from_kibibytes_u32(0),
                quota: args.size_details.as_ref().and_then(|sd| sd.quota),
                reservation: args
                    .size_details
                    .as_ref()
                    .and_then(|sd| sd.reservation),
                compression: args
                    .size_details
                    .as_ref()
                    .map(|sd| sd.compression.to_string())
                    .unwrap_or_else(|| "on".to_string()),
            };
            state.datasets.insert(props.name.clone(), props);
            Ok(())
        }

        async fn ensure_nested_dataset_mounted(
            &self,
            name: String,
            _mountpoint: Mountpoint,
        ) -> Result<(), NestedDatasetMountError> {
            let mut state = self.inner.lock().unwrap();

            // Parent must exist (if there is one)
            if let Some(parent) = name.rsplit_once('/').map(|(p, _)| p) {
                if !state.datasets.contains_key(parent) {
                    return Err(NestedDatasetMountError::TestError(
                        "parent does not exist",
                    ));
                }
            }

            // Dataset must exist
            let dataset = match state.datasets.get_mut(&name) {
                Some(ds) => ds,
                None => {
                    return Err(NestedDatasetMountError::TestError(
                        "dataset does not exist",
                    ));
                }
            };

            dataset.mounted = true;
            Ok(())
        }

        async fn destroy_dataset(
            &self,
            name: String,
        ) -> Result<(), DestroyDatasetError> {
            let mut state = self.inner.lock().unwrap();

            // Remove the dataset...
            if state.datasets.remove(&name).is_none() {
                return Err(DestroyDatasetError {
                    name,
                    err: DestroyDatasetErrorVariant::NotFound,
                });
            }

            // ...and any children.
            let prefix = format!("{name}/");
            state.datasets.retain(|k, _| !k.starts_with(&prefix));

            Ok(())
        }

        async fn get_dataset_properties(
            &self,
            datasets: &[String],
            which: WhichDatasets,
        ) -> anyhow::Result<Vec<DatasetProperties>> {
            let state = self.inner.lock().unwrap();
            let mut result = Vec::new();

            for dataset in datasets {
                match which {
                    WhichDatasets::SelfOnly => {
                        if let Some(props) = state.datasets.get(dataset) {
                            result.push(props.clone());
                        }
                    }
                    WhichDatasets::SelfAndChildren => {
                        for (name, props) in state.datasets.iter() {
                            if name == dataset
                                || name.starts_with(&format!("{}/", dataset))
                            {
                                result.push(props.clone());
                            }
                        }
                    }
                }
            }

            Ok(result)
        }
    }

    #[derive(Debug, Arbitrary, PartialEq, Eq, PartialOrd, Ord)]
    struct ArbitraryZpoolName {
        #[strategy(any::<u128>())]
        #[map(|n| ZpoolName::new_external(ZpoolUuid::from_u128(n)))]
        name: ZpoolName,
    }

    impl From<ArbitraryZpoolName> for ZpoolName {
        fn from(value: ArbitraryZpoolName) -> Self {
            value.name
        }
    }

    // All our tests operate on the in-memory ZFS impl above, so the mount
    // config shouldn't matter. Populate something that won't exist on real
    // systems so if we miss something and try to operate on a real disk it will
    // fail.
    fn nonexistent_mount_config() -> Arc<MountConfig> {
        Arc::new(MountConfig {
            root: "/tmp/test-dataset-serialization/bogus/root".into(),
            synthetic_disk_root: "/tmp/test-dataset-serialization/bogus/disk"
                .into(),
        })
    }

    fn with_test_runtime<Fut, T>(fut: Fut) -> T
    where
        Fut: Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("tokio Runtime built successfully");
        runtime.block_on(fut)
    }

    fn make_dataset_config(
        zpool: ZpoolName,
        kind: DatasetKind,
    ) -> DatasetConfig {
        DatasetConfig {
            id: DatasetUuid::new_v4(),
            name: DatasetName::new(zpool, kind),
            inner: SharedDatasetConfig {
                compression: CompressionAlgorithm::On,
                quota: None,
                reservation: None,
            },
        }
    }

    #[proptest]
    fn datasets_skipped_if_zpool_not_managed(
        #[strategy(
            btree_map(any::<ArbitraryZpoolName>(), any::<bool>(), 1..20)
        )]
        pools: BTreeMap<ArbitraryZpoolName, bool>,
    ) {
        with_test_runtime(async move {
            let (managed_pools, unmanaged_pools): (BTreeSet<_>, BTreeSet<_>) =
                pools.into_iter().partition(|(_, is_managed)| *is_managed);
            datasets_skipped_if_zpool_not_managed_impl(
                managed_pools.into_iter().map(|(p, _)| p.into()).collect(),
                unmanaged_pools.into_iter().map(|(p, _)| p.into()).collect(),
            )
            .await;
        })
    }

    async fn datasets_skipped_if_zpool_not_managed_impl(
        managed_pools: BTreeSet<ZpoolName>,
        unmanaged_pools: BTreeSet<ZpoolName>,
    ) {
        let logctx =
            dev::test_setup_log("datasets_skipped_if_zpool_not_managed");

        // Create a pile of `DatasetConfig`s, backed by both managed and
        // unmanaged pools.
        let datasets = managed_pools
            .iter()
            .chain(unmanaged_pools.iter())
            .cycle()
            .map(|zpool| {
                make_dataset_config(zpool.clone(), DatasetKind::Crucible)
            })
            .take(100)
            .collect::<IdMap<_>>();

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools_rx =
            CurrentlyManagedZpoolsReceiver::fake_static(
                managed_pools.clone().into_iter(),
            );
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            currently_managed_zpools_rx,
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");

        // The returned map should record success for all datasets on managed
        // zpools and errors on all unmanaged pools.
        assert_eq!(result.0.len(), datasets.len());
        let mut num_datasets_on_managed_pools = 0;
        for dataset in &datasets {
            let single_result = result
                .0
                .get(&dataset.id)
                .expect("result contains entry for each dataset");

            if managed_pools.contains(dataset.name.pool()) {
                assert_matches!(single_result.state, DatasetState::Mounted);
                num_datasets_on_managed_pools += 1;
            } else {
                assert_matches!(
                    single_result.state,
                    DatasetState::ZpoolNotFound
                );
            }
        }

        // Our in-memory zfs should only have entries for datasets backed by
        // managed disks; unmanaged disks should have been skipped.
        let zfs = zfs.inner.lock().unwrap();
        let mut nfound = 0;
        for dataset in &datasets {
            if managed_pools.contains(dataset.name.pool()) {
                assert!(
                    zfs.datasets.contains_key(&dataset.name.full_name()),
                    "missing entry for dataset {dataset:?}"
                );
                nfound += 1;
            }
        }
        assert_eq!(nfound, num_datasets_on_managed_pools);

        logctx.cleanup_successful();
    }

    #[derive(Debug, Arbitrary, PartialEq, Eq)]
    enum TransientZoneRootBehavior {
        // zone root is present in config and mounts successfully
        Succeed,
        // zone root is present in config but fails to mount
        Fail,
        // zone root is missing from config
        Omit,
    }

    #[proptest]
    fn transient_zone_datasets_skipped_if_no_transient_zone_root(
        #[strategy(btree_map(
            any::<ArbitraryZpoolName>(),
            any::<TransientZoneRootBehavior>(),
            1..20,
        ))]
        pools: BTreeMap<ArbitraryZpoolName, TransientZoneRootBehavior>,
    ) {
        with_test_runtime(async move {
            transient_zone_datasets_skipped_if_no_transient_zone_root_impl(
                pools.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            )
            .await;
        })
    }

    async fn transient_zone_datasets_skipped_if_no_transient_zone_root_impl(
        pools: BTreeMap<ZpoolName, TransientZoneRootBehavior>,
    ) {
        let logctx = dev::test_setup_log(
            "transient_zone_datasets_skipped_if_no_transient_zone_root",
        );

        // Create the set of datasets we want to attempt. For each pool:
        //
        // 1. Add a transient zone root dataset if requested
        // 2. Add a few transient zone datasets
        let datasets = pools
            .iter()
            .flat_map(|(zpool, behavior)| {
                let maybe_zone_root = match behavior {
                    TransientZoneRootBehavior::Succeed
                    | TransientZoneRootBehavior::Fail => {
                        Some(make_dataset_config(
                            zpool.clone(),
                            DatasetKind::TransientZoneRoot,
                        ))
                    }
                    TransientZoneRootBehavior::Omit => None,
                };

                maybe_zone_root.into_iter().chain((0..5).map(|_| {
                    make_dataset_config(
                        zpool.clone(),
                        DatasetKind::TransientZone {
                            name: OmicronZoneUuid::new_v4().to_string(),
                        },
                    )
                }))
            })
            .collect::<IdMap<_>>();

        // Configure our fake ZFS to fail to mount some of the transient zone
        // root.
        let zfs = InMemoryZfs::default();
        for dataset in &datasets {
            let behavior = pools
                .get(dataset.name.pool())
                .expect("datasets only exist for pools we have");
            if matches!(dataset.name.kind(), DatasetKind::TransientZoneRoot)
                && *behavior == TransientZoneRootBehavior::Fail
            {
                zfs.ensure_should_fail(dataset.name.full_name(), "forced fail");
            }
        }

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools_rx =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.keys().cloned());
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            currently_managed_zpools_rx,
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");

        // The returned map should record:
        //
        // * success for all zone roots with the `Succeed` behavior
        // * errors for all zone roots with the `Fail` behavior
        // * success for all transient zones on pools where the zone root has
        //   the `Succeed` behavior
        // * errors for all other transient zones (with the specific error
        //   depending on whether the parent failed or was omitted)
        assert_eq!(result.0.len(), datasets.len());
        for dataset in &datasets {
            let behavior = pools
                .get(dataset.name.pool())
                .expect("datasets only exist for pools we have");
            let result = result
                .0
                .get(&dataset.id)
                .expect("result contains entry for each dataset");

            match (behavior, dataset.name.kind()) {
                (
                    TransientZoneRootBehavior::Succeed,
                    DatasetKind::TransientZoneRoot
                    | DatasetKind::TransientZone { .. },
                ) => assert_matches!(result.state, DatasetState::Mounted),
                (
                    TransientZoneRootBehavior::Fail,
                    DatasetKind::TransientZoneRoot,
                ) => assert_matches!(
                    result.state,
                    DatasetState::FailedToEnsure(_)
                ),
                (
                    TransientZoneRootBehavior::Fail,
                    DatasetKind::TransientZone { .. },
                ) => assert_matches!(
                    result.state,
                    DatasetState::ParentFailedToMount
                ),
                (
                    TransientZoneRootBehavior::Omit,
                    DatasetKind::TransientZone { .. },
                ) => assert_matches!(
                    result.state,
                    DatasetState::ParentMissingFromConfig
                ),
                (x, y) => panic!("unexpected combo: ({x:?}, {y:?})"),
            }
        }

        logctx.cleanup_successful();
    }

    // `DatasetTask` ensures datasets concurrently, but should never try to
    // ensure a `TransientZone` dataset before its parent `TransientZoneRoot`.
    // This is pretty awkward to test. We'll use a very high number of pools,
    // each of which has one of each, which should probably catch any
    // concurrency problems (not guaranteed! but if we take out the "ensure zone
    // roots before transient zones" implementation, this test consistently
    // fails).
    //
    // Because we use such a large number of pools, cut the number of cases down
    // to keep the test runtime reasonable.
    #[proptest(ProptestConfig { cases: 8, ..Default::default() })]
    fn transient_zone_ensured_after_transient_zone_root(
        #[any(size_range(1000..2000).lift())] pools: BTreeSet<
            ArbitraryZpoolName,
        >,
    ) {
        with_test_runtime(async move {
            transient_zone_ensured_after_transient_zone_root_impl(
                pools.into_iter().map(From::from).collect(),
            )
            .await;
        })
    }

    async fn transient_zone_ensured_after_transient_zone_root_impl(
        pools: BTreeSet<ZpoolName>,
    ) {
        let logctx = dev::test_setup_log(
            "transient_zone_ensured_after_transient_zone_root",
        );

        // As described above, create a transient zone and a transient zone root
        // for each of the (many) pools.
        let datasets = pools
            .iter()
            .flat_map(|zpool| {
                [
                    make_dataset_config(
                        zpool.clone(),
                        DatasetKind::TransientZoneRoot,
                    ),
                    make_dataset_config(
                        zpool.clone(),
                        DatasetKind::TransientZone {
                            name: OmicronZoneUuid::new_v4().to_string(),
                        },
                    ),
                ]
            })
            .collect::<IdMap<_>>();

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools_rx =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.iter().cloned());
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            currently_managed_zpools_rx,
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");

        // Our in-memory ZFS will return an error if we tried to mount a
        // transient zone before its parent zone root, so it's sufficient to
        // check that all the datasets ensured successfully.
        assert_eq!(result.0.len(), datasets.len());
        for single_result in result.0 {
            assert_matches!(
                single_result.state,
                DatasetState::Mounted,
                "bad state for {:?}",
                single_result.config
            );
        }

        logctx.cleanup_successful();
    }

    #[proptest]
    fn ensure_dataset_calls_skipped_if_not_needed(
        #[any(size_range(1..20).lift())] pools: BTreeSet<ArbitraryZpoolName>,
    ) {
        with_test_runtime(async move {
            ensure_dataset_calls_skipped_if_not_needed_impl(
                pools.into_iter().map(From::from).collect(),
            )
            .await;
        })
    }

    async fn ensure_dataset_calls_skipped_if_not_needed_impl(
        pools: BTreeSet<ZpoolName>,
    ) {
        let logctx =
            dev::test_setup_log("ensure_dataset_calls_skipped_if_not_needed");

        // Make a few datasets for each zpool.
        let datasets = pools
            .iter()
            .flat_map(|zpool| {
                [
                    DatasetKind::Cockroach,
                    DatasetKind::Crucible,
                    DatasetKind::ExternalDns,
                    DatasetKind::Debug,
                ]
                .into_iter()
                .map(|kind| make_dataset_config(zpool.clone(), kind))
            })
            .collect::<IdMap<_>>();

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools_rx =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.iter().cloned());
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            currently_managed_zpools_rx,
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");

        // Each dataset should have been ensured exactly once.
        assert_eq!(result.0.len(), datasets.len());
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets {
                assert_matches!(
                    result.0.get(&dataset.id).unwrap().state,
                    DatasetState::Mounted
                );
                assert_eq!(
                    zfs.ensure_call_counts.get(&dataset.name.full_name()),
                    Some(&1)
                );
            }
        }

        // Ensuring the exact same set of datasets should not cause any more
        // calls to ZFS, because they all have the properties they need already.
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");
        assert_eq!(result.0.len(), datasets.len());
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets {
                assert_matches!(
                    result.0.get(&dataset.id).unwrap().state,
                    DatasetState::Mounted
                );
                assert_eq!(
                    zfs.ensure_call_counts.get(&dataset.name.full_name()),
                    Some(&1)
                );
            }
        }

        // Pick a couple datasets and mutate some properties at the ZFS level.
        let mut mutated_datasets = Vec::new();
        {
            let mut zfs = zfs.inner.lock().unwrap();
            let mut datasets_iter = datasets.iter();
            mutated_datasets.push({
                let dataset = datasets_iter.next().unwrap();
                zfs.datasets
                    .get_mut(&dataset.name.full_name())
                    .unwrap()
                    .quota = Some(ByteCount::from_kibibytes_u32(12345));
                dataset.id
            });

            // Only non-zoned datasets should be mounted automatically; find one
            // such and mark it as not mounted at the ZFS level.
            for dataset in datasets_iter {
                if !dataset.name.kind().zoned() {
                    zfs.datasets
                        .get_mut(&dataset.name.full_name())
                        .unwrap()
                        .mounted = false;
                    mutated_datasets.push(dataset.id);
                    break;
                }
            }
        }
        eprintln!("mutated datasets: {mutated_datasets:?}");

        // Ensure the same set of datasets again; the ones we mutated should get
        // re-ensured.
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");
        assert_eq!(result.0.len(), datasets.len());
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets {
                assert_matches!(
                    result.0.get(&dataset.id).unwrap().state,
                    DatasetState::Mounted
                );
                let expected_count =
                    if mutated_datasets.contains(&dataset.id) { 2 } else { 1 };
                assert_eq!(
                    zfs.ensure_call_counts.get(&dataset.name.full_name()),
                    Some(&expected_count),
                    "unexpected count for dataset {dataset:?}"
                );
            }
        }

        logctx.cleanup_successful();
    }

    #[derive(Debug, Arbitrary)]
    struct ArbitraryNestedDatasetLocations {
        is_parent_mounted: bool,
        #[any(size_range(0..64).lift())]
        nested_dataset_name_suffixes: BTreeSet<u64>,
    }

    #[proptest]
    fn nested_dataset_operations(
        #[strategy(
            btree_map(
                any::<ArbitraryZpoolName>(),
                any::<ArbitraryNestedDatasetLocations>(),
                1..20,
            )
        )]
        pools: BTreeMap<
            ArbitraryZpoolName,
            ArbitraryNestedDatasetLocations,
        >,
    ) {
        let inputs = pools
            .into_iter()
            .map(|(pool, nested)| {
                let pool = ZpoolName::from(pool);
                let root = DatasetName::new(pool.clone(), DatasetKind::Debug);
                let inner = SharedDatasetConfig {
                    compression: CompressionAlgorithm::On,
                    quota: None,
                    reservation: None,
                };
                NestedDatasetTestInput {
                    pool: pool.clone(),
                    is_debug_datset_mounted: nested.is_parent_mounted,
                    nested: nested
                        .nested_dataset_name_suffixes
                        .into_iter()
                        .map(|suffix| NestedDatasetConfig {
                            name: NestedDatasetLocation {
                                path: format!("nested-{suffix}"),
                                root: root.clone(),
                            },
                            inner: inner.clone(),
                        })
                        .collect(),
                }
            })
            .collect::<Vec<_>>();
        with_test_runtime(async move {
            nested_dataset_operations_impl(inputs).await;
        })
    }

    struct NestedDatasetTestInput {
        pool: ZpoolName,
        is_debug_datset_mounted: bool,
        nested: BTreeSet<NestedDatasetConfig>,
    }

    async fn nested_dataset_operations_impl(
        inputs: Vec<NestedDatasetTestInput>,
    ) {
        let logctx = dev::test_setup_log("nested_dataset_operations");

        // Setup: Create our pile of zpools, and a pile of debug datasets for a
        // subset of them.
        let currently_managed_zpools_rx =
            CurrentlyManagedZpoolsReceiver::fake_static(
                inputs.iter().map(|input| input.pool.clone()),
            );
        let datasets = inputs
            .iter()
            .filter(|input| input.is_debug_datset_mounted)
            .map(|input| {
                make_dataset_config(input.pool.clone(), DatasetKind::Debug)
            })
            .collect::<IdMap<_>>();

        // Spawn the task and feed our setup to it. All the debug datasets
        // should successfully be created.
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            currently_managed_zpools_rx,
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone())
            .await
            .expect("no task error");
        assert_eq!(result.0.len(), datasets.len());
        assert!(
            result.0.iter().all(|r| matches!(r.state, DatasetState::Mounted))
        );

        // Try to ensure each of the nested datasets. This should succeed for
        // any where the debug dataset was mounted, and fail with an appropriate
        // error for the others.
        for input in &inputs {
            for config in &input.nested {
                let result = task_handle
                    .nested_dataset_ensure(config.clone())
                    .await
                    .expect("no task error");
                match (input.is_debug_datset_mounted, result) {
                    (true, Ok(())) => (),
                    (
                        false,
                        Err(NestedDatasetEnsureError::ParentDatasetNotMounted(
                            name,
                        )),
                    ) => assert_eq!(name, config.name.root),
                    (x, y) => panic!("unexpected result: ({x}, {y:?})"),
                }
            }
        }

        // Listing the nested datasets should succeed for all the ones we
        // successfully created and return empty lists for those we didn't.
        for input in &inputs {
            let parent =
                DatasetName::new(input.pool.clone(), DatasetKind::Debug);

            // Listing the children only should only show our nested
            // datasets if the debug dataset is mounted.
            let listed = task_handle
                .nested_dataset_list(
                    parent.clone(),
                    NestedDatasetListOptions::ChildrenOnly,
                )
                .await
                .expect("no task error")
                .expect("no zfs error");
            if input.is_debug_datset_mounted {
                let listed = listed.iter().cloned().collect::<BTreeSet<_>>();
                assert_eq!(listed, input.nested);
            } else {
                assert_eq!(listed, Vec::new());
            }

            // SelfAndChildren should report our nested datasets and the parent
            // dataset itself (if that parent is mounted).
            let listed = task_handle
                .nested_dataset_list(
                    parent.clone(),
                    NestedDatasetListOptions::SelfAndChildren,
                )
                .await
                .expect("no task error")
                .expect("no zfs error");
            if input.is_debug_datset_mounted {
                let listed = listed.iter().cloned().collect::<BTreeSet<_>>();
                let mut expected = input.nested.clone();
                expected.insert(NestedDatasetConfig {
                    name: NestedDatasetLocation {
                        // The "nested" dataset config for the parent has an
                        // empty path.
                        path: String::new(),
                        root: parent.clone(),
                    },
                    inner: SharedDatasetConfig {
                        compression: CompressionAlgorithm::On,
                        quota: None,
                        reservation: None,
                    },
                });
                assert_eq!(listed, expected);
            } else {
                assert_eq!(listed, Vec::new());
            }
        }

        // We should successfully destroy the nested datasets we created, and
        // fail with the expected error if we try to destroy those we failed to
        // create.
        for input in &inputs {
            for config in &input.nested {
                let result = task_handle
                    .nested_dataset_destroy(config.name.clone())
                    .await
                    .expect("no task error");
                match (input.is_debug_datset_mounted, result) {
                    (true, Ok(())) => (),
                    (
                        false,
                        Err(NestedDatasetDestroyError::DestroyFailed(err)),
                    ) => {
                        assert_eq!(err.name, config.name.full_name());
                        assert_matches!(
                            err.err,
                            DestroyDatasetErrorVariant::NotFound
                        );
                    }
                    (x, y) => panic!("unexpected result: ({x}, {y:?})"),
                }
            }
        }

        logctx.cleanup_successful();
    }
}
