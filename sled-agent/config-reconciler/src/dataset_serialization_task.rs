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

use crate::CurrentlyManagedZpools;
use crate::InventoryError;
use camino::Utf8PathBuf;
use debug_ignore::DebugIgnore;
use futures::StreamExt;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use illumos_utils::zfs;
use illumos_utils::zfs::CanMount;
use illumos_utils::zfs::DatasetEnsureArgs;
use illumos_utils::zfs::DatasetProperties;
use illumos_utils::zfs::DestroyDatasetError;
use illumos_utils::zfs::Mountpoint;
use illumos_utils::zfs::WhichDatasets;
use illumos_utils::zfs::Zfs;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::SharedDatasetConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use sled_storage::config::MountConfig;
use sled_storage::dataset::CRYPT_DATASET;
use sled_storage::dataset::ZONE_DATASET;
use sled_storage::nested_dataset::NestedDatasetConfig;
use sled_storage::nested_dataset::NestedDatasetListOptions;
use sled_storage::nested_dataset::NestedDatasetLocation;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::str::FromStr;
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
    #[error("could not find matching zpool {0}")]
    ZpoolNotFound(ZpoolName),
    #[error("transient zone root not in config for zpool {0}")]
    TransientZoneRootNoConfig(ZpoolName),
    #[error("ensuring transient zone root failed on zpool {zpool}")]
    TransientZoneRootFailure {
        zpool: ZpoolName,
        #[source]
        err: Arc<DatasetEnsureError>,
    },
    #[error(
        "dataset {name} exists with unexpected ID \
         (expected {expected}, got {got})"
    )]
    UuidMismatch { name: String, expected: DatasetUuid, got: DatasetUuid },
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

impl DatasetEnsureError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            // These errors might be retryable; there are probably cases where
            // they won't be, but we need more context than we have available
            // from just the error to know for sure. For now, assume they are
            // retryable - that may mean we churn on something doomed, but
            // that's better than failing to retry something we should have
            // retried.
            DatasetEnsureError::ZpoolNotFound(_)
            | DatasetEnsureError::EnsureFailed { .. } => true,

            // Errors that we know aren't retryable: recovering from these
            // require config changes, so there's no need to retry until that
            // happens.
            DatasetEnsureError::TransientZoneRootNoConfig(_)
            | DatasetEnsureError::UuidMismatch { .. } => false,

            DatasetEnsureError::TransientZoneRootFailure { err, .. } => {
                err.is_retryable()
            }

            #[cfg(test)]
            DatasetEnsureError::TestError(_) => false,
        }
    }
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
    #[error("cannot destroy nested dataset with empty name")]
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

#[derive(Debug)]
pub(crate) struct DatasetEnsureResult {
    pub(crate) config: DatasetConfig,
    pub(crate) result: Result<(), Arc<DatasetEnsureError>>,
}

impl IdOrdItem for DatasetEnsureResult {
    type Key<'a> = DatasetUuid;

    fn key(&self) -> Self::Key<'_> {
        self.config.id
    }

    id_upcast!();
}

#[derive(Debug, Clone)]
pub(crate) struct DatasetTaskHandle(mpsc::Sender<DatasetTaskRequest>);

impl DatasetTaskHandle {
    // For testing, create a handle on which requests will always fail with a
    // `DatasetTaskError`.
    #[cfg(test)]
    pub(crate) fn spawn_noop() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self(tx)
    }

    pub fn spawn_dataset_task(
        mount_config: Arc<MountConfig>,
        base_log: &Logger,
    ) -> Self {
        Self::spawn_with_zfs_impl(mount_config, base_log, RealZfs)
    }

    fn spawn_with_zfs_impl<T: ZfsImpl>(
        mount_config: Arc<MountConfig>,
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
                request_rx,
                ensured_datasets: BTreeSet::new(),
                log: base_log.new(slog::o!("component" => "DatasetTask")),
            }
            .run(zfs),
        );

        Self(request_tx)
    }

    pub async fn inventory(
        &self,
        zpools: BTreeSet<ZpoolName>,
    ) -> Result<Result<Vec<InventoryDataset>, InventoryError>, DatasetTaskError>
    {
        self.try_send_request(|tx| DatasetTaskRequest::Inventory { zpools, tx })
            .await
    }

    /// Destroy any orphaned Omicron datasets.
    ///
    /// This method will _not_ destroy arbitrary datasets. For each dataset
    /// present on a zpool in `currently_managed_zpools`, it will look at its
    /// name and perform the following checks:
    ///
    /// * Can we parse the name as a [`DatasetName`]? If not, ignore the
    ///   dataset; we don't know what it is and it won't conflict with any
    ///   datasets we want to create or use.
    /// * Is the dataset still present in the config specified by `datasets`? If
    ///   so, don't destroy it: it's not an orphan.
    /// * Is the [`DatasetKind`] (which we've inferred from the parsed name) a
    ///   kind that we ought to destroy? See
    ///   `reason_to_skip_orphaned_dataset_destruction` for kinds we refuse to
    ///   destroy.
    ///
    /// The returned map lists includes the orphaned datasets we found but did
    /// not destroy, either because of one of the checks above or because we
    /// attempted destruction but it failed.
    pub async fn datasets_destroy_orphans(
        &self,
        datasets: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
    ) -> Result<anyhow::Result<IdOrdMap<OrphanedDataset>>, DatasetTaskError>
    {
        self.try_send_request(|tx| DatasetTaskRequest::DatasetsDestroyOrphans {
            datasets,
            currently_managed_zpools,
            tx,
        })
        .await
    }

    pub async fn datasets_ensure(
        &self,
        datasets: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
    ) -> Result<IdOrdMap<DatasetEnsureResult>, DatasetTaskError> {
        self.try_send_request(|tx| DatasetTaskRequest::DatasetsEnsure {
            datasets,
            currently_managed_zpools,
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

    // Returns a "not found" error if the dataset does not exist
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
    ensured_datasets: BTreeSet<DatasetName>,
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
            DatasetTaskRequest::Inventory { zpools, tx } => {
                _ = tx.0.send(self.inventory(zpools, zfs).await);
            }
            DatasetTaskRequest::DatasetsDestroyOrphans {
                datasets,
                currently_managed_zpools,
                tx,
            } => {
                _ = tx.0.send(
                    self.datasets_destroy_orphans(
                        datasets,
                        currently_managed_zpools,
                        zfs,
                    )
                    .await,
                );
            }
            DatasetTaskRequest::DatasetsEnsure {
                datasets,
                currently_managed_zpools,
                tx,
            } => {
                _ = tx.0.send(
                    self.datasets_ensure(
                        datasets,
                        currently_managed_zpools,
                        zfs,
                    )
                    .await,
                );
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

    async fn inventory<T: ZfsImpl>(
        &mut self,
        zpools: BTreeSet<ZpoolName>,
        zfs: &T,
    ) -> Result<Vec<InventoryDataset>, InventoryError> {
        let datasets_of_interest = zpools
            .iter()
            .flat_map(|zpool| {
                [
                    // We care about the zpool itself, and all direct children.
                    zpool.to_string(),
                    // Likewise, we care about the encrypted dataset, and all
                    // direct children.
                    format!("{zpool}/{CRYPT_DATASET}"),
                    // The zone dataset gives us additional context on "what
                    // zones have datasets provisioned".
                    format!("{zpool}/{ZONE_DATASET}"),
                ]
            })
            .collect::<Vec<_>>();

        let props = zfs
            .get_dataset_properties(
                &datasets_of_interest,
                WhichDatasets::SelfAndChildren,
            )
            .await
            .map_err(InventoryError::ListDatasetProperties)?;

        Ok(props.into_iter().map(From::from).collect())
    }

    async fn datasets_destroy_orphans<T: ZfsImpl>(
        &mut self,
        datasets: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
        zfs: &T,
    ) -> anyhow::Result<IdOrdMap<OrphanedDataset>> {
        let mut orphaned_datasets = IdOrdMap::new();

        let datasets_of_interest = currently_managed_zpools
            .iter()
            .flat_map(|zpool| {
                [
                    // We care about the direct children of the zpool itself.
                    // (This will include any unencrypted datasets, such as
                    // `crucible`.)
                    zpool.to_string(),
                    // We also care about the direct children of the encrypted
                    // dataset. (This includes non-crucible Omicron zone durable
                    // datasets.)
                    format!("{zpool}/{CRYPT_DATASET}"),
                    // We _don't_ need to check children of
                    // `{zpool}/{ZONE_DATASET}`: these are all transient, and
                    // should be destroyed/created on demand.
                ]
            })
            .collect::<Vec<_>>();

        for properties in zfs
            .get_dataset_properties(
                &datasets_of_interest,
                WhichDatasets::SelfAndChildren,
            )
            .await?
        {
            // Skip any errors parsing dataset names: we expect to get some
            // errors! We won't be able to parse any dataset names that can't be
            // represented by `DatasetKind`, which will always include at least
            // the two parent datasets we asked for above (`zpool` and
            // `zpool/crypt`).
            //
            // Our goal here is to prune datasets that _we_ created; we only
            // create datasets that have representable `DatasetName`s, so this
            // also acts as a safeguard that we should never delete datasets we
            // don't fully understand.
            let Ok(dataset) = DatasetName::from_str(&properties.name) else {
                continue;
            };
            let dataset_full_name = dataset.full_name();

            // Does this dataset have an ID set? If we created it, we expect it
            // does, and we can easily check whether it still exists in our
            // config. A dataset with a known `DatasetKind` without an ID set is
            // pretty unexpected: we'll search through our config datasets to
            // find a matching name; if we do, we'll assume it's that one.
            let present_in_config = match properties.id {
                Some(id) => datasets.contains_key(&id),
                None => {
                    if datasets.iter().any(|d| d.name == dataset) {
                        warn!(
                            self.log,
                            "found on-disk dataset without an ID \
                             that matches a config dataset by name";
                            "dataset" => &dataset_full_name,
                        );
                        true
                    } else {
                        warn!(
                            self.log,
                            "found on-disk dataset without an ID \
                             that doesn't match any config datasets; assuming \
                             it should be marked as an orphan";
                            "dataset" => &dataset_full_name,
                        );
                        false
                    }
                }
            };
            if present_in_config {
                continue;
            }

            // Should we skip destroying this dataset based on its `kind`?
            if let Some(reason) =
                reason_to_skip_orphaned_dataset_destruction(dataset.kind())
            {
                orphaned_datasets.insert_overwrite(OrphanedDataset {
                    name: dataset,
                    reason,
                    id: properties.id,
                    mounted: properties.mounted,
                    available: properties.avail,
                    used: properties.used,
                });
                continue;
            }

            // Try to destroy this dataset. If we fail, record the reason this
            // dataset is left orphaned.
            let maybe_reason =
                match zfs.destroy_dataset(&dataset_full_name).await {
                    Ok(()) => {
                        info!(
                            self.log,
                            "destroyed orphaned dataset";
                            "dataset" => &dataset_full_name,
                        );
                        None
                    }
                    Err(err) => {
                        warn!(
                            self.log,
                            "failed to destroy orphaned dataset";
                            "dataset" => &dataset_full_name,
                            InlineErrorChain::new(&err),
                        );
                        Some(InlineErrorChain::new(&err).to_string())
                    }
                };

            if let Some(reason) = maybe_reason {
                orphaned_datasets.insert_overwrite(OrphanedDataset {
                    name: dataset,
                    reason,
                    id: properties.id,
                    mounted: properties.mounted,
                    available: properties.avail,
                    used: properties.used,
                });
            }
        }

        Ok(orphaned_datasets)
    }

    async fn datasets_ensure<T: ZfsImpl>(
        &mut self,
        config: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
        zfs: &T,
    ) -> IdOrdMap<DatasetEnsureResult> {
        let mut ensure_results = IdOrdMap::new();

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

        for dataset in config {
            let zpool = dataset.name.pool();

            if !currently_managed_zpools.contains(&zpool) {
                warn!(
                    self.log,
                    "configured dataset on zpool we're not managing";
                    "dataset" => ?dataset,
                );
                let err = DatasetEnsureError::ZpoolNotFound(*zpool);
                ensure_results
                    .insert_unique(DatasetEnsureResult {
                        config: dataset,
                        result: Err(Arc::new(err)),
                    })
                    .expect(
                        "DatasetConfig and DatasetEnsureResult both use \
                         DatasetUuid as their key, so this is unique",
                    );
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
                | DatasetKind::LocalStorage => {
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
                let result = Self::ensure_one_dataset(
                    DatasetCreationDetails::Config(
                        &dataset,
                        old_datasets.get(&dataset.name.full_name()),
                    ),
                    &mount_config,
                    &log,
                    zfs,
                )
                .await;
                (dataset, result.map_err(Arc::new))
            }),
        )
        .buffer_unordered(DATASET_ENSURE_CONCURRENCY_LIMIT);

        while let Some((config, result)) = non_transient_zones.next().await {
            ensure_results
                .insert_overwrite(DatasetEnsureResult { config, result });
        }

        // For each transient zone dataset: either ensure it or mark down why we
        // don't try.
        let mut transient_zone_futures = Vec::new();
        for (zpool_id, datasets) in transient_zone_configs_by_zpool {
            for dataset in datasets {
                let zpool = *dataset.name.pool();

                // Did we have the parent `TransientZoneRoot` for this zpool?
                let Some(zpool_transient_zone_root_dataset_id) =
                    transient_zone_root_by_zpool.get(&zpool_id)
                else {
                    let err =
                        DatasetEnsureError::TransientZoneRootNoConfig(zpool);
                    // Inserting into ensure_results here should also be unique
                    // but it's hard to tell by code inspection.
                    ensure_results.insert_overwrite(DatasetEnsureResult {
                        config: dataset,
                        result: Err(Arc::new(err)),
                    });
                    continue;
                };

                // Have we successfully ensured that parent dataset?
                match ensure_results
                    .get(zpool_transient_zone_root_dataset_id)
                    .map(|d| &d.result)
                {
                    Some(Ok(())) => (),
                    Some(Err(err)) => {
                        let err =
                            DatasetEnsureError::TransientZoneRootFailure {
                                zpool,
                                err: Arc::clone(err),
                            };
                        // Inserting into ensure_results here should also be
                        // unique but it's hard to tell by code inspection.
                        ensure_results.insert_overwrite(DatasetEnsureResult {
                            config: dataset,
                            result: Err(Arc::new(err)),
                        });
                        continue;
                    }
                    None => {
                        // The only way to have no state for a transient zone
                        // root is if it's not in the config at all, which we
                        // just checked above.
                        unreachable!("all transient zone roots have states")
                    }
                }

                transient_zone_futures.push(async move {
                    let result = Self::ensure_one_dataset(
                        DatasetCreationDetails::Config(
                            &dataset,
                            old_datasets.get(&dataset.name.full_name()),
                        ),
                        &mount_config,
                        &log,
                        zfs,
                    )
                    .await;
                    (dataset, result.map_err(Arc::new))
                });
            }
        }

        let mut transient_zones = futures::stream::iter(transient_zone_futures)
            .buffer_unordered(DATASET_ENSURE_CONCURRENCY_LIMIT);
        while let Some((config, result)) = transient_zones.next().await {
            // Inserting into ensure_results here should also be unique but it's
            // hard to tell by code inspection.
            ensure_results
                .insert_overwrite(DatasetEnsureResult { config, result });
        }

        // Remember all successfully-ensured datasets (used by
        // `nested_dataset_ensure()` to check that any nested datasets' parents
        // have been ensured).
        self.ensured_datasets = ensure_results
            .iter()
            .filter_map(|d| {
                if d.result.is_ok() {
                    Some(d.config.name.clone())
                } else {
                    None
                }
            })
            .collect();

        ensure_results
    }

    /// Compare `dataset`'s properties against `old_dataset` (an set of
    /// recently-retrieved properties from ZFS). If we already know
    /// the state of `dataset` based on those properties, return `Some(state)`;
    /// otherwise, return `None`.
    fn is_dataset_ensure_result_known(
        dataset: &DatasetConfig,
        old_dataset: Option<&DatasetProperties>,
        log: &Logger,
    ) -> Option<Result<(), DatasetEnsureError>> {
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
            // We cannot do anything here: we already have a dataset with this
            // name, but it has a different ID. Nexus has sent us bad
            // information (or we have a bug somewhere); refuse to proceed.
            return Some(Err(DatasetEnsureError::UuidMismatch {
                name: dataset.name.full_name(),
                expected: dataset.id,
                got: old_id,
            }));
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
        return Some(Ok(()));
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
    ) -> Result<(), DatasetEnsureError> {
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
                    if let Some(result) = Self::is_dataset_ensure_result_known(
                        config, old_props, log,
                    ) {
                        return result;
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
        .await
    }

    async fn nested_dataset_mount<T: ZfsImpl>(
        &self,
        name: NestedDatasetLocation,
        zfs: &T,
    ) -> Result<Utf8PathBuf, NestedDatasetMountError> {
        let mountpoint = name.mountpoint(&self.mount_config.root);

        zfs.ensure_nested_dataset_mounted(
            &name.full_name(),
            &Mountpoint(mountpoint.clone()),
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
        if !self.ensured_datasets.contains(&config.name.root) {
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

    // Returns a "not found" error if the dataset does not exist
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

        zfs.destroy_dataset(&full_name).await.map_err(From::from)
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
                std::slice::from_ref(&root_path),
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
    Inventory {
        zpools: BTreeSet<ZpoolName>,
        tx: DebugIgnore<
            oneshot::Sender<Result<Vec<InventoryDataset>, InventoryError>>,
        >,
    },
    DatasetsDestroyOrphans {
        datasets: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
        tx: DebugIgnore<
            oneshot::Sender<anyhow::Result<IdOrdMap<OrphanedDataset>>>,
        >,
    },
    DatasetsEnsure {
        datasets: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
        tx: DebugIgnore<oneshot::Sender<IdOrdMap<DatasetEnsureResult>>>,
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

// This function is tightly coupled to `datasets_report_orphans()` above, but
// it's handy for it to be separated out for tests to call explicitly. Based on
// the kind:
//
// * Return `None` if we're willing to destroy it, if it's an orphan
// * Return `Some(reason)` if we're unwilling to destroy it even if we think
//   it's an orphan
fn reason_to_skip_orphaned_dataset_destruction(
    kind: &DatasetKind,
) -> Option<String> {
    // Explicitly match all variants here, so if we expand this list we're
    // forced to consider whether they should be removeable.
    match kind {
        // Zone durable datasets: these are the kinds we expect to remove (e.g.,
        // when the corresponding zone is expunged).
        DatasetKind::Cockroach
        | DatasetKind::Crucible
        | DatasetKind::Clickhouse
        | DatasetKind::ClickhouseKeeper
        | DatasetKind::ClickhouseServer
        | DatasetKind::ExternalDns
        | DatasetKind::InternalDns => None,

        // These kinds are part of our config, but it would be surprising to
        // have them disappear: they should always be present for any managed
        // disk. Refuse to remove them.
        DatasetKind::TransientZoneRoot
        | DatasetKind::Debug
        | DatasetKind::LocalStorage => Some(format!(
            "refusing to delete dataset of kind {kind:?} \
             (expected to exist for all managed disks)",
        )),

        // These should be grandchildren of the datasets we ask for when
        // checking for orphans; we shouldn't see them as direct children.
        // Refuse to remove them. (They should also be recreated on demand
        // anyway.)
        DatasetKind::TransientZone { .. } => {
            Some(format!("unexpectedly found transient zone root: {kind:?}"))
        }
    }
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
        name: &str,
        mountpoint: &Mountpoint,
    ) -> impl Future<Output = Result<(), NestedDatasetMountError>> + Send;

    fn destroy_dataset(
        &self,
        name: &str,
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
        let full_name = args.name;
        Zfs::ensure_dataset(args).await.map_err(|err| {
            DatasetEnsureError::EnsureFailed {
                name: full_name.to_string(),
                err,
            }
        })
    }

    async fn ensure_nested_dataset_mounted(
        &self,
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<(), NestedDatasetMountError> {
        Zfs::ensure_dataset_mounted_and_exists(name, mountpoint).await.map_err(
            |err| NestedDatasetMountError::MountFailed {
                name: name.to_string(),
                err,
            },
        )
    }

    async fn destroy_dataset(
        &self,
        name: &str,
    ) -> Result<(), DestroyDatasetError> {
        Zfs::destroy_dataset(name).await
    }

    async fn get_dataset_properties(
        &self,
        datasets: &[String],
        which: WhichDatasets,
    ) -> anyhow::Result<Vec<DatasetProperties>> {
        Zfs::get_dataset_properties(datasets, which).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CurrentlyManagedZpoolsReceiver;
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
            name: &str,
            _mountpoint: &Mountpoint,
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
            let dataset = match state.datasets.get_mut(name) {
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
            name: &str,
        ) -> Result<(), DestroyDatasetError> {
            let mut state = self.inner.lock().unwrap();

            // Remove the dataset...
            if state.datasets.remove(name).is_none() {
                return Err(DestroyDatasetError {
                    name: name.to_string(),
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
            // Helper to break a dataset name into its /-delimited parts.
            fn split_fn(name: &String) -> Vec<&str> {
                name.split('/').collect()
            }

            // Helper to check whether a list of dataset name parts is a direct
            // child of a given parent.
            fn is_direct_child(parent: &[&str], child: &[&str]) -> bool {
                child.len() == parent.len() + 1
                    && &child[..child.len() - 1] == parent
            }

            // Filter function: given the name of a dataset we have "on disk"
            // (i.e., in our in-memory ZFS state), should we include its
            // properties in the returned set?
            let filter_fn: &dyn Fn(&String) -> bool = match which {
                WhichDatasets::SelfOnly => &|name| datasets.contains(name),
                WhichDatasets::SelfAndChildren => {
                    let datasets =
                        datasets.iter().map(split_fn).collect::<Vec<_>>();
                    &move |name| {
                        let name = split_fn(name);
                        datasets.iter().any(|dataset| {
                            name == *dataset || is_direct_child(dataset, &name)
                        })
                    }
                }
            };

            Ok(self
                .inner
                .lock()
                .unwrap()
                .datasets
                .iter()
                .filter(|(name, _)| filter_fn(name))
                .map(|(_, props)| props.clone())
                .collect())
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

    pub(super) fn make_dataset_config(
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
            .copied()
            .cycle()
            .map(|zpool| make_dataset_config(zpool, DatasetKind::Crucible))
            .take(100)
            .collect::<IdOrdMap<_>>();

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(
                managed_pools.clone().into_iter(),
            )
            .current();
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone(), currently_managed_zpools)
            .await
            .expect("no task error");

        // The returned map should record success for all datasets on managed
        // zpools and errors on all unmanaged pools.
        assert_eq!(result.len(), datasets.len());
        let mut num_datasets_on_managed_pools = 0;
        for dataset in &datasets {
            let single_dataset = result
                .get(&dataset.id)
                .expect("result contains entry for each dataset");

            if managed_pools.contains(dataset.name.pool()) {
                assert_matches!(single_dataset.result, Ok(()));
                num_datasets_on_managed_pools += 1;
            } else {
                assert_matches!(
                    &single_dataset.result,
                    Err(err)
                        if matches!(**err, DatasetEnsureError::ZpoolNotFound(_))
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

    #[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq)]
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
            .flat_map(|(&zpool, &behavior)| {
                let maybe_zone_root = match behavior {
                    TransientZoneRootBehavior::Succeed
                    | TransientZoneRootBehavior::Fail => {
                        Some(make_dataset_config(
                            zpool,
                            DatasetKind::TransientZoneRoot,
                        ))
                    }
                    TransientZoneRootBehavior::Omit => None,
                };

                maybe_zone_root.into_iter().chain((0..5).map(move |_| {
                    make_dataset_config(
                        zpool,
                        DatasetKind::TransientZone {
                            name: OmicronZoneUuid::new_v4().to_string(),
                        },
                    )
                }))
            })
            .collect::<IdOrdMap<_>>();

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
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.keys().cloned())
                .current();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone(), currently_managed_zpools)
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
        assert_eq!(result.len(), datasets.len());
        for dataset in &datasets {
            let behavior = pools
                .get(dataset.name.pool())
                .expect("datasets only exist for pools we have");
            let result = &result
                .get(&dataset.id)
                .expect("result contains entry for each dataset")
                .result;

            match (behavior, dataset.name.kind()) {
                (
                    TransientZoneRootBehavior::Succeed,
                    DatasetKind::TransientZoneRoot
                    | DatasetKind::TransientZone { .. },
                ) => assert_matches!(result, Ok(())),
                (
                    TransientZoneRootBehavior::Fail,
                    DatasetKind::TransientZoneRoot,
                ) => assert_matches!(result, Err(_)),
                (
                    TransientZoneRootBehavior::Fail,
                    DatasetKind::TransientZone { .. },
                ) => assert_matches!(
                    result,
                    Err(err) if matches!(
                        **err,
                        DatasetEnsureError::TransientZoneRootFailure { .. }
                    )
                ),
                (
                    TransientZoneRootBehavior::Omit,
                    DatasetKind::TransientZone { .. },
                ) => assert_matches!(
                    result,
                    Err(err) if matches!(
                        **err, DatasetEnsureError::TransientZoneRootNoConfig(_)
                    )
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
            .copied()
            .flat_map(|zpool| {
                [
                    make_dataset_config(zpool, DatasetKind::TransientZoneRoot),
                    make_dataset_config(
                        zpool,
                        DatasetKind::TransientZone {
                            name: OmicronZoneUuid::new_v4().to_string(),
                        },
                    ),
                ]
            })
            .collect::<IdOrdMap<_>>();

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.iter().cloned())
                .current();
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone(), currently_managed_zpools)
            .await
            .expect("no task error");

        // Our in-memory ZFS will return an error if we tried to mount a
        // transient zone before its parent zone root, so it's sufficient to
        // check that all the datasets ensured successfully.
        assert_eq!(result.len(), datasets.len());
        for single_result in result {
            assert_matches!(
                single_result.result,
                Ok(()),
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
            .copied()
            .flat_map(|zpool| {
                [
                    DatasetKind::Cockroach,
                    DatasetKind::Crucible,
                    DatasetKind::ExternalDns,
                    DatasetKind::Debug,
                ]
                .into_iter()
                .map(move |kind| make_dataset_config(zpool, kind))
            })
            .collect::<IdOrdMap<_>>();

        // Send this pile of datasets to the task as a set to ensure.
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.iter().cloned())
                .current();
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone(), currently_managed_zpools.clone())
            .await
            .expect("no task error");

        // Each dataset should have been ensured exactly once.
        assert_eq!(result.len(), datasets.len());
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets {
                assert_matches!(
                    result.get(&dataset.id).unwrap().result,
                    Ok(())
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
            .datasets_ensure(datasets.clone(), currently_managed_zpools.clone())
            .await
            .expect("no task error");
        assert_eq!(result.len(), datasets.len());
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets {
                assert_matches!(
                    result.get(&dataset.id).unwrap().result,
                    Ok(())
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
            .datasets_ensure(datasets.clone(), currently_managed_zpools)
            .await
            .expect("no task error");
        assert_eq!(result.len(), datasets.len());
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets {
                assert_matches!(
                    result.get(&dataset.id).unwrap().result,
                    Ok(())
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

    #[derive(Debug, Arbitrary, PartialEq, PartialOrd, Eq, Ord)]
    struct ZpoolWithDatasets {
        name: ArbitraryZpoolName,
        datasets: BTreeMap<DatasetKind, bool>,
    }

    #[proptest]
    fn find_orphaned_datasets(
        #[any(size_range(1..20).lift())] pools: BTreeSet<ZpoolWithDatasets>,
    ) {
        with_test_runtime(async move {
            find_orphaned_datasets_impl(pools).await;
        })
    }

    async fn find_orphaned_datasets_impl(inputs: BTreeSet<ZpoolWithDatasets>) {
        let logctx = dev::test_setup_log("find_orphaned_datasets");

        // Convert our test input into:
        //
        // 1. The set of pools
        // 2. The set of `DatasetConfig`s
        // 3. The set of datasets that don't have `DatasetConfig`s
        let mut pools: BTreeSet<ZpoolName> = BTreeSet::new();
        let mut dataset_configs = IdOrdMap::new();
        let mut datasets_in_config = BTreeSet::new();
        let mut datasets_not_in_config = BTreeSet::new();
        for input in inputs {
            let ZpoolWithDatasets { name, datasets } = input;
            let name = name.into();
            pools.insert(name);
            for (kind, in_config) in datasets {
                let name = DatasetName::new(name, kind);
                if in_config {
                    assert!(!datasets_not_in_config.contains(&name));
                    assert!(datasets_in_config.insert(name.clone()));
                    dataset_configs.insert_overwrite(DatasetConfig {
                        id: DatasetUuid::new_v4(),
                        name,
                        inner: SharedDatasetConfig::default(),
                    });
                } else {
                    assert!(!datasets_in_config.contains(&name));
                    assert!(datasets_not_in_config.insert(name));
                }
            }
        }

        // Build up our in-memory ZFS: insert _all_ the datasets.
        let zfs = InMemoryZfs::default();
        {
            let mut zfs = zfs.inner.lock().unwrap();
            for dataset in
                datasets_in_config.iter().chain(datasets_not_in_config.iter())
            {
                let name = dataset.full_name();
                zfs.datasets.insert(
                    name.clone(),
                    DatasetProperties {
                        id: None,
                        name,
                        mounted: false,
                        avail: ByteCount::from_gibibytes_u32(1),
                        used: ByteCount::from_gibibytes_u32(0),
                        quota: None,
                        reservation: None,
                        compression: CompressionAlgorithm::On.to_string(),
                    },
                );
            }
        }

        // Spawn our task.
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(pools.iter().copied())
                .current();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            &logctx.log,
            zfs.clone(),
        );

        // Destroy all orphans.
        let orphans = task_handle
            .datasets_destroy_orphans(dataset_configs, currently_managed_zpools)
            .await
            .expect("no task error")
            .expect("no zfs error");

        // As above, we should report no orphans that were present in the
        // config.
        for dataset in &datasets_in_config {
            assert!(
                !orphans.contains_key(&dataset),
                "found unexpected orphan {}",
                dataset.full_name()
            );
        }

        // All the datasets in `datasets_not_in_config` should be orphans, and
        // the ones with kinds we're willing to remove should have been removed.
        {
            let zfs = zfs.inner.lock().unwrap();
            for dataset in &datasets_not_in_config {
                let name = dataset.full_name();
                if let Some(reason) =
                    reason_to_skip_orphaned_dataset_destruction(dataset.kind())
                {
                    let expected_reason = if matches!(
                        dataset.kind(),
                        DatasetKind::TransientZone { .. }
                    ) {
                        None
                    } else {
                        Some(&reason)
                    };

                    assert_eq!(
                        orphans.get(dataset).map(|orphan| &orphan.reason),
                        expected_reason,
                    );
                    assert!(
                        zfs.datasets.contains_key(&name),
                        "dataset should not have been destroyed \
                        (due to its kind): {name}"
                    );
                } else {
                    let orphan = orphans.get(dataset);
                    assert!(
                        orphan.is_none(),
                        "found unexpected orphan: {orphan:?}",
                    );
                    assert!(
                        !zfs.datasets.contains_key(&name),
                        "dataset should have been destroyed: {name}"
                    );
                }
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
                let root = DatasetName::new(pool, DatasetKind::Debug);
                let inner = SharedDatasetConfig {
                    compression: CompressionAlgorithm::On,
                    quota: None,
                    reservation: None,
                };
                NestedDatasetTestInput {
                    pool,
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
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(
                inputs.iter().map(|input| input.pool),
            )
            .current();
        let datasets = inputs
            .iter()
            .filter(|input| input.is_debug_datset_mounted)
            .map(|input| make_dataset_config(input.pool, DatasetKind::Debug))
            .collect::<IdOrdMap<_>>();

        // Spawn the task and feed our setup to it. All the debug datasets
        // should successfully be created.
        let zfs = InMemoryZfs::default();
        let task_handle = DatasetTaskHandle::spawn_with_zfs_impl(
            nonexistent_mount_config(),
            &logctx.log,
            zfs.clone(),
        );
        let result = task_handle
            .datasets_ensure(datasets.clone(), currently_managed_zpools)
            .await
            .expect("no task error");
        assert_eq!(result.len(), datasets.len());
        assert!(result.iter().all(|r| matches!(r.result, Ok(()))));

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
            let parent = DatasetName::new(input.pool, DatasetKind::Debug);

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

// On illumos, run a set of tests that use the `RealZfs` implementation on
// zpools backed by vdevs.
#[cfg(all(test, target_os = "illumos"))]
mod illumos_tests {
    use super::tests::make_dataset_config;
    use super::*;
    use crate::CurrentlyManagedZpoolsReceiver;
    use assert_matches::assert_matches;
    use camino_tempfile::Utf8TempDir;
    use iddqd::id_ord_map;
    use illumos_utils::zpool::Zpool;
    use key_manager::KeyManager;
    use key_manager::SecretRetriever;
    use key_manager::SecretRetrieverError;
    use key_manager::StorageKeyRequester;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_common::zpool_name::ZpoolKind;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_storage::disk::Disk;
    use sled_storage::disk::RawSyntheticDisk;
    use tokio::sync::watch;
    use xshell::Shell;
    use xshell::cmd;

    /// A [`key-manager::SecretRetriever`] that only returns hardcoded IKM for
    /// epoch 0
    #[derive(Debug, Default)]
    struct HardcodedSecretRetriever;

    #[async_trait::async_trait]
    impl SecretRetriever for HardcodedSecretRetriever {
        async fn get_latest(
            &self,
        ) -> Result<key_manager::VersionedIkm, SecretRetrieverError> {
            let epoch = 0;
            let salt = [0u8; 32];
            let secret = [0x1d; 32];

            Ok(key_manager::VersionedIkm::new(epoch, salt, &secret))
        }

        async fn get(
            &self,
            epoch: u64,
        ) -> Result<key_manager::SecretState, SecretRetrieverError> {
            if epoch != 0 {
                return Err(SecretRetrieverError::NoSuchEpoch(epoch));
            }
            Ok(key_manager::SecretState::Current(self.get_latest().await?))
        }
    }

    #[derive(Debug)]
    struct RealZfsTestHarness {
        // `Some(_)` until `cleanup()` is called.
        vdev_dir: Option<Utf8TempDir>,
        next_slot: i64,
        currently_managed_zpools_tx: watch::Sender<BTreeSet<ZpoolName>>,
        currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
        mount_config: MountConfig,
        key_requester: StorageKeyRequester,
        key_manager_task: tokio::task::JoinHandle<()>,
        log: Logger,
    }

    impl Drop for RealZfsTestHarness {
        fn drop(&mut self) {
            if let Some(vdev_dir) = &self.vdev_dir {
                eprintln!(
                    "WARNING: RealZfsTestHarness dropped without 'cleanup()'"
                );
                eprintln!(
                    "Attempting automated cleanup of {}",
                    vdev_dir.path(),
                );

                // NOTE: There is an assertion in RealZfsTestHarness::new
                // which should make this a safe invocation to call.
                //
                // Blocking the calling thread like this isn't ideal, but
                // the scope is limited to "tests which are failing anyway".
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async move {
                        self.cleanup().await;
                    });
                });
            }
        }
    }

    impl RealZfsTestHarness {
        pub const DEFAULT_VDEV_SIZE: u64 = 64 * (1 << 20);

        fn new(log: Logger) -> Self {
            assert_eq!(
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread,
                "RealZfsTestHarness requires a multi-threaded runtime to ensure deletion on drop"
            );

            let vdev_dir =
                Utf8TempDir::new_in("/var/tmp").expect("created tempdir");

            let (currently_managed_zpools_tx, currently_managed_zpools_rx) =
                watch::channel(BTreeSet::new());
            let currently_managed_zpools_rx =
                CurrentlyManagedZpoolsReceiver::fake_dynamic(
                    currently_managed_zpools_rx,
                );

            let (mut key_manager, key_requester) =
                KeyManager::new(&log, HardcodedSecretRetriever);
            let key_manater_task =
                tokio::spawn(async move { key_manager.run().await });

            let mount_config = MountConfig {
                root: vdev_dir.path().to_owned(),
                ..Default::default()
            };
            Self {
                vdev_dir: Some(vdev_dir),
                next_slot: 0,
                currently_managed_zpools_tx,
                currently_managed_zpools_rx,
                mount_config,
                key_requester,
                key_manager_task: key_manater_task,
                log,
            }
        }

        fn current_zpools(&self) -> Arc<CurrentlyManagedZpools> {
            self.currently_managed_zpools_rx.current()
        }

        async fn add_zpool(&mut self, kind: ZpoolKind) -> ZpoolName {
            self.add_zpool_with_size(kind, Self::DEFAULT_VDEV_SIZE).await
        }

        async fn add_zpool_with_size(
            &mut self,
            kind: ZpoolKind,
            size: u64,
        ) -> ZpoolName {
            let vdev_dir = self
                .vdev_dir
                .as_ref()
                .expect("test harness not yet cleaned up");

            let zpool_id = ZpoolUuid::new_v4();
            let (zpool, prefix) = match kind {
                ZpoolKind::External => {
                    (ZpoolName::new_external(zpool_id), "u2_")
                }
                ZpoolKind::Internal => {
                    (ZpoolName::new_internal(zpool_id), "m2_")
                }
            };

            let vdev_name = { format!("{prefix}{}.vdev", zpool.id()) };

            let full_path = vdev_dir.path().join(&vdev_name);
            let raw_disk = RawSyntheticDisk::new_with_length(
                &full_path,
                size,
                self.next_slot,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to create synthetic disk for {vdev_name}: {err:#}",
                )
            });
            self.next_slot += 1;

            let _disk = Disk::new(
                &self.log,
                &self.mount_config,
                raw_disk.into(),
                Some(zpool.id()),
                Some(&self.key_requester),
            )
            .await
            .expect("started managing vdev-backed disk");

            eprintln!("created vdev-backed zpool at {full_path}");
            self.currently_managed_zpools_tx.send_modify(|zpools| {
                zpools.insert(zpool);
            });

            zpool
        }

        async fn cleanup(&mut self) {
            let Some(vdev_dir) = self.vdev_dir.take() else {
                // Already terminated
                return;
            };

            self.key_manager_task.abort();

            for pool in self.currently_managed_zpools_tx.borrow().iter() {
                eprintln!("destroying pool: {pool}");
                if let Err(err) = Zpool::destroy(&pool).await {
                    eprintln!(
                        "failed to destroy {pool}: {}",
                        InlineErrorChain::new(&err)
                    );
                }
            }

            // Make sure that we're actually able to delete everything within
            // the temporary directory.
            //
            // This is necessary because the act of mounting datasets within
            // this directory may have created directories owned by root, and
            // the test process may not have been started as root.
            //
            // Since we're about to delete all these files anyway, make them
            // accessible to everyone before destroying them.
            let mut command = std::process::Command::new("/usr/bin/pfexec");
            let mount = vdev_dir.path();

            let sh = Shell::new().unwrap();
            let dirs =
                cmd!(sh, "find {mount} -type d").read().expect("found dirs");
            for dir in dirs.lines() {
                println!("Making {dir} mutable");
                cmd!(sh, "pfexec chmod S-ci {dir}")
                    .quiet()
                    .run()
                    .expect("made directories mutable");
            }

            let cmd = command.args(["chmod", "-R", "a+rw", mount.as_str()]);
            cmd.output().expect("changed ownership of files");

            // Actually delete everything, and check the result to fail loud if
            // something goes wrong.
            vdev_dir.close().expect("cleaned up vdev tempdir");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_datasets() {
        let logctx = dev::test_setup_log("ensure_datasets");
        let mut harness = RealZfsTestHarness::new(logctx.log.clone());

        // Test setup: Add an external zpool and start the dataset task.
        let zpool = harness.add_zpool(ZpoolKind::External).await;
        let task_handle = DatasetTaskHandle::spawn_dataset_task(
            Arc::new(harness.mount_config.clone()),
            &logctx.log,
        );

        // Create a dataset on the newly formatted U.2
        let dataset = make_dataset_config(zpool, DatasetKind::Crucible);
        let result = task_handle
            .datasets_ensure(
                [dataset.clone()].into_iter().collect(),
                harness.current_zpools(),
            )
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), 1);
        assert_matches!(
            result
                .get(&dataset.id)
                .expect("result contains entry for dataset")
                .result,
            Ok(())
        );

        // Calling "datasets_ensure" with the same input should succeed.
        let result = task_handle
            .datasets_ensure(
                [dataset.clone()].into_iter().collect(),
                harness.current_zpools(),
            )
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), 1);
        assert_matches!(
            result
                .get(&dataset.id)
                .expect("result contains entry for dataset")
                .result,
            Ok(())
        );

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    async fn is_mounted(dataset: &DatasetName) -> bool {
        let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
        let cmd =
            command.args(&["list", "-Hpo", "mounted", &dataset.full_name()]);
        let output = cmd.output().await.unwrap();
        assert!(output.status.success(), "Failed to list dataset: {output:?}");
        dbg!(String::from_utf8_lossy(&output.stdout)).trim() == "yes"
    }

    async fn unmount(dataset: &DatasetName) {
        let mut command = tokio::process::Command::new(illumos_utils::PFEXEC);
        let cmd = command.args(&[
            illumos_utils::zfs::ZFS,
            "unmount",
            "-f",
            &dataset.full_name(),
        ]);
        let output = cmd.output().await.unwrap();
        assert!(
            output.status.success(),
            "Failed to unmount dataset: {output:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_datasets_get_mounted() {
        let logctx = dev::test_setup_log("ensure_datasets_get_mounted");
        let mut harness = RealZfsTestHarness::new(logctx.log.clone());

        // Test setup: Add an external zpool and start the dataset task.
        let zpool = harness.add_zpool(ZpoolKind::External).await;
        let task_handle = DatasetTaskHandle::spawn_dataset_task(
            Arc::new(harness.mount_config.clone()),
            &logctx.log,
        );

        // Create a dataset on the newly formatted U.2
        let dataset = make_dataset_config(zpool, DatasetKind::Debug);
        let name = &dataset.name;

        let result = task_handle
            .datasets_ensure(
                [dataset.clone()].into_iter().collect(),
                harness.current_zpools(),
            )
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), 1);
        assert_matches!(
            result
                .get(&dataset.id)
                .expect("result contains entry for dataset")
                .result,
            Ok(())
        );

        // Creating the dataset should have mounted it
        assert!(is_mounted(name).await);

        // We can unmount the dataset manually
        unmount(name).await;
        assert!(!is_mounted(name).await);

        // We can re-apply the same config...
        let result = task_handle
            .datasets_ensure(
                [dataset.clone()].into_iter().collect(),
                harness.current_zpools(),
            )
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), 1);
        assert_matches!(
            result
                .get(&dataset.id)
                .expect("result contains entry for dataset")
                .result,
            Ok(())
        );

        // ... and doing so mounts the dataset again.
        assert!(is_mounted(name).await);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_datasets_get_mounted_even_with_data() {
        let logctx =
            dev::test_setup_log("ensure_datasets_get_mounted_even_with_data");
        let mut harness = RealZfsTestHarness::new(logctx.log.clone());

        // Test setup: Add an external zpool and start the dataset task.
        let zpool = harness.add_zpool(ZpoolKind::External).await;
        let task_handle = DatasetTaskHandle::spawn_dataset_task(
            Arc::new(harness.mount_config.clone()),
            &logctx.log,
        );

        // Create a config for a dataset on the newly formatted U.2.
        let kind = DatasetKind::TransientZone { name: "foo".to_string() };
        // If we use the "Debug" dataset, it'll get created and made immutable
        // So: We pick a different non-zoned dataset.
        assert!(
            !kind.zoned(),
            "We need to use a non-zoned dataset for this test"
        );
        let dataset = make_dataset_config(zpool, kind);

        // Before we actually make the dataset - create the mountpoint, and
        // stick a file there.
        let mountpoint = dataset.name.mountpoint(&harness.mount_config.root);
        std::fs::create_dir_all(&mountpoint).unwrap();
        std::fs::write(mountpoint.join("marker.txt"), "hello").unwrap();
        assert!(mountpoint.join("marker.txt").exists());

        // Because `dataset` has kind `TransientZone { .. }`, we also need to
        // supply its parent root.
        let dataset_configs = id_ord_map! {
            dataset.clone(),
            make_dataset_config(zpool, DatasetKind::TransientZoneRoot),
        };

        // Create the datasets.
        let result = task_handle
            .datasets_ensure(dataset_configs.clone(), harness.current_zpools())
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), dataset_configs.len());
        for result in &result {
            assert_matches!(result.result, Ok(()));
        }

        // Creating the dataset should have mounted it
        assert!(is_mounted(&dataset.name).await);

        // Creating the dataset should have moved the marker file
        let mount_parent = mountpoint.parent().unwrap();
        let old_data_dir = mount_parent
            .read_dir_utf8()
            .unwrap()
            .map(|entry| entry.unwrap())
            .find(|entry| entry.file_name().starts_with("old-under-mountpoint"))
            .expect("Could not find relocated data directory");
        assert!(
            old_data_dir.path().join("marker.txt").exists(),
            "Missing marker file"
        );
        // Test meta-note: If you try to keep this open across the call to
        // "harness.cleanup()", you'll see "device busy" errors. Drop it now.
        drop(old_data_dir);

        // We can unmount the dataset manually
        unmount(&dataset.name).await;
        assert!(!is_mounted(&dataset.name).await);

        // After unmounting the dataset, the directory underneath should
        // exist, but be immutable.
        assert!(mountpoint.exists(), "Mountpoint {mountpoint} does not exist?");
        let err =
            std::fs::write(mountpoint.join("another-marker.txt"), "goodbye")
                .unwrap_err();
        assert!(
            matches!(err.kind(), std::io::ErrorKind::PermissionDenied),
            "err: {err}"
        );

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_many_datasets() {
        let logctx = dev::test_setup_log("ensure_many_datasets");
        let mut harness = RealZfsTestHarness::new(logctx.log.clone());

        // Test setup: Add many external zpools and start the dataset task.
        let zpools = {
            let mut zpools = Vec::new();
            for _ in 0..10 {
                zpools.push(harness.add_zpool(ZpoolKind::External).await);
            }
            zpools
        };
        let task_handle = DatasetTaskHandle::spawn_dataset_task(
            Arc::new(harness.mount_config.clone()),
            &logctx.log,
        );

        // Build configs for a few datasets on each zpool
        let mut datasets = IdOrdMap::new();
        for &zpool in &zpools {
            datasets
                .insert_unique(make_dataset_config(
                    zpool,
                    DatasetKind::Crucible,
                ))
                .unwrap();
            datasets
                .insert_unique(make_dataset_config(zpool, DatasetKind::Debug))
                .unwrap();
            datasets
                .insert_unique(make_dataset_config(
                    zpool,
                    DatasetKind::TransientZoneRoot,
                ))
                .unwrap();
        }

        // Create the datasets.
        let result = task_handle
            .datasets_ensure(datasets.clone(), harness.current_zpools())
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), datasets.len());
        for result in &result {
            assert_matches!(result.result, Ok(()));
        }

        // Calling "datasets_ensure" with the same input should succeed.
        let result = task_handle
            .datasets_ensure(datasets.clone(), harness.current_zpools())
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), datasets.len());
        for result in &result {
            assert_matches!(result.result, Ok(()));
        }

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nested_dataset() {
        let logctx = dev::test_setup_log("nested_dataset");

        let mut harness = RealZfsTestHarness::new(logctx.log.clone());

        // Test setup: Add an external zpool and start the dataset task.
        let zpool = harness.add_zpool(ZpoolKind::External).await;
        let task_handle = DatasetTaskHandle::spawn_dataset_task(
            Arc::new(harness.mount_config.clone()),
            &logctx.log,
        );

        // Add a `Debug` dataset to our zpool.
        let debug_dataset = make_dataset_config(zpool, DatasetKind::Debug);
        let result = task_handle
            .datasets_ensure(
                [debug_dataset.clone()].into_iter().collect(),
                harness.current_zpools(),
            )
            .await
            .expect("task should not fail");
        assert_eq!(result.len(), 1);
        assert_matches!(
            result
                .get(&debug_dataset.id)
                .expect("result contains entry for dataset")
                .result,
            Ok(())
        );

        // Start querying the state of nested datasets.
        //
        // When we ask about the root of a dataset, we only get information
        // about the dataset we're asking for.
        let root_location = NestedDatasetLocation {
            path: String::new(),
            root: debug_dataset.name.clone(),
        };
        let nested_datasets = task_handle
            .nested_dataset_list(
                root_location.root.clone(),
                NestedDatasetListOptions::SelfAndChildren,
            )
            .await
            .expect("no task error")
            .expect("no error listing datasets");
        assert_eq!(nested_datasets.len(), 1);
        assert_eq!(nested_datasets[0].name, root_location);

        // If we ask about children of this dataset, we see nothing.
        let nested_datasets = task_handle
            .nested_dataset_list(
                root_location.root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await
            .expect("no task error")
            .expect("no error listing datasets");
        assert_eq!(nested_datasets.len(), 0);

        // We can't destroy non-nested datasets through this API
        let err = task_handle
            .nested_dataset_destroy(root_location.clone())
            .await
            .expect("no task error")
            .expect_err("Should not be able to delete dataset root");
        assert_matches!(err, NestedDatasetDestroyError::EmptyName);

        // Create a nested dataset within the root one
        let nested_location = NestedDatasetLocation {
            path: "nested".to_string(),
            ..root_location.clone()
        };
        let nested_config = SharedDatasetConfig {
            compression: CompressionAlgorithm::On,
            ..Default::default()
        };
        task_handle
            .nested_dataset_ensure(NestedDatasetConfig {
                name: nested_location.clone(),
                inner: nested_config.clone(),
            })
            .await
            .expect("no task error")
            .expect("ensured dataset");

        // We can re-send the ensure request
        task_handle
            .nested_dataset_ensure(NestedDatasetConfig {
                name: nested_location.clone(),
                inner: nested_config.clone(),
            })
            .await
            .expect("no task error")
            .expect("re-ensured dataset");

        // We can observe the nested dataset
        let nested_datasets = task_handle
            .nested_dataset_list(
                root_location.root.clone(),
                NestedDatasetListOptions::SelfAndChildren,
            )
            .await
            .expect("no task error")
            .expect("no error listing datasets");
        assert_eq!(nested_datasets.len(), 2);
        assert_eq!(nested_datasets[0].name, root_location);
        assert_eq!(nested_datasets[1].name, nested_location);
        let nested_datasets = task_handle
            .nested_dataset_list(
                root_location.root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await
            .expect("no task error")
            .expect("no error listing datasets");
        assert_eq!(nested_datasets.len(), 1);
        assert_eq!(nested_datasets[0].name, nested_location);

        // We can also destroy the nested dataset
        task_handle
            .nested_dataset_destroy(nested_location.clone())
            .await
            .expect("no task error")
            .expect("Should have been able to destroy nested dataset");

        let err = task_handle
            .nested_dataset_destroy(nested_location.clone())
            .await
            .expect("no task error")
            .expect_err(
                "Should not be able to destroy nested dataset a second time",
            );
        assert_matches!(
            err,
            NestedDatasetDestroyError::DestroyFailed(DestroyDatasetError {
                err: zfs::DestroyDatasetErrorVariant::NotFound,
                ..
            })
        );

        // The nested dataset should now be gone
        let nested_datasets = task_handle
            .nested_dataset_list(
                root_location.root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await
            .expect("no task error")
            .expect("no error listing datasets");
        assert_eq!(nested_datasets.len(), 0);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }
}
