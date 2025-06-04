// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use illumos_utils::zpool::PathInPool;
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::disk::DatasetName;
use sled_agent_api::ArtifactConfig;
use sled_storage::config::MountConfig;
use sled_storage::disk::Disk;
use sled_storage::manager::NestedDatasetConfig;
use sled_storage::manager::NestedDatasetListOptions;
use sled_storage::manager::NestedDatasetLocation;
use slog::Logger;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;

#[cfg(feature = "testing")]
use camino_tempfile::Utf8TempDir;
#[cfg(feature = "testing")]
use illumos_utils::zpool::ZpoolName;
#[cfg(feature = "testing")]
use illumos_utils::zpool::ZpoolOrRamdisk;
#[cfg(feature = "testing")]
use sled_storage::dataset::U2_DEBUG_DATASET;
#[cfg(feature = "testing")]
use sled_storage::dataset::ZONE_DATASET;

use crate::DatasetTaskError;
use crate::InternalDisksWithBootDisk;
use crate::LedgerArtifactConfigError;
use crate::LedgerNewConfigError;
use crate::LedgerTaskError;
use crate::NestedDatasetDestroyError;
use crate::NestedDatasetEnsureError;
use crate::NestedDatasetListError;
use crate::SledAgentArtifactStore;
use crate::SledAgentFacilities;
use crate::TimeSyncStatus;
use crate::dataset_serialization_task::DatasetTaskHandle;
use crate::dataset_serialization_task::NestedDatasetMountError;
use crate::dump_setup_task;
use crate::internal_disks::InternalDisksReceiver;
use crate::ledger::CurrentSledConfig;
use crate::ledger::LedgerTaskHandle;
use crate::raw_disks;
use crate::raw_disks::RawDisksReceiver;
use crate::raw_disks::RawDisksSender;
use crate::reconciler_task;
use crate::reconciler_task::CurrentlyManagedZpools;
use crate::reconciler_task::CurrentlyManagedZpoolsReceiver;
use crate::reconciler_task::ReconcilerResult;

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("ledger contents not yet available")]
    LedgerContentsNotAvailable,
    #[error("could not contact dataset task")]
    DatasetTaskError(#[from] DatasetTaskError),
    #[error("could not list dataset properties")]
    ListDatasetProperties(#[source] anyhow::Error),
}

#[derive(Debug, Clone, Copy)]
pub enum TimeSyncConfig {
    // Waits for NTP to confirm that time has been synchronized.
    Normal,
    // Skips timesync unconditionally.
    Skip,
}

#[derive(Debug)]
pub struct ConfigReconcilerSpawnToken {
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    reconciler_result_tx: watch::Sender<ReconcilerResult>,
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
    external_disks_tx: watch::Sender<HashSet<Disk>>,
    raw_disks_rx: RawDisksReceiver,
    ledger_task_log: Logger,
    reconciler_task_log: Logger,
}

#[derive(Debug)]
pub struct ConfigReconcilerHandle {
    raw_disks_tx: RawDisksSender,
    internal_disks_rx: InternalDisksReceiver,
    dataset_task: DatasetTaskHandle,
    reconciler_result_rx: watch::Receiver<ReconcilerResult>,
    currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,

    // Empty until `spawn_reconciliation_task()` is called.
    ledger_task: OnceLock<LedgerTaskHandle>,
}

impl ConfigReconcilerHandle {
    /// Create a `ConfigReconcilerHandle` and spawn many of the early-sled-agent
    /// background tasks (e.g., managing internal disks).
    ///
    /// The config reconciler subsystem splits initialization into two phases:
    /// the main reconcilation task will not be spawned until
    /// `spawn_reconciliation_task()` is called on the returned handle.
    /// `spawn_reconciliation_task()` cannot be called by sled-agent proper
    /// until rack setup has occurred (or sled-agent has found its config from a
    /// prior rack setup, during a cold boot).
    pub fn new(
        mount_config: MountConfig,
        key_requester: StorageKeyRequester,
        time_sync_config: TimeSyncConfig,
        base_log: &Logger,
    ) -> (Self, ConfigReconcilerSpawnToken) {
        let mount_config = Arc::new(mount_config);

        // Spawn the task that monitors our internal disks (M.2s).
        let (raw_disks_tx, raw_disks_rx) = raw_disks::new();
        let internal_disks_rx =
            InternalDisksReceiver::spawn_internal_disks_task(
                Arc::clone(&mount_config),
                raw_disks_rx.clone(),
                base_log,
            );

        // Spawn the task that manages dump devices.
        let (external_disks_tx, external_disks_rx) =
            watch::channel(HashSet::new());
        dump_setup_task::spawn(
            internal_disks_rx.clone(),
            external_disks_rx,
            Arc::clone(&mount_config),
            base_log,
        );

        let (reconciler_result_tx, reconciler_result_rx) =
            watch::channel(ReconcilerResult::new(Arc::clone(&mount_config)));
        let (currently_managed_zpools_tx, currently_managed_zpools_rx) =
            watch::channel(Arc::default());
        let currently_managed_zpools_rx =
            CurrentlyManagedZpoolsReceiver::new(currently_managed_zpools_rx);

        // Spawn the task that serializes dataset operations.
        let dataset_task = DatasetTaskHandle::spawn_dataset_task(
            Arc::clone(&mount_config),
            base_log,
        );

        (
            Self {
                raw_disks_tx,
                internal_disks_rx,
                dataset_task,
                ledger_task: OnceLock::new(),
                reconciler_result_rx,
                currently_managed_zpools_rx,
            },
            // Stash the dependencies the reconciler task will need in
            // `spawn_reconciliation_task()` inside this token that the caller
            // has to hold until it has the other outside dependencies ready.
            ConfigReconcilerSpawnToken {
                key_requester,
                time_sync_config,
                reconciler_result_tx,
                currently_managed_zpools_tx,
                external_disks_tx,
                raw_disks_rx,
                ledger_task_log: base_log
                    .new(slog::o!("component" => "SledConfigLedgerTask")),
                reconciler_task_log: base_log
                    .new(slog::o!("component" => "ConfigReconcilerTask")),
            },
        )
    }

    /// Spawn the primary config reconciliation task.
    ///
    /// This method can effectively only be called once, because the caller must
    /// supply the `token` returned by `new()` when this handle was created.
    ///
    /// # Panics
    ///
    /// Panics if called multiple times, which is statically impossible outside
    /// shenanigans to get a second [`ConfigReconcilerSpawnToken`].
    pub fn spawn_reconciliation_task<
        T: SledAgentFacilities,
        U: SledAgentArtifactStore,
    >(
        &self,
        sled_agent_facilities: T,
        sled_agent_artifact_store: U,
        token: ConfigReconcilerSpawnToken,
    ) {
        let ConfigReconcilerSpawnToken {
            key_requester,
            time_sync_config,
            reconciler_result_tx,
            currently_managed_zpools_tx,
            external_disks_tx,
            raw_disks_rx,
            ledger_task_log,
            reconciler_task_log,
        } = token;

        // Spawn the task that manages our config ledger.
        let (ledger_task, current_config_rx) =
            LedgerTaskHandle::spawn_ledger_task(
                self.internal_disks_rx.clone(),
                sled_agent_artifact_store,
                ledger_task_log,
            );
        match self.ledger_task.set(ledger_task) {
            Ok(()) => (),
            // We can only be called with the `token` we returned in `new()` and
            // we document that we panic if called multiple times via some
            // multi-token shenanigans.
            Err(_) => {
                panic!(
                    "spawn_reconciliation_task() called with multiple tokens"
                )
            }
        }

        reconciler_task::spawn(
            Arc::clone(self.internal_disks_rx.mount_config()),
            self.dataset_task.clone(),
            key_requester,
            time_sync_config,
            current_config_rx,
            reconciler_result_tx,
            currently_managed_zpools_tx,
            external_disks_tx,
            raw_disks_rx,
            sled_agent_facilities,
            reconciler_task_log,
        );
    }

    /// Get the current timesync status.
    pub fn timesync_status(&self) -> TimeSyncStatus {
        self.reconciler_result_rx.borrow().timesync_status()
    }

    /// Get a handle to update the set of raw disks visible to sled-agent.
    pub fn raw_disks_tx(&self) -> RawDisksSender {
        self.raw_disks_tx.clone()
    }

    /// Get a watch channel to receive changes to the set of managed internal
    /// disks.
    pub fn internal_disks_rx(&self) -> &InternalDisksReceiver {
        &self.internal_disks_rx
    }

    /// Get a watch channel to receive changes to the set of available external
    /// disk datasets.
    pub fn available_datasets_rx(&self) -> AvailableDatasetsReceiver {
        AvailableDatasetsReceiver {
            inner: AvailableDatasetsReceiverInner::Real(
                self.reconciler_result_rx.clone(),
            ),
        }
    }

    /// Get a watch channel to receive changes to the set of managed zpools.
    pub fn currently_managed_zpools_rx(
        &self,
    ) -> &CurrentlyManagedZpoolsReceiver {
        &self.currently_managed_zpools_rx
    }

    /// Wait for the internal disks task to start managing the boot disk.
    pub async fn wait_for_boot_disk(&mut self) -> InternalDisksWithBootDisk {
        self.internal_disks_rx.wait_for_boot_disk().await
    }

    /// Ensure a nested dataset is mounted.
    pub async fn nested_dataset_ensure_mounted(
        &self,
        dataset: NestedDatasetLocation,
    ) -> Result<Result<Utf8PathBuf, NestedDatasetMountError>, DatasetTaskError>
    {
        self.dataset_task.nested_dataset_ensure_mounted(dataset).await
    }

    /// Ensure the existence of a nested dataset.
    pub async fn nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<Result<(), NestedDatasetEnsureError>, DatasetTaskError> {
        self.dataset_task.nested_dataset_ensure(config).await
    }

    /// Destroy a nested dataset.
    pub async fn nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<Result<(), NestedDatasetDestroyError>, DatasetTaskError> {
        self.dataset_task.nested_dataset_destroy(name).await
    }

    /// List a set of nested datasets.
    pub async fn nested_dataset_list(
        &self,
        dataset: DatasetName,
        options: NestedDatasetListOptions,
    ) -> Result<
        Result<Vec<NestedDatasetConfig>, NestedDatasetListError>,
        DatasetTaskError,
    > {
        self.dataset_task.nested_dataset_list(dataset, options).await
    }

    /// Write a new sled config to the ledger.
    pub async fn set_sled_config(
        &self,
        new_config: OmicronSledConfig,
    ) -> Result<Result<(), LedgerNewConfigError>, LedgerTaskError> {
        self.ledger_task
            .get()
            .ok_or(LedgerTaskError::NotYetStarted)?
            .set_new_config(new_config)
            .await
    }

    /// Validate that a new artifact config is legal (i.e., it doesn't remove
    /// any artifact hashes in use by the currently-ledgered sled config).
    pub async fn validate_artifact_config(
        &self,
        new_config: ArtifactConfig,
    ) -> Result<Result<(), LedgerArtifactConfigError>, LedgerTaskError> {
        self.ledger_task
            .get()
            .ok_or(LedgerTaskError::NotYetStarted)?
            .validate_artifact_config(new_config)
            .await
    }

    /// Return the currently-ledgered [`OmicronSledConfig`].
    ///
    /// # Errors
    ///
    /// Fails if `spawn_reconciliation_task()` has not yet been called or if we
    /// have not yet checked the internal disks for a ledgered config.
    pub fn ledgered_sled_config(
        &self,
    ) -> Result<Option<OmicronSledConfig>, InventoryError> {
        match self.ledger_task.get().map(LedgerTaskHandle::current_config) {
            // If we haven't yet spawned the ledger task, or we have but
            // it's still waiting on disks, we don't know whether we have a
            // ledgered sled config. It's not reasonable to report `None` in
            // this case (since `None` means "we don't have a config"), so
            // bail out.
            //
            // This shouldn't happen in practice: sled-agent should both wait
            // for the boot disk and spawn the reconciler task before starting
            // the dropshot server that allows Nexus to collect inventory.
            None | Some(CurrentSledConfig::WaitingForInternalDisks) => {
                Err(InventoryError::LedgerContentsNotAvailable)
            }
            Some(CurrentSledConfig::WaitingForInitialConfig) => Ok(None),
            Some(CurrentSledConfig::Ledgered(config)) => Ok(Some(config)),
        }
    }

    /// Collect inventory fields relevant to config reconciliation.
    pub async fn inventory(
        &self,
        log: &Logger,
    ) -> Result<ReconcilerInventory, InventoryError> {
        let ledgered_sled_config = self.ledgered_sled_config()?;
        let zpools = self.currently_managed_zpools_rx.to_inventory(log).await;

        let datasets = self
            .dataset_task
            .inventory(zpools.iter().map(|&(name, _)| name).collect())
            .await??;

        let (reconciler_status, last_reconciliation) =
            self.reconciler_result_rx.borrow().to_inventory();

        Ok(ReconcilerInventory {
            disks: self.raw_disks_tx.to_inventory(),
            zpools: zpools
                .into_iter()
                .map(|(name, total_size)| InventoryZpool {
                    id: name.id(),
                    total_size,
                })
                .collect(),
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
        })
    }
}

#[derive(Debug)]
struct ReconcilerTaskDependencies {
    key_requester: StorageKeyRequester,
    time_sync_config: TimeSyncConfig,
    reconciler_result_tx: watch::Sender<ReconcilerResult>,
    currently_managed_zpools_tx: watch::Sender<Arc<CurrentlyManagedZpools>>,
    ledger_task_log: Logger,
    reconciler_task_log: Logger,
}

/// Fields of sled-agent inventory reported by the config reconciler subsystem.
///
/// Note that much like inventory in general, these fields are not collected
/// atomically; if there are active changes being made while this struct is
/// being assembled, different fields may have be populated from different
/// states of the world.
#[derive(Debug)]
pub struct ReconcilerInventory {
    pub disks: Vec<InventoryDisk>,
    pub zpools: Vec<InventoryZpool>,
    pub datasets: Vec<InventoryDataset>,
    pub ledgered_sled_config: Option<OmicronSledConfig>,
    pub reconciler_status: ConfigReconcilerInventoryStatus,
    pub last_reconciliation: Option<ConfigReconcilerInventory>,
}

#[derive(Debug, Clone)]
pub struct AvailableDatasetsReceiver {
    inner: AvailableDatasetsReceiverInner,
}

impl AvailableDatasetsReceiver {
    #[cfg(feature = "testing")]
    pub fn fake_in_tempdir_for_tests(zpool: ZpoolOrRamdisk) -> Self {
        let tempdir = Arc::new(Utf8TempDir::new().expect("created temp dir"));
        std::fs::create_dir_all(tempdir.path().join(U2_DEBUG_DATASET))
            .expect("created test debug dataset directory");
        std::fs::create_dir_all(tempdir.path().join(ZONE_DATASET))
            .expect("created test zone root dataset directory");
        Self {
            inner: AvailableDatasetsReceiverInner::FakeTempDir {
                zpool,
                tempdir,
            },
        }
    }

    #[cfg(feature = "testing")]
    pub fn fake_static(
        pools: impl Iterator<Item = (ZpoolName, Utf8PathBuf)>,
    ) -> Self {
        Self {
            inner: AvailableDatasetsReceiverInner::FakeStatic(pools.collect()),
        }
    }

    pub fn all_mounted_debug_datasets(&self) -> Vec<PathInPool> {
        match &self.inner {
            AvailableDatasetsReceiverInner::Real(receiver) => {
                receiver.borrow().all_mounted_debug_datasets().collect()
            }
            #[cfg(feature = "testing")]
            AvailableDatasetsReceiverInner::FakeTempDir { zpool, tempdir } => {
                vec![PathInPool {
                    pool: zpool.clone(),
                    path: tempdir.path().join(U2_DEBUG_DATASET),
                }]
            }
            #[cfg(feature = "testing")]
            AvailableDatasetsReceiverInner::FakeStatic(pools) => pools
                .iter()
                .map(|(pool, path)| PathInPool {
                    pool: ZpoolOrRamdisk::Zpool(*pool),
                    path: path.join(U2_DEBUG_DATASET),
                })
                .collect(),
        }
    }

    pub fn all_mounted_zone_root_datasets(&self) -> Vec<PathInPool> {
        match &self.inner {
            AvailableDatasetsReceiverInner::Real(receiver) => {
                receiver.borrow().all_mounted_zone_root_datasets().collect()
            }
            #[cfg(feature = "testing")]
            AvailableDatasetsReceiverInner::FakeTempDir { zpool, tempdir } => {
                vec![PathInPool {
                    pool: zpool.clone(),
                    path: tempdir.path().join(ZONE_DATASET),
                }]
            }
            #[cfg(feature = "testing")]
            AvailableDatasetsReceiverInner::FakeStatic(pools) => pools
                .iter()
                .map(|(pool, path)| PathInPool {
                    pool: ZpoolOrRamdisk::Zpool(*pool),
                    path: path.join(ZONE_DATASET),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
enum AvailableDatasetsReceiverInner {
    // The production path: available datasets are based on the results of the
    // most recent reconciliation result.
    Real(watch::Receiver<ReconcilerResult>),
    // Test path: allow tests to place datasets in a temp directory.
    #[cfg(feature = "testing")]
    FakeTempDir {
        zpool: ZpoolOrRamdisk,
        tempdir: Arc<Utf8TempDir>,
    },
    // Test path: allow tests to specify their own directories.
    #[cfg(feature = "testing")]
    FakeStatic(Vec<(ZpoolName, Utf8PathBuf)>),
}
