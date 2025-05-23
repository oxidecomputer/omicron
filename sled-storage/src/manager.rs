// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use crate::config::MountConfig;
use crate::dataset::{CONFIG_DATASET, CRYPT_DATASET, ZONE_DATASET};
use crate::disk::RawDisk;
use crate::error::Error;
use crate::resources::{AllDisks, StorageResources};
use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use debug_ignore::DebugIgnore;
use futures::Stream;
use futures::StreamExt;
use futures::future::FutureExt;
use illumos_utils::zfs::{
    CanMount, DatasetEnsureArgs, DatasetProperties, Mountpoint, WhichDatasets,
    Zfs,
};
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::{
    DatasetConfig, DatasetManagementStatus, DatasetName, DatasetsConfig,
    DatasetsManagementResult, DiskIdentity, DiskVariant, DisksManagementResult,
    OmicronPhysicalDisksConfig, SharedDatasetConfig,
};
use omicron_common::ledger::Ledger;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use slog::{Logger, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, MissedTickBehavior, interval};

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
pub(crate) const QUEUE_SIZE: usize = 256;

const SYNCHRONIZE_INTERVAL: Duration = Duration::from_secs(10);

// The filename of the ledger storing physical disk info
const DISKS_LEDGER_FILENAME: &str = "omicron-physical-disks.json";

// The filename of the ledger storing dataset info
const DATASETS_LEDGER_FILENAME: &str = "omicron-datasets.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageManagerState {
    // We know that any attempts to manage disks will fail, as the key manager
    // is not ready yet.
    WaitingForKeyManager,

    // This state is used to indicate that the set of "control plane" physical
    // disks and the set of "observed" disks may be out-of-sync.
    //
    // This can happen when:
    // - The sled boots, and the ledger of "control plane disks" is initially
    //    loaded.
    // - A U.2 is added to the disk after initial boot.
    //
    // In both of these cases, if trust quorum hasn't been established, it's
    // possible that the request to [Self::manage_disks] will need to retry.
    SynchronizationNeeded,

    // This state indicates the key manager is ready, and the storage manager
    // believes that the set of control plane disks is in-sync with the set of
    // observed disks.
    Synchronized,
}

#[derive(Debug)]
pub(crate) struct NewFilesystemRequest {
    dataset_id: Option<DatasetUuid>,
    dataset_name: DatasetName,
    responder: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
}

#[derive(Debug)]
pub enum NestedDatasetListOptions {
    /// Returns children of the requested dataset, but not the dataset itself.
    ChildrenOnly,
    /// Returns both the requested dataset as well as all children.
    SelfAndChildren,
}

/// Configuration information necessary to request a single nested dataset.
///
/// These datasets must be placed within one of the top-level datasets
/// managed directly by Nexus.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NestedDatasetConfig {
    /// Location of this nested dataset
    pub name: NestedDatasetLocation,

    /// Configuration of this dataset
    pub inner: SharedDatasetConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NestedDatasetLocation {
    /// A path, within the dataset root, which is being requested.
    pub path: String,

    /// The root in which this dataset is being requested
    pub root: DatasetName,
}

impl NestedDatasetLocation {
    /// Returns the desired mountpoint of this dataset.
    ///
    /// Does not ensure that the dataset is mounted.
    pub fn mountpoint(&self, mount_root: &Utf8Path) -> Utf8PathBuf {
        let mut path = Utf8Path::new(&self.path);

        // This path must be nested, so we need it to be relative to
        // "self.root". However, joining paths in Rust is quirky,
        // as it chooses to replace the path entirely if the argument
        // to `.join(...)` is absolute.
        //
        // Here, we "fix" the path to make non-absolute before joining
        // the paths.
        while path.is_absolute() {
            path = path
                .strip_prefix("/")
                .expect("Path is absolute, but we cannot strip '/' character");
        }

        // mount_root: Usually "/", but can be a tmp dir for tests
        // self.root:  Parent dataset mountpoint
        // path:       Path to nested dataset within parent dataset
        self.root.mountpoint(mount_root).join(path)
    }

    /// Access the mountpoint of this nested dataset, and ensure it's mounted.
    ///
    /// If it is not mounted, or cannot be mounted, return an error.
    pub async fn ensure_mounted_and_get_mountpoint(
        &self,
        mount_root: &Utf8Path,
    ) -> Result<Utf8PathBuf, Error> {
        let mountpoint = self.mountpoint(mount_root);
        Zfs::ensure_dataset_mounted_and_exists(
            &self.full_name(),
            &Mountpoint(mountpoint.clone()),
        )
        .await?;

        return Ok(mountpoint);
    }

    /// Returns the full name of the nested dataset.
    ///
    /// This is a combination of the parent and child dataset names.
    pub fn full_name(&self) -> String {
        if self.path.is_empty() {
            self.root.full_name().to_string()
        } else {
            format!("{}/{}", self.root.full_name(), self.path)
        }
    }
}

#[derive(Debug)]
pub(crate) enum StorageRequest {
    // Requests to manage which devices the sled considers active.
    // These are manipulated by hardware management.
    DetectedRawDisk {
        raw_disk: RawDisk,
        tx: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
    },
    DetectedRawDiskUpdate {
        raw_disk: RawDisk,
        tx: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
    },
    DetectedRawDiskRemoval {
        raw_disk: RawDisk,
        tx: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
    },
    DetectedRawDisksChanged {
        raw_disks: HashSet<RawDisk>,
        tx: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
    },

    DatasetsEnsure {
        config: DatasetsConfig,
        tx: DebugIgnore<
            oneshot::Sender<Result<DatasetsManagementResult, Error>>,
        >,
    },
    DatasetsConfigList {
        tx: DebugIgnore<oneshot::Sender<Result<DatasetsConfig, Error>>>,
    },
    DatasetsList {
        zpool: ZpoolName,
        tx: DebugIgnore<oneshot::Sender<Result<Vec<DatasetProperties>, Error>>>,
    },

    NestedDatasetEnsure {
        config: NestedDatasetConfig,
        tx: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
    },
    NestedDatasetDestroy {
        name: NestedDatasetLocation,
        tx: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
    },
    NestedDatasetList {
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
        tx: DebugIgnore<
            oneshot::Sender<Result<Vec<NestedDatasetConfig>, Error>>,
        >,
    },

    // Requests to explicitly manage or stop managing a set of devices
    OmicronPhysicalDisksEnsure {
        config: OmicronPhysicalDisksConfig,
        tx: DebugIgnore<oneshot::Sender<Result<DisksManagementResult, Error>>>,
    },

    // Reads the last set of physical disks that were successfully ensured.
    OmicronPhysicalDisksList {
        tx: DebugIgnore<
            oneshot::Sender<Result<OmicronPhysicalDisksConfig, Error>>,
        >,
    },

    // Requests the creation of a new dataset within a managed disk.
    NewFilesystem(NewFilesystemRequest),

    KeyManagerReady,

    /// This will always grab the latest state after any new updates, as it
    /// serializes through the `StorageManager` task after all prior requests.
    /// This serialization is particularly useful for tests.
    GetLatestResources(DebugIgnore<oneshot::Sender<AllDisks>>),
}

#[derive(Debug)]
struct DatasetCreationDetails {
    zoned: bool,
    mountpoint: Mountpoint,
    full_name: String,
}

/// A mechanism for interacting with the [`StorageManager`]
#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<StorageRequest>,
    disk_updates: watch::Receiver<AllDisks>,
    mount_config: MountConfig,
}

impl StorageHandle {
    pub(crate) fn new(
        tx: mpsc::Sender<StorageRequest>,
        disk_updates: watch::Receiver<AllDisks>,
        mount_config: MountConfig,
    ) -> Self {
        Self { tx, disk_updates, mount_config }
    }

    pub fn mount_config(&self) -> &MountConfig {
        &self.mount_config
    }

    /// Adds a disk and associated zpool to the storage manager.
    ///
    /// Returns a future which completes once the notification has been
    /// processed. Awaiting this future is optional.
    pub async fn detected_raw_disk(
        &self,
        raw_disk: RawDisk,
    ) -> impl Future<Output = Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DetectedRawDisk { raw_disk, tx: tx.into() })
            .await
            .unwrap();

        rx.map(|result| result.unwrap())
    }

    /// Removes a disk, if it's tracked by the storage manager, as well
    /// as any associated zpools.
    ///
    /// Returns a future which completes once the notification has been
    /// processed. Awaiting this future is optional.
    pub async fn detected_raw_disk_removal(
        &self,
        raw_disk: RawDisk,
    ) -> impl Future<Output = Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DetectedRawDiskRemoval {
                raw_disk,
                tx: tx.into(),
            })
            .await
            .unwrap();

        rx.map(|result| result.unwrap())
    }

    /// Updates a disk, if it's tracked by the storage manager, as well
    /// as any associated zpools.
    ///
    /// Returns a future which completes once the notification has been
    /// processed. Awaiting this future is optional.
    pub async fn detected_raw_disk_update(
        &self,
        raw_disk: RawDisk,
    ) -> impl Future<Output = Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DetectedRawDiskUpdate {
                raw_disk,
                tx: tx.into(),
            })
            .await
            .unwrap();

        rx.map(|result| result.unwrap())
    }

    /// Ensures that the storage manager tracks exactly the provided disks.
    ///
    /// This acts similar to a batch [Self::detected_raw_disk] for all new disks, and
    /// [Self::detected_raw_disk_removal] for all removed disks.
    ///
    /// If errors occur, an arbitrary "one" of them will be returned, but a
    /// best-effort attempt to add all disks will still be attempted.
    ///
    /// Returns a future which completes once the notification has been
    /// processed. Awaiting this future is optional.
    pub async fn ensure_using_exactly_these_disks<I>(
        &self,
        raw_disks: I,
    ) -> impl Future<Output = Result<(), Error>>
    where
        I: IntoIterator<Item = RawDisk>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DetectedRawDisksChanged {
                raw_disks: raw_disks.into_iter().collect(),
                tx: tx.into(),
            })
            .await
            .unwrap();
        rx.map(|result| result.unwrap())
    }

    pub async fn datasets_ensure(
        &self,
        config: DatasetsConfig,
    ) -> Result<DatasetsManagementResult, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DatasetsEnsure { config, tx: tx.into() })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    /// Reads the last value written to storage by
    /// [Self::datasets_ensure].
    pub async fn datasets_config_list(&self) -> Result<DatasetsConfig, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DatasetsConfigList { tx: tx.into() })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    /// Lists the datasets contained within a zpool.
    ///
    /// Note that this might be distinct from the last configuration
    /// the Sled Agent was told to use.
    ///
    /// Although this operation is basically doing "zfs list", by issuing
    /// it over a [StorageHandle] it is serialized with other storage
    /// management operations.
    pub async fn datasets_list(
        &self,
        zpool: ZpoolName,
    ) -> Result<Vec<DatasetProperties>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::DatasetsList { zpool, tx: tx.into() })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    /// Ensures that a dataset exists, nested somewhere arbitrary within
    /// a Nexus-controlled dataset.
    ///
    /// This function does mount the dataset according to `config`.
    /// However, this dataset is not automatically mounted on reboot.
    ///
    /// If you're trying to access a nested dataset later, consider
    /// using the [NestedDatasetLocation::ensure_mounted_and_get_mountpoint]
    /// function.
    pub async fn nested_dataset_ensure(
        &self,
        config: NestedDatasetConfig,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::NestedDatasetEnsure { config, tx: tx.into() })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    pub async fn nested_dataset_destroy(
        &self,
        name: NestedDatasetLocation,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::NestedDatasetDestroy { name, tx: tx.into() })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    pub async fn nested_dataset_list(
        &self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::NestedDatasetList {
                name,
                options,
                tx: tx.into(),
            })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    pub async fn omicron_physical_disks_ensure(
        &self,
        config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::OmicronPhysicalDisksEnsure {
                config,
                tx: tx.into(),
            })
            .await
            .unwrap();

        rx.await.unwrap()
    }

    /// Reads the last value written to storage by
    /// [Self::omicron_physical_disks_ensure].
    ///
    /// This should be contrasted with both inventory and the result
    /// of [Self::get_latest_disks] -- since this function focuses on
    /// "Control Plane disks", it may return information about disks
    /// that are no longer detected within the hardware of this sled.
    pub async fn omicron_physical_disks_list(
        &self,
    ) -> Result<OmicronPhysicalDisksConfig, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::OmicronPhysicalDisksList { tx: tx.into() })
            .await
            .unwrap();

        rx.await.unwrap()
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
        // We create a distinct receiver to avoid colliding with
        // the receiver used by [Self::wait_for_changes].
        let mut receiver = self.disk_updates.clone();
        loop {
            let resources = receiver.borrow_and_update();
            if let Some((disk_id, zpool_name)) = resources.boot_disk() {
                return (disk_id, zpool_name);
            }
            drop(resources);
            // We panic if the sender is dropped, as this means
            // the StorageManager has gone away, which it should not do.
            receiver.changed().await.unwrap();
        }
    }

    /// Wait for any storage resource changes
    pub async fn wait_for_changes(&mut self) -> AllDisks {
        self.disk_updates.changed().await.unwrap();
        self.disk_updates.borrow_and_update().clone()
    }

    /// Retrieve the latest value of `AllDisks` from the
    /// `StorageManager` task.
    pub async fn get_latest_disks(&self) -> AllDisks {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StorageRequest::GetLatestResources(tx.into()))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    // TODO(https://github.com/oxidecomputer/omicron/issues/6043):
    //
    // Deprecate usage of this function, prefer to call "datasets_ensure"
    // and ask for the set of all datasets from Nexus.
    pub async fn upsert_filesystem(
        &self,
        dataset_id: Option<DatasetUuid>,
        dataset_name: DatasetName,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let request = NewFilesystemRequest {
            dataset_id,
            dataset_name,
            responder: tx.into(),
        };
        self.tx.send(StorageRequest::NewFilesystem(request)).await.unwrap();
        rx.await.unwrap()
    }
}
/// The storage manager responsible for the state of the storage
/// on a sled. The storage manager runs in its own task and is interacted
/// with via the [`StorageHandle`].
pub struct StorageManager {
    log: Logger,
    state: StorageManagerState,
    rx: mpsc::Receiver<StorageRequest>,
    resources: StorageResources,
}

impl StorageManager {
    pub fn new(
        log: &Logger,
        mount_config: MountConfig,
        key_requester: StorageKeyRequester,
    ) -> (StorageManager, StorageHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let resources =
            StorageResources::new(log, mount_config.clone(), key_requester);
        let disk_updates = resources.watch_disks();
        (
            StorageManager {
                log: log.new(o!("component" => "StorageManager")),
                state: StorageManagerState::WaitingForKeyManager,
                rx,
                resources,
            },
            StorageHandle::new(tx, disk_updates, mount_config),
        )
    }

    /// Run the main receive loop of the `StorageManager`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        let mut interval = interval(SYNCHRONIZE_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::pin!(interval);

        loop {
            tokio::select! {
                Some(req) = self.rx.recv() => {
                    // It's critical that we don't "step" directly in the select
                    // branch, as that could cancel an ongoing request if it
                    // fires while a request is being processed.
                    //
                    // Instead, if we receive any request, we stop
                    // "select!"-ing and fully process the request before
                    // continuing.
                    if let Err(e) = self.step(req).await {
                        warn!(self.log, "{e}");
                    }
                }
                _ = interval.tick(),
                    if self.state == StorageManagerState::SynchronizationNeeded =>
                {
                    info!(self.log, "automatically managing disks");
                    self.manage_disks().await;
                }
            }
        }
    }

    /// Process the next event
    ///
    /// This is useful for testing/debugging
    async fn step(&mut self, req: StorageRequest) -> Result<(), Error> {
        info!(self.log, "Received {:?}", req);

        match req {
            StorageRequest::DetectedRawDisk { raw_disk, tx } => {
                let result = self.detected_raw_disk(raw_disk).await;
                if let Err(ref err) = &result {
                    warn!(self.log, "Failed to add raw disk"; "err" => ?err);
                }
                let _ = tx.0.send(result);
            }
            StorageRequest::DetectedRawDiskUpdate { raw_disk, tx } => {
                let result = self.detected_raw_disk_update(raw_disk).await;
                if let Err(ref err) = &result {
                    warn!(self.log, "Failed to apply raw disk update"; "err" => ?err);
                }
                let _ = tx.0.send(result);
            }
            StorageRequest::DetectedRawDiskRemoval { raw_disk, tx } => {
                self.detected_raw_disk_removal(raw_disk);
                let _ = tx.0.send(Ok(()));
            }
            StorageRequest::DetectedRawDisksChanged { raw_disks, tx } => {
                self.ensure_using_exactly_these_disks(raw_disks).await;
                let _ = tx.0.send(Ok(()));
            }
            StorageRequest::DatasetsEnsure { config, tx } => {
                let _ = tx.0.send(self.datasets_ensure(config).await);
            }
            StorageRequest::DatasetsConfigList { tx } => {
                let _ = tx.0.send(self.datasets_config_list().await);
            }
            StorageRequest::DatasetsList { zpool, tx } => {
                let _ = tx.0.send(self.datasets_list(&zpool).await);
            }
            StorageRequest::NestedDatasetEnsure { config, tx } => {
                let _ = tx.0.send(self.nested_dataset_ensure(config).await);
            }
            StorageRequest::NestedDatasetDestroy { name, tx } => {
                let _ = tx.0.send(self.nested_dataset_destroy(name).await);
            }
            StorageRequest::NestedDatasetList { name, options, tx } => {
                let _ =
                    tx.0.send(self.nested_dataset_list(name, options).await);
            }
            StorageRequest::OmicronPhysicalDisksEnsure { config, tx } => {
                let _ =
                    tx.0.send(self.omicron_physical_disks_ensure(config).await);
            }
            StorageRequest::OmicronPhysicalDisksList { tx } => {
                let _ = tx.0.send(self.omicron_physical_disks_list().await);
            }
            StorageRequest::NewFilesystem(request) => {
                let result = self.add_dataset(&request).await;
                if let Err(ref err) = &result {
                    warn!(self.log, "Failed to add dataset"; "err" => ?err);
                }
                let _ = request.responder.0.send(result);
            }
            StorageRequest::KeyManagerReady => {
                self.key_manager_ready().await?;
            }
            StorageRequest::GetLatestResources(tx) => {
                let _ = tx.0.send(self.resources.disks().clone());
            }
        };

        Ok(())
    }

    async fn manage_disks(&mut self) {
        let result = self.resources.synchronize_disk_management().await;

        if result.has_retryable_error() {
            // This is logged as "info", not "warn", as it can happen before
            // trust quorum has been established.
            info!(
                self.log,
                "Failed to synchronize disks, but will retry";
                "result" => ?result,
            );
            return;
        }

        self.state = StorageManagerState::Synchronized;

        if result.has_error() {
            warn!(
                self.log,
                "Failed to synchronize disks due to permanant error";
                "result" => #?result,
            );
            return;
        }

        info!(
            self.log,
            "Successfully synchronized disks without error";
            "result" => ?result,
        );
    }

    // Sled Agents can remember which disks they need to manage by reading
    // a configuration file from the M.2s.
    //
    // This function returns the paths to those configuration files.
    async fn all_omicron_disk_ledgers(&self) -> Vec<Utf8PathBuf> {
        self.resources
            .disks()
            .all_m2_mountpoints(CONFIG_DATASET)
            .into_iter()
            .map(|p| p.join(DISKS_LEDGER_FILENAME))
            .collect()
    }

    // Sled Agents can remember which datasets they need to manage by reading
    // a configuration file from the M.2s.
    //
    // This function returns the paths to those configuration files.
    async fn all_omicron_dataset_ledgers(&self) -> Vec<Utf8PathBuf> {
        self.resources
            .disks()
            .all_m2_mountpoints(CONFIG_DATASET)
            .into_iter()
            .map(|p| p.join(DATASETS_LEDGER_FILENAME))
            .collect()
    }

    // Manages a newly detected disk that has been attached to this sled.
    //
    // For U.2s: we update our inventory.
    // For M.2s: we do the same, but also begin "managing" the disk so
    // it can automatically be in-use.
    async fn detected_raw_disk(
        &mut self,
        raw_disk: RawDisk,
    ) -> Result<(), Error> {
        // In other words, the decision of "should we use this U.2" requires
        // coordination with the control plane at large.
        let needs_synchronization =
            matches!(raw_disk.variant(), DiskVariant::U2);
        self.resources.insert_or_update_disk(raw_disk).await?;

        if needs_synchronization {
            match self.state {
                // We'll synchronize once the key manager comes up.
                StorageManagerState::WaitingForKeyManager => (),
                // In these cases, we'd benefit from another call
                // to "manage_disks" from StorageManager task runner.
                StorageManagerState::SynchronizationNeeded
                | StorageManagerState::Synchronized => {
                    self.state = StorageManagerState::SynchronizationNeeded;

                    // TODO(https://github.com/oxidecomputer/omicron/issues/5328):
                    // We can remove this call once we've migrated everyone to a
                    // world that uses the ledger -- normally we'd only need to
                    // load the storage config once, when we know that the key
                    // manager is ready, but without a ledger, we may need to
                    // retry auto-management when any new U.2 appears.
                    self.load_storage_config().await?;
                }
            }
        }

        Ok(())
    }

    /// Updates some information about the underlying disk within this sled.
    ///
    /// Things that can currently be updated:
    /// - DiskFirmware
    async fn detected_raw_disk_update(
        &mut self,
        raw_disk: RawDisk,
    ) -> Result<(), Error> {
        // We aren't worried about synchronizing as the disk should already be managed.
        self.resources.insert_or_update_disk(raw_disk).await
    }

    async fn load_disks_ledger(
        &self,
    ) -> Option<Ledger<OmicronPhysicalDisksConfig>> {
        let ledger_paths = self.all_omicron_disk_ledgers().await;
        let log = self.log.new(o!("request" => "load_disks_ledger"));
        let maybe_ledger = Ledger::<OmicronPhysicalDisksConfig>::new(
            &log,
            ledger_paths.clone(),
        )
        .await;

        match maybe_ledger {
            Some(ledger) => {
                info!(self.log, "Ledger of physical disks exists");
                return Some(ledger);
            }
            None => {
                info!(self.log, "No ledger of physical disks exists");
                return None;
            }
        }
    }

    async fn key_manager_ready(&mut self) -> Result<(), Error> {
        self.load_storage_config().await
    }

    async fn load_storage_config(&mut self) -> Result<(), Error> {
        info!(self.log, "Loading storage config");
        // Set the state to "synchronization needed", to force us to try to
        // asynchronously ensure that disks are ready.
        self.state = StorageManagerState::SynchronizationNeeded;

        // Now that we're actually able to unpack U.2s, attempt to load the
        // set of disks which we previously stored in the ledger, if one
        // existed.
        let ledger = self.load_disks_ledger().await;
        if let Some(ledger) = ledger {
            info!(self.log, "Setting StorageResources state to match ledger");

            // Identify which disks should be managed by the control
            // plane, and adopt all requested disks into the control plane
            // in a background task (see: [Self::manage_disks]).
            self.resources.set_config(&ledger.data());
        } else {
            info!(self.log, "KeyManager ready, but no ledger detected");
        }

        // We don't load any configuration for datasets, since we aren't
        // currently storing any dataset information in-memory.
        //
        // If we ever wanted to do so, however, we could load that information
        // here.

        Ok(())
    }

    async fn datasets_ensure(
        &mut self,
        config: DatasetsConfig,
    ) -> Result<DatasetsManagementResult, Error> {
        let log = self.log.new(o!("request" => "datasets_ensure"));

        // As a small input-check, confirm that the UUID of the map of inputs
        // matches the DatasetConfig.
        //
        // The dataset configs are sorted by UUID so they always appear in the
        // same order, but this check prevents adding an entry of:
        // - (UUID: X, Config(UUID: Y)), for X != Y
        if !config.datasets.iter().all(|(id, config)| *id == config.id) {
            return Err(Error::ConfigUuidMismatch);
        }

        // We rely on the schema being stable across reboots -- observe
        // "test_datasets_schema" below for that property guarantee.
        let ledger_paths = self.all_omicron_dataset_ledgers().await;
        let maybe_ledger =
            Ledger::<DatasetsConfig>::new(&log, ledger_paths.clone()).await;

        let mut ledger = match maybe_ledger {
            Some(ledger) => {
                info!(
                    log,
                    "Comparing 'requested datasets' to ledger on internal storage"
                );
                let ledger_data = ledger.data();
                if config.generation < ledger_data.generation {
                    warn!(
                        log,
                        "Request looks out-of-date compared to prior request";
                        "requested_generation" => ?config.generation,
                        "ledger_generation" => ?ledger_data.generation,
                    );
                    return Err(Error::DatasetConfigurationOutdated {
                        requested: config.generation,
                        current: ledger_data.generation,
                    });
                } else if config.generation == ledger_data.generation {
                    info!(
                        log,
                        "Requested generation number matches prior request",
                    );

                    if ledger_data != &config {
                        error!(
                            log,
                            "Requested configuration changed (with the same generation)";
                            "generation" => ?config.generation
                        );
                        return Err(Error::DatasetConfigurationChanged {
                            generation: config.generation,
                        });
                    }
                } else {
                    info!(
                        log,
                        "Request looks newer than prior requests";
                        "requested_generation" => ?config.generation,
                        "ledger_generation" => ?ledger_data.generation,
                    );
                }
                ledger
            }
            None => {
                info!(
                    log,
                    "No previously-stored 'requested datasets', creating new ledger"
                );
                Ledger::<DatasetsConfig>::new_with(
                    &log,
                    ledger_paths.clone(),
                    DatasetsConfig::default(),
                )
            }
        };

        let result = self.datasets_ensure_internal(&log, &config).await;

        let ledger_data = ledger.data_mut();
        if *ledger_data == config {
            return Ok(result);
        }
        *ledger_data = config;
        ledger.commit().await?;

        Ok(result)
    }

    // Attempts to ensure that each dataset exist.
    //
    // Does not return an error, because the [DatasetsManagementResult] type
    // includes details about all possible errors that may occur on
    // a per-dataset granularity.
    async fn datasets_ensure_internal(
        &self,
        log: &Logger,
        config: &DatasetsConfig,
    ) -> DatasetsManagementResult {
        // Gather properties about these datasets, if they exist.
        //
        // This pre-fetching lets us avoid individually querying them later.
        let old_datasets = Zfs::get_dataset_properties(
            config
                .datasets
                .values()
                .map(|config| config.name.full_name())
                .collect::<Vec<String>>()
                .as_slice(),
            WhichDatasets::SelfOnly,
        )
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|props| (props.name.clone(), props))
        .collect::<BTreeMap<String, _>>();

        let futures = config.datasets.values().map(|dataset| async {
            self.dataset_ensure_internal(
                log,
                dataset,
                old_datasets.get(&dataset.name.full_name()),
            )
            .await
        });

        // This "Box::pin" is a workaround for: https://github.com/rust-lang/rust/issues/64552
        //
        // Ideally, we would just use:
        //
        // ```
        // let status: Vec<_> = futures::stream::iter(futures)
        //      .buffered(...)
        //      .collect()
        //      .await;
        // ```
        const DATASET_ENSURE_CONCURRENCY_LIMIT: usize = 16;
        let results: std::pin::Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(
            futures::stream::iter(futures)
                .buffered(DATASET_ENSURE_CONCURRENCY_LIMIT),
        );

        let status: Vec<DatasetManagementStatus> = results.collect().await;

        DatasetsManagementResult { status }
    }

    fn should_skip_dataset_ensure(
        log: &Logger,
        config: &DatasetConfig,
        old_dataset: Option<&DatasetProperties>,
    ) -> Result<bool, Error> {
        let Some(old_dataset) = old_dataset else {
            info!(log, "This dataset did not exist");
            return Ok(false);
        };

        let Some(old_id) = old_dataset.id else {
            info!(log, "Old properties missing UUID");
            return Ok(false);
        };

        if old_id != config.id {
            return Err(Error::UuidMismatch {
                name: config.name.full_name(),
                old: old_id.into_untyped_uuid(),
                new: config.id.into_untyped_uuid(),
            });
        }

        let old_props = match SharedDatasetConfig::try_from(old_dataset) {
            Ok(old_props) => old_props,
            Err(err) => {
                warn!(log, "Failed to parse old properties"; "err" => #%err);
                return Ok(false);
            }
        };

        info!(log, "Parsed old dataset properties"; "props" => ?old_props);
        if old_props != config.inner {
            info!(
                log,
                "Dataset properties changed";
                "old_props" => ?old_props,
                "requested_props" => ?config.inner,
            );
            return Ok(false);
        }

        if !config.name.kind().zoned() && !old_dataset.mounted {
            info!(
                log,
                "Dataset might need to be mounted";
                "old_dataset" => ?old_dataset,
                "requested_props" => ?config.inner,
            );
            return Ok(false);
        }

        info!(log, "No changes necessary, returning early");
        return Ok(true);
    }

    async fn dataset_ensure_internal(
        &self,
        log: &Logger,
        config: &DatasetConfig,
        old_dataset: Option<&DatasetProperties>,
    ) -> DatasetManagementStatus {
        let log = log.new(o!("name" => config.name.full_name()));
        info!(log, "Ensuring dataset");
        let mut status = DatasetManagementStatus {
            dataset_name: config.name.clone(),
            err: None,
        };

        match Self::should_skip_dataset_ensure(&log, config, old_dataset) {
            Ok(true) => return status,
            Ok(false) => (),
            Err(err) => {
                status.err = Some(err.to_string());
                return status;
            }
        }

        let disks = self.resources.disks();
        let mountpoint_root = &disks.mount_config().root;
        let mountpoint_path = config.name.mountpoint(mountpoint_root);
        let details = DatasetCreationDetails {
            zoned: config.name.kind().zoned(),
            mountpoint: Mountpoint(mountpoint_path),
            full_name: config.name.full_name(),
        };

        if let Err(err) = self
            .ensure_dataset(
                config.name.pool(),
                Some(config.id),
                &config.inner,
                &details,
            )
            .await
        {
            warn!(log, "Failed to ensure dataset"; "dataset" => ?status.dataset_name, "err" => ?err);
            status.err = Some(InlineErrorChain::new(&err).to_string());
        };

        status
    }

    // Lists datasets that this sled is configured to use.
    async fn datasets_config_list(&mut self) -> Result<DatasetsConfig, Error> {
        let log = self.log.new(o!("request" => "datasets_config_list"));

        let ledger_paths = self.all_omicron_dataset_ledgers().await;
        let maybe_ledger =
            Ledger::<DatasetsConfig>::new(&log, ledger_paths.clone()).await;

        match maybe_ledger {
            Some(ledger) => {
                info!(log, "Found ledger on internal storage");
                return Ok(ledger.data().clone());
            }
            None => {
                info!(log, "No ledger detected on internal storage");
                return Err(Error::LedgerNotFound);
            }
        }
    }

    // Lists datasets that this zpool contains.
    //
    // See also: [StorageHandle::datasets_list]
    async fn datasets_list(
        &self,
        zpool: &ZpoolName,
    ) -> Result<Vec<DatasetProperties>, Error> {
        let log = self.log.new(o!("request" => "datasets_list"));

        let datasets_of_interest = [
            // We care about the zpool itself, and all direct children.
            zpool.to_string(),
            // Likewise, we care about the encrypted dataset, and all
            // direct children.
            format!("{zpool}/{CRYPT_DATASET}"),
            // The zone dataset gives us additional context on "what zones
            // have datasets provisioned".
            format!("{zpool}/{ZONE_DATASET}"),
        ];

        info!(log, "Listing datasets within zpool"; "zpool" => zpool.to_string());
        Zfs::get_dataset_properties(
            datasets_of_interest.as_slice(),
            WhichDatasets::SelfAndChildren,
        )
        .await
        .map_err(Error::Other)
    }

    // Ensures that a dataset exists, nested somewhere arbitrary within
    // a Nexus-controlled dataset.
    async fn nested_dataset_ensure(
        &mut self,
        config: NestedDatasetConfig,
    ) -> Result<(), Error> {
        let log = self.log.new(o!("request" => "nested_dataset_ensure"));

        let disks = self.resources.disks();
        let root = &disks.mount_config().root;
        let mountpoint_path = config.name.mountpoint(root);

        info!(
            log,
            "Ensuring nested dataset";
            "mountpoint" => ?mountpoint_path.as_path()
        );

        let details = DatasetCreationDetails {
            zoned: false,
            mountpoint: Mountpoint(mountpoint_path),
            full_name: config.name.full_name(),
        };

        self.ensure_dataset(
            config.name.root.pool(),
            None,
            &config.inner,
            &details,
        )
        .await?;

        Ok(())
    }

    async fn nested_dataset_destroy(
        &mut self,
        name: NestedDatasetLocation,
    ) -> Result<(), Error> {
        let log = self.log.new(o!("request" => "nested_dataset_destroy"));
        let full_name = name.full_name();
        info!(log, "Destroying nested dataset"; "name" => full_name.clone());

        if name.path.is_empty() {
            let msg = "Cannot destroy nested dataset with empty name";
            warn!(log, "{msg}");
            return Err(anyhow!(msg).into());
        }

        Zfs::destroy_dataset(&full_name).await.map_err(|e| anyhow!(e))?;
        Ok(())
    }

    // Lists the properties of 'name' and all children within
    async fn nested_dataset_list(
        &mut self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, Error> {
        let log = self.log.new(o!("request" => "nested_dataset_list"));
        info!(log, "Listing nested datasets");

        // Observe all propreties for this nested datasets, including
        // children. We'll apply user-specified filters later.
        let full_name = name.full_name();
        let properties = Zfs::get_dataset_properties(
            &[full_name],
            WhichDatasets::SelfAndChildren,
        )
        .await
        .map_err(|e| {
            warn!(
                log,
                "Failed to access nested dataset";
                "name" => ?name
            );
            crate::dataset::DatasetError::Other(e)
        })?;

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

    // Makes an U.2 disk managed by the control plane within [`StorageResources`].
    async fn omicron_physical_disks_ensure(
        &mut self,
        mut config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, Error> {
        let log =
            self.log.new(o!("request" => "omicron_physical_disks_ensure"));

        // Ensure that the set of disks arrives in a consistent order.
        config
            .disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());

        // We rely on the schema being stable across reboots -- observe
        // "test_omicron_physical_disks_schema" below for that property
        // guarantee.
        let ledger_paths = self.all_omicron_disk_ledgers().await;
        let maybe_ledger = Ledger::<OmicronPhysicalDisksConfig>::new(
            &log,
            ledger_paths.clone(),
        )
        .await;

        let mut ledger = match maybe_ledger {
            Some(ledger) => {
                info!(
                    log,
                    "Comparing 'requested disks' to ledger on internal storage"
                );
                let ledger_data = ledger.data();
                if config.generation < ledger_data.generation {
                    warn!(
                        log,
                        "Request looks out-of-date compared to prior request"
                    );
                    return Err(Error::PhysicalDiskConfigurationOutdated {
                        requested: config.generation,
                        current: ledger_data.generation,
                    });
                } else if config.generation == ledger_data.generation {
                    info!(
                        log,
                        "Requested generation number matches prior request",
                    );

                    if ledger_data != &config {
                        error!(
                            log,
                            "Requested configuration changed (with the same generation)";
                            "generation" => ?config.generation
                        );
                        return Err(Error::PhysicalDiskConfigurationChanged {
                            generation: config.generation,
                        });
                    }
                    info!(
                        log,
                        "Request looks identical to last request, re-sending"
                    );
                } else {
                    info!(log, "Request looks newer than prior requests");
                }
                ledger
            }
            None => {
                info!(
                    log,
                    "No previously-stored 'requested disks', creating new ledger"
                );
                Ledger::<OmicronPhysicalDisksConfig>::new_with(
                    &log,
                    ledger_paths.clone(),
                    OmicronPhysicalDisksConfig::new(),
                )
            }
        };

        let result =
            self.omicron_physical_disks_ensure_internal(&log, &config).await?;

        let ledger_data = ledger.data_mut();
        if *ledger_data == config {
            return Ok(result);
        }
        *ledger_data = config;
        ledger.commit().await?;

        Ok(result)
    }

    // Updates [StorageResources] to manage the disks requested by `config`, if
    // those disks exist.
    //
    // Makes no attempts to manipulate the ledger storage.
    async fn omicron_physical_disks_ensure_internal(
        &mut self,
        log: &Logger,
        config: &OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, Error> {
        if self.state == StorageManagerState::WaitingForKeyManager {
            warn!(
                log,
                "Not ready to manage storage yet (waiting for the key manager)"
            );
            return Err(Error::KeyManagerNotReady);
        }

        // Identify which disks should be managed by the control
        // plane, and adopt all requested disks into the control plane.
        self.resources.set_config(&config);

        // Actually try to "manage" those disks, which may involve formatting
        // zpools and conforming partitions to those expected by the control
        // plane.
        Ok(self.resources.synchronize_disk_management().await)
    }

    async fn omicron_physical_disks_list(
        &mut self,
    ) -> Result<OmicronPhysicalDisksConfig, Error> {
        let log = self.log.new(o!("request" => "omicron_physical_disks_list"));

        // TODO(https://github.com/oxidecomputer/omicron/issues/5328): This
        // could just use "resources.get_config", but that'll be more feasible
        // once we don't have to cons up a fake "Generation" number.

        let ledger_paths = self.all_omicron_disk_ledgers().await;
        let maybe_ledger = Ledger::<OmicronPhysicalDisksConfig>::new(
            &log,
            ledger_paths.clone(),
        )
        .await;

        match maybe_ledger {
            Some(ledger) => {
                info!(log, "Found ledger on internal storage");
                return Ok(ledger.data().clone());
            }
            None => {
                info!(log, "No ledger detected on internal storage");
                return Err(Error::LedgerNotFound);
            }
        }
    }

    // Delete a real disk and return `true` if the disk was actually removed
    fn detected_raw_disk_removal(&mut self, raw_disk: RawDisk) {
        self.resources.remove_disk(raw_disk.identity());
    }

    // Find all disks to remove that are not in raw_disks and remove them. Then
    // take the remaining disks and try to add them all. `StorageResources` will
    // inform us if anything changed, and if so we return true, otherwise we
    // return false.
    async fn ensure_using_exactly_these_disks(
        &mut self,
        raw_disks: HashSet<RawDisk>,
    ) {
        let all_ids: HashSet<_> =
            raw_disks.iter().map(|d| d.identity()).collect();

        // Find all existing disks not in the current set
        let to_remove: Vec<DiskIdentity> = self
            .resources
            .disks()
            .iter_all()
            .filter_map(|(id, _variant, _slot, _firmware)| {
                if !all_ids.contains(id) { Some(id.clone()) } else { None }
            })
            .collect();

        for id in to_remove {
            self.resources.remove_disk(&id);
        }

        for raw_disk in raw_disks {
            let disk_id = raw_disk.identity().clone();
            if let Err(err) = self.detected_raw_disk(raw_disk).await {
                warn!(
                    self.log,
                    "Failed to add disk to storage resources: {err}";
                    "disk_id" => ?disk_id
                );
            }
        }
    }

    // Ensures a dataset exists within a zpool.
    //
    // Confirms that the zpool exists and is managed by this sled.
    async fn ensure_dataset(
        &self,
        zpool: &ZpoolName,
        dataset_id: Option<DatasetUuid>,
        config: &SharedDatasetConfig,
        details: &DatasetCreationDetails,
    ) -> Result<(), Error> {
        info!(
            self.log,
            "ensure_dataset";
            "config" => ?config,
            "details" => ?details,
        );

        // We can only place datasets within managed disks.
        // If a disk is attached to this sled, but not a part of the Control
        // Plane, it is treated as "not found" for dataset placement.
        if !self
            .resources
            .disks()
            .iter_managed()
            .any(|(_, disk)| disk.zpool_name() == zpool)
        {
            warn!(self.log, "Failed to find zpool");
            return Err(Error::ZpoolNotFound(format!("{}", zpool)));
        }

        let DatasetCreationDetails { zoned, mountpoint, full_name } = details;
        // The "crypt" dataset needs these details, but should already exist
        // by the time we're creating datasets inside.
        let encryption_details = None;
        let size_details = Some(illumos_utils::zfs::SizeDetails {
            quota: config.quota,
            reservation: config.reservation,
            compression: config.compression,
        });
        Zfs::ensure_dataset(DatasetEnsureArgs {
            name: &full_name,
            mountpoint: mountpoint.clone(),
            can_mount: CanMount::On,
            zoned: *zoned,
            encryption_details,
            size_details,
            id: dataset_id,
            additional_options: None,
        })
        .await?;

        Ok(())
    }

    // Attempts to add a dataset within a zpool, according to `request`.
    async fn add_dataset(
        &mut self,
        request: &NewFilesystemRequest,
    ) -> Result<(), Error> {
        info!(self.log, "add_dataset"; "request" => ?request);
        if !self
            .resources
            .disks()
            .iter_managed()
            .any(|(_, disk)| disk.zpool_name() == request.dataset_name.pool())
        {
            return Err(Error::ZpoolNotFound(format!(
                "{}",
                request.dataset_name.pool(),
            )));
        }

        let zoned = true;
        let fs_name = &request.dataset_name.full_name();
        let encryption_details = None;
        let size_details = None;
        Zfs::ensure_dataset(DatasetEnsureArgs {
            name: fs_name,
            mountpoint: Mountpoint(Utf8PathBuf::from("/data")),
            can_mount: CanMount::On,
            zoned,
            encryption_details,
            size_details,
            id: request.dataset_id,
            additional_options: None,
        })
        .await?;

        Ok(())
    }
}

/// All tests only use synthetic disks, but are expected to be run on illumos
/// systems.
#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use crate::disk::RawSyntheticDisk;
    use crate::manager_test_harness::StorageManagerTestHarness;

    use super::*;
    use camino_tempfile::tempdir_in;
    use omicron_common::api::external::Generation;
    use omicron_common::disk::CompressionAlgorithm;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DiskManagementError;
    use omicron_common::ledger;
    use omicron_test_utils::dev::test_setup_log;
    use sled_hardware::DiskFirmware;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::atomic::Ordering;

    // A helper struct to advance time.
    struct TimeTravel {}

    impl TimeTravel {
        pub fn new() -> Self {
            tokio::time::pause();
            Self {}
        }

        pub async fn enough_to_start_synchronization(&self) {
            tokio::time::advance(SYNCHRONIZE_INTERVAL).await;
        }
    }

    #[tokio::test]
    async fn add_control_plane_disks_requires_keymanager() {
        let logctx =
            test_setup_log("add_control_plane_disks_requires_keymanager");

        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;

        // These disks should exist, but only the M.2 should have a zpool.
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert_eq!(0, all_disks.all_u2_zpools().len());
        assert_eq!(1, all_disks.all_m2_zpools().len());

        // If we try to "act like nexus" and request a control-plane disk, we'll
        // see a failure because the key manager isn't ready.
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await;
        assert!(matches!(result, Err(Error::KeyManagerNotReady)));

        // If we make the key manager ready and try again, it'll work.
        harness.handle().key_manager_ready().await;
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // If we look at the disks again, we'll now see one U.2 zpool.
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert_eq!(1, all_disks.all_u2_zpools().len());
        assert_eq!(1, all_disks.all_m2_zpools().len());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ledger_writes_require_at_least_one_m2() {
        let logctx = test_setup_log("ledger_writes_require_at_least_one_m2");

        // Create a single U.2 under test, with a ready-to-go key manager.
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        let raw_disks = harness.add_vdevs(&["u2_under_test.vdev"]).await;
        harness.handle().key_manager_ready().await;
        let config = harness.make_config(1, &raw_disks);

        // Attempting to adopt this U.2 fails (we don't have anywhere to put the
        // ledger).
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await;
        assert!(
            matches!(
                result,
                Err(Error::Ledger(ledger::Error::FailedToWrite { .. }))
            ),
            "Saw unexpected result: {:?}",
            result
        );

        // Add an M.2 which can store the ledger.
        let _raw_disks =
            harness.add_vdevs(&["m2_finally_showed_up.vdev"]).await;
        harness.handle_mut().wait_for_boot_disk().await;

        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("After adding an M.2, the ledger write should have worked");
        assert!(!result.has_error(), "{:?}", result);

        // Wait for the add disk notification
        let tt = TimeTravel::new();
        tt.enough_to_start_synchronization().await;
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(all_disks.all_u2_zpools().len(), 1);
        assert_eq!(all_disks.all_m2_zpools().len(), 1);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn add_raw_u2_does_not_create_zpool() {
        let logctx = test_setup_log("add_raw_u2_does_not_create_zpool");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        harness.handle().key_manager_ready().await;

        // Add a representative scenario for a small sled: a U.2 and M.2.
        let _raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;

        // This disks should exist, but only the M.2 should have a zpool.
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert_eq!(0, all_disks.all_u2_zpools().len());
        assert_eq!(1, all_disks.all_m2_zpools().len());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn update_rawdisk_firmware() {
        const FW_REV: &str = "firmware-2.0";
        let logctx = test_setup_log("update_u2_firmware");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        harness.handle().key_manager_ready().await;

        // Add a representative scenario for a small sled: a U.2 and M.2.
        let mut raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;

        // This disks should exist, but only the M.2 should have a zpool.
        let all_disks_gen1 = harness.handle().get_latest_disks().await;

        for rd in &mut raw_disks {
            if let RawDisk::Synthetic(ref mut disk) = rd {
                let mut slots = disk.firmware.slots().to_vec();
                // "Install" a new firmware version into slot2
                slots.push(Some(FW_REV.to_string()));
                disk.firmware = DiskFirmware::new(
                    disk.firmware.active_slot(),
                    disk.firmware.next_active_slot(),
                    disk.firmware.slot1_read_only(),
                    disk.firmware.number_of_slots(),
                    slots,
                );
            }
            harness.update_vdev(rd).await;
        }

        let all_disks_gen2 = harness.handle().get_latest_disks().await;

        // Disks should now be different due to the mock firmware update.
        assert_ne!(all_disks_gen1, all_disks_gen2);

        // Now let's verify we saw the correct firmware update.
        for rd in &raw_disks {
            let firmware = all_disks_gen2
                .iter_all()
                .find_map(|(identity, _, _, fw)| {
                    if identity == rd.identity() { Some(fw) } else { None }
                })
                .expect("disk exists");
            assert_eq!(firmware, rd.firmware(), "didn't see firmware update");
        }

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn wait_for_boot_disk() {
        let logctx = test_setup_log("wait_for_boot_disk");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        let _raw_disks = harness.add_vdevs(&["u2_under_test.vdev"]).await;

        // When we wait for changes, we can see the U.2 being added, but no boot
        // disk.
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(1, all_disks.iter_all().collect::<Vec<_>>().len());
        assert!(all_disks.boot_disk().is_none());

        // Waiting for the boot disk should time out.
        assert!(
            tokio::time::timeout(
                tokio::time::Duration::from_millis(10),
                harness.handle_mut().wait_for_boot_disk(),
            )
            .await
            .is_err()
        );

        // Now we add a boot disk.
        let boot_disk = harness.add_vdevs(&["m2_under_test.vdev"]).await;

        // It shows up through the general "wait for changes" API.
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert!(all_disks.boot_disk().is_some());

        // We can wait for, and see, the boot disk.
        let (id, _) = harness.handle_mut().wait_for_boot_disk().await;
        assert_eq!(&id, boot_disk[0].identity());

        // We can keep calling this function without blocking.
        let (id, _) = harness.handle_mut().wait_for_boot_disk().await;
        assert_eq!(&id, boot_disk[0].identity());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn disks_automatically_managed_after_key_manager_ready() {
        let logctx = test_setup_log(
            "disks_automatically_managed_after_key_manager_ready",
        );
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Boot normally, add an M.2 and a U.2, and let them
        // create pools.
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        harness.handle().key_manager_ready().await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .unwrap();
        assert!(!result.has_error(), "{:?}", result);

        // Both pools exist
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert_eq!(1, all_disks.all_u2_zpools().len());
        assert_eq!(1, all_disks.all_m2_zpools().len());

        // "reboot" the storage manager, and let it see the disks before
        // the key manager is ready.
        let mut harness = harness.reboot(&logctx.log).await;

        // Both disks exist, but the U.2's pool is not yet accessible.
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert_eq!(0, all_disks.all_u2_zpools().len());
        assert_eq!(1, all_disks.all_m2_zpools().len());

        // Mark the key manaager ready. This should eventually lead to the
        // U.2 being managed, since it exists in the M.2 ledger.
        harness.handle().key_manager_ready().await;
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(1, all_disks.all_u2_zpools().len());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn queued_disks_get_requeued_on_secret_retriever_error() {
        let logctx = test_setup_log(
            "queued_disks_get_requeued_on_secret_retriever_error",
        );
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Queue up a disk, as we haven't told the `StorageManager` that
        // the `KeyManager` is ready yet.
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await;
        assert!(matches!(result, Err(Error::KeyManagerNotReady)));

        // As usual, the U.2 isn't ready yet.
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(2, all_disks.iter_all().collect::<Vec<_>>().len());
        assert_eq!(0, all_disks.all_u2_zpools().len());

        // Mark the key manager ready, but throwing errors.
        harness.key_manager_error_injector().store(true, Ordering::SeqCst);
        harness.handle().key_manager_ready().await;

        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .unwrap();
        assert!(result.has_error());
        assert!(matches!(
            result.status[0].err.as_ref(),
            Some(DiskManagementError::KeyManager(_))
        ));
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(0, all_disks.all_u2_zpools().len());

        // After toggling KeyManager errors off, the U.2 can be successfully added.
        harness.key_manager_error_injector().store(false, Ordering::SeqCst);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring control plane disks should have worked");
        assert!(!result.has_error(), "{:?}", result);
        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(1, all_disks.all_u2_zpools().len());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn detected_raw_disk_removal_triggers_notification() {
        let logctx =
            test_setup_log("detected_raw_disk_removal_triggers_notification");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        harness.handle().key_manager_ready().await;
        let mut raw_disks = harness.add_vdevs(&["u2_under_test.vdev"]).await;

        // Access the add disk notification
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(1, all_disks.iter_all().collect::<Vec<_>>().len());

        // Delete the disk and wait for a notification
        harness
            .handle()
            .detected_raw_disk_removal(raw_disks.remove(0))
            .await
            .await
            .unwrap();
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(0, all_disks.iter_all().collect::<Vec<_>>().len());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_using_exactly_these_disks() {
        let logctx = test_setup_log("ensure_using_exactly_these_disks");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Create a bunch of file backed external disks
        let vdev_dir = tempdir_in("/var/tmp").unwrap();
        let disks: Vec<RawDisk> = (0..10)
            .map(|serial| {
                let vdev_path =
                    vdev_dir.path().join(format!("u2_{serial}.vdev"));
                RawSyntheticDisk::new_with_length(&vdev_path, 1 << 20, serial)
                    .unwrap()
                    .into()
            })
            .collect();

        // Observe the first three disks
        harness
            .handle()
            .ensure_using_exactly_these_disks(disks.iter().take(3).cloned())
            .await
            .await
            .unwrap();

        let all_disks = harness.handle().get_latest_disks().await;
        assert_eq!(3, all_disks.iter_all().collect::<Vec<_>>().len());

        // Add first three disks after the initial one. The returned disks
        // should not contain the first disk.
        harness
            .handle()
            .ensure_using_exactly_these_disks(
                disks.iter().skip(1).take(3).cloned(),
            )
            .await
            .await
            .unwrap();

        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(3, all_disks.iter_all().collect::<Vec<_>>().len());

        let expected: HashSet<_> =
            disks.iter().skip(1).take(3).map(|d| d.identity()).collect();
        let actual: HashSet<_> =
            all_disks.iter_all().map(|(identity, _, _, _)| identity).collect();
        assert_eq!(expected, actual);

        // Ensure the same set of disks and make sure no change occurs
        // Note that we directly request the disks this time so we aren't
        // waiting forever for a change notification.
        harness
            .handle()
            .ensure_using_exactly_these_disks(
                disks.iter().skip(1).take(3).cloned(),
            )
            .await
            .await
            .unwrap();
        let all_disks2 = harness.handle().get_latest_disks().await;
        assert_eq!(
            all_disks.iter_all().collect::<Vec<_>>(),
            all_disks2.iter_all().collect::<Vec<_>>()
        );

        // Add a disjoint set of disks and see that only they come through
        harness
            .handle()
            .ensure_using_exactly_these_disks(
                disks.iter().skip(4).take(5).cloned(),
            )
            .await
            .await
            .unwrap();

        let all_disks = harness.handle().get_latest_disks().await;
        let expected: HashSet<_> =
            disks.iter().skip(4).take(5).map(|d| d.identity()).collect();
        let actual: HashSet<_> =
            all_disks.iter_all().map(|(identity, _, _, _)| identity).collect();
        assert_eq!(expected, actual);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_filesystem() {
        let logctx = test_setup_log("upsert_filesystem");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add a U.2 and M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Create a filesystem on the newly formatted U.2.
        //
        // We can call "upsert_filesystem" both with and without a UUID.
        let dataset_id = DatasetUuid::new_v4();
        let zpool_name = ZpoolName::new_external(config.disks[0].pool_id);
        let dataset_name = DatasetName::new(zpool_name, DatasetKind::Crucible);
        harness
            .handle()
            .upsert_filesystem(Some(dataset_id), dataset_name.clone())
            .await
            .unwrap();
        // Observe the dataset exists, and the UUID is set.
        let observed_dataset = &Zfs::get_dataset_properties(
            &[dataset_name.full_name()],
            WhichDatasets::SelfOnly,
        )
        .await
        .unwrap()[0];
        assert_eq!(observed_dataset.id, Some(dataset_id));

        harness
            .handle()
            .upsert_filesystem(None, dataset_name.clone())
            .await
            .unwrap();
        // Observe the dataset still exists, and the UUID is still set,
        // even though we did not ask for a new value explicitly.
        let observed_dataset = &Zfs::get_dataset_properties(
            &[dataset_name.full_name()],
            WhichDatasets::SelfOnly,
        )
        .await
        .unwrap()[0];
        assert_eq!(observed_dataset.id, Some(dataset_id));

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_filesystem_no_uuid() {
        let logctx = test_setup_log("upsert_filesystem");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add a U.2 and M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Create a filesystem on the newly formatted U.2, without a UUID
        let zpool_name = ZpoolName::new_external(config.disks[0].pool_id);
        let dataset_name = DatasetName::new(zpool_name, DatasetKind::Crucible);
        harness
            .handle()
            .upsert_filesystem(None, dataset_name.clone())
            .await
            .unwrap();
        let observed_dataset = &Zfs::get_dataset_properties(
            &[dataset_name.full_name()],
            WhichDatasets::SelfOnly,
        )
        .await
        .unwrap()[0];
        assert_eq!(observed_dataset.id, None);

        // Later, we can set the UUID to a specific value
        let dataset_id = DatasetUuid::new_v4();
        harness
            .handle()
            .upsert_filesystem(Some(dataset_id), dataset_name.clone())
            .await
            .unwrap();
        let observed_dataset = &Zfs::get_dataset_properties(
            &[dataset_name.full_name()],
            WhichDatasets::SelfOnly,
        )
        .await
        .unwrap()[0];
        assert_eq!(observed_dataset.id, Some(dataset_id));

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_datasets() {
        let logctx = test_setup_log("ensure_datasets");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add a U.2 and M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Create a dataset on the newly formatted U.2
        let id = DatasetUuid::new_v4();
        let zpool_name = ZpoolName::new_external(config.disks[0].pool_id);
        let name = DatasetName::new(zpool_name, DatasetKind::Crucible);
        let datasets = BTreeMap::from([(
            id,
            DatasetConfig { id, name, inner: SharedDatasetConfig::default() },
        )]);
        // "Generation = 1" is reserved as "no requests seen yet", so we jump
        // past it.
        let generation = Generation::new().next();
        let mut config = DatasetsConfig { generation, datasets };

        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        // List datasets, expect to see what we just created
        let observed_config =
            harness.handle().datasets_config_list().await.unwrap();
        assert_eq!(config, observed_config);

        // Calling "datasets_ensure" with the same input should succeed.
        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        let current_config_generation = config.generation;
        let next_config_generation = config.generation.next();

        // Calling "datasets_ensure" with an old generation should fail
        config.generation = Generation::new();
        let err =
            harness.handle().datasets_ensure(config.clone()).await.unwrap_err();
        assert!(matches!(err, Error::DatasetConfigurationOutdated { .. }));

        // However, calling it with a different input and the same generation
        // number should fail.
        config.generation = current_config_generation;
        config.datasets.values_mut().next().unwrap().inner.reservation =
            Some(1024.into());
        let err =
            harness.handle().datasets_ensure(config.clone()).await.unwrap_err();
        assert!(matches!(err, Error::DatasetConfigurationChanged { .. }));

        // If we bump the generation number while making a change, updated
        // configs will work.
        config.generation = next_config_generation;
        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    async fn is_mounted(dataset: &DatasetName) -> bool {
        let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
        let cmd =
            command.args(&["list", "-Hpo", "mounted", &dataset.full_name()]);
        let output = cmd.output().await.unwrap();
        assert!(output.status.success(), "Failed to list dataset: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim() == "yes"
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

    #[tokio::test]
    async fn ensure_datasets_get_mounted() {
        let logctx = test_setup_log("ensure_datasets_get_mounted");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add a U.2 and M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Create a dataset on the newly formatted U.2
        let id = DatasetUuid::new_v4();
        let zpool_name = ZpoolName::new_external(config.disks[0].pool_id);
        let name = DatasetName::new(zpool_name, DatasetKind::Debug);
        let datasets = BTreeMap::from([(
            id,
            DatasetConfig {
                id,
                name: name.clone(),
                inner: SharedDatasetConfig::default(),
            },
        )]);
        // "Generation = 1" is reserved as "no requests seen yet", so we jump
        // past it.
        let generation = Generation::new().next();
        let config = DatasetsConfig { generation, datasets };

        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        // Creating the dataset should have mounted it
        assert!(is_mounted(&name).await);

        // We can unmount the dataset manually
        unmount(&name).await;
        assert!(!is_mounted(&name).await);

        // We can re-apply the same config...
        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        // ... and doing so mounts the dataset again.
        assert!(is_mounted(&name).await);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_datasets_get_mounted_even_with_data() {
        let logctx =
            test_setup_log("ensure_datasets_get_mounted_even_with_data");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add a U.2 and M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Create a dataset on the newly formatted U.2.
        let id = DatasetUuid::new_v4();
        let zpool_name = ZpoolName::new_external(config.disks[0].pool_id);
        let kind = DatasetKind::TransientZone { name: "foo".to_string() };
        // If we use the "Debug" dataset, it'll get created and made immutable
        // during our call to "omicron_physical_disks_ensure".
        // So: We pick a different non-zoned dataset.
        assert!(
            !kind.zoned(),
            "We need to use a non-zoned dataset for this test"
        );
        let name = DatasetName::new(zpool_name, kind);
        let datasets = BTreeMap::from([(
            id,
            DatasetConfig {
                id,
                name: name.clone(),
                inner: SharedDatasetConfig::default(),
            },
        )]);
        // "Generation = 1" is reserved as "no requests seen yet", so we jump
        // past it.
        let generation = Generation::new().next();
        let config = DatasetsConfig { generation, datasets };

        // Before we actually make the dataset - create the mountpoint, and
        // stick a file there.
        let mountpoint = name.mountpoint(&harness.mount_config().root);
        std::fs::create_dir_all(&mountpoint).unwrap();
        std::fs::write(mountpoint.join("marker.txt"), "hello").unwrap();
        assert!(mountpoint.join("marker.txt").exists());

        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error(), "{status:?}");

        // Creating the dataset should have mounted it
        assert!(is_mounted(&name).await);

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
        unmount(&name).await;
        assert!(!is_mounted(&name).await);

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

    #[tokio::test]
    async fn ensure_many_datasets() {
        let logctx = test_setup_log("ensure_many_datasets");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add U.2s and an M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks = harness
            .add_vdevs(&[
                "u2_0.vdev",
                "u2_1.vdev",
                "u2_2.vdev",
                "u2_3.vdev",
                "u2_4.vdev",
                "u2_5.vdev",
                "u2_6.vdev",
                "u2_7.vdev",
                "u2_8.vdev",
                "u2_9.vdev",
                "m2_helping.vdev",
            ])
            .await;
        let config = harness.make_config(1, &raw_disks);

        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Create datasets on the newly formatted U.2s
        let mut datasets = BTreeMap::new();
        for i in 0..10 {
            let zpool_name = ZpoolName::new_external(config.disks[i].pool_id);

            let id = DatasetUuid::new_v4();
            let name = DatasetName::new(zpool_name, DatasetKind::Crucible);
            datasets.insert(
                id,
                DatasetConfig {
                    id,
                    name,
                    inner: SharedDatasetConfig::default(),
                },
            );

            let id = DatasetUuid::new_v4();
            let name = DatasetName::new(zpool_name, DatasetKind::Debug);
            datasets.insert(
                id,
                DatasetConfig {
                    id,
                    name,
                    inner: SharedDatasetConfig::default(),
                },
            );

            let id = DatasetUuid::new_v4();
            let name =
                DatasetName::new(zpool_name, DatasetKind::TransientZoneRoot);
            datasets.insert(
                id,
                DatasetConfig {
                    id,
                    name,
                    inner: SharedDatasetConfig::default(),
                },
            );
        }
        // "Generation = 1" is reserved as "no requests seen yet", so we jump
        // past it.
        let generation = Generation::new().next();
        let config = DatasetsConfig { generation, datasets };

        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        // List datasets, expect to see what we just created
        let observed_config =
            harness.handle().datasets_config_list().await.unwrap();
        assert_eq!(config, observed_config);

        // Calling "datasets_ensure" with the same input should succeed.
        let status =
            harness.handle().datasets_ensure(config.clone()).await.unwrap();
        assert!(!status.has_error());

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn nested_dataset() {
        let logctx = test_setup_log("nested_dataset");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Add a U.2 and M.2, adopt them into the "control plane"
        // for usage.
        harness.handle().key_manager_ready().await;
        let raw_disks =
            harness.add_vdevs(&["u2_under_test.vdev", "m2_helping.vdev"]).await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Ensuring disks should work after key manager is ready");
        assert!(!result.has_error(), "{:?}", result);

        // Use the dataset on the newly formatted U.2
        let all_disks = harness.handle().get_latest_disks().await;
        let zpool = all_disks.all_u2_zpools()[0];
        let datasets = harness.handle().datasets_list(zpool).await.unwrap();

        let dataset = datasets
            .iter()
            .find(|dataset| {
                dataset.name.contains(&DatasetKind::Debug.to_string())
            })
            .expect("Debug dataset not found");

        // This is a little magic; we can infer the zpool name from the "raw
        // string" dataset name.
        let zpool =
            ZpoolName::from_str(dataset.name.split('/').next().unwrap())
                .unwrap();
        let dataset_name = DatasetName::new(zpool, DatasetKind::Debug);

        // Start querying the state of nested datasets.
        //
        // When we ask about the root of a dataset, we only get information
        // about the dataset we're asking for.
        let root_location = NestedDatasetLocation {
            path: String::new(),
            root: dataset_name.clone(),
        };
        let nested_datasets = harness
            .handle()
            .nested_dataset_list(
                root_location.clone(),
                NestedDatasetListOptions::SelfAndChildren,
            )
            .await
            .unwrap();
        assert_eq!(nested_datasets.len(), 1);
        assert_eq!(nested_datasets[0].name, root_location);

        // If we ask about children of this dataset, we see nothing.
        let nested_datasets = harness
            .handle()
            .nested_dataset_list(
                root_location.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await
            .unwrap();
        assert_eq!(nested_datasets.len(), 0);

        // We can't destroy non-nested datasets through this API
        let err = harness
            .handle()
            .nested_dataset_destroy(root_location.clone())
            .await
            .expect_err("Should not be able to delete dataset root");
        assert!(
            err.to_string()
                .contains("Cannot destroy nested dataset with empty name"),
            "{err:?}"
        );

        // Create a nested dataset within the root one
        let nested_location = NestedDatasetLocation {
            path: "nested".to_string(),
            ..root_location.clone()
        };
        let nested_config = SharedDatasetConfig {
            compression: CompressionAlgorithm::On,
            ..Default::default()
        };
        harness
            .handle()
            .nested_dataset_ensure(NestedDatasetConfig {
                name: nested_location.clone(),
                inner: nested_config.clone(),
            })
            .await
            .unwrap();

        // We can re-send the ensure request
        harness
            .handle()
            .nested_dataset_ensure(NestedDatasetConfig {
                name: nested_location.clone(),
                inner: nested_config.clone(),
            })
            .await
            .expect("Ensuring nested datasets should be idempotent");

        // We can observe the nested dataset
        let nested_datasets = harness
            .handle()
            .nested_dataset_list(
                root_location.clone(),
                NestedDatasetListOptions::SelfAndChildren,
            )
            .await
            .unwrap();
        assert_eq!(nested_datasets.len(), 2);
        assert_eq!(nested_datasets[0].name, root_location);
        assert_eq!(nested_datasets[1].name, nested_location);
        let nested_datasets = harness
            .handle()
            .nested_dataset_list(
                root_location.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await
            .unwrap();
        assert_eq!(nested_datasets.len(), 1);
        assert_eq!(nested_datasets[0].name, nested_location);

        // We can also destroy the nested dataset
        harness
            .handle()
            .nested_dataset_destroy(nested_location.clone())
            .await
            .expect("Should have been able to destroy nested dataset");

        let err = harness
            .handle()
            .nested_dataset_destroy(nested_location.clone())
            .await
            .expect_err(
                "Should not be able to destroy nested dataset a second time",
            );
        let err = InlineErrorChain::new(&err).to_string();
        assert!(err.contains("Dataset not found"), "{err:?}");

        // The nested dataset should now be gone
        let nested_datasets = harness
            .handle()
            .nested_dataset_list(
                root_location.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .await
            .unwrap();
        assert_eq!(nested_datasets.len(), 0);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_omicron_physical_disks_schema() {
        let schema = schemars::schema_for!(OmicronPhysicalDisksConfig);
        expectorate::assert_contents(
            "../schema/omicron-physical-disks.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }

    #[test]
    fn test_datasets_schema() {
        let schema = schemars::schema_for!(DatasetsConfig);
        expectorate::assert_contents(
            "../schema/omicron-datasets.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
