// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use std::collections::HashSet;

use crate::config::MountConfig;
use crate::dataset::{DatasetName, CONFIG_DATASET};
use crate::disk::{
    OmicronPhysicalDiskConfig, OmicronPhysicalDisksConfig, RawDisk,
};
use crate::error::Error;
use crate::resources::{AllDisks, DisksManagementResult, StorageResources};
use camino::Utf8PathBuf;
use debug_ignore::DebugIgnore;
use futures::future::FutureExt;
use illumos_utils::zfs::{Mountpoint, Zfs};
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use omicron_common::ledger::Ledger;
use sled_hardware::DiskVariant;
use slog::{info, o, warn, Logger};
use std::future::Future;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, Duration, MissedTickBehavior};
use uuid::Uuid;

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
    dataset_id: Uuid,
    dataset_name: DatasetName,
    responder: DebugIgnore<oneshot::Sender<Result<(), Error>>>,
}

#[derive(Debug)]
pub(crate) enum StorageRequest {
    // Requests to manage which devices the sled considers active.
    // These are manipulated by hardware management.
    DetectedRawDisk {
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

/// A mechanism for interacting with the [`StorageManager`]
#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<StorageRequest>,
    disk_updates: watch::Receiver<AllDisks>,
}

impl StorageHandle {
    pub(crate) fn new(
        tx: mpsc::Sender<StorageRequest>,
        disk_updates: watch::Receiver<AllDisks>,
    ) -> Self {
        Self { tx, disk_updates }
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

    pub async fn upsert_filesystem(
        &self,
        dataset_id: Uuid,
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
        let resources = StorageResources::new(log, mount_config, key_requester);
        let disk_updates = resources.watch_disks();
        (
            StorageManager {
                log: log.new(o!("component" => "StorageManager")),
                state: StorageManagerState::WaitingForKeyManager,
                rx,
                resources,
            },
            StorageHandle::new(tx, disk_updates),
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
            StorageRequest::DetectedRawDiskRemoval { raw_disk, tx } => {
                self.detected_raw_disk_removal(raw_disk);
                let _ = tx.0.send(Ok(()));
            }
            StorageRequest::DetectedRawDisksChanged { raw_disks, tx } => {
                self.ensure_using_exactly_these_disks(raw_disks).await;
                let _ = tx.0.send(Ok(()));
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

    async fn all_omicron_disk_ledgers(&self) -> Vec<Utf8PathBuf> {
        self.resources
            .disks()
            .all_m2_mountpoints(CONFIG_DATASET)
            .into_iter()
            .map(|p| p.join(DISKS_LEDGER_FILENAME))
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
        self.resources.insert_disk(raw_disk).await?;

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

    async fn load_ledger(&self) -> Option<Ledger<OmicronPhysicalDisksConfig>> {
        let ledger_paths = self.all_omicron_disk_ledgers().await;
        let log = self.log.new(o!("request" => "load_ledger"));
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
        let ledger = self.load_ledger().await;
        if let Some(ledger) = ledger {
            info!(self.log, "Setting StorageResources state to match ledger");

            // Identify which disks should be managed by the control
            // plane, and adopt all requested disks into the control plane
            // in a background task (see: [Self::manage_disks]).
            self.resources.set_config(&ledger.data().disks);
        } else {
            info!(self.log, "KeyManager ready, but no ledger detected");
            let mut synthetic_config =
                self.resources.get_config().values().cloned().collect();
            // TODO(https://github.com/oxidecomputer/omicron/issues/5328): Once
            // we are confident that we have migrated to a world where this
            // ledger is universally used, we should remove the following
            // kludge. The sled agent should not need to "self-manage" anything!
            let changed = self
                .self_manage_disks_with_zpools(&mut synthetic_config)
                .await?;
            if !changed {
                info!(self.log, "No disks to be automatically managed");
                return Ok(());
            }
            info!(self.log, "auto-managed disks"; "count" => synthetic_config.len());
            self.resources.set_config(&synthetic_config);
        }

        Ok(())
    }

    // NOTE: What follows is an exceptional case: one where we have
    // no record of "Control Plane Physical Disks", but we have zpools
    // on our U.2s, and we want to use them regardless.
    //
    // THIS WOULD NORMALLY BE INCORRECT BEHAVIOR. In the future, these
    // zpools will not be "automatically imported", and instead, we'll
    // let Nexus decide whether or not to reformat the disks.
    //
    // However, because we are transitioning from "the set of disks /
    // zpools is implicit" to a world where that set is explicit, this
    // is a necessary transitional tool.
    //
    // Returns "true" if the synthetic_config has changed.
    async fn self_manage_disks_with_zpools(
        &mut self,
        synthetic_config: &mut Vec<OmicronPhysicalDiskConfig>,
    ) -> Result<bool, Error> {
        let mut changed = false;
        for (identity, disk) in self.resources.disks().values.iter() {
            match disk {
                crate::resources::ManagedDisk::Unmanaged(raw) => {
                    let zpool_path = match raw.u2_zpool_path() {
                        Ok(zpool_path) => zpool_path,
                        Err(err) => {
                            info!(self.log, "Cannot find zpool path"; "identity" => ?identity, "err" => ?err);
                            continue;
                        }
                    };

                    let zpool_name =
                        match sled_hardware::disk::check_if_zpool_exists(
                            &zpool_path,
                        ) {
                            Ok(zpool_name) => zpool_name,
                            Err(err) => {
                                info!(self.log, "Zpool does not exist"; "identity" => ?identity, "err" => ?err);
                                continue;
                            }
                        };

                    info!(self.log, "Found existing zpool on device without ledger";
                        "identity" => ?identity,
                        "zpool" => ?zpool_name);

                    // We found an unmanaged disk with a zpool, even though
                    // we have no prior record of a ledger of control-plane
                    // disks.
                    synthetic_config.push(
                        // These disks don't have a control-plane UUID --
                        // report "nil" until they're overwritten with real
                        // values.
                        OmicronPhysicalDiskConfig {
                            identity: identity.clone(),
                            id: Uuid::nil(),
                            pool_id: zpool_name.id(),
                        },
                    );
                    changed = true;
                }
                _ => continue,
            }
        }
        Ok(changed)
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
                }

                // TODO: If the generation is equal, check that the values are
                // also equal.

                info!(log, "Request looks newer than prior requests");
                ledger
            }
            None => {
                info!(log, "No previously-stored 'requested disks', creating new ledger");
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
        self.resources.set_config(&config.disks);

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
            .filter_map(|(id, _variant, _slot)| {
                if !all_ids.contains(id) {
                    Some(id.clone())
                } else {
                    None
                }
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
        let do_format = true;
        let encryption_details = None;
        let size_details = None;
        Zfs::ensure_filesystem(
            fs_name,
            Mountpoint::Path(Utf8PathBuf::from("/data")),
            zoned,
            do_format,
            encryption_details,
            size_details,
            None,
        )?;
        // Ensure the dataset has a usable UUID.
        if let Ok(id_str) = Zfs::get_oxide_value(&fs_name, "uuid") {
            if let Ok(id) = id_str.parse::<Uuid>() {
                if id != request.dataset_id {
                    return Err(Error::UuidMismatch {
                        name: Box::new(request.dataset_name.clone()),
                        old: id,
                        new: request.dataset_id,
                    });
                }
                return Ok(());
            }
        }
        Zfs::set_oxide_value(
            &fs_name,
            "uuid",
            &request.dataset_id.to_string(),
        )?;

        Ok(())
    }
}

/// All tests only use synthetic disks, but are expected to be run on illumos
/// systems.
#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use crate::dataset::DatasetKind;
    use crate::disk::RawSyntheticDisk;
    use crate::manager_test_harness::StorageManagerTestHarness;
    use crate::resources::DiskManagementError;

    use super::*;
    use camino_tempfile::tempdir_in;
    use omicron_common::api::external::Generation;
    use omicron_common::ledger;
    use omicron_test_utils::dev::test_setup_log;
    use std::sync::atomic::Ordering;
    use uuid::Uuid;

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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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
    async fn wait_for_boot_disk() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("wait_for_boot_disk");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;
        let _raw_disks = harness.add_vdevs(&["u2_under_test.vdev"]).await;

        // When we wait for changes, we can see the U.2 being added, but no boot
        // disk.
        let all_disks = harness.handle_mut().wait_for_changes().await;
        assert_eq!(1, all_disks.iter_all().collect::<Vec<_>>().len());
        assert!(all_disks.boot_disk().is_none());

        // Waiting for the boot disk should time out.
        assert!(tokio::time::timeout(
            tokio::time::Duration::from_millis(10),
            harness.handle_mut().wait_for_boot_disk(),
        )
        .await
        .is_err());

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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log(
            "queued_disks_get_requeued_on_secret_retriever_error",
        );
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Queue up a disks, as we haven't told the `StorageManager` that
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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
        let actual: HashSet<_> = all_disks.values.keys().collect();
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
        assert_eq!(all_disks.values, all_disks2.values);

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
        let actual: HashSet<_> = all_disks.values.keys().collect();
        assert_eq!(expected, actual);

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_filesystem() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
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

        // Create a filesystem on the newly formatted U.2
        let dataset_id = Uuid::new_v4();
        let zpool_name = ZpoolName::new_external(config.disks[0].pool_id);
        let dataset_name =
            DatasetName::new(zpool_name.clone(), DatasetKind::Crucible);
        harness
            .handle()
            .upsert_filesystem(dataset_id, dataset_name)
            .await
            .unwrap();

        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ledgerless_to_ledgered_migration() {
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        let logctx = test_setup_log("ledgerless_to_ledgered_migration");
        let mut harness = StorageManagerTestHarness::new(&logctx.log).await;

        // Test setup: Create two U.2s and an M.2
        let raw_disks = harness
            .add_vdevs(&[
                "u2_under_test.vdev",
                "u2_that_shows_up_late.vdev",
                "m2_helping.vdev",
            ])
            .await;

        // First, we format the U.2s to have a zpool. This should work, even
        // without looping in the StorageManager.
        let first_u2 = &raw_disks[0];
        let first_pool_id = Uuid::new_v4();
        let _disk = crate::disk::Disk::new(
            &logctx.log,
            &harness.mount_config(),
            first_u2.clone(),
            Some(first_pool_id),
            Some(harness.key_requester()),
        )
        .await
        .expect("Failed to format U.2");

        let second_u2 = &raw_disks[1];
        let second_pool_id = Uuid::new_v4();
        let _disk = crate::disk::Disk::new(
            &logctx.log,
            &harness.mount_config(),
            second_u2.clone(),
            Some(second_pool_id),
            Some(harness.key_requester()),
        )
        .await
        .expect("Failed to format U.2");

        // Because we did that formatting "behind the back" of the
        // StorageManager, we should see no evidence of the U.2 being managed.
        //
        // This currently matches the format of "existing systems, which were
        // initialized before the storage ledger was created".

        // We should still see no ledger.
        let result = harness.handle().omicron_physical_disks_list().await;
        assert!(matches!(result, Err(Error::LedgerNotFound)), "{:?}", result);

        // We should also not see any managed U.2s.
        let disks = harness.handle().get_latest_disks().await;
        assert!(disks.all_u2_zpools().is_empty());

        // Leave one of the U.2s attached, but "remove" the other one.
        harness.remove_vdev(second_u2).await;

        // When the system activates, we should see a single Zpool, and
        // "auto-manage" it.
        harness.handle().key_manager_ready().await;

        // It might take a moment for synchronization to be handled by the
        // background task, but we'll eventually see the U.2 zpool.
        //
        // This is the equivalent of us "loading a zpool, even though
        // it was not backed by a ledger".
        let tt = TimeTravel::new();
        tt.enough_to_start_synchronization().await;
        while harness
            .handle_mut()
            .wait_for_changes()
            .await
            .all_u2_zpools()
            .is_empty()
        {
            info!(&logctx.log, "Waiting for U.2 to automatically show up");
        }
        let u2s = harness.handle().get_latest_disks().await.all_u2_zpools();
        assert_eq!(u2s.len(), 1, "{:?}", u2s);

        // If we attach the second U.2 -- the equivalent of it appearing after
        // the key manager is ready -- it'll also be included in the set of
        // auto-maanged U.2s.
        harness.add_vdev_as(second_u2.clone()).await;
        tt.enough_to_start_synchronization().await;
        while harness
            .handle_mut()
            .wait_for_changes()
            .await
            .all_u2_zpools()
            .len()
            == 1
        {
            info!(&logctx.log, "Waiting for U.2 to automatically show up");
        }
        let u2s = harness.handle().get_latest_disks().await.all_u2_zpools();
        assert_eq!(u2s.len(), 2, "{:?}", u2s);

        // This is the equivalent of the "/omicron-physical-disks GET" API,
        // which Nexus might use to contact this sled.
        //
        // This means that we'll bootstrap the sled successfully, but report a
        // 404 if nexus asks us for the latest configuration.
        let result = harness.handle().omicron_physical_disks_list().await;
        assert!(matches!(result, Err(Error::LedgerNotFound),), "{:?}", result);

        // At this point, Nexus may want to explicitly tell sled agent which
        // disks it should use. This is the equivalent of invoking
        // "/omicron-physical-disks PUT".
        let mut disks = vec![
            OmicronPhysicalDiskConfig {
                identity: first_u2.identity().clone(),
                id: Uuid::new_v4(),
                pool_id: first_pool_id,
            },
            OmicronPhysicalDiskConfig {
                identity: second_u2.identity().clone(),
                id: Uuid::new_v4(),
                pool_id: second_pool_id,
            },
        ];
        // Sort the disks to ensure the "output" matches the "input" when we
        // query later.
        disks.sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        let config =
            OmicronPhysicalDisksConfig { generation: Generation::new(), disks };
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Failed to ensure disks with 'new' Config");
        assert!(!result.has_error(), "{:?}", result);

        let observed_config = harness
            .handle()
            .omicron_physical_disks_list()
            .await
            .expect("Failed to retreive config after ensuring it");
        assert_eq!(observed_config, config);

        let u2s = harness.handle().get_latest_disks().await.all_u2_zpools();
        assert_eq!(u2s.len(), 2, "{:?}", u2s);

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
}
