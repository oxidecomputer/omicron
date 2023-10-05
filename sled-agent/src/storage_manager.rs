// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of sled-local storage.

use crate::nexus::NexusClientWithResolver;
use crate::storage::dataset::DatasetName;
use crate::storage::dump_setup::DumpSetup;
use crate::zone_bundle::ZoneBundler;
use camino::Utf8PathBuf;
use derive_more::From;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;
use illumos_utils::zpool::{ZpoolKind, ZpoolName};
use illumos_utils::{zfs::Mountpoint, zpool::ZpoolInfo};
use key_manager::StorageKeyRequester;
use nexus_client::types::PhysicalDiskDeleteRequest;
use nexus_client::types::PhysicalDiskKind;
use nexus_client::types::PhysicalDiskPutRequest;
use nexus_client::types::ZpoolPutRequest;
use omicron_common::api::external::{ByteCount, ByteCountRangeError};
use omicron_common::backoff;
use omicron_common::disk::DiskIdentity;
use sled_hardware::{Disk, DiskVariant, UnparsedDisk};
use slog::Logger;
use std::collections::hash_map;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use uuid::Uuid;

#[cfg(test)]
use illumos_utils::{zfs::MockZfs as Zfs, zpool::MockZpool as Zpool};
#[cfg(not(test))]
use illumos_utils::{zfs::Zfs, zpool::Zpool};

// A key manager can only become ready once. This occurs during RSS or cold
// boot when the bootstore has detected it has a key share.
static KEY_MANAGER_READY: OnceLock<()> = OnceLock::new();

// The type of a future which is used to send a notification to Nexus.
type NotifyFut =
    Pin<Box<dyn futures::Future<Output = Result<(), String>> + Send>>;

#[derive(Debug)]
struct NewFilesystemRequest {
    dataset_id: Uuid,
    dataset_name: DatasetName,
    responder: oneshot::Sender<Result<DatasetName, Error>>,
}

struct UnderlayRequest {
    underlay: UnderlayAccess,
    responder: oneshot::Sender<Result<(), Error>>,
}

// The directory within the debug dataset in which bundles are created.
const BUNDLE_DIRECTORY: &str = "bundle";

// The directory for zone bundles.
const ZONE_BUNDLE_DIRECTORY: &str = "zone";

/// Describes the access to the underlay used by the StorageManager.
pub struct UnderlayAccess {
    pub nexus_client: NexusClientWithResolver,
    pub sled_id: Uuid,
}

// A worker that starts zones for pools as they are received.
struct StorageWorker {
    log: Logger,
    nexus_notifications: FuturesOrdered<NotifyFut>,
    rx: mpsc::Receiver<StorageWorkerRequest>,
    underlay: Arc<Mutex<Option<UnderlayAccess>>>,

    // A mechanism for requesting disk encryption keys from the
    // [`key_manager::KeyManager`]
    key_requester: StorageKeyRequester,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: Arc<DumpSetup>,
}

#[derive(Clone, Debug)]
enum NotifyDiskRequest {
    Add { identity: DiskIdentity, variant: DiskVariant },
    Remove(DiskIdentity),
}

#[derive(From, Clone, Debug, PartialEq, Eq, Hash)]
enum QueuedDiskCreate {
    Real(UnparsedDisk),
    Synthetic(ZpoolName),
}

impl QueuedDiskCreate {
    fn is_synthetic(&self) -> bool {
        if let QueuedDiskCreate::Synthetic(_) = self {
            true
        } else {
            false
        }
    }
}

impl StorageWorker {
    // Adds a "notification to nexus" to `nexus_notifications`,
    // informing it about the addition of `pool_id` to this sled.
    async fn add_zpool_notify(&mut self, pool: &Pool, size: ByteCount) {
        // The underlay network is setup once at sled-agent startup. Before
        // there is an underlay we want to avoid sending notifications to nexus for
        // two reasons:
        //  1. They can't possibly succeed
        //  2. They increase the backoff time exponentially, so that once
        //   sled-agent does start it may take much longer to notify nexus
        //   than it would if we avoid this. This goes especially so for rack
        //   setup, when bootstrap agent is waiting an aribtrary time for RSS
        //   initialization.
        if self.underlay.lock().await.is_none() {
            return;
        }

        let pool_id = pool.name.id();
        let DiskIdentity { vendor, serial, model } = pool.parent.clone();
        let underlay = self.underlay.clone();

        let notify_nexus = move || {
            let zpool_request = ZpoolPutRequest {
                size: size.into(),
                disk_vendor: vendor.clone(),
                disk_serial: serial.clone(),
                disk_model: model.clone(),
            };
            let underlay = underlay.clone();

            async move {
                let underlay_guard = underlay.lock().await;
                let Some(underlay) = underlay_guard.as_ref() else {
                    return Err(backoff::BackoffError::transient(
                        Error::UnderlayNotInitialized.to_string(),
                    ));
                };
                let sled_id = underlay.sled_id;
                let nexus_client = underlay.nexus_client.client().clone();
                drop(underlay_guard);

                nexus_client
                    .zpool_put(&sled_id, &pool_id, &zpool_request)
                    .await
                    .map_err(|e| {
                        backoff::BackoffError::transient(e.to_string())
                    })?;
                Ok(())
            }
        };
        let log = self.log.clone();
        let name = pool.name.clone();
        let disk = pool.parent().clone();
        let log_post_failure = move |_, call_count, total_duration| {
            if call_count == 0 {
                info!(log, "failed to notify nexus about a new pool {name} on disk {disk:?}");
            } else if total_duration > std::time::Duration::from_secs(30) {
                warn!(log, "failed to notify nexus about a new pool {name} on disk {disk:?}";
                    "total duration" => ?total_duration);
            }
        };
        self.nexus_notifications.push_back(
            backoff::retry_notify_ext(
                backoff::retry_policy_internal_service_aggressive(),
                notify_nexus,
                log_post_failure,
            )
            .boxed(),
        );
    }

    async fn ensure_using_exactly_these_disks(
        &mut self,
        resources: &StorageResources,
        unparsed_disks: Vec<UnparsedDisk>,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
    ) -> Result<(), Error> {
        // Queue U.2 drives if necessary
        // We clear all existing queued drives that are not synthetic and add
        // new ones in the loop below
        if let Some(queued) = queued_u2_drives {
            info!(
                self.log,
                "Ensure exact disks: clearing non-synthetic queued disks."
            );
            queued.retain(|d| d.is_synthetic());
        }

        let mut new_disks = HashMap::new();

        // We may encounter errors while parsing any of the disks; keep track of
        // any errors that occur and return any of them if something goes wrong.
        //
        // That being said, we should not prevent access to the other disks if
        // only one failure occurs.
        let mut err: Option<Error> = None;

        // Ensure all disks conform to the expected partition layout.
        for disk in unparsed_disks.into_iter() {
            if disk.variant() == DiskVariant::U2 {
                if let Some(queued) = queued_u2_drives {
                    info!(self.log, "Queuing disk for upsert: {disk:?}");
                    queued.insert(disk.into());
                    continue;
                }
            }
            match self.add_new_disk(disk, queued_u2_drives).await.map_err(
                |err| {
                    warn!(self.log, "Could not ensure partitions: {err}");
                    err
                },
            ) {
                Ok(disk) => {
                    new_disks.insert(disk.identity().clone(), disk);
                }
                Err(e) => {
                    warn!(self.log, "Cannot parse disk: {e}");
                    err = Some(e.into());
                }
            };
        }

        let mut disks = resources.disks.lock().await;

        // Remove disks that don't appear in the "new_disks" set.
        //
        // This also accounts for zpools and notifies Nexus.
        let disks_to_be_removed = disks
            .iter_mut()
            .filter(|(key, old_disk)| {
                // If this disk appears in the "new" and "old" set, it should
                // only be removed if it has changed.
                //
                // This treats a disk changing in an unexpected way as a
                // "removal and re-insertion".
                match old_disk {
                    DiskWrapper::Real { disk, .. } => {
                        if let Some(new_disk) = new_disks.get(*key) {
                            // Changed Disk -> Disk should be removed.
                            new_disk != disk
                        } else {
                            // Real disk, not in the new set -> Disk should be removed.
                            true
                        }
                    }
                    // Synthetic disk -> Disk should NOT be removed.
                    DiskWrapper::Synthetic { .. } => false,
                }
            })
            .map(|(_key, disk)| disk.clone())
            .collect::<Vec<_>>();

        for disk in disks_to_be_removed {
            if let Err(e) = self
                .delete_disk_locked(&resources, &mut disks, &disk.identity())
                .await
            {
                warn!(self.log, "Failed to delete disk: {e}");
                err = Some(e);
            }
        }

        // Add new disks to `resources.disks`.
        //
        // This also accounts for zpools and notifies Nexus.
        for (key, new_disk) in new_disks {
            if let Some(old_disk) = disks.get(&key) {
                // In this case, the disk should be unchanged.
                //
                // This assertion should be upheld by the filter above, which
                // should remove disks that changed.
                assert!(old_disk == &new_disk.into());
            } else {
                let disk = DiskWrapper::Real {
                    disk: new_disk.clone(),
                    devfs_path: new_disk.devfs_path().clone(),
                };
                if let Err(e) =
                    self.upsert_disk_locked(&resources, &mut disks, disk).await
                {
                    warn!(self.log, "Failed to upsert disk: {e}");
                    err = Some(e);
                }
            }
        }

        if let Some(err) = err {
            Err(err)
        } else {
            Ok(())
        }
    }

    // Attempt to create a new disk via `sled_hardware::Disk::new()`. If the
    // disk addition fails because the the key manager cannot load a secret,
    // this indicates a transient error, and so we queue the disk so we can
    // try again.
    async fn add_new_disk(
        &mut self,
        unparsed_disk: UnparsedDisk,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
    ) -> Result<Disk, sled_hardware::PooledDiskError> {
        match sled_hardware::Disk::new(
            &self.log,
            unparsed_disk.clone(),
            Some(&self.key_requester),
        )
        .await
        {
            Ok(disk) => Ok(disk),
            Err(sled_hardware::PooledDiskError::KeyManager(err)) => {
                warn!(
                    self.log,
                    "Transient error: {err} - queuing disk {:?}", unparsed_disk
                );
                if let Some(queued) = queued_u2_drives {
                    queued.insert(unparsed_disk.into());
                } else {
                    *queued_u2_drives =
                        Some(HashSet::from([unparsed_disk.into()]));
                }
                Err(sled_hardware::PooledDiskError::KeyManager(err))
            }
            Err(err) => {
                error!(
                    self.log,
                    "Persistent error: {err} - not queueing disk {:?}",
                    unparsed_disk
                );
                Err(err)
            }
        }
    }

    // Attempt to create a new synthetic disk via
    // `sled_hardware::Disk::ensure_zpool_ready()`. If the disk addition fails
    // because the the key manager cannot load a secret, this indicates a
    // transient error, and so we queue the disk so we can try again.
    async fn add_new_synthetic_disk(
        &mut self,
        zpool_name: ZpoolName,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
    ) -> Result<(), sled_hardware::PooledDiskError> {
        let synthetic_id = DiskIdentity {
            vendor: "fake_vendor".to_string(),
            serial: "fake_serial".to_string(),
            model: zpool_name.id().to_string(),
        };
        match sled_hardware::Disk::ensure_zpool_ready(
            &self.log,
            &zpool_name,
            &synthetic_id,
            Some(&self.key_requester),
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(sled_hardware::PooledDiskError::KeyManager(err)) => {
                warn!(
                    self.log,
                    "Transient error: {err} - queuing synthetic disk: {:?}",
                    zpool_name
                );
                if let Some(queued) = queued_u2_drives {
                    queued.insert(zpool_name.into());
                } else {
                    *queued_u2_drives =
                        Some(HashSet::from([zpool_name.into()]));
                }
                Err(sled_hardware::PooledDiskError::KeyManager(err))
            }
            Err(err) => {
                error!(
                    self.log,
                    "Persistent error: {} - not queueing synthetic disk {:?}",
                    err,
                    zpool_name
                );
                Err(err)
            }
        }
    }

    async fn upsert_disk(
        &mut self,
        resources: &StorageResources,
        disk: UnparsedDisk,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
    ) -> Result<(), Error> {
        // Queue U.2 drives if necessary
        if let Some(queued) = queued_u2_drives {
            if disk.variant() == DiskVariant::U2 {
                info!(self.log, "Queuing disk for upsert: {disk:?}");
                queued.insert(disk.into());
                return Ok(());
            }
        }

        info!(self.log, "Upserting disk: {disk:?}");

        // Ensure the disk conforms to an expected partition layout.
        let disk =
            self.add_new_disk(disk, queued_u2_drives).await.map_err(|err| {
                warn!(self.log, "Could not ensure partitions: {err}");
                err
            })?;

        let mut disks = resources.disks.lock().await;
        let disk = DiskWrapper::Real {
            disk: disk.clone(),
            devfs_path: disk.devfs_path().clone(),
        };
        self.upsert_disk_locked(resources, &mut disks, disk).await
    }

    async fn upsert_synthetic_disk(
        &mut self,
        resources: &StorageResources,
        zpool_name: ZpoolName,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
    ) -> Result<(), Error> {
        // Queue U.2 drives if necessary
        if let Some(queued) = queued_u2_drives {
            if zpool_name.kind() == ZpoolKind::External {
                info!(
                    self.log,
                    "Queuing synthetic disk for upsert: {zpool_name:?}"
                );
                queued.insert(zpool_name.into());
                return Ok(());
            }
        }

        info!(self.log, "Upserting synthetic disk for: {zpool_name:?}");

        self.add_new_synthetic_disk(zpool_name.clone(), queued_u2_drives)
            .await?;
        let disk = DiskWrapper::Synthetic { zpool_name };
        let mut disks = resources.disks.lock().await;
        self.upsert_disk_locked(resources, &mut disks, disk).await
    }

    async fn upsert_disk_locked(
        &mut self,
        resources: &StorageResources,
        disks: &mut tokio::sync::MutexGuard<
            '_,
            HashMap<DiskIdentity, DiskWrapper>,
        >,
        disk: DiskWrapper,
    ) -> Result<(), Error> {
        disks.insert(disk.identity(), disk.clone());
        self.physical_disk_notify(NotifyDiskRequest::Add {
            identity: disk.identity(),
            variant: disk.variant(),
        })
        .await;
        self.upsert_zpool(&resources, disk.identity(), disk.zpool_name())
            .await?;

        self.dump_setup.update_dumpdev_setup(disks).await;

        Ok(())
    }

    async fn delete_disk(
        &mut self,
        resources: &StorageResources,
        disk: UnparsedDisk,
    ) -> Result<(), Error> {
        info!(self.log, "Deleting disk: {disk:?}");
        // TODO: Don't we need to do some accounting, e.g. for all the information
        // that's no longer accessible? Or is that up to Nexus to figure out at
        // a later point-in-time?
        //
        // If we're storing zone images on the M.2s for internal services, how
        // do we reconcile them?
        let mut disks = resources.disks.lock().await;
        self.delete_disk_locked(resources, &mut disks, disk.identity()).await
    }

    async fn delete_disk_locked(
        &mut self,
        resources: &StorageResources,
        disks: &mut tokio::sync::MutexGuard<
            '_,
            HashMap<DiskIdentity, DiskWrapper>,
        >,
        key: &DiskIdentity,
    ) -> Result<(), Error> {
        if let Some(parsed_disk) = disks.remove(key) {
            resources.pools.lock().await.remove(&parsed_disk.zpool_name().id());
            self.physical_disk_notify(NotifyDiskRequest::Remove(key.clone()))
                .await;
        }

        self.dump_setup.update_dumpdev_setup(disks).await;

        Ok(())
    }

    /// When the underlay becomes available, we need to notify nexus about any
    /// discovered disks and pools, since we don't attempt to notify until there
    /// is an underlay available.
    async fn notify_nexus_about_existing_resources(
        &mut self,
        resources: &StorageResources,
    ) -> Result<(), Error> {
        let disks = resources.disks.lock().await;
        for disk in disks.values() {
            self.physical_disk_notify(NotifyDiskRequest::Add {
                identity: disk.identity(),
                variant: disk.variant(),
            })
            .await;
        }

        // We may encounter errors while processing any of the pools; keep track of
        // any errors that occur and return any of them if something goes wrong.
        //
        // That being said, we should not prevent notification to nexus of the
        // other pools if only one failure occurs.
        let mut err: Option<Error> = None;

        let pools = resources.pools.lock().await;
        for pool in pools.values() {
            match ByteCount::try_from(pool.info.size()).map_err(|err| {
                Error::BadPoolSize { name: pool.name.to_string(), err }
            }) {
                Ok(size) => self.add_zpool_notify(pool, size).await,
                Err(e) => {
                    warn!(self.log, "Failed to notify nexus about pool: {e}");
                    err = Some(e)
                }
            }
        }

        if let Some(err) = err {
            Err(err)
        } else {
            Ok(())
        }
    }

    // Adds a "notification to nexus" to `self.nexus_notifications`, informing it
    // about the addition/removal of a physical disk to this sled.
    async fn physical_disk_notify(&mut self, disk: NotifyDiskRequest) {
        // The underlay network is setup once at sled-agent startup. Before
        // there is an underlay we want to avoid sending notifications to nexus for
        // two reasons:
        //  1. They can't possibly succeed
        //  2. They increase the backoff time exponentially, so that once
        //   sled-agent does start it may take much longer to notify nexus
        //   than it would if we avoid this. This goes especially so for rack
        //   setup, when bootstrap agent is waiting an aribtrary time for RSS
        //   initialization.
        if self.underlay.lock().await.is_none() {
            return;
        }
        let underlay = self.underlay.clone();
        let disk2 = disk.clone();
        let notify_nexus = move || {
            let disk = disk.clone();
            let underlay = underlay.clone();
            async move {
                let underlay_guard = underlay.lock().await;
                let Some(underlay) = underlay_guard.as_ref() else {
                    return Err(backoff::BackoffError::transient(
                        Error::UnderlayNotInitialized.to_string(),
                    ));
                };
                let sled_id = underlay.sled_id;
                let nexus_client = underlay.nexus_client.client().clone();
                drop(underlay_guard);

                match &disk {
                    NotifyDiskRequest::Add { identity, variant } => {
                        let request = PhysicalDiskPutRequest {
                            model: identity.model.clone(),
                            serial: identity.serial.clone(),
                            vendor: identity.vendor.clone(),
                            variant: match variant {
                                DiskVariant::U2 => PhysicalDiskKind::U2,
                                DiskVariant::M2 => PhysicalDiskKind::M2,
                            },
                            sled_id,
                        };
                        nexus_client
                            .physical_disk_put(&request)
                            .await
                            .map_err(|e| {
                                backoff::BackoffError::transient(e.to_string())
                            })?;
                    }
                    NotifyDiskRequest::Remove(disk_identity) => {
                        let request = PhysicalDiskDeleteRequest {
                            model: disk_identity.model.clone(),
                            serial: disk_identity.serial.clone(),
                            vendor: disk_identity.vendor.clone(),
                            sled_id,
                        };
                        nexus_client
                            .physical_disk_delete(&request)
                            .await
                            .map_err(|e| {
                                backoff::BackoffError::transient(e.to_string())
                            })?;
                    }
                }
                Ok(())
            }
        };
        let log = self.log.clone();
        // This notification is often invoked before Nexus has started
        // running, so avoid flagging any errors as concerning until some
        // time has passed.
        let log_post_failure = move |_, call_count, total_duration| {
            if call_count == 0 {
                info!(log, "failed to notify nexus about {disk2:?}");
            } else if total_duration > std::time::Duration::from_secs(30) {
                warn!(log, "failed to notify nexus about {disk2:?}";
                    "total duration" => ?total_duration);
            }
        };
        self.nexus_notifications.push_back(
            backoff::retry_notify_ext(
                backoff::retry_policy_internal_service_aggressive(),
                notify_nexus,
                log_post_failure,
            )
            .boxed(),
        );
    }

    async fn upsert_zpool(
        &mut self,
        resources: &StorageResources,
        parent: DiskIdentity,
        pool_name: &ZpoolName,
    ) -> Result<(), Error> {
        let mut pools = resources.pools.lock().await;
        let zpool = Pool::new(pool_name.clone(), parent)?;

        let pool = match pools.entry(pool_name.id()) {
            hash_map::Entry::Occupied(mut entry) => {
                // The pool already exists.
                entry.get_mut().info = zpool.info;
                return Ok(());
            }
            hash_map::Entry::Vacant(entry) => entry.insert(zpool),
        };
        info!(&self.log, "Storage manager processing zpool: {:#?}", pool.info);

        let size = ByteCount::try_from(pool.info.size()).map_err(|err| {
            Error::BadPoolSize { name: pool_name.to_string(), err }
        })?;
        // Notify Nexus of the zpool.
        self.add_zpool_notify(&pool, size).await;
        Ok(())
    }

    // Small wrapper around `Self::do_work_internal` that ensures we always
    // emit info to the log when we exit.
    async fn do_work(
        &mut self,
        resources: StorageResources,
    ) -> Result<(), Error> {
        // We queue U.2 sleds until the StorageKeyRequester is ready to use.
        let mut queued_u2_drives = Some(HashSet::new());
        loop {
            match self.do_work_internal(&resources, &mut queued_u2_drives).await
            {
                Ok(()) => {
                    info!(self.log, "StorageWorker exited successfully");
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "StorageWorker encountered unexpected error: {}", e
                    );
                    // ... for now, keep trying.
                }
            }
        }
    }

    async fn do_work_internal(
        &mut self,
        resources: &StorageResources,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
    ) -> Result<(), Error> {
        const QUEUED_DISK_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
        let mut interval = interval(QUEUED_DISK_RETRY_TIMEOUT);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.nexus_notifications.next(),
                    if !self.nexus_notifications.is_empty() => {},
                Some(request) = self.rx.recv() => {
                    // We want to queue failed requests related to the key manager
                    match self.handle_storage_worker_request(
                        resources, queued_u2_drives, request)
                    .await {
                        Err(Error::DiskError(_)) => {
                            // We already handle and log disk errors, no need to
                            // return here.
                        }
                        Err(e) => return Err(e),
                        Ok(()) => {}
                    }
               }
               _ = interval.tick(), if queued_u2_drives.is_some() &&
                   KEY_MANAGER_READY.get().is_some()=>
                {
                    self.upsert_queued_disks(resources, queued_u2_drives).await;
                }
            }
        }
    }

    async fn handle_storage_worker_request(
        &mut self,
        resources: &StorageResources,
        queued_u2_drives: &mut Option<HashSet<QueuedDiskCreate>>,
        request: StorageWorkerRequest,
    ) -> Result<(), Error> {
        use StorageWorkerRequest::*;
        match request {
            AddDisk(disk) => {
                self.upsert_disk(&resources, disk, queued_u2_drives).await?;
            }
            AddSyntheticDisk(zpool_name) => {
                self.upsert_synthetic_disk(
                    &resources,
                    zpool_name,
                    queued_u2_drives,
                )
                .await?;
            }
            RemoveDisk(disk) => {
                self.delete_disk(&resources, disk).await?;
            }
            NewFilesystem(request) => {
                let result = self.add_dataset(&resources, &request).await;
                let _ = request.responder.send(result);
            }
            DisksChanged(disks) => {
                self.ensure_using_exactly_these_disks(
                    &resources,
                    disks,
                    queued_u2_drives,
                )
                .await?;
            }
            SetupUnderlayAccess(UnderlayRequest { underlay, responder }) => {
                // If this is the first time establishing an
                // underlay we should notify nexus of all existing
                // disks and zpools.
                //
                // Instead of individual notifications, we should
                // send a bulk notification as described in https://
                // github.com/oxidecomputer/omicron/issues/1917
                if self.underlay.lock().await.replace(underlay).is_none() {
                    self.notify_nexus_about_existing_resources(&resources)
                        .await?;
                }
                let _ = responder.send(Ok(()));
            }
            KeyManagerReady => {
                let _ = KEY_MANAGER_READY.set(());
                self.upsert_queued_disks(resources, queued_u2_drives).await;
            }
        }
        Ok(())
    }
}

enum StorageWorkerRequest {
    AddDisk(UnparsedDisk),
    AddSyntheticDisk(ZpoolName),
    RemoveDisk(UnparsedDisk),
    DisksChanged(Vec<UnparsedDisk>),
    NewFilesystem(NewFilesystemRequest),
    SetupUnderlayAccess(UnderlayRequest),
    KeyManagerReady,
}

struct StorageManagerInner {
    log: Logger,

    resources: StorageResources,

    tx: mpsc::Sender<StorageWorkerRequest>,

    // A handle to a worker which updates "pools".
    task: JoinHandle<Result<(), Error>>,
}

/// A sled-local view of all attached storage.
#[derive(Clone)]
pub struct StorageManager {
    inner: Arc<StorageManagerInner>,
    zone_bundler: ZoneBundler,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] which should manage local storage.
    pub async fn new(log: &Logger, key_requester: StorageKeyRequester) -> Self {
        let log = log.new(o!("component" => "StorageManager"));
        let resources = StorageResources {
            disks: Arc::new(Mutex::new(HashMap::new())),
            pools: Arc::new(Mutex::new(HashMap::new())),
        };
        let (tx, rx) = mpsc::channel(30);

        let zb_log = log.new(o!("component" => "ZoneBundler"));
        let zone_bundler =
            ZoneBundler::new(zb_log, resources.clone(), Default::default());

        StorageManager {
            inner: Arc::new(StorageManagerInner {
                log: log.clone(),
                resources: resources.clone(),
                tx,
                task: tokio::task::spawn(async move {
                    let dump_setup = Arc::new(DumpSetup::new(&log));
                    let mut worker = StorageWorker {
                        log,
                        nexus_notifications: FuturesOrdered::new(),
                        rx,
                        underlay: Arc::new(Mutex::new(None)),
                        key_requester,
                        dump_setup,
                    };

                    worker.do_work(resources).await
                }),
            }),
            zone_bundler,
        }
    }

    /// Return a reference to the object used to manage zone bundles.
    ///
    /// This can be cloned by other code wishing to create and manage their own
    /// zone bundles.
    pub fn zone_bundler(&self) -> &ZoneBundler {
        &self.zone_bundler
    }

    /// Adds underlay access to the storage manager.
    pub async fn setup_underlay_access(
        &self,
        underlay: UnderlayAccess,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(StorageWorkerRequest::SetupUnderlayAccess(UnderlayRequest {
                underlay,
                responder: tx,
            }))
            .await
            .map_err(|e| e.to_string())
            .expect("Failed to send SetupUnderlayAccess request");
        rx.await.expect("Failed to await underlay setup")
    }

    pub async fn get_zpools(&self) -> Result<Vec<crate::params::Zpool>, Error> {
        let disks = self.inner.resources.disks.lock().await;
        let pools = self.inner.resources.pools.lock().await;

        let mut zpools = Vec::with_capacity(pools.len());

        for (id, pool) in pools.iter() {
            let disk_identity = &pool.parent;
            let disk_type = if let Some(disk) = disks.get(&disk_identity) {
                disk.variant().into()
            } else {
                // If the zpool claims to be attached to a disk that we
                // don't know about, that's an error.
                return Err(Error::ZpoolNotFound(
                    format!("zpool: {id} claims to be from unknown disk: {disk_identity:#?}")
                ));
            };
            zpools.push(crate::params::Zpool { id: *id, disk_type });
        }

        Ok(zpools)
    }

    pub async fn upsert_filesystem(
        &self,
        dataset_id: Uuid,
        dataset_name: DatasetName,
    ) -> Result<DatasetName, Error> {
        let (tx, rx) = oneshot::channel();
        let request =
            NewFilesystemRequest { dataset_id, dataset_name, responder: tx };

        self.inner
            .tx
            .send(StorageWorkerRequest::NewFilesystem(request))
            .await
            .map_err(|e| e.to_string())
            .expect("Storage worker bug (not alive)");
        let dataset_name = rx.await.expect(
            "Storage worker bug (dropped responder without responding)",
        )?;

        Ok(dataset_name)
    }
}
