// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for storage events from [`sled_storage::manager::StorageManager`]
//! and dispatches them to other parst of the bootstrap agent and sled agent
//! code.

use crate::dump_setup::DumpSetup;
use crate::nexus::{ConvertInto, NexusClientWithResolver};
use derive_more::From;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;
use nexus_client::types::PhysicalDiskDeleteRequest;
use nexus_client::types::PhysicalDiskPutRequest;
use nexus_client::types::ZpoolPutRequest;
use omicron_common::api::external::ByteCount;
use omicron_common::backoff;
use omicron_common::disk::DiskIdentity;
use sled_storage::manager::StorageHandle;
use sled_storage::pool::Pool;
use sled_storage::resources::StorageResources;
use slog::Logger;
use std::fmt::Debug;
use std::pin::Pin;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(From, Clone, Debug)]
enum NexusDiskRequest {
    Put(PhysicalDiskPutRequest),
    Delete(PhysicalDiskDeleteRequest),
}

/// Describes the access to the underlay used by the StorageManager.
#[derive(Clone)]
pub struct UnderlayAccess {
    pub nexus_client: NexusClientWithResolver,
    pub sled_id: Uuid,
}

impl Debug for UnderlayAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnderlayAccess")
            .field("sled_id", &self.sled_id)
            .finish()
    }
}

pub struct StorageMonitor {
    log: Logger,
    storage_manager: StorageHandle,

    // Receive a onetime notification that the underlay is available
    underlay_available_rx: oneshot::Receiver<UnderlayAccess>,

    // A cached copy of the `StorageResources` from the last update
    storage_resources: StorageResources,

    // Ability to access the underlay network
    underlay: Option<UnderlayAccess>,

    // A queue for sending nexus notifications in order
    nexus_notifications: FuturesOrdered<NotifyFut>,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: DumpSetup,
}

impl StorageMonitor {
    pub fn new(
        log: &Logger,
        storage_manager: StorageHandle,
    ) -> (StorageMonitor, oneshot::Sender<UnderlayAccess>) {
        let (underlay_available_tx, underlay_available_rx) = oneshot::channel();
        let storage_resources = StorageResources::default();
        let dump_setup = DumpSetup::new(&log);
        let log = log.new(o!("component" => "StorageMonitor"));
        (
            StorageMonitor {
                log,
                storage_manager,
                underlay_available_rx,
                storage_resources,
                underlay: None,
                nexus_notifications: FuturesOrdered::new(),
                dump_setup,
            },
            underlay_available_tx,
        )
    }

    /// Run the main receive loop of the `StorageMonitor`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                res = self.nexus_notifications.next(),
                    if !self.nexus_notifications.is_empty() =>
                {
                    match res {
                        Some(Ok(s)) => {
                            info!(self.log, "Nexus notification complete: {s}");
                        }
                        e => error!(self.log, "Nexus notification error: {e:?}")
                    }
                }
                resources = self.storage_manager.wait_for_changes() => {
                    info!(
                        self.log,
                        "Received storage manager update";
                        "resources" => ?resources
                    );
                    self.handle_resource_update(resources).await;
                }
                Ok(underlay) = &mut self.underlay_available_rx,
                    if self.underlay.is_none() =>
                {
                    let sled_id = underlay.sled_id;
                    info!(
                        self.log,
                        "Underlay Available"; "sled_id" => %sled_id
                    );
                    self.underlay = Some(underlay);
                    self.notify_nexus_about_existing_resources(sled_id).await;
                }
            }
        }
    }

    /// When the underlay becomes available, we need to notify nexus about any
    /// discovered disks and pools, since we don't attempt to notify until there
    /// is an underlay available.
    async fn notify_nexus_about_existing_resources(&mut self, sled_id: Uuid) {
        let current = StorageResources::default();
        let updated = &self.storage_resources;
        let nexus_updates =
            compute_resource_diffs(&self.log, &sled_id, &current, updated);
        for put in nexus_updates.disk_puts {
            self.physical_disk_notify(put.into()).await;
        }
        for (pool, put) in nexus_updates.zpool_puts {
            self.add_zpool_notify(pool, put).await;
        }
    }

    async fn handle_resource_update(
        &mut self,
        updated_resources: StorageResources,
    ) {
        // If the underlay isn't available, we only record the changes. Nexus
        // isn't yet reachable to notify.
        if self.underlay.is_some() {
            let nexus_updates = compute_resource_diffs(
                &self.log,
                &self.underlay.as_ref().unwrap().sled_id,
                &self.storage_resources,
                &updated_resources,
            );

            for put in nexus_updates.disk_puts {
                self.physical_disk_notify(put.into()).await;
            }
            for del in nexus_updates.disk_deletes {
                self.physical_disk_notify(del.into()).await;
            }
            for (pool, put) in nexus_updates.zpool_puts {
                self.add_zpool_notify(pool, put).await;
            }
        }
        self.dump_setup.update_dumpdev_setup(updated_resources.disks()).await;

        // Save the updated `StorageResources`
        self.storage_resources = updated_resources;
    }

    // Adds a "notification to nexus" to `self.nexus_notifications`, informing it
    // about the addition/removal of a physical disk to this sled.
    async fn physical_disk_notify(&mut self, disk: NexusDiskRequest) {
        let underlay = self.underlay.as_ref().unwrap().clone();
        let disk2 = disk.clone();
        let notify_nexus = move || {
            let underlay = underlay.clone();
            let disk = disk.clone();
            async move {
                let nexus_client = underlay.nexus_client.client().clone();

                match &disk {
                    NexusDiskRequest::Put(request) => {
                        nexus_client
                            .physical_disk_put(&request)
                            .await
                            .map_err(|e| {
                                backoff::BackoffError::transient(e.to_string())
                            })?;
                    }
                    NexusDiskRequest::Delete(request) => {
                        nexus_client
                            .physical_disk_delete(&request)
                            .await
                            .map_err(|e| {
                                backoff::BackoffError::transient(e.to_string())
                            })?;
                    }
                }
                let msg = format!("{:?}", disk);
                Ok(msg)
            }
        };

        let log = self.log.clone();
        // This notification is often invoked before Nexus has started
        // running, so avoid flagging any errors as concerning until some
        // time has passed.
        let log_post_failure = move |err, call_count, total_duration| {
            if call_count == 0 {
                info!(log, "failed to notify nexus about {disk2:?}";
                    "err" => ?err
                );
            } else if total_duration > std::time::Duration::from_secs(30) {
                warn!(log, "failed to notify nexus about {disk2:?}";
                    "err" => ?err,
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

    // Adds a "notification to nexus" to `nexus_notifications`,
    // informing it about the addition of `pool_id` to this sled.
    async fn add_zpool_notify(
        &mut self,
        pool: Pool,
        zpool_request: ZpoolPutRequest,
    ) {
        let pool_id = pool.name.id();
        let underlay = self.underlay.as_ref().unwrap().clone();

        let notify_nexus = move || {
            let underlay = underlay.clone();
            let zpool_request = zpool_request.clone();
            async move {
                let sled_id = underlay.sled_id;
                let nexus_client = underlay.nexus_client.client().clone();
                nexus_client
                    .zpool_put(&sled_id, &pool_id, &zpool_request)
                    .await
                    .map_err(|e| {
                        backoff::BackoffError::transient(e.to_string())
                    })?;
                let msg = format!("{:?}", zpool_request);
                Ok(msg)
            }
        };

        let log = self.log.clone();
        let name = pool.name.clone();
        let disk = pool.parent.clone();
        let log_post_failure = move |err, call_count, total_duration| {
            if call_count == 0 {
                info!(log, "failed to notify nexus about a new pool {name} on disk {disk:?}";
                    "err" => ?err);
            } else if total_duration > std::time::Duration::from_secs(30) {
                warn!(log, "failed to notify nexus about a new pool {name} on disk {disk:?}";
                    "err" => ?err,
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
}

// The type of a future which is used to send a notification to Nexus.
type NotifyFut =
    Pin<Box<dyn futures::Future<Output = Result<String, String>> + Send>>;

struct NexusUpdates {
    disk_puts: Vec<PhysicalDiskPutRequest>,
    disk_deletes: Vec<PhysicalDiskDeleteRequest>,
    zpool_puts: Vec<(Pool, ZpoolPutRequest)>,
}

fn compute_resource_diffs(
    log: &Logger,
    sled_id: &Uuid,
    current: &StorageResources,
    updated: &StorageResources,
) -> NexusUpdates {
    let mut disk_puts = vec![];
    let mut disk_deletes = vec![];
    let mut zpool_puts = vec![];

    let mut put_pool = |disk_id: &DiskIdentity, updated_pool: &Pool| {
        match ByteCount::try_from(updated_pool.info.size()) {
            Ok(size) => zpool_puts.push((
                updated_pool.clone(),
                ZpoolPutRequest {
                    size: size.into(),
                    disk_model: disk_id.model.clone(),
                    disk_serial: disk_id.serial.clone(),
                    disk_vendor: disk_id.vendor.clone(),
                },
            )),
            Err(err) => {
                error!(
                    log,
                    "Error parsing pool size";
                    "name" => updated_pool.name.to_string(),
                    "err" => ?err);
            }
        }
    };

    // Diff the existing resources with the update to see what has changed
    // This loop finds disks and pools that were modified or deleted
    for (disk_id, (disk, pool)) in current.disks().iter() {
        match updated.disks().get(disk_id) {
            Some((updated_disk, updated_pool)) => {
                if disk != updated_disk {
                    disk_puts.push(PhysicalDiskPutRequest {
                        sled_id: *sled_id,
                        model: disk_id.model.clone(),
                        serial: disk_id.serial.clone(),
                        vendor: disk_id.vendor.clone(),
                        variant: updated_disk.variant().convert(),
                    });
                }
                if pool != updated_pool {
                    put_pool(disk_id, updated_pool);
                }
            }
            None => disk_deletes.push(PhysicalDiskDeleteRequest {
                model: disk_id.model.clone(),
                serial: disk_id.serial.clone(),
                vendor: disk_id.vendor.clone(),
                sled_id: *sled_id,
            }),
        }
    }

    // Diff the existing resources with the update to see what has changed
    // This loop finds new disks and pools
    for (disk_id, (updated_disk, updated_pool)) in updated.disks().iter() {
        if !current.disks().contains_key(disk_id) {
            disk_puts.push(PhysicalDiskPutRequest {
                sled_id: *sled_id,
                model: disk_id.model.clone(),
                serial: disk_id.serial.clone(),
                vendor: disk_id.vendor.clone(),
                variant: updated_disk.variant().convert(),
            });
            put_pool(disk_id, updated_pool);
        }
    }

    NexusUpdates { disk_puts, disk_deletes, zpool_puts }
}
