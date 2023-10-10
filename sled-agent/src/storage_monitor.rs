// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for storage events from [`sled_storage::StorageMonitor`]
//! and dispatches them to other parst of the bootstrap agent and sled agent
//! code.

use crate::nexus::NexusClientWithResolver;
use nexus_client::types::PhysicalDiskDeleteRequest;
use nexus_client::types::PhysicalDiskKind;
use nexus_client::types::PhysicalDiskPutRequest;
use nexus_client::types::ZpoolPutRequest;
use omicron_common::api::external::ByteCount;
use sled_storage::disk::Disk;
use sled_storage::manager::StorageHandle;
use sled_storage::resources::StorageResources;
use slog::Logger;
use std::fmt::Debug;
use tokio::sync::mpsc;
use uuid::Uuid;

const QUEUE_SIZE: usize = 10;

/// A message sent from the `StorageMonitorHandle` to the `StorageMonitor`.
#[derive(Debug)]
pub enum StorageMonitorMsg {
    UnderlayAvailable(UnderlayAccess),
}

/// Describes the access to the underlay used by the StorageManager.
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

/// A mechanism for interacting with the StorageMonitor
#[derive(Clone)]
pub struct StorageMonitorHandle {
    tx: mpsc::Sender<StorageMonitorMsg>,
}

pub struct StorageMonitor {
    log: Logger,
    storage_manager: StorageHandle,
    handle_rx: mpsc::Receiver<StorageMonitorMsg>,

    // A cached copy of the `StorageResources` from the last update
    storage_resources: StorageResources,

    // Ability to access the underlay network
    underlay: Option<UnderlayAccess>,
}

impl StorageMonitor {
    pub fn new(
        log: &Logger,
        storage_manager: StorageHandle,
    ) -> (StorageMonitor, StorageMonitorHandle) {
        let (handle_tx, handle_rx) = mpsc::channel(QUEUE_SIZE);
        let storage_resources = StorageResources::default();
        let log = log.new(o!("component" => "StorageMonitor"));
        (
            StorageMonitor {
                log,
                storage_manager,
                handle_rx,
                storage_resources,
                underlay: None,
            },
            StorageMonitorHandle { tx: handle_tx },
        )
    }

    /// Run the main receive loop of the `StorageMonitor`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                resources = self.storage_manager.wait_for_changes() => {
                    info!(
                        self.log,
                        "Received storage manager update";
                        "resources" => ?resources
                    );
                    self.handle_resource_update(resources).await;
                }
                Some(msg) = self.handle_rx.recv() => {
                    info!(
                        self.log,
                        "Received storage monitor message";
                        "msg" => ?msg
                    );
                    self.handle_monitor_msg(msg).await;
                }
            }
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
            // TODO: Notify nexus about diffs
        }
        // Save the updated `StorageResources`
        self.storage_resources = updated_resources;
    }
}

struct NexusUpdates {
    disk_puts: Vec<PhysicalDiskPutRequest>,
    disk_deletes: Vec<PhysicalDiskDeleteRequest>,
    zpool_puts: Vec<ZpoolPutRequest>,
}

async fn compute_resource_diffs(
    log: &Logger,
    sled_id: &Uuid,
    current: &StorageResources,
    updated: &StorageResources,
) -> NexusUpdates {
    let mut disk_puts = vec![];
    let mut disk_deletes = vec![];
    let mut zpool_puts = vec![];

    // Diff the existing resources with the update to see what has changed
    // This loop finds disks and pools that were modified or deleted
    for (disk_id, (disk, pool)) in current.disks.iter() {
        match updated.disks.get(disk_id) {
            Some((updated_disk, updated_pool)) => {
                if disk != updated_disk {
                    disk_puts.push(PhysicalDiskPutRequest {
                        sled_id: *sled_id,
                        model: disk_id.model.clone(),
                        serial: disk_id.serial.clone(),
                        vendor: disk_id.vendor.clone(),
                        variant: updated_disk.variant().into(),
                    });
                }
                if pool != updated_pool {
                    match ByteCount::try_from(pool.info.size()) {
                        Ok(size) => zpool_puts.push(ZpoolPutRequest {
                            size: size.into(),
                            disk_model: disk_id.model.clone(),
                            disk_serial: disk_id.serial.clone(),
                            disk_vendor: disk_id.vendor.clone(),
                        }),
                        Err(err) => error!(
                            log, 
                            "Error parsing pool size";
                            "name" => pool.name.to_string(),
                            "err" => ?err),
                    }
                }
            }
            None => disk_deletes.push(PhysicalDiskDeleteRequest {
                model: disk_id.model.clone(),
                serial: disk_id.serial.clone(),
                vendor: disk_id.vendor.clone(),
                sled_id,
            }),
        }
    }

    // Diff the existing resources with the update to see what has changed
    // This loop finds new disks and pools
    for (disk_id, (updated_disk, updated_pool)) in updated.disks.iter() {
        if !current.disks.contains_key(disk_id) {
            disk_puts.push(PhysicalDiskPutRequest {
                sled_id: *sled_id,
                model: disk_id.model.clone(),
                serial: disk_id.serial.clone(),
                vendor: disk_id.vendor.clone(),
                variant: updated_disk.variant().into(),
            });
        }
    }

    NexusUpdates { disk_puts, disk_deletes, zpool_puts }
}
