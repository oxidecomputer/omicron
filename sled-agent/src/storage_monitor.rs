// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for storage events from [`sled_storage::manager::StorageManager`]
//! and dispatches them to other parts of the bootstrap agent and sled agent
//! code.

use omicron_common::api::external::Generation;
use sled_agent_config_reconciler::dump_setup::DumpSetup;
use sled_storage::config::MountConfig;
use sled_storage::manager::StorageHandle;
use sled_storage::resources::AllDisks;
use slog::Logger;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Storage Monitor no longer running")]
    NotRunning,
}

pub struct StorageMonitor {
    log: Logger,
    storage_manager: StorageHandle,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: DumpSetup,

    tx: watch::Sender<StorageMonitorStatus>,
}

/// Emits status about storage monitoring.
#[derive(Debug, Clone)]
pub struct StorageMonitorStatus {
    /// The latest generation of physical disks to be processed
    /// by the storage monitor.
    pub latest_gen: Option<Generation>,
}

impl StorageMonitorStatus {
    fn new() -> Self {
        Self { latest_gen: None }
    }
}

#[derive(Clone)]
pub struct StorageMonitorHandle {
    rx: watch::Receiver<StorageMonitorStatus>,
}

impl StorageMonitorHandle {
    pub async fn await_generation(
        &self,
        wanted: Generation,
    ) -> Result<(), Error> {
        self.rx
            .clone()
            .wait_for(|status| {
                let Some(observed) = status.latest_gen else {
                    return false;
                };
                return observed >= wanted;
            })
            .await
            .map_err(|_| Error::NotRunning)?;
        Ok(())
    }
}

impl StorageMonitor {
    pub fn new(
        log: &Logger,
        mount_config: MountConfig,
        storage_manager: StorageHandle,
    ) -> (StorageMonitor, StorageMonitorHandle) {
        let dump_setup = DumpSetup::new(&log, Arc::new(mount_config));
        let log = log.new(o!("component" => "StorageMonitor"));
        let (tx, rx) = watch::channel(StorageMonitorStatus::new());
        (
            StorageMonitor { log, storage_manager, dump_setup, tx },
            StorageMonitorHandle { rx },
        )
    }

    /// Run the main receive loop of the `StorageMonitor`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                disks = self.storage_manager.wait_for_changes() => {
                    info!(
                        self.log,
                        "Received storage manager update";
                        "disks" => ?disks
                    );
                    self.handle_resource_update(disks).await;
                }
            }
        }
    }

    async fn handle_resource_update(&mut self, updated_disks: AllDisks) {
        let generation = updated_disks.generation();
        self.dump_setup
            .update_dumpdev_setup(
                updated_disks.iter_managed().map(|(_id, disk)| disk),
            )
            .await;
        self.tx.send_replace(StorageMonitorStatus {
            latest_gen: Some(*generation),
        });
    }
}
