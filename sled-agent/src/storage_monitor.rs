// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for storage events from [`sled_storage::manager::StorageManager`]
//! and dispatches them to other parts of the bootstrap agent and sled agent
//! code.

use std::sync::Arc;

use crate::config_reconciler::InternalDisksReceiver;
use crate::config_reconciler::ReconcilerStateReceiver;
use crate::dump_setup::DumpSetup;
use slog::Logger;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Storage Monitor no longer running")]
    NotRunning,
}

pub struct StorageMonitor {
    log: Logger,
    internal_disks_rx: InternalDisksReceiver,
    reconciler_state_rx: ReconcilerStateReceiver,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: DumpSetup,
}

/*
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
*/

impl StorageMonitor {
    pub fn new(
        log: &Logger,
        internal_disks_rx: InternalDisksReceiver,
        reconciler_state_rx: ReconcilerStateReceiver,
    ) -> Self {
        let dump_setup =
            DumpSetup::new(&log, Arc::clone(internal_disks_rx.mount_config()));
        let log = log.new(o!("component" => "StorageMonitor"));
        Self { log, internal_disks_rx, reconciler_state_rx, dump_setup }
    }

    /// Run the main receive loop of the `StorageMonitor`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        loop {
            self.handle_resource_update().await;

            // Wait until either the internal disks or the set of mounted debug
            // datasets changes.
            tokio::select! {
                res = self.internal_disks_rx.changed() => {
                    if res.is_err() {
                        // This should never happen in production, but may
                        // happen in tests.
                        warn!(
                            self.log,
                            "internal disk task exited; exiting StorageMonitor",
                        );
                    }
                }

                res = self.reconciler_state_rx.changed() => {
                    if res.is_err() {
                        // This should never happen in production, but may
                        // happen in tests.
                        warn!(
                            self.log,
                            "reconciler task exited; exiting StorageMonitor",
                        );
                    }
                }
            }
        }
    }

    async fn handle_resource_update(&mut self) {
        let internal_disks = self.internal_disks_rx.current_and_update();
        let reconciler_state = self.reconciler_state_rx.current_and_update();
        self.dump_setup
            .update_dumpdev_setup(
                internal_disks.managed_disks(),
                reconciler_state.all_mounted_debug_datasets(),
            )
            .await;
    }
}
