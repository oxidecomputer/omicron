// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for storage events from [`sled_storage::manager::StorageManager`]
//! and dispatches them to other parts of the bootstrap agent and sled agent
//! code.

use crate::dump_setup::DumpSetup;
use sled_storage::config::MountConfig;
use sled_storage::manager::StorageHandle;
use sled_storage::resources::AllDisks;
use slog::Logger;

pub struct StorageMonitor {
    log: Logger,
    storage_manager: StorageHandle,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: DumpSetup,
}

impl StorageMonitor {
    pub fn new(
        log: &Logger,
        mount_config: MountConfig,
        storage_manager: StorageHandle,
    ) -> StorageMonitor {
        let dump_setup = DumpSetup::new(&log, mount_config);
        let log = log.new(o!("component" => "StorageMonitor"));
        StorageMonitor { log, storage_manager, dump_setup }
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
        self.dump_setup
            .update_dumpdev_setup(
                updated_disks.iter_managed().map(|(_id, disk)| disk),
            )
            .await;
    }
}
