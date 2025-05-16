// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Long-running tokio task responsible for updating the dump device setup in
//! response to changes in available disks.

use crate::InternalDisksReceiver;
use crate::dump_setup::DumpSetup;
use sled_storage::config::MountConfig;
use sled_storage::disk::Disk;
use slog::Logger;
use slog::error;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) fn spawn(
    internal_disks_rx: InternalDisksReceiver,
    external_disks_rx: watch::Receiver<HashSet<Disk>>,
    mount_config: Arc<MountConfig>,
    base_log: &Logger,
) {
    tokio::spawn(
        DumpSetupTask::new(
            internal_disks_rx,
            external_disks_rx,
            mount_config,
            base_log,
        )
        .run(),
    );
}

struct DumpSetupTask {
    // Input channels on which we receive updates about disk changes.
    internal_disks_rx: InternalDisksReceiver,
    external_disks_rx: watch::Receiver<HashSet<Disk>>,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    dump_setup: DumpSetup,

    // Set of internal + external disks we most recently passed to `dump_setup`.
    last_disks_used: HashSet<Disk>,

    log: Logger,
}

impl DumpSetupTask {
    fn new(
        internal_disks_rx: InternalDisksReceiver,
        external_disks_rx: watch::Receiver<HashSet<Disk>>,
        mount_config: Arc<MountConfig>,
        base_log: &Logger,
    ) -> Self {
        Self {
            internal_disks_rx,
            external_disks_rx,
            dump_setup: DumpSetup::new(base_log, mount_config),
            last_disks_used: HashSet::new(),
            log: base_log.new(slog::o!("component" => "DumpSetupTask")),
        }
    }

    async fn run(mut self) {
        loop {
            self.update_setup_if_needed().await;

            // Wait for changes on either input channel. Exit if either channel
            // is closed, which should never happen in production.
            tokio::select! {
                // Cancel-safe per docs on `changed()`
                res = self.internal_disks_rx.changed() => {
                    if res.is_err() {
                        error!(
                            self.log,
                            "internal disks channel closed: exiting task"
                        );
                        return;
                    }
                }

                // Cancel-safe per docs on `changed()`
                res = self.external_disks_rx.changed() => {
                    if res.is_err() {
                        error!(
                            self.log,
                            "external disks channel closed: exiting task"
                        );
                        return;
                    }
                }
            }
        }
    }

    async fn update_setup_if_needed(&mut self) {
        // Combine internal and external disks.
        let disks_avail = self
            .internal_disks_rx
            .borrow_and_update_raw_disks()
            .iter()
            .map(|d| d.deref().clone())
            .chain(self.external_disks_rx.borrow_and_update().iter().cloned())
            .collect::<HashSet<_>>();

        if disks_avail != self.last_disks_used {
            self.dump_setup.update_dumpdev_setup(disks_avail.iter()).await;
            self.last_disks_used = disks_avail;
        }
    }
}
