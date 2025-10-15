// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Long-running tokio task responsible for updating the dump device setup in
//! response to changes in available disks.

use crate::InternalDisksReceiver;
use crate::dump_setup::DumpSetup;
use crate::dump_setup::FormerZoneRootRequest;
use sled_storage::config::MountConfig;
use sled_storage::disk::Disk;
use slog::Logger;
use slog::error;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;

pub(crate) fn spawn(
    internal_disks_rx: InternalDisksReceiver,
    external_disks_rx: watch::Receiver<HashSet<Disk>>,
    former_zone_roots_tx: mpsc::Receiver<FormerZoneRootRequest>,
    mount_config: Arc<MountConfig>,
    base_log: &Logger,
) {
    tokio::spawn(
        DumpSetupTask::new(
            internal_disks_rx,
            external_disks_rx,
            former_zone_roots_tx,
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
    // Input channel on which we receive requests to archive zone roots.
    former_zone_roots_rx: mpsc::Receiver<FormerZoneRootRequest>,

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
        former_zone_roots_rx: mpsc::Receiver<FormerZoneRootRequest>,
        mount_config: Arc<MountConfig>,
        base_log: &Logger,
    ) -> Self {
        Self {
            internal_disks_rx,
            external_disks_rx,
            former_zone_roots_rx,
            dump_setup: DumpSetup::new(base_log, mount_config),
            last_disks_used: HashSet::new(),
            log: base_log.new(slog::o!("component" => "DumpSetupTask")),
        }
    }

    async fn run(mut self) {
        self.update_setup_if_needed().await;

        loop {
            // Wait for changes on any input channel. Exit if either channel
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

                    self.update_setup_if_needed().await;
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

                    self.update_setup_if_needed().await;
                }

                // Cancel-safe per docs on `recv()`
                // If this returns `None`, we'll stop polling it until the next
                // iteration of the loop.  That's good because it means the
                // channel is closed.
                Some(request) = self.former_zone_roots_rx.recv() => {
                    // XXX-dap completion reporting
                    self.dump_setup.archive_former_zone_root(request).await;
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
