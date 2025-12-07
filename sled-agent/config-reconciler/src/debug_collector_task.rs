// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Long-running tokio task responsible for updating the dump device setup in
//! response to changes in available disks.

use crate::InternalDisksReceiver;
use crate::debug_collector::DebugCollector;
use camino::Utf8PathBuf;
use debug_ignore::DebugIgnore;
use sled_storage::config::MountConfig;
use sled_storage::disk::Disk;
use slog::Logger;
use slog::error;
use slog_error_chain::InlineErrorChain;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

pub(crate) fn spawn(
    internal_disks_rx: InternalDisksReceiver,
    external_disks_rx: watch::Receiver<HashSet<Disk>>,
    mount_config: Arc<MountConfig>,
    base_log: &Logger,
) -> FormerZoneRootArchiver {
    // We choose a buffer size of 1 because there's not much point in
    // buffering requests.  Callers are going to wait for each one to
    // complete synchronously anyway so it doesn't matter whether they wait
    // to enqueue the request or for the request to complete.
    let (archive_tx, archive_rx) = mpsc::channel(1);

    let debug_collector_task = DebugCollectorTask {
        internal_disks_rx,
        external_disks_rx,
        archive_rx,
        debug_collector: DebugCollector::new(base_log, mount_config),
        last_disks_used: HashSet::new(),
        log: base_log.new(slog::o!("component" => "DebugCollectorTask")),
    };

    tokio::spawn(debug_collector_task.run());

    FormerZoneRootArchiver {
        log: DebugIgnore(
            base_log.new(slog::o!("component" => "FormerZoneRootArchiver")),
        ),
        archive_tx: DebugIgnore(archive_tx),
    }
}

struct DebugCollectorTask {
    // Input channels on which we receive updates about disk changes.
    internal_disks_rx: InternalDisksReceiver,
    external_disks_rx: watch::Receiver<HashSet<Disk>>,
    // Input channel on which we receive requests to archive zone roots.
    archive_rx: mpsc::Receiver<FormerZoneRootArchiveRequest>,

    // Invokes dumpadm(8) and savecore(8) when new disks are encountered
    debug_collector: DebugCollector,

    // Set of internal + external disks we most recently passed to the
    // Debug Collector
    last_disks_used: HashSet<Disk>,

    log: Logger,
}

impl DebugCollectorTask {
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
                Some(request) = self.archive_rx.recv() => {
                    // One of the cases where we're asked to archive former zone
                    // roots is that we've just imported a disk.  That disk may
                    // also have the only debug datasets that we can use for
                    // archival.  So before we send the request to archive the
                    // former zone root, update the disk information.
                    self.update_setup_if_needed().await;

                    let FormerZoneRootArchiveRequest {
                         path,
                         completion_tx
                    } = request;
                    self
                        .debug_collector
                        .archive_former_zone_root(&path, completion_tx)
                        .await;
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
            self.debug_collector.update_dumpdev_setup(disks_avail.iter()).await;
            self.last_disks_used = disks_avail;
        }
    }
}

/// Handle for requesting archival of logs from a zone that is no longer running
// We derive Debug even though it's not that useful because several consumers
// want their own structs to impl `Debug` and this way they don't all have to
// use DebugIgnore on this struct.  Plus, it's still helpful to see this in the
// Debug output for those structs.
#[derive(Clone, Debug)]
pub struct FormerZoneRootArchiver {
    log: DebugIgnore<Logger>,
    archive_tx: DebugIgnore<mpsc::Sender<FormerZoneRootArchiveRequest>>,
}

impl FormerZoneRootArchiver {
    #[cfg(test)]
    pub fn noop(log: &Logger) -> Self {
        let (archive_tx, _) = mpsc::channel(1);
        Self {
            log: DebugIgnore(log.clone()),
            archive_tx: DebugIgnore(archive_tx),
        }
    }

    /// Archives logs from the given zone root filesystem (identified by path in
    /// the global zone)
    ///
    /// The requested path is expected to be the path to a mounted ZFS dataset
    /// that's used for an Oxide zone's root filesystem.
    ///
    /// Errors are logged but not propagated back to the caller.
    pub async fn archive_former_zone_root(&self, path: Utf8PathBuf) {
        let (completion_tx, completion_rx) = oneshot::channel();
        let request = FormerZoneRootArchiveRequest { path, completion_tx };
        if let Err(_) = self.archive_tx.send(request).await {
            error!(
                self.log,
                "failed to request archive of former zone root";
                "error" => "archive_tx channel closed",
            );
            return;
        }

        if let Err(error) = completion_rx.await {
            error!(
                self.log,
                "failed to wait for archive of former zone root";
                InlineErrorChain::new(&error),
            );
        }
    }
}

struct FormerZoneRootArchiveRequest {
    path: Utf8PathBuf,
    completion_tx: oneshot::Sender<()>,
}
