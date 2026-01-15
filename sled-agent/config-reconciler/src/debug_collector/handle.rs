// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::helpers::RealCoreDumpAdm;
use super::helpers::RealZfs;
use super::helpers::RealZone;
use super::worker::CoreZpool;
use super::worker::DebugCollectorCmd;
use super::worker::DebugCollectorWorker;
use super::worker::DebugZpool;
use super::worker::DumpSlicePath;
use camino::Utf8Path;
use illumos_utils::zpool::ZpoolHealth;
use omicron_common::disk::DiskVariant;
use sled_storage::config::MountConfig;
use sled_storage::disk::Disk;
use slog::Logger;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Handle to the DebugCollectorWorker, used by the DebugCollectorTask
///
/// The DebugCollectorTask (a tiny task that passes information from the rest of
/// sled agent to this subystem) has this handle and uses it to send commands to
/// the DebugCollectorWorker.
pub struct DebugCollector {
    tx: tokio::sync::mpsc::Sender<DebugCollectorCmd>,
    mount_config: Arc<MountConfig>,
    _poller: tokio::task::JoinHandle<()>,
    log: Logger,
}

impl DebugCollector {
    pub fn new(log: &Logger, mount_config: Arc<MountConfig>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let worker = DebugCollectorWorker::new(
            Box::new(RealCoreDumpAdm {}),
            Box::new(RealZfs {}),
            Box::new(RealZone {}),
            log.new(o!("component" => "DebugCollector-worker")),
            rx,
        );
        let _poller =
            tokio::spawn(async move { worker.poll_file_archival().await });
        let log = log.new(o!("component" => "DebugCollector"));
        Self { tx, mount_config, _poller, log }
    }

    /// Given the set of all managed disks, updates the dump device location
    /// for logs and dumps.
    ///
    /// This function returns only once this request has been handled, which
    /// can be used as a signal by callers that any "old disks" are no longer
    /// being used by [DebugCollector].
    pub async fn update_dumpdev_setup(
        &self,
        disks: impl Iterator<Item = &Disk>,
    ) {
        let log = &self.log;
        let mut m2_dump_slices = Vec::new();
        let mut u2_debug_datasets = Vec::new();
        let mut m2_core_datasets = Vec::new();
        let mount_config = self.mount_config.clone();
        for disk in disks {
            match disk.variant() {
                DiskVariant::M2 => {
                    // We only setup dump devices on real disks
                    if !disk.is_synthetic() {
                        match disk.dump_device_devfs_path(false) {
                            Ok(path) => {
                                m2_dump_slices.push(DumpSlicePath::from(path))
                            }
                            Err(err) => {
                                warn!(
                                    log,
                                    "Error getting dump device devfs path: \
                                     {err:?}"
                                );
                            }
                        }
                    }
                    let name = disk.zpool_name();
                    if let Ok(info) =
                        illumos_utils::zpool::Zpool::get_info(&name.to_string())
                            .await
                    {
                        if info.health() == ZpoolHealth::Online {
                            m2_core_datasets.push(CoreZpool {
                                mount_config: mount_config.clone(),
                                name: *name,
                            });
                        } else {
                            warn!(
                                log,
                                "Zpool {name:?} not online, won't attempt to \
                                 save process core dumps there"
                            );
                        }
                    }
                }
                DiskVariant::U2 => {
                    let name = disk.zpool_name();
                    if let Ok(info) =
                        illumos_utils::zpool::Zpool::get_info(&name.to_string())
                            .await
                    {
                        if info.health() == ZpoolHealth::Online {
                            u2_debug_datasets.push(DebugZpool {
                                mount_config: mount_config.clone(),
                                name: *name,
                            });
                        } else {
                            warn!(
                                log,
                                "Zpool {name:?} not online, won't attempt to \
                                 save kernel core dumps there"
                            );
                        }
                    }
                }
            }
        }

        let (tx, rx) = oneshot::channel();
        if let Err(err) = self
            .tx
            .send(DebugCollectorCmd::UpdateDumpdevSetup {
                dump_slices: m2_dump_slices,
                debug_datasets: u2_debug_datasets,
                core_datasets: m2_core_datasets,
                update_complete_tx: tx,
            })
            .await
        {
            error!(log, "DebugCollector channel closed: {:?}", err.0);
        };

        if let Err(err) = rx.await {
            error!(log, "DebugCollector failed to await update"; "err" => ?err);
        }
    }

    /// Request archive of logs from the specified directory, which is assumed
    /// to correspond to the filesystem of a non-global zone that is no longer
    /// running.
    ///
    /// Unlike typical log file archival, this includes non-rotated log files.
    ///
    /// This makes a best-effort and logs failures rather than reporting them to
    /// the caller.
    ///
    /// When this future completes, the request has only been enqueued.  To know
    /// when archival has completed, you must wait on the receive side of
    /// `completion_tx`.
    pub async fn archive_former_zone_root(
        &self,
        zone_path: &Utf8Path,
        completion_tx: oneshot::Sender<()>,
    ) {
        let log = self.log.new(o!("zone_path" => zone_path.to_string()));

        // Validate the path that we were given.  We're only ever given zone
        // root filesystems, whose basename is always a zonename, and we always
        // prefix our zone names with `oxz_`.  If that's not what we find here,
        // log an error and bail out.  These error cases should be impossible to
        // hit in practice.
        let Some(file_name) = zone_path.file_name() else {
            error!(
                log,
                "cannot archive former zone root";
                "error" => "path has no filename part",
            );
            return;
        };

        if !file_name.starts_with("oxz_") {
            error!(
                log,
                "cannot archive former zone root";
                "error" => "filename does not start with \"oxz_\"",
            );
            return;
        }

        info!(log, "requesting archive of former zone root");
        let zone_path = zone_path.to_owned();
        let zone_name = file_name.to_string();
        let cmd = DebugCollectorCmd::ArchiveFormerZoneRoot {
            zone_path,
            zone_name,
            completion_tx,
        };
        if let Err(_) = self.tx.send(cmd).await {
            error!(
                log,
                "failed to request archive of former zone root";
                "error" => "DebugCollector channel closed"
            );
        }
    }
}
