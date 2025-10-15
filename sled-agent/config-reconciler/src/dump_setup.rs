// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module is responsible for moving debug info (kernel crash dumps,
//! userspace process core dumps, and rotated logs) onto external drives for
//! perusal/archival, and to prevent internal drives from filling up.
//! (For background on the paths and datasets being used, see RFD 118)
//!
//! The behaviors documented below describe current behavior, but are not
//! necessarily a long-term guarantee, and details may be subject to change.
//!
//! ## Choice of destination external drive for archived logs and dumps
//! As zpools on external (U.2) drives come online, their proportion of space
//! used is checked, any that are over 70% are skipped, and of the remaining
//! candidates the one with the *most* content is designated as the target onto
//! which diagnostic files will be archived every 5 minutes.
//!
//! If *all* drives are over 70% utilization, the one with the oldest average
//! file modification time is chosen for cleanup, wherein its oldest archived
//! file are removed until the space used is under the 70% threshold again.
//!
//! If the chosen drive eventually exceeds 80% of its capacity used, then a
//! different drive is chosen by the same algorithm.
//!
//! ## Kernel crash dumps
//! As internal (M.2) drives are discovered, their designated dump slices are
//! checked for the presence of a previous kernel crash dump that hasn't been
//! archived. If a dump is present that has not yet been archived, and an
//! external debug directory has been chosen, `savecore(8)` is invoked to save
//! the dump slice's contents there and mark the slice as processed.
//!
//! If an active dump slice (into which the running kernel should dump) has not
//! yet been designated, and the slice being observed was either successfully
//! archived or vacant to begin with, that slice is configured as the running
//! system's dump slice with `dumpadm(8)`.
//!
//! If no vacant slices are available and no external volume is online with
//! sufficient free space to serve as a `savecore(8)` destination, we simply
//! do not configure a dump slice, preferring to preserve evidence of the
//! original root cause of an issue rather than overwriting it with confounding
//! variables (in the event adjacent systems begin behaving erratically due to
//! the initial failure).
//! In this event, as soon as an external drive becomes available to archive
//! one or all of the occupied dump slices' contents, the golden-path procedure
//! detailed above occurs and a dump slice is configured.
//!
//! ## Process core dumps
//! As zpools on internal (M.2) drives come online, the first one seen by the
//! poll loop is chosen to be the destination of process cores in all zones:
//! ```text
//!     /pool/int/*/crash/core.[zone-name].[exe-filename].[pid].[time]
//! ```
//!
//! For reference, at time of writing, the invocation of coreadm(8) looks like:
//! ```sh
//!     coreadm \
//!       -d process -d proc-setid \
//!       -e global -e global-setid \
//!       -g "/pool/int/${CHOSEN_ZFS}/crash/core.%z.%f.%p.%t" \
//!       -G default+debug
//! ```
//!
//! Every 5 minutes, all core files found on internal drives are moved to the
//! DUMP_DATASET of the (similarly chosen) removable U.2 drive, like so:
//! ```text
//!     /pool/int/*/crash/core.global.sled-agent.101.34784217
//!         -> /pool/ext/*/crypt/debug/core.global.sled-agent.101.34784217
//! ```
//!
//! ## Log rotation and archival
//! Every 5 minutes, each log that logadm(8) has rotated (in every zone) gets
//! archived into the DUMP_DATASET of the chosen U.2, with the suffixed
//! number replaced by the modified timestamp, like so:
//! ```text
//!     /var/svc/log/foo.log.0
//!         -> /pool/ext/*/crypt/debug/global/foo.log.34784217
//!     /pool/int/*/crypt/zone/oxz_bar/root/var/svc/log/baz.log.0
//!         -> /pool/ext/*/crypt/debug/oxz_bar/baz.log.34784217
//! ```
//!
//! If the log file's modified time is unavailable or invalid, we fall back to
//! the time of archival, and if that fails, we simply count up from 0.
//!
//! In the event of filename collisions (i.e. several instances of a service's
//! rotated log files having the same modified time to the second), the
//! number is incremented by 1 until no conflict remains.

use async_trait::async_trait;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use derive_more::{AsRef, From};
use illumos_utils::ExecutionError;
use illumos_utils::coreadm::{CoreAdm, CoreFileOption};
use illumos_utils::dumpadm::{DumpAdm, DumpContentType};
use illumos_utils::zone::ZONE_PREFIX;
use illumos_utils::zpool::{ZpoolHealth, ZpoolName};
use omicron_common::disk::DiskVariant;
use sled_agent_types::support_bundle::BUNDLE_FILE_NAME;
use sled_agent_types::support_bundle::BUNDLE_TMP_FILE_NAME;
use sled_storage::config::MountConfig;
use sled_storage::dataset::{CRASH_DATASET, DUMP_DATASET};
use sled_storage::disk::Disk;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::trace;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use zone::{Zone, ZoneError};

const ZFS_PROP_USED: &str = "used";
const ZFS_PROP_AVAILABLE: &str = "available";

const DATASET_USAGE_PERCENT_CHOICE: u64 = 70;
const DATASET_USAGE_PERCENT_CLEANUP: u64 = 80;

const ARCHIVAL_INTERVAL: Duration = Duration::from_secs(300);

// we sure are passing a lot of Utf8PathBufs around, let's be careful about it
#[derive(AsRef, Clone, Debug, Eq, From, Hash, Ord, PartialEq, PartialOrd)]
struct DumpSlicePath(Utf8PathBuf);
#[derive(AsRef, Clone, Debug, Eq, From, Hash, Ord, PartialEq, PartialOrd)]
struct DebugDataset(Utf8PathBuf);
#[derive(AsRef, Clone, Debug, Eq, From, Hash, Ord, PartialEq, PartialOrd)]
struct CoreDataset(Utf8PathBuf);

#[derive(Debug, Clone)]
pub struct FormerZoneRoot {
    path: Utf8PathBuf,
    zone_name: String,
}

impl FormerZoneRoot {
    pub fn for_prod_path<'a>(
        path: &'a Utf8Path,
    ) -> Result<FormerZoneRoot, FormerZoneRootPathError<'a>> {
        let file_name = path.file_name().ok_or_else(|| {
            FormerZoneRootPathError { path, reason: "path has no file name" }
        })?;

        if !file_name.starts_with("oxz_") {
            return Err(FormerZoneRootPathError {
                path,
                reason: "file name does not start with \"oxz_\"",
            });
        }

        Ok(FormerZoneRoot {
            path: path.to_owned(),
            zone_name: file_name.to_string(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unable to parse zone name from path {path:?}: {reason}")]
pub struct FormerZoneRootPathError<'a> {
    path: &'a Utf8Path,
    reason: &'static str,
}

#[derive(AsRef, Clone, Debug, From)]
pub(super) struct CoreZpool {
    mount_config: Arc<MountConfig>,
    name: ZpoolName,
}

#[derive(AsRef, Clone, Debug, From)]
pub(super) struct DebugZpool {
    mount_config: Arc<MountConfig>,
    name: ZpoolName,
}

impl GetMountpoint for DebugZpool {
    type NewType = DebugDataset;
    const MOUNTPOINT: &'static str = DUMP_DATASET;
    fn mount_config(&self) -> &MountConfig {
        &self.mount_config
    }
}
impl GetMountpoint for CoreZpool {
    type NewType = CoreDataset;
    const MOUNTPOINT: &'static str = CRASH_DATASET;
    fn mount_config(&self) -> &MountConfig {
        &self.mount_config
    }
}

// only want to access these directories after they're mounted!
trait GetMountpoint: AsRef<ZpoolName> {
    type NewType: From<Utf8PathBuf>;
    const MOUNTPOINT: &'static str;

    fn mount_config(&self) -> &MountConfig;

    fn mountpoint(
        &self,
        invoker: &dyn ZfsInvoker,
    ) -> Result<Option<Self::NewType>, ZfsGetError> {
        if invoker.zfs_get_prop(&self.as_ref().to_string(), "mounted")? == "yes"
        {
            Ok(Some(Self::NewType::from(invoker.mountpoint(
                self.mount_config(),
                self.as_ref(),
                Self::MOUNTPOINT,
            ))))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
enum DumpSetupCmd {
    ArchiveFormerZoneRoots {
        zone_roots: Vec<FormerZoneRoot>,
        completion_tx: oneshot::Sender<()>,
    },
    UpdateDumpdevSetup {
        dump_slices: Vec<DumpSlicePath>,
        debug_datasets: Vec<DebugZpool>,
        core_datasets: Vec<CoreZpool>,
        update_complete_tx: oneshot::Sender<()>,
    },
}

struct DumpSetupWorker {
    core_dataset_names: Vec<CoreZpool>,
    debug_dataset_names: Vec<DebugZpool>,

    chosen_dump_slice: Option<DumpSlicePath>,
    chosen_debug_dir: Option<DebugDataset>,
    chosen_core_dir: Option<CoreDataset>,

    known_dump_slices: Vec<DumpSlicePath>,
    known_debug_dirs: Vec<DebugDataset>,
    known_core_dirs: Vec<CoreDataset>,

    savecored_slices: HashSet<DumpSlicePath>,

    log: Logger,
    rx: Receiver<DumpSetupCmd>,
    coredumpadm_invoker: Box<dyn CoreDumpAdmInvoker + Send + Sync>,
    zfs_invoker: Box<dyn ZfsInvoker + Send + Sync>,
    zone_invoker: Box<dyn ZoneInvoker + Send + Sync>,
}

pub struct DumpSetup {
    tx: tokio::sync::mpsc::Sender<DumpSetupCmd>,
    mount_config: Arc<MountConfig>,
    _poller: tokio::task::JoinHandle<()>,
    log: Logger,
}

impl DumpSetup {
    pub fn new(log: &Logger, mount_config: Arc<MountConfig>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let worker = DumpSetupWorker::new(
            Box::new(RealCoreDumpAdm {}),
            Box::new(RealZfs {}),
            Box::new(RealZone {}),
            log.new(o!("component" => "DumpSetup-worker")),
            rx,
        );
        let _poller =
            tokio::spawn(async move { worker.poll_file_archival().await });
        let log = log.new(o!("component" => "DumpSetup"));
        Self { tx, mount_config, _poller, log }
    }

    /// Given the set of all managed disks, updates the dump device location
    /// for logs and dumps.
    ///
    /// This function returns only once this request has been handled, which
    /// can be used as a signal by callers that any "old disks" are no longer
    /// being used by [DumpSetup].
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
            if disk.is_synthetic() {
                // We only setup dump devices on real disks
                continue;
            }
            match disk.variant() {
                DiskVariant::M2 => {
                    match disk.dump_device_devfs_path(false) {
                        Ok(path) => m2_dump_slices.push(DumpSlicePath(path)),
                        Err(err) => {
                            warn!(
                                log,
                                "Error getting dump device devfs path: {err:?}"
                            );
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
                                "Zpool {name:?} not online, won't attempt to save process core dumps there"
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
                                "Zpool {name:?} not online, won't attempt to save kernel core dumps there"
                            );
                        }
                    }
                }
            }
        }

        let (tx, rx) = oneshot::channel();
        if let Err(err) = self
            .tx
            .send(DumpSetupCmd::UpdateDumpdevSetup {
                dump_slices: m2_dump_slices,
                debug_datasets: u2_debug_datasets,
                core_datasets: m2_core_datasets,
                update_complete_tx: tx,
            })
            .await
        {
            error!(log, "DumpSetup channel closed: {:?}", err.0);
        };

        if let Err(err) = rx.await {
            error!(log, "DumpSetup failed to await update"; "err" => ?err);
        }
    }

    /// Perform log archival from the specified directories, which are assumed
    /// to correspond to zones that are no longer running.
    ///
    /// Unlike typical log file archival, this includes non-rotated log files.
    ///
    /// This makes a best-effort and does not report failures to the caller.
    pub async fn archive_former_zone_roots(
        &self,
        zone_roots: Vec<FormerZoneRoot>,
    ) {
        let log = &self.log;

        info!(
            log,
            "archive_former_zone_roots: start";
            "zone_roots" => ?zone_roots
        );

        // XXX-dap commonize with previous function
        let (completion_tx, completion_rx) = oneshot::channel();
        if let Err(_) = self
            .tx
            .send(DumpSetupCmd::ArchiveFormerZoneRoots {
                zone_roots: zone_roots.clone(),
                completion_tx,
            })
            .await
        {
            error!(log, "DumpSetup channel closed");
        }

        if let Err(err) = completion_rx.await {
            error!(
                log,
                "DumpSetup failed to await archive_former_zone_roots";
                InlineErrorChain::new(&err)
            );
        }

        info!(
            log,
            "archive_former_zone_roots: done";
            "zone_roots" => ?zone_roots,
        );
    }
}

#[derive(Debug, thiserror::Error)]
enum ZfsGetError {
    #[error("Error executing 'zfs get' command: {0}")]
    IoError(#[from] std::io::Error),
    #[error(
        "Output of 'zfs get' was not only not an integer string, it wasn't even UTF-8: {0}"
    )]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Error parsing output of 'zfs get' command as integer: {0}")]
    Parse(#[from] std::num::ParseIntError),
}

#[async_trait]
trait CoreDumpAdmInvoker {
    fn coreadm(&self, core_dir: &Utf8PathBuf) -> Result<(), ExecutionError>;
    async fn dumpadm(
        &self,
        dump_slice: &Utf8PathBuf,
        savecore_dir: Option<&Utf8PathBuf>,
    ) -> Result<Option<OsString>, ExecutionError>;
}

trait ZfsInvoker {
    fn zfs_get_prop(
        &self,
        mountpoint_or_name: &str,
        property: &str,
    ) -> Result<String, ZfsGetError>;

    fn zfs_get_integer(
        &self,
        mountpoint_or_name: &str,
        property: &str,
    ) -> Result<u64, ZfsGetError> {
        self.zfs_get_prop(mountpoint_or_name, property)?
            .parse()
            .map_err(Into::into)
    }

    fn below_thresh(
        &self,
        mountpoint: &Utf8PathBuf,
        percent: u64,
    ) -> Result<(bool, u64), ZfsGetError> {
        let used = self.zfs_get_integer(mountpoint.as_str(), ZFS_PROP_USED)?;
        let available =
            self.zfs_get_integer(mountpoint.as_str(), ZFS_PROP_AVAILABLE)?;
        let capacity = used + available;
        let below = (used * 100) / capacity < percent;
        Ok((below, used))
    }

    fn mountpoint(
        &self,
        mount_config: &MountConfig,
        zpool: &ZpoolName,
        mountpoint: &'static str,
    ) -> Utf8PathBuf;
}

#[async_trait]
trait ZoneInvoker {
    async fn get_zones(&self) -> Result<Vec<Zone>, ArchiveLogsError>;
}

struct RealCoreDumpAdm {}
struct RealZfs {}
struct RealZone {}

#[async_trait]
impl CoreDumpAdmInvoker for RealCoreDumpAdm {
    fn coreadm(&self, core_dir: &Utf8PathBuf) -> Result<(), ExecutionError> {
        let mut cmd = CoreAdm::new();

        // disable per-process core patterns
        cmd.disable(CoreFileOption::Process);
        cmd.disable(CoreFileOption::ProcSetid);

        // use the global core pattern
        cmd.enable(CoreFileOption::Global);
        cmd.enable(CoreFileOption::GlobalSetid);

        // set the global pattern to place all cores into core_dir,
        // with filenames of "core.[zone-name].[exe-filename].[pid].[time]"
        cmd.global_pattern(core_dir.join("core.%z.%f.%p.%t"));

        // also collect DWARF data from the exe and its library deps
        cmd.global_contents("default+debug");

        cmd.execute()
    }

    // Invokes `dumpadm(8)` to configure the kernel to dump core into the given
    // `dump_slice` block device in the event of a panic. If a core is already
    // present in that block device, and a `savecore_dir` is provided, this
    // function also invokes `savecore(8)` to save it into that directory.
    // On success, returns Ok(Some(stdout)) if `savecore(8)` was invoked, or
    // Ok(None) if it wasn't.
    async fn dumpadm(
        &self,
        dump_slice: &Utf8PathBuf,
        savecore_dir: Option<&Utf8PathBuf>,
    ) -> Result<Option<OsString>, ExecutionError> {
        let savecore_dir_cloned = if let Some(dir) = savecore_dir.cloned() {
            dir
        } else {
            // if we don't have a savecore destination yet, still create and use
            // a tmpfs path (rather than the default location under /var/crash,
            // which is in the ramdisk pool), because dumpadm refuses to do what
            // we ask otherwise.
            let tmp_crash = "/tmp/crash";
            tokio::fs::create_dir_all(tmp_crash).await.map_err(|err| {
                ExecutionError::ExecutionStart {
                    command: format!("mkdir {tmp_crash:?}"),
                    err,
                }
            })?;
            Utf8PathBuf::from(tmp_crash)
        };

        // Use the given block device path for dump storage:
        let mut cmd = DumpAdm::new(dump_slice.to_owned(), savecore_dir_cloned);

        // Include memory from the current process if there is one for the panic
        // context, in addition to kernel memory:
        cmd.content_type(DumpContentType::CurProc);

        // Compress crash dumps:
        cmd.compress(true);

        // Do not run savecore(8) automatically on boot (irrelevant anyhow, as the
        // config file being mutated by dumpadm won't survive reboots on gimlets).
        // The sled-agent will invoke it manually instead.
        cmd.no_boot_time_savecore();

        cmd.execute()?;

        // do we have a destination for the saved dump
        if savecore_dir.is_some() {
            // and does the dump slice have one to save off
            if let Ok(true) =
                illumos_utils::dumpadm::dump_flag_is_valid(dump_slice).await
            {
                return illumos_utils::dumpadm::SaveCore.execute();
            }
        }
        Ok(None)
    }
}

impl ZfsInvoker for RealZfs {
    fn zfs_get_prop(
        &self,
        mountpoint_or_name: &str,
        property: &str,
    ) -> Result<String, ZfsGetError> {
        let mut cmd = std::process::Command::new(illumos_utils::zfs::ZFS);
        cmd.arg("get").arg("-Hpo").arg("value");
        cmd.arg(property);
        cmd.arg(mountpoint_or_name);
        let output = cmd.output()?;
        Ok(String::from_utf8(output.stdout)?.trim().to_string())
    }

    fn mountpoint(
        &self,
        mount_config: &MountConfig,
        zpool: &ZpoolName,
        mountpoint: &'static str,
    ) -> Utf8PathBuf {
        zpool.dataset_mountpoint(&mount_config.root, mountpoint)
    }
}

#[async_trait]
impl ZoneInvoker for RealZone {
    async fn get_zones(&self) -> Result<Vec<Zone>, ArchiveLogsError> {
        Ok(zone::Adm::list()
            .await?
            .into_iter()
            .filter(|z| z.global() || z.name().starts_with(ZONE_PREFIX))
            .collect::<Vec<_>>())
    }
}

fn safe_to_delete(path: &Utf8Path, meta: &std::fs::Metadata) -> bool {
    if !meta.is_file() {
        // Ignore non-files (e.g. directories, symlinks)
        return false;
    }

    // Confirm that the file name is "safe to delete".
    //
    // This list may expand as we continue using the Debug dataset.
    let Some(file_name) = path.file_name() else {
        return false;
    };
    // Ignore support bundles
    if file_name == BUNDLE_FILE_NAME || file_name == BUNDLE_TMP_FILE_NAME {
        return false;
    }
    return true;
}

impl DumpSetupWorker {
    fn new(
        coredumpadm_invoker: Box<dyn CoreDumpAdmInvoker + Send + Sync>,
        zfs_invoker: Box<dyn ZfsInvoker + Send + Sync>,
        zone_invoker: Box<dyn ZoneInvoker + Send + Sync>,
        log: Logger,
        rx: Receiver<DumpSetupCmd>,
    ) -> Self {
        Self {
            core_dataset_names: vec![],
            debug_dataset_names: vec![],
            chosen_dump_slice: None,
            chosen_debug_dir: None,
            chosen_core_dir: None,
            known_dump_slices: vec![],
            known_debug_dirs: vec![],
            known_core_dirs: vec![],
            savecored_slices: Default::default(),
            log,
            rx,
            coredumpadm_invoker,
            zfs_invoker,
            zone_invoker,
        }
    }

    async fn poll_file_archival(mut self) {
        info!(self.log, "DumpSetup poll loop started.");

        // A oneshot which helps callers track when updates have propagated.
        //
        // This is particularly useful for disk expungement, when a caller
        // wants to ensure that the dump device is no longer accessing an
        // old device.
        let mut evaluation_and_archiving_complete_tx = None;

        loop {
            match tokio::time::timeout(ARCHIVAL_INTERVAL, self.rx.recv()).await
            {
                Ok(Some(DumpSetupCmd::UpdateDumpdevSetup {
                    dump_slices,
                    debug_datasets,
                    core_datasets,
                    update_complete_tx,
                })) => {
                    evaluation_and_archiving_complete_tx =
                        Some(update_complete_tx);
                    self.update_disk_loadout(
                        dump_slices,
                        debug_datasets,
                        core_datasets,
                    );
                }
                Ok(Some(DumpSetupCmd::ArchiveFormerZoneRoots {
                    zone_roots,
                    completion_tx,
                })) => {
                    if let Err(error) =
                        self.archive_extra_former_zone_roots(&zone_roots).await
                    {
                        if !matches!(error, ArchiveLogsError::NoDebugDirYet) {
                            error!(
                                self.log,
                                "Failure while trying to archive extra \
                                 zone roots";
                                InlineErrorChain::new(&error),
                            );
                        }
                    }

                    if let Err(()) = completion_tx.send(()) {
                        warn!(
                            self.log,
                            "failed to notify archive_former_zone_roots() \
                             (channel closed)"
                        );
                    }
                }
                Ok(None) => {
                    warn!(
                        self.log,
                        "Control channel closed, no more dump archival!"
                    );
                    break;
                }
                Err(_elapsed) => {
                    // no new disks, just pump cores/logs with what we've got
                }
            }
            // regardless of whether we updated disks,
            // at least every ARCHIVAL_INTERVAL,
            // figure out if we should change our target volumes...
            self.reevaluate_choices().await;
            // and then do the actual archiving.
            if let Err(err) = self.archive_files().await {
                error!(self.log, "Failed to archive debug/dump files: {err:?}");
            }

            if let Some(tx) = evaluation_and_archiving_complete_tx.take() {
                if let Err(err) = tx.send(()) {
                    error!(self.log, "DumpDevice failed to notify caller"; "err" => ?err);
                }
            }
        }
    }

    fn update_disk_loadout(
        &mut self,
        dump_slices: Vec<DumpSlicePath>,
        debug_datasets: Vec<DebugZpool>,
        core_datasets: Vec<CoreZpool>,
    ) {
        info!(
            self.log,
            "Updated view of disks";
            "core_datasets" => %core_datasets.len(),
            "debug_datasets" => %debug_datasets.len(),
            "dump_slices" => %dump_slices.len(),
        );
        self.core_dataset_names = core_datasets;
        self.debug_dataset_names = debug_datasets;
        self.known_dump_slices = dump_slices;
    }

    // only allow mounted zfs datasets into 'known_*_dirs',
    // such that we don't render them non-auto-mountable by zfs
    fn update_mounted_dirs(&mut self) {
        self.known_debug_dirs = self
            .debug_dataset_names
            .iter()
            .flat_map(|ds| ds.mountpoint(self.zfs_invoker.as_ref()))
            .flatten()
            .collect();
        self.known_core_dirs = self
            .core_dataset_names
            .iter()
            .flat_map(|ds| ds.mountpoint(self.zfs_invoker.as_ref()))
            .flatten()
            .collect();
    }

    async fn reevaluate_choices(&mut self) {
        self.update_mounted_dirs();

        self.known_dump_slices.sort();
        // sort key: prefer to choose a dataset where there's already other
        // dumps so we don't shotgun them across every U.2, but only if they're
        // below a certain usage threshold.
        self.known_debug_dirs.sort_by_cached_key(
            |mountpoint: &DebugDataset| {
                match self.zfs_invoker.below_thresh(mountpoint.as_ref(), DATASET_USAGE_PERCENT_CHOICE) {
                    Ok((below, used)) => {
                        let priority = if below { 0 } else { 1 };
                        (priority, used, mountpoint.clone())
                    }
                    Err(err) => {
                        error!(self.log, "Could not query zfs properties of debug dump dir: {err:?}");
                        // deprioritize anything we get errors querying.
                        (usize::MAX, u64::MAX, mountpoint.clone())
                    }
                }
            },
        );
        self.known_core_dirs.sort_by_cached_key(|mnt| {
            // these get archived periodically anyway, pick one with room
            let available = self
                .zfs_invoker
                .zfs_get_integer(mnt.as_ref().as_str(), "available")
                .unwrap_or(0);
            (u64::MAX - available, mnt.clone())
        });

        if let Some(x) = &self.chosen_debug_dir {
            if !self.known_debug_dirs.contains(x) {
                warn!(
                    self.log,
                    "Previously-chosen debug/dump dir {x:?} no longer exists in our view of reality"
                );
                self.chosen_debug_dir = None;
            } else {
                match self
                    .zfs_invoker
                    .below_thresh(x.as_ref(), DATASET_USAGE_PERCENT_CLEANUP)
                {
                    Ok((true, _)) => {}
                    Ok((false, _)) => {
                        if self.known_debug_dirs.iter().any(|x| {
                            self.zfs_invoker
                                .below_thresh(
                                    x.as_ref(),
                                    DATASET_USAGE_PERCENT_CHOICE,
                                )
                                .unwrap_or((false, 0))
                                .0
                        }) {
                            info!(
                                self.log,
                                "Previously-chosen debug/dump dir {x:?} is over usage threshold, choosing a more vacant disk"
                            );
                            self.chosen_debug_dir = None;
                        } else {
                            warn!(
                                self.log,
                                "All candidate debug/dump dirs are over usage threshold, removing older archived files"
                            );
                            if let Err(err) = self.cleanup().await {
                                error!(
                                    self.log,
                                    "Couldn't clean up any debug/dump dirs, may hit dataset quota in {x:?}: {err:?}"
                                );
                            } else {
                                self.chosen_debug_dir = None;
                            }
                        }
                    }
                    Err(err) => {
                        error!(
                            self.log,
                            "Previously-chosen debug/dump dir {x:?} couldn't be queried for zfs properties!  Choosing another. {err:?}"
                        );
                        self.chosen_debug_dir = None;
                    }
                }
            }
        }
        if let Some(x) = &self.chosen_dump_slice {
            if !self.known_dump_slices.contains(x) {
                warn!(
                    self.log,
                    "Previously-chosen dump slice {x:?} no longer exists in our view of reality"
                );
                self.chosen_dump_slice = None;
            }
        }
        if let Some(x) = &self.chosen_core_dir {
            if !self.known_core_dirs.contains(x) {
                warn!(
                    self.log,
                    "Previously-chosen core dir {x:?} no longer exists in our view of reality"
                );
                self.chosen_core_dir = None;
            }
        }

        if self.chosen_debug_dir.is_none() {
            self.chosen_debug_dir = self.known_debug_dirs.first().cloned();
        }

        if self.chosen_core_dir.is_none() {
            for core_dir in &self.known_core_dirs {
                // tell the system to write *userspace process* cores here.
                match self.coredumpadm_invoker.coreadm(core_dir.as_ref()) {
                    Ok(()) => {
                        self.chosen_core_dir = Some(core_dir.clone());
                        info!(
                            self.log,
                            "Set process core dump directory to {core_dir:?}"
                        );
                        break;
                    }
                    Err(err) => {
                        error!(
                            self.log,
                            "Couldn't configure process core dump directory to {core_dir:?}: {err:?}"
                        );
                    }
                }
            }
        }

        if self.chosen_dump_slice.is_none() {
            if self.chosen_debug_dir.is_some() {
                for dump_slice in self.known_dump_slices.clone() {
                    // Let's try to see if it appears to have a kernel dump already
                    match illumos_utils::dumpadm::dump_flag_is_valid(
                        dump_slice.as_ref(),
                    )
                    .await
                    {
                        Ok(true) => {
                            debug!(
                                self.log,
                                "Dump slice {dump_slice:?} appears to have a valid header; will attempt to savecore"
                            );
                        }
                        Ok(false) => {
                            info!(
                                self.log,
                                "Dump slice {dump_slice:?} appears to have already been saved"
                            );
                        }
                        Err(err) => {
                            debug!(
                                self.log,
                                "Dump slice {dump_slice:?} appears to be unused: {err:?}"
                            );
                        }
                    }
                    if let Ok(saved) =
                        self.dumpadm_and_savecore(&dump_slice).await
                    {
                        if let Some(out) = saved {
                            info!(
                                self.log,
                                "Previous dump on slice {dump_slice:?} saved, configured slice as target for new dumps. {out:?}"
                            );
                        }
                        self.chosen_dump_slice = Some(dump_slice);
                        break;
                    }
                }
            } else {
                // Don't risk overwriting an existing kernel dump if there's
                // already one there until we can attempt to savecore(8)
                // it away and clear the flag to make room.
                for dump_slice in &self.known_dump_slices {
                    match illumos_utils::dumpadm::dump_flag_is_valid(
                        dump_slice.as_ref(),
                    )
                    .await
                    {
                        Ok(false) => {
                            // Have dumpadm write the config for crash dumps to be
                            // on this slice, at least, until a U.2 comes along.
                            match self
                                .coredumpadm_invoker
                                .dumpadm(dump_slice.as_ref(), None)
                                .await
                            {
                                Ok(_) => {
                                    info!(
                                        self.log,
                                        "Using dump device {dump_slice:?} with no savecore destination (no U.2 debug zvol yet)"
                                    );
                                    self.chosen_dump_slice =
                                        Some(dump_slice.clone());
                                    break;
                                }
                                Err(err) => {
                                    warn!(
                                        self.log,
                                        "Could not configure {dump_slice:?} as dump device: {err:?}"
                                    );
                                }
                            }
                        }
                        Ok(true) => {
                            warn!(
                                self.log,
                                "Not configuring {dump_slice:?} as it appears to contain a dump we cannot yet send to a U.2 debug zvol"
                            );
                        }
                        Err(err) => {
                            debug!(
                                self.log,
                                "Dump slice {dump_slice:?} appears to be unused : {err:?}",
                            );
                        }
                    }
                }
            }
        }

        if let Some(debug_dir) = self.chosen_debug_dir.clone() {
            let mut changed_slice = false;
            for dump_slice in self.known_dump_slices.clone() {
                if !self.savecored_slices.contains(&dump_slice) {
                    changed_slice = true;
                    // temporarily changes the system's dump slice so savecore(8)
                    // can update the header in the slice when it finishes...
                    match self.dumpadm_and_savecore(&dump_slice).await {
                        Ok(saved) => {
                            if let Some(stdout) = &saved {
                                info!(
                                    self.log,
                                    "Saved dump from {dump_slice:?} to {debug_dir:?}: {stdout:?}"
                                );
                            } else {
                                info!(
                                    self.log,
                                    "Set {dump_slice:?} as system dump slice",
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                self.log,
                                "Could not configure {dump_slice:?} as dump device with {debug_dir:?} as savecore destination: {err:?}"
                            );
                        }
                    }
                }
            }

            // ...so then we restore the chosen dump slice for the system to use
            // in the event of a kernel crash
            if changed_slice {
                if let Some(dump_slice) = &self.chosen_dump_slice {
                    if let Err(err) = self
                        .coredumpadm_invoker
                        .dumpadm(dump_slice.as_ref(), None)
                        .await
                    {
                        error!(
                            self.log,
                            "Could not restore dump slice to {dump_slice:?}: {err:?}"
                        );
                    }
                }
            }
        }
    }

    async fn archive_files(&self) -> tokio::io::Result<()> {
        if let Some(debug_dir) = &self.chosen_debug_dir {
            if self.known_core_dirs.is_empty() {
                info!(self.log, "No core dump locations yet known.");
            }
            for core_dir in &self.known_core_dirs {
                if let Ok(dir) = core_dir.as_ref().read_dir() {
                    for entry in dir.flatten() {
                        if let Some(path) = entry.file_name().to_str() {
                            let dest = debug_dir.as_ref().join(path);

                            if let Err(err) =
                                Self::copy_sync_and_remove(&entry.path(), &dest)
                                    .await
                            {
                                error!(
                                    self.log,
                                    "Failed to archive {entry:?}: {err:?}"
                                );
                            } else {
                                info!(
                                    self.log,
                                    "Relocated {entry:?} to {dest:?}"
                                );
                            }
                        } else {
                            error!(
                                self.log,
                                "Non-UTF8 path found while archiving core dumps: {entry:?}"
                            );
                        }
                    }
                }
            }
        } else {
            info!(
                self.log,
                "No archival destination for crash dumps yet chosen."
            );
        }

        if let Err(err) = self.archive_logs_from_running_zones().await {
            if !matches!(err, ArchiveLogsError::NoDebugDirYet) {
                error!(
                    self.log,
                    "Failure while trying to archive logs to debug dataset: {err:?}"
                );
            }
        }

        Ok(())
    }

    async fn copy_sync_and_remove(
        source: impl AsRef<Path>,
        dest: impl AsRef<Path>,
    ) -> tokio::io::Result<()> {
        let source = source.as_ref();
        let dest = dest.as_ref();
        let mut dest_f = tokio::fs::File::create(&dest).await?;
        let mut src_f = tokio::fs::File::open(&source).await?;

        tokio::io::copy(&mut src_f, &mut dest_f).await?;

        dest_f.sync_all().await?;

        drop(src_f);
        drop(dest_f);

        tokio::fs::remove_file(source).await?;
        Ok(())
    }

    async fn archive_logs_from_running_zones(
        &self,
    ) -> Result<(), ArchiveLogsError> {
        let debug_dir = self
            .chosen_debug_dir
            .as_ref()
            .ok_or(ArchiveLogsError::NoDebugDirYet)?;
        let oxz_zones = self.zone_invoker.get_zones().await?;
        for zone in oxz_zones {
            let logdir = if zone.global() {
                PathBuf::from("/var/svc/log")
            } else {
                zone.path().join("root/var/svc/log")
            };
            let zone_name = zone.name();
            self.archive_logs_from_zone_path(
                debug_dir, logdir, zone_name, false,
            )
            .await?;
        }
        Ok(())
    }

    async fn archive_extra_former_zone_roots(
        &self,
        zone_roots: &[FormerZoneRoot],
    ) -> Result<(), ArchiveLogsError> {
        let debug_dir = self
            .chosen_debug_dir
            .as_ref()
            .ok_or(ArchiveLogsError::NoDebugDirYet)?;
        for zone_root in zone_roots {
            let logdir = zone_root.path.join("root/var/svc/log");
            let zone_name = &zone_root.zone_name;
            self.archive_logs_from_zone_path(
                debug_dir,
                logdir.into(),
                zone_name,
                true,
            )
            .await?;
        }
        Ok(())
    }

    async fn archive_logs_from_zone_path(
        &self,
        debug_dir: &DebugDataset,
        logdir: PathBuf,
        zone_name: &str,
        include_live: bool,
    ) -> Result<(), ArchiveLogsError> {
        let mut rotated_log_files = Vec::new();
        if include_live {
            let pattern = logdir
                .join(format!("*.log"))
                .to_str()
                .ok_or_else(|| ArchiveLogsError::Utf8(zone_name.to_string()))?
                .to_string();
            rotated_log_files.extend(glob::glob(&pattern)?.flatten());
        } else {
            // patterns matching archived logs, e.g. foo.log.3
            // keep checking for greater numbers of digits until we don't find
            // any
            for n in 1..9 {
                let pattern = logdir
                    .join(format!("*.log.{}", "[0-9]".repeat(n)))
                    .to_str()
                    .ok_or_else(|| {
                        ArchiveLogsError::Utf8(zone_name.to_string())
                    })?
                    .to_string();
                rotated_log_files.extend(glob::glob(&pattern)?.flatten());
            }
        }
        let dest_dir = debug_dir.as_ref().join(zone_name).into_std_path_buf();
        if !rotated_log_files.is_empty() {
            tokio::fs::create_dir_all(&dest_dir).await?;
            let count = rotated_log_files.len();
            info!(
                self.log,
                "Archiving {count} log files from {zone_name} zone"
            );
        }
        for entry in rotated_log_files {
            let src_name = entry.file_name().unwrap();
            // as we archive them, logadm will keep resetting to .log.0,
            // so we need to maintain our own numbering in the dest dataset.
            // we'll use the modified date of the rotated log file, or try
            // falling back to the time of archival if that fails, and
            // falling back to counting up from 0 if *that* somehow fails.
            let mut n = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or_else(|_| SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let mut dest;
            loop {
                dest = dest_dir.join(src_name).with_extension(format!("{n}"));
                if dest.exists() {
                    n += 1;
                } else {
                    break;
                }
            }
            if let Err(err) = Self::copy_sync_and_remove(&entry, dest).await {
                warn!(self.log, "Failed to archive {entry:?}: {err:?}");
            }
        }
        Ok(())
    }

    // Have dumpadm write the config for crash dumps to be
    // on this slice, and then invoke savecore(8) to save any
    // dump that's already present there.
    //
    // NOTE: because of the need to have dumpadm change the global
    // state of which slice the system is using for dumps in order
    // for savecore to behave the way we want (i.e. clear the flag
    // after succeeding), we could hypothetically miss a dump if
    // the kernel crashes again while savecore is still running.
    async fn dumpadm_and_savecore(
        &mut self,
        dump_slice: &DumpSlicePath,
    ) -> Result<Option<OsString>, ExecutionError> {
        // TODO: untangle savecore from illumos_utils::dumpadm
        assert!(self.chosen_debug_dir.is_some());

        let savecore_dir = self.chosen_debug_dir.clone().unwrap().0;

        match self
            .coredumpadm_invoker
            .dumpadm(dump_slice.as_ref(), Some(&savecore_dir))
            .await
        {
            Ok(saved) => {
                self.savecored_slices.insert(dump_slice.clone());
                Ok(saved)
            }
            Err(err) => Err(err),
        }
    }

    async fn cleanup(&self) -> Result<(), CleanupError> {
        let mut dir_info = Vec::new();
        for dir in &self.known_debug_dirs {
            match self.scope_dir_for_cleanup(dir).await {
                Ok(info) => {
                    dir_info.push((info, dir));
                }
                Err(err) => {
                    error!(
                        self.log,
                        "Could not analyze {dir:?} for debug dataset cleanup task: {err:?}"
                    );
                }
            }
        }
        if dir_info.is_empty() {
            return Err(CleanupError::NoDatasetsToClean);
        }
        // find dir with oldest average time of files that must be deleted
        // to achieve desired threshold, and reclaim that space.
        dir_info.sort();
        'next_debug_dir: for (dir_info, dir) in dir_info {
            let CleanupDirInfo { average_time: _, num_to_delete, file_list } =
                dir_info;
            for (_time, _bytes, path) in &file_list[..num_to_delete as usize] {
                info!(
                    self.log,
                    "Removing file on debug dataset to make space";
                    "path" => %path
                );
                // if we are unable to remove a file, we cannot guarantee
                // that we will reach our target size threshold, and suspect
                // the i/o error *may* be an issue with the underlying disk, so
                // we continue to the dataset with the next-oldest average age
                // of files-to-delete in the sorted list.
                if let Err(err) = tokio::fs::remove_file(&path).await {
                    error!(
                        self.log,
                        "Couldn't delete {path} from debug dataset, skipping {dir:?}. {err:?}"
                    );
                    continue 'next_debug_dir;
                }
            }
            // we made it through all the files we planned to remove, thereby
            // freeing up enough space on one of the debug datasets for it to
            // be chosen when reevaluating targets.
            break;
        }
        Ok(())
    }

    async fn scope_dir_for_cleanup(
        &self,
        debug_dir: &DebugDataset,
    ) -> Result<CleanupDirInfo, CleanupError> {
        let used = self
            .zfs_invoker
            .zfs_get_integer(debug_dir.as_ref().as_str(), ZFS_PROP_USED)?;
        let available = self
            .zfs_invoker
            .zfs_get_integer(debug_dir.as_ref().as_str(), ZFS_PROP_AVAILABLE)?;
        let capacity = used + available;

        let target_used = capacity * DATASET_USAGE_PERCENT_CHOICE / 100;

        let mut file_list = Vec::new();
        // find all files in the debug dataset and sort by modified time
        for path in
            glob::glob(debug_dir.as_ref().join("**/*").as_str())?.flatten()
        {
            let path = Utf8PathBuf::try_from(path)?;
            let meta = match tokio::fs::metadata(&path).await {
                Ok(meta) => meta,
                // "Not found" errors could be caused by TOCTTOU -- observing a
                // file which gets renamed, removed, etc. We'll ignore these
                // files.
                Err(err)
                    if matches!(err.kind(), std::io::ErrorKind::NotFound) =>
                {
                    info!(
                        self.log,
                        "File was removed before metadata could be read - ignoring";
                        "path" => %path
                    );
                    continue;
                }
                // Other errors get propagated.
                Err(err) => {
                    warn!(
                        self.log,
                        "Debug auto-cleaner could not read metadata from path";
                        "err" => %err,
                        "path" => %path
                    );
                    return Err(err.into());
                }
            };

            if !safe_to_delete(&path, &meta) {
                trace!(self.log, "Debug auto-cleaner ignoring file: {path}");
                continue;
            }

            // we need this to be a Duration rather than SystemTime so we can
            // do math to it later.
            let time = meta.modified()?.duration_since(UNIX_EPOCH)?;
            let size = meta.len();

            file_list.push((time, size, path));
        }
        file_list.sort();

        // find how many old files must be deleted to get the dataset under
        // the limit, and what the average age of that set is.
        let mut possible_bytes = 0;
        let mut total_time = Duration::ZERO;
        let mut num_to_delete = 0;
        for (time, size, _path) in &file_list {
            if used - possible_bytes < target_used {
                break;
            } else {
                total_time += *time;
                num_to_delete += 1;
                possible_bytes += size;
            }
        }
        let average_time =
            total_time.checked_div(num_to_delete).unwrap_or(Duration::MAX);

        Ok(CleanupDirInfo { average_time, num_to_delete, file_list })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ArchiveLogsError {
    #[error("I/O error: {0}")]
    IoError(#[from] tokio::io::Error),
    #[error("Error calling zoneadm: {0}")]
    Zoneadm(#[from] ZoneError),
    #[error("Non-UTF8 zone path for zone {0}")]
    Utf8(String),
    #[error("Glob pattern invalid: {0}")]
    Glob(#[from] glob::PatternError),
    #[error(
        "No debug dir into which we should archive logs has yet been chosen"
    )]
    NoDebugDirYet,
}

#[derive(thiserror::Error, Debug)]
enum CleanupError {
    #[error("No debug datasets were successfully evaluated for cleanup")]
    NoDatasetsToClean,
    #[error("Failed to parse path as UTF-8")]
    ParsePath(#[from] camino::FromPathBufError),
    #[error("Failed to query ZFS properties")]
    ZfsError(#[from] ZfsGetError),
    #[error("I/O error during cleanup")]
    IoError(#[from] tokio::io::Error),
    #[error("Glob pattern invalid")]
    Glob(#[from] glob::PatternError),
    #[error("A file's observed modified time was before the Unix epoch")]
    TimelineWentSideways(#[from] SystemTimeError),
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct CleanupDirInfo {
    average_time: Duration,
    num_to_delete: u32,
    file_list: Vec<(Duration, u64, Utf8PathBuf)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use illumos_utils::dumpadm::{
        DF_VALID, DUMP_MAGIC, DUMP_OFFSET, DUMP_VERSION,
    };
    use sled_storage::dataset::{CRASH_DATASET, DUMP_DATASET};
    use std::collections::HashMap;
    use std::str::FromStr;
    use tokio::io::AsyncWriteExt;

    impl Clone for ZfsGetError {
        fn clone(&self) -> Self {
            match self {
                ZfsGetError::IoError(_err) => unimplemented!(),
                ZfsGetError::Utf8(err) => ZfsGetError::Utf8(err.clone()),
                ZfsGetError::Parse(err) => ZfsGetError::Parse(err.clone()),
            }
        }
    }

    #[derive(Default)]
    struct FakeCoreDumpAdm {}
    #[derive(Default)]
    struct FakeZfs {
        pub zpool_props:
            HashMap<String, HashMap<&'static str, Result<String, ZfsGetError>>>,
    }
    #[derive(Default)]
    struct FakeZone {
        pub zones: Vec<Zone>,
    }

    #[async_trait]
    impl CoreDumpAdmInvoker for FakeCoreDumpAdm {
        fn coreadm(
            &self,
            _core_dir: &Utf8PathBuf,
        ) -> Result<(), ExecutionError> {
            Ok(())
        }

        async fn dumpadm(
            &self,
            _dump_slice: &Utf8PathBuf,
            _savecore_dir: Option<&Utf8PathBuf>,
        ) -> Result<Option<OsString>, ExecutionError> {
            Ok(None)
        }
    }
    impl ZfsInvoker for FakeZfs {
        fn zfs_get_prop(
            &self,
            mountpoint_or_name: &str,
            property: &str,
        ) -> Result<String, ZfsGetError> {
            self.zpool_props
                .get(mountpoint_or_name)
                .unwrap_or_else(|| {
                    panic!(
                        "Test did not provide fake zpool {}",
                        mountpoint_or_name
                    )
                })
                .get(property)
                .unwrap_or_else(|| {
                    panic!(
                        "Test did not provide property {property} for fake zpool {}",
                        mountpoint_or_name
                    )
                })
                .clone()
        }

        fn mountpoint(
            &self,
            _mount_config: &MountConfig,
            zpool: &ZpoolName,
            mountpoint: &'static str,
        ) -> Utf8PathBuf {
            Utf8PathBuf::from(
                self.zpool_props
                    .get(zpool.to_string().as_str())
                    .unwrap_or_else(|| {
                        panic!("Test did not provide fake zpool {}", zpool)
                    })
                    .get("mountpoint")
                    .unwrap_or_else(|| {
                        panic!(
                            "Test did not provide mountpoint for fake zpool {}",
                            zpool
                        )
                    })
                    .clone()
                    .unwrap(),
            )
            .join(mountpoint)
        }
    }
    #[async_trait]
    impl ZoneInvoker for FakeZone {
        async fn get_zones(&self) -> Result<Vec<Zone>, ArchiveLogsError> {
            Ok(self.zones.clone())
        }
    }

    #[tokio::test]
    async fn test_does_not_configure_coreadm_when_no_crash_dataset_mounted() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_does_not_configure_coreadm_when_no_crash_dataset_mounted",
        );
        const NOT_MOUNTED_INTERNAL: &str =
            "oxi_acab2069-6e63-6c75-de73-20c06c756db0";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [(
                    NOT_MOUNTED_INTERNAL.to_string(),
                    [("mounted", Ok("no".to_string()))].into_iter().collect(),
                )]
                .into_iter()
                .collect(),
            }),
            Box::<FakeZone>::default(),
            logctx.log.clone(),
            tokio::sync::mpsc::channel(1).1,
        );

        // nothing when no disks
        worker.update_disk_loadout(vec![], vec![], vec![]);
        worker.reevaluate_choices().await;
        assert_eq!(worker.chosen_core_dir, None);

        // nothing when only a disk that's not ready
        let non_mounted_zpool = CoreZpool {
            mount_config: Arc::default(),
            name: ZpoolName::from_str(NOT_MOUNTED_INTERNAL).unwrap(),
        };
        worker.update_disk_loadout(vec![], vec![], vec![non_mounted_zpool]);
        worker.reevaluate_choices().await;
        assert_eq!(worker.chosen_core_dir, None);
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_configures_coreadm_only_when_crash_dataset_mounted() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_configures_coreadm_only_when_crash_dataset_mounted",
        );
        const NOT_MOUNTED_INTERNAL: &str =
            "oxi_acab2069-6e63-6c75-de73-20c06c756db0";
        const MOUNTED_INTERNAL: &str =
            "oxi_474e554e-6174-616c-6965-4e677579656e";
        const ERROR_INTERNAL: &str = "oxi_4861636b-2054-6865-2050-6c616e657421";
        let mount_config = Arc::new(MountConfig::default());
        let mounted_zpool = CoreZpool {
            mount_config: Arc::clone(&mount_config),
            name: ZpoolName::from_str(MOUNTED_INTERNAL).unwrap(),
        };
        let non_mounted_zpool = CoreZpool {
            mount_config: Arc::clone(&mount_config),
            name: ZpoolName::from_str(NOT_MOUNTED_INTERNAL).unwrap(),
        };
        let err_zpool = CoreZpool {
            mount_config,
            name: ZpoolName::from_str(ERROR_INTERNAL).unwrap(),
        };
        const ZPOOL_MNT: &str = "/path/to/internal/zpool";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [
                    (
                        NOT_MOUNTED_INTERNAL.to_string(),
                        [("mounted", Ok("no".to_string()))]
                            .into_iter()
                            .collect(),
                    ),
                    (
                        MOUNTED_INTERNAL.to_string(),
                        [
                            ("mounted", Ok("yes".to_string())),
                            ("mountpoint", Ok(ZPOOL_MNT.to_string())),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    (
                        ERROR_INTERNAL.to_string(),
                        [(
                            "mounted",
                            Err("asdf".parse::<u32>().unwrap_err().into()),
                        )]
                        .into_iter()
                        .collect(),
                    ),
                ]
                .into_iter()
                .collect(),
            }),
            Box::<FakeZone>::default(),
            logctx.log.clone(),
            tokio::sync::mpsc::channel(1).1,
        );

        // something when there's one that's ready!
        worker.update_disk_loadout(
            vec![],
            vec![],
            vec![non_mounted_zpool.clone(), mounted_zpool],
        );
        worker.reevaluate_choices().await;
        assert_eq!(
            worker.chosen_core_dir.as_ref().expect("core dir wasn't chosen").0,
            Utf8PathBuf::from(ZPOOL_MNT).join(CRASH_DATASET)
        );

        // back to nothing if it becomes unavailable
        worker.update_disk_loadout(
            vec![],
            vec![],
            vec![non_mounted_zpool, err_zpool],
        );
        worker.reevaluate_choices().await;
        assert_eq!(worker.chosen_core_dir, None);
        logctx.cleanup_successful();
    }

    // we make these so illumos_utils::dumpadm::dump_flag_is_valid returns what we want
    async fn populate_tempdir_with_fake_dumps(
        tempdir: &Utf8TempDir,
    ) -> (DumpSlicePath, DumpSlicePath) {
        let occupied = DumpSlicePath(tempdir.path().join("occupied.bin"));
        let mut f = tokio::fs::File::create(occupied.as_ref()).await.unwrap();
        f.write_all(&[0u8; DUMP_OFFSET as usize]).await.unwrap();
        f.write_all(&DUMP_MAGIC.to_le_bytes()).await.unwrap();
        f.write_all(&DUMP_VERSION.to_le_bytes()).await.unwrap();
        f.write_all(&DF_VALID.to_le_bytes()).await.unwrap();
        drop(f);

        let vacant = DumpSlicePath(tempdir.path().join("vacant.bin"));
        let mut f = tokio::fs::File::create(vacant.as_ref()).await.unwrap();
        f.write_all(&[0u8; DUMP_OFFSET as usize]).await.unwrap();
        f.write_all(&DUMP_MAGIC.to_le_bytes()).await.unwrap();
        f.write_all(&DUMP_VERSION.to_le_bytes()).await.unwrap();
        f.write_all(&0u32.to_le_bytes()).await.unwrap();
        drop(f);

        (occupied, vacant)
    }

    // if we only have two filled dump slices and nowhere to evacuate them,
    // don't configure a dump slice at all.
    #[tokio::test]
    async fn test_savecore_and_dumpadm_not_called_when_occupied_and_no_dir() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_savecore_and_dumpadm_not_called_when_occupied_and_no_dir",
        );
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::<FakeZfs>::default(),
            Box::<FakeZone>::default(),
            logctx.log.clone(),
            tokio::sync::mpsc::channel(1).1,
        );
        let tempdir = Utf8TempDir::new().unwrap();
        let (occupied, _) = populate_tempdir_with_fake_dumps(&tempdir).await;

        worker.update_disk_loadout(
            vec![occupied.clone(), occupied],
            vec![],
            vec![],
        );
        worker.reevaluate_choices().await;
        assert!(worker.chosen_dump_slice.is_none());
        logctx.cleanup_successful();
    }

    // if we have one dump slice that's free and one that's full,
    // and nowhere to savecore the full one,
    // we should always call dumpadm with the free one.
    #[tokio::test]
    async fn test_dumpadm_called_when_vacant_slice_but_no_dir() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_dumpadm_called_when_vacant_slice_but_no_dir",
        );
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::<FakeZfs>::default(),
            Box::<FakeZone>::default(),
            logctx.log.clone(),
            tokio::sync::mpsc::channel(1).1,
        );
        let tempdir = Utf8TempDir::new().unwrap();
        let (occupied, vacant) =
            populate_tempdir_with_fake_dumps(&tempdir).await;
        worker.update_disk_loadout(
            vec![occupied, vacant.clone()],
            vec![],
            vec![],
        );
        worker.reevaluate_choices().await;
        assert_eq!(worker.chosen_dump_slice.as_ref(), Some(&vacant));
        logctx.cleanup_successful();
    }

    // if we have two occupied dump slices,
    // but we also have somewhere to unload them,
    // call dumpadm and savecore.
    #[tokio::test]
    async fn test_savecore_and_dumpadm_invoked_when_slices_occupied_and_dir_is_available()
     {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_savecore_and_dumpadm_invoked_when_slices_occupied_and_dir_is_available",
        );
        const MOUNTED_EXTERNAL: &str =
            "oxp_446f6e74-4469-6557-6f6e-646572696e67";
        const ZPOOL_MNT: &str = "/path/to/external/zpool";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [(
                    MOUNTED_EXTERNAL.to_string(),
                    [
                        ("mounted", Ok("yes".to_string())),
                        ("mountpoint", Ok(ZPOOL_MNT.to_string())),
                    ]
                    .into_iter()
                    .collect(),
                )]
                .into_iter()
                .collect(),
            }),
            Box::<FakeZone>::default(),
            logctx.log.clone(),
            tokio::sync::mpsc::channel(1).1,
        );
        let tempdir = Utf8TempDir::new().unwrap();
        let (occupied, _) = populate_tempdir_with_fake_dumps(&tempdir).await;

        let mounted_zpool = DebugZpool {
            mount_config: Arc::default(),
            name: ZpoolName::from_str(MOUNTED_EXTERNAL).unwrap(),
        };
        worker.update_disk_loadout(
            vec![occupied.clone()],
            vec![mounted_zpool],
            vec![],
        );
        worker.reevaluate_choices().await;

        assert_eq!(worker.chosen_dump_slice.as_ref(), Some(&occupied));
        assert_eq!(
            worker.chosen_debug_dir.unwrap().0,
            Utf8PathBuf::from(ZPOOL_MNT).join(DUMP_DATASET)
        );
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_archives_rotated_logs_and_cores() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_archives_rotated_logs_and_cores",
        );

        let tempdir = Utf8TempDir::new().unwrap();
        let core_dir = tempdir.path().join(CRASH_DATASET);
        let debug_dir = tempdir.path().join(DUMP_DATASET);
        let zone_logs = tempdir.path().join("root/var/svc/log");

        let tempdir_path = tempdir.path().as_str().to_string();
        let zone = Zone::from_str(&format!(
            "1:myzone:running:{tempdir_path}::ipkg:shared"
        ))
        .unwrap();

        const MOUNTED_INTERNAL: &str =
            "oxi_474e554e-6174-616c-6965-4e677579656e";
        const MOUNTED_EXTERNAL: &str =
            "oxp_446f6e74-4469-6557-6f6e-646572696e67";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [
                    (
                        MOUNTED_INTERNAL.to_string(),
                        [
                            ("mounted", Ok("yes".to_string())),
                            ("mountpoint", Ok(tempdir_path.clone())),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    (
                        MOUNTED_EXTERNAL.to_string(),
                        [
                            ("mounted", Ok("yes".to_string())),
                            ("mountpoint", Ok(tempdir_path)),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                ]
                .into_iter()
                .collect(),
            }),
            Box::new(FakeZone { zones: vec![zone.clone()] }),
            logctx.log.clone(),
            tokio::sync::mpsc::channel(1).1,
        );

        tokio::fs::create_dir_all(&core_dir).await.unwrap();
        tokio::fs::create_dir_all(&debug_dir).await.unwrap();
        tokio::fs::create_dir_all(&zone_logs).await.unwrap();
        const LOG_NAME: &'static str = "foo.log.0";
        tokio::fs::File::create(zone_logs.join(LOG_NAME))
            .await
            .expect("creating fake log")
            .write_all(b"hello")
            .await
            .expect("writing fake log");

        const CORE_NAME: &str = "core.myzone.myexe.123.1690540950";
        tokio::fs::File::create(core_dir.join(CORE_NAME))
            .await
            .expect("creating fake core")
            .write_all(b"crunch")
            .await
            .expect("writing fake core");

        let mount_config = Arc::new(MountConfig::default());
        let mounted_core_zpool = CoreZpool {
            mount_config: Arc::clone(&mount_config),
            name: ZpoolName::from_str(MOUNTED_INTERNAL).unwrap(),
        };
        let mounted_debug_zpool = DebugZpool {
            mount_config,
            name: ZpoolName::from_str(MOUNTED_EXTERNAL).unwrap(),
        };

        worker.update_disk_loadout(
            vec![],
            vec![mounted_debug_zpool],
            vec![mounted_core_zpool],
        );
        worker.reevaluate_choices().await;
        worker.archive_files().await.unwrap();

        // it'll be renamed to use an epoch timestamp instead of .0
        let log_glob =
            debug_dir.join(zone.name()).join(LOG_NAME.replace(".0", ".*"));
        assert_eq!(glob::glob(log_glob.as_str()).unwrap().count(), 1);
        assert!(!zone_logs.join(LOG_NAME).is_file());
        assert!(debug_dir.join(CORE_NAME).is_file());
        assert!(!core_dir.join(CORE_NAME).is_file());
        logctx.cleanup_successful();
    }

    fn create_test_file(path: &Utf8Path, size: u64, time: SystemTime) {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .expect("Cannot create parent directories");
            }
        }
        let file = std::fs::File::create(path).expect("Cannot create file");
        file.set_len(size).expect("Failed to set file size");
        file.set_modified(time).expect("Failed to set mtime");
    }

    fn assert_all_files_exist(paths: &[&Utf8Path]) {
        for path in paths {
            assert!(path.exists(), "Expected {path} to exist");
        }
    }

    fn assert_all_files_do_not_exist(paths: &[&Utf8Path]) {
        for path in paths {
            assert!(!path.exists(), "Expected {path} not to exist");
        }
    }

    // Utility to help with testing "debug dataset file cleanup".
    struct TestDebugFileHarness {
        log: Logger,
        tempdir: Utf8TempDir,

        debug_path: Utf8PathBuf,
        mtime: SystemTime,
        files: Vec<TestFile>,
    }

    impl TestDebugFileHarness {
        // Creates a new test file harness within a particular debug directory
        async fn new(log: &Logger) -> Self {
            let tempdir = Utf8TempDir::new().unwrap();
            let debug_dir = tempdir.path().join(DUMP_DATASET);
            tokio::fs::create_dir_all(&debug_dir).await.unwrap();

            Self {
                log: log.clone(),
                tempdir,
                debug_path: debug_dir.to_path_buf(),
                mtime: SystemTime::now(),
                files: vec![],
            }
        }

        async fn new_dump_setup_worker(
            &self,
            used: u64,
            available: u64,
        ) -> DumpSetupWorker {
            let tempdir_path = self.tempdir.path().to_string();
            const MOUNTED_EXTERNAL: &str =
                "oxp_446f6e74-4469-6557-6f6e-646572696e67";
            let mut worker = DumpSetupWorker::new(
                Box::<FakeCoreDumpAdm>::default(),
                Box::new(FakeZfs {
                    zpool_props: [
                        (
                            MOUNTED_EXTERNAL.to_string(),
                            [
                                ("mounted", Ok("yes".to_string())),
                                ("mountpoint", Ok(tempdir_path)),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                        (
                            self.debug_path.to_string(),
                            [
                                ("mounted", Ok("yes".to_string())),
                                ("mountpoint", Ok(self.debug_path.to_string())),
                                ("used", Ok(used.to_string())),
                                ("available", Ok(available.to_string())),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                }),
                Box::new(FakeZone { zones: vec![] }),
                self.log.clone(),
                tokio::sync::mpsc::channel(1).1,
            );

            let mounted_debug_zpool = DebugZpool {
                mount_config: Arc::default(),
                name: ZpoolName::from_str(MOUNTED_EXTERNAL).unwrap(),
            };

            worker.update_disk_loadout(
                vec![],
                vec![mounted_debug_zpool],
                vec![],
            );
            worker.reevaluate_choices().await;
            worker
        }

        // Plans to add a new file with a path inside the debug directory
        //
        // Not actually created until "Self::create_all" is invoked.
        fn add_file(&mut self, path: impl AsRef<Utf8Path>) -> &mut TestFile {
            assert!(
                path.as_ref().is_relative(),
                "We need to put this file inside a debug dir, it should be relative"
            );

            self.files.push(TestFile::new(
                self.debug_path.join(path.as_ref()),
                self.mtime,
            ));
            self.files.as_mut_slice().last_mut().unwrap()
        }

        // Validate that, against a particular capacity size, we think the
        // deletion of all "Marked for deletion" files will just barely cause
        // us to drop below the expected threshold size.
        fn confirm_that_deleting_marked_files_will_drop_below_capacity(
            &self,
            used: u64,
            capacity: u64,
        ) {
            let mut sizes_to_be_removed = self
                .files
                .iter()
                .filter_map(|f| {
                    if f.should_be_deleted { Some(f.size) } else { None }
                })
                .collect::<Vec<u64>>();
            sizes_to_be_removed.sort_by_key(|&s| std::cmp::Reverse(s));

            let mut cumulative_space_removed = 0;
            for file_size in sizes_to_be_removed {
                assert!(
                    used - cumulative_space_removed
                        > capacity * DATASET_USAGE_PERCENT_CHOICE / 100,
                    "Deleting files from largest -> smallest might clear dataset usage threshold unexpectedly early",
                );

                cumulative_space_removed += file_size;
            }
            assert!(
                used - cumulative_space_removed
                    < capacity * DATASET_USAGE_PERCENT_CHOICE / 100,
                "Deleting marked files won't reduce space usage below threshold",
            );
        }

        // Actually create all files (and their parent directories)
        fn create_all(&self) {
            for file in &self.files {
                create_test_file(&file.path, file.size, file.mtime);
            }
        }

        fn check_all_files_exist(&self) {
            let all_paths: Vec<_> =
                self.files.iter().map(|f| f.path.as_path()).collect();
            assert_all_files_exist(all_paths.as_slice());
        }

        fn check_expected_files_removed(&self) {
            let (should_be_deleted, should_still_exist): (Vec<_>, Vec<_>) =
                self.files.iter().partition(|f| f.should_be_deleted);
            let should_be_deleted = should_be_deleted
                .iter()
                .map(|f| f.path.as_path())
                .collect::<Vec<_>>();
            let should_still_exist = should_still_exist
                .iter()
                .map(|f| f.path.as_path())
                .collect::<Vec<_>>();

            assert_all_files_exist(should_still_exist.as_slice());
            assert_all_files_do_not_exist(should_be_deleted.as_slice());
        }
    }

    struct TestFile {
        path: Utf8PathBuf,
        size: u64,
        mtime: SystemTime,

        should_be_deleted: bool,
    }

    impl TestFile {
        fn new(path: Utf8PathBuf, mtime: SystemTime) -> Self {
            Self { path, size: 0, mtime, should_be_deleted: false }
        }

        // Set the size of the file length, once it gets created.
        fn set_size(&mut self, size: u64) -> &mut Self {
            self.size = size;
            self
        }

        // Pushes the timestamp back behind the baseline system time.
        //
        // This causes the file to appear more viable for deletion.
        fn make_older(&mut self) -> &mut Self {
            self.mtime -= Duration::from_secs(10);
            self
        }

        // Pushes the timestamp back far behind the baseline system time.
        //
        // This causes the file to appear more viable for deletion.
        fn make_much_older(&mut self) -> &mut Self {
            self.mtime -= Duration::from_secs(100);
            self
        }

        // Identifies that we THINK this file will be deleted when cleanup runs.
        //
        // This is used purely to validate the input / output files.
        // This information is not communicated to the cleanup function.
        fn should_be_deleted(&mut self) -> &mut Self {
            self.should_be_deleted = true;
            self
        }
    }

    #[tokio::test]
    async fn test_debug_dir_not_cleaned_under_threshold() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_debug_dir_not_cleaned_under_threshold",
        );

        const USED: u64 = 1024;
        const AVAILABLE: u64 = 1024;

        let mut files = TestDebugFileHarness::new(&logctx.log).await;

        files.add_file("test1.log").set_size(512);
        files.add_file("test2.log").set_size(512);
        files.create_all();

        // This is an assertion that "the test expectations are valid", before
        // we actually try to do any cleanup.
        //
        // In this test in particular, it's also a bit of a no-op -- we aren't
        // planning on deleting any files, because we're already below the
        // threshold -- but it'll flag loudly if that threshold changes.
        const CAPACITY: u64 = USED + AVAILABLE;
        files.confirm_that_deleting_marked_files_will_drop_below_capacity(
            USED, CAPACITY,
        );

        let worker = files.new_dump_setup_worker(USED, AVAILABLE).await;

        // Before we cleanup: All files in "debug" exist
        files.check_all_files_exist();
        worker.cleanup().await.unwrap();

        // After cleanup: All files still exist
        files.check_all_files_exist();

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_debug_dir_cleaned_over_threshold() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_debug_dir_cleaned_over_threshold",
        );

        const USED: u64 = 1024;
        const AVAILABLE: u64 = 0;

        // Create some test files.
        //
        // Their sizes sum to "USED".

        let mut files = TestDebugFileHarness::new(&logctx.log).await;
        files.add_file("test-1.log").set_size(256);
        files.add_file("test-2.log").set_size(256);
        files
            .add_file("test-3.log")
            .set_size(512)
            .make_older()
            .should_be_deleted();
        files.create_all();

        // This is an assertion that "the test expectations are valid", before
        // we actually try to do any cleanup.
        const CAPACITY: u64 = USED + AVAILABLE;
        files.confirm_that_deleting_marked_files_will_drop_below_capacity(
            USED, CAPACITY,
        );

        let worker = files.new_dump_setup_worker(USED, AVAILABLE).await;

        // Before we cleanup: All files in "debug" exist
        files.check_all_files_exist();
        worker.cleanup().await.unwrap();

        // After we cleanup: The files we marked as "should_be_deleted" are
        // removed, but the rest still exist.
        files.check_expected_files_removed();

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_debug_dir_cleaned_over_threshold_many_files() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_debug_dir_cleaned_over_threshold_many_files",
        );

        const USED: u64 = 1000;
        const AVAILABLE: u64 = 0;

        // Create some test files.
        //
        // Their sizes sum to "USED".

        let mut files = TestDebugFileHarness::new(&logctx.log).await;

        // 650 bytes "to be kept"
        files.add_file("global/test.log").set_size(300);
        files.add_file("foo-zone/test.log").set_size(300);
        files.add_file("bar-zone/test.log").set_size(50);

        // 350 bytes "to be removed"
        files
            .add_file("global/test-archived.log")
            .set_size(100)
            .make_older()
            .should_be_deleted();
        files
            .add_file("foo-zone/test-archived.log")
            .set_size(150)
            .make_older()
            .should_be_deleted();
        files
            .add_file("bar-zone/nested/test-archived.log")
            .set_size(100)
            .make_older()
            .should_be_deleted();
        files.create_all();

        // This is an assertion that "the test expectations are valid", before
        // we actually try to do any cleanup.
        const CAPACITY: u64 = USED + AVAILABLE;
        files.confirm_that_deleting_marked_files_will_drop_below_capacity(
            USED, CAPACITY,
        );

        let worker = files.new_dump_setup_worker(USED, AVAILABLE).await;

        // Before we cleanup: All files in "debug" exist
        files.check_all_files_exist();
        worker.cleanup().await.unwrap();

        // After we cleanup: The files we marked as "should_be_deleted" are
        // removed, but the rest still exist.
        files.check_expected_files_removed();

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_debug_dir_cleanup_ignores_support_bundles() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_debug_dir_cleanup_ignores_support_bundles",
        );

        const USED: u64 = 1000;
        const AVAILABLE: u64 = 0;

        // Create some test files.
        //
        // Their sizes sum to "USED".

        let mut files = TestDebugFileHarness::new(&logctx.log).await;

        // Make 200 bytes of support bundles.
        //
        // Make them really old, so they look "attractive to remove".
        files
            .add_file("cb1be051-9748-4e7e-9195-a222956e6d59/bundle.zip")
            .set_size(100)
            .make_much_older();
        files
            .add_file("c4640fac-c67c-4480-b736-5d9a7fe336ba/bundle.zip.tmp")
            .set_size(100)
            .make_much_older();

        // 400 bytes of other data "to be kept"
        files.add_file("global/test.log").set_size(100);
        files.add_file("foo-zone/test.log").set_size(100);
        files.add_file("bar-zone/test.log").set_size(200);

        // 400 bytes of data that seems reasonable to delete.
        files
            .add_file("bar-zone/test-archived.log")
            .set_size(400)
            .make_older()
            .should_be_deleted();

        files.create_all();

        // This is an assertion that "the test expectations are valid", before
        // we actually try to do any cleanup.
        const CAPACITY: u64 = USED + AVAILABLE;
        files.confirm_that_deleting_marked_files_will_drop_below_capacity(
            USED, CAPACITY,
        );

        let worker = files.new_dump_setup_worker(USED, AVAILABLE).await;

        // Before we cleanup: All files in "debug" exist
        files.check_all_files_exist();
        worker.cleanup().await.unwrap();

        // After we cleanup: The files we marked as "should_be_deleted" are
        // removed, but the rest still exist.
        files.check_expected_files_removed();

        logctx.cleanup_successful();
    }
}
