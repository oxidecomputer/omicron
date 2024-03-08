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
use camino::Utf8PathBuf;
use derive_more::{AsRef, From};
use illumos_utils::coreadm::{CoreAdm, CoreFileOption};
use illumos_utils::dumpadm::{DumpAdm, DumpContentType};
use illumos_utils::zone::ZONE_PREFIX;
use illumos_utils::zpool::{ZpoolHealth, ZpoolName};
use illumos_utils::ExecutionError;
use sled_hardware::DiskVariant;
use sled_storage::config::MountConfig;
use sled_storage::dataset::{CRASH_DATASET, DUMP_DATASET};
use sled_storage::disk::Disk;
use slog::Logger;
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;
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

#[derive(AsRef, Clone, From)]
pub(super) struct CoreZpool {
    mount_config: MountConfig,
    name: ZpoolName,
}

#[derive(AsRef, Clone, From)]
pub(super) struct DebugZpool {
    mount_config: MountConfig,
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
    UpdateDumpdevSetup {
        dump_slices: Vec<DumpSlicePath>,
        debug_datasets: Vec<DebugZpool>,
        core_datasets: Vec<CoreZpool>,
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
    mount_config: MountConfig,
    _poller: tokio::task::JoinHandle<()>,
    log: Logger,
}

impl DumpSetup {
    pub fn new(log: &Logger, mount_config: MountConfig) -> Self {
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

    pub(crate) async fn update_dumpdev_setup(
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
                    {
                        if info.health() == ZpoolHealth::Online {
                            m2_core_datasets.push(CoreZpool {
                                mount_config: mount_config.clone(),
                                name: name.clone(),
                            });
                        } else {
                            warn!(log, "Zpool {name:?} not online, won't attempt to save process core dumps there");
                        }
                    }
                }
                DiskVariant::U2 => {
                    let name = disk.zpool_name();
                    if let Ok(info) =
                        illumos_utils::zpool::Zpool::get_info(&name.to_string())
                    {
                        if info.health() == ZpoolHealth::Online {
                            u2_debug_datasets.push(DebugZpool {
                                mount_config: mount_config.clone(),
                                name: name.clone(),
                            });
                        } else {
                            warn!(log, "Zpool {name:?} not online, won't attempt to save kernel core dumps there");
                        }
                    }
                }
            }
        }

        if let Err(err) = self
            .tx
            .send(DumpSetupCmd::UpdateDumpdevSetup {
                dump_slices: m2_dump_slices,
                debug_datasets: u2_debug_datasets,
                core_datasets: m2_core_datasets,
            })
            .await
        {
            error!(log, "DumpSetup channel closed: {:?}", err.0);
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ZfsGetError {
    #[error("Error executing 'zfs get' command: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Output of 'zfs get' was not only not an integer string, it wasn't even UTF-8: {0}")]
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
        loop {
            match tokio::time::timeout(ARCHIVAL_INTERVAL, self.rx.recv()).await
            {
                Ok(Some(DumpSetupCmd::UpdateDumpdevSetup {
                    dump_slices,
                    debug_datasets,
                    core_datasets,
                })) => {
                    self.update_disk_loadout(
                        dump_slices,
                        debug_datasets,
                        core_datasets,
                    );
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
        }
    }

    fn update_disk_loadout(
        &mut self,
        dump_slices: Vec<DumpSlicePath>,
        debug_datasets: Vec<DebugZpool>,
        core_datasets: Vec<CoreZpool>,
    ) {
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
                warn!(self.log, "Previously-chosen debug/dump dir {x:?} no longer exists in our view of reality");
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
                            info!(self.log, "Previously-chosen debug/dump dir {x:?} is over usage threshold, choosing a more vacant disk");
                            self.chosen_debug_dir = None;
                        } else {
                            warn!(self.log, "All candidate debug/dump dirs are over usage threshold, removing older archived files");
                            if let Err(err) = self.cleanup().await {
                                error!(self.log, "Couldn't clean up any debug/dump dirs, may hit dataset quota in {x:?}: {err:?}");
                            } else {
                                self.chosen_debug_dir = None;
                            }
                        }
                    }
                    Err(err) => {
                        error!(self.log, "Previously-chosen debug/dump dir {x:?} couldn't be queried for zfs properties!  Choosing another. {err:?}");
                        self.chosen_debug_dir = None;
                    }
                }
            }
        }
        if let Some(x) = &self.chosen_dump_slice {
            if !self.known_dump_slices.contains(x) {
                warn!(self.log, "Previously-chosen dump slice {x:?} no longer exists in our view of reality");
                self.chosen_dump_slice = None;
            }
        }
        if let Some(x) = &self.chosen_core_dir {
            if !self.known_core_dirs.contains(x) {
                warn!(self.log, "Previously-chosen core dir {x:?} no longer exists in our view of reality");
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
                        error!(self.log, "Couldn't configure process core dump directory to {core_dir:?}: {err:?}");
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
                            debug!(self.log, "Dump slice {dump_slice:?} appears to have a valid header; will attempt to savecore");
                        }
                        Ok(false) => {
                            info!(self.log, "Dump slice {dump_slice:?} appears to have already been saved");
                        }
                        Err(err) => {
                            debug!(self.log, "Dump slice {dump_slice:?} appears to be unused: {err:?}");
                        }
                    }
                    if let Ok(saved) =
                        self.dumpadm_and_savecore(&dump_slice).await
                    {
                        if let Some(out) = saved {
                            info!(self.log, "Previous dump on slice {dump_slice:?} saved, configured slice as target for new dumps. {out:?}");
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
                                    info!(self.log, "Using dump device {dump_slice:?} with no savecore destination (no U.2 debug zvol yet)");
                                    self.chosen_dump_slice =
                                        Some(dump_slice.clone());
                                    break;
                                }
                                Err(err) => {
                                    warn!(self.log, "Could not configure {dump_slice:?} as dump device: {err:?}");
                                }
                            }
                        }
                        Ok(true) => {
                            warn!(self.log, "Not configuring {dump_slice:?} as it appears to contain a dump we cannot yet send to a U.2 debug zvol");
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
                            warn!(self.log, "Could not configure {dump_slice:?} as dump device with {debug_dir:?} as savecore destination: {err:?}");
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
                        error!(self.log, "Could not restore dump slice to {dump_slice:?}: {err:?}");
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
                            error!(self.log, "Non-UTF8 path found while archiving core dumps: {entry:?}");
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

        if let Err(err) = self.archive_logs().await {
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

    async fn archive_logs(&self) -> Result<(), ArchiveLogsError> {
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
            self.archive_logs_inner(debug_dir, logdir, zone_name).await?;
        }
        Ok(())
    }

    async fn archive_logs_inner(
        &self,
        debug_dir: &DebugDataset,
        logdir: PathBuf,
        zone_name: &str,
    ) -> Result<(), ArchiveLogsError> {
        let mut rotated_log_files = Vec::new();
        // patterns matching archived logs, e.g. foo.log.3
        // keep checking for greater numbers of digits until we don't find any
        for n in 1..9 {
            let pattern = logdir
                .join(format!("*.log.{}", "[0-9]".repeat(n)))
                .to_str()
                .ok_or_else(|| ArchiveLogsError::Utf8(zone_name.to_string()))?
                .to_string();
            rotated_log_files.extend(glob::glob(&pattern)?.flatten());
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
                    error!(self.log, "Could not analyze {dir:?} for debug dataset cleanup task: {err:?}");
                }
            }
        }
        if dir_info.is_empty() {
            return Err(CleanupError::NoDatasetsToClean);
        }
        // find dir with oldest average time of files that must be deleted
        // to achieve desired threshold, and reclaim that space.
        dir_info.sort();
        'outer: for (dir_info, dir) in dir_info {
            let CleanupDirInfo { average_time: _, num_to_delete, file_list } =
                dir_info;
            for (_time, _bytes, path) in &file_list[..num_to_delete as usize] {
                // if we are unable to remove a file, we cannot guarantee
                // that we will reach our target size threshold, and suspect
                // the i/o error *may* be an issue with the underlying disk, so
                // we continue to the dataset with the next-oldest average age
                // of files-to-delete in the sorted list.
                if let Err(err) = tokio::fs::remove_file(&path).await {
                    error!(self.log, "Couldn't delete {path:?} from debug dataset, skipping {dir:?}. {err:?}");
                    continue 'outer;
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
            let meta = tokio::fs::metadata(&path).await?;
            // we need this to be a Duration rather than SystemTime so we can
            // do math to it later.
            let time = meta.modified()?.duration_since(UNIX_EPOCH)?;
            let size = meta.len();

            file_list.push((time, size, path))
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
    #[error("Failed to query ZFS properties: {0}")]
    ZfsError(#[from] ZfsGetError),
    #[error("I/O error: {0}")]
    IoError(#[from] tokio::io::Error),
    #[error("Glob pattern invalid: {0}")]
    Glob(#[from] glob::PatternError),
    #[error("A file's observed modified time was before the Unix epoch: {0}")]
    TimelineWentSideways(#[from] SystemTimeError),
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct CleanupDirInfo {
    average_time: Duration,
    num_to_delete: u32,
    file_list: Vec<(Duration, u64, PathBuf)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use illumos_utils::dumpadm::{
        DF_VALID, DUMP_MAGIC, DUMP_OFFSET, DUMP_VERSION,
    };
    use sled_storage::dataset::{CRASH_DATASET, DUMP_DATASET};
    use std::collections::HashMap;
    use std::io::Write;
    use std::str::FromStr;
    use tempfile::TempDir;

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
        pub zpool_props: HashMap<
            &'static str,
            HashMap<&'static str, Result<String, ZfsGetError>>,
        >,
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

    #[test]
    fn test_does_not_configure_coreadm_when_no_crash_dataset_mounted() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_does_not_configure_coreadm_when_no_crash_dataset_mounted",
        );
        const NOT_MOUNTED_INTERNAL: &str =
            "oxi_acab2069-6e63-6c75-de73-20c06c756db0";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [(
                    NOT_MOUNTED_INTERNAL,
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
        assert_eq!(worker.chosen_core_dir, None);

        // nothing when only a disk that's not ready
        let non_mounted_zpool = CoreZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(NOT_MOUNTED_INTERNAL).unwrap(),
        };
        worker.update_disk_loadout(vec![], vec![], vec![non_mounted_zpool]);
        assert_eq!(worker.chosen_core_dir, None);
        logctx.cleanup_successful();
    }

    #[test]
    fn test_configures_coreadm_only_when_crash_dataset_mounted() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_configures_coreadm_only_when_crash_dataset_mounted",
        );
        const NOT_MOUNTED_INTERNAL: &str =
            "oxi_acab2069-6e63-6c75-de73-20c06c756db0";
        const MOUNTED_INTERNAL: &str =
            "oxi_474e554e-6174-616c-6965-4e677579656e";
        const ERROR_INTERNAL: &str = "oxi_4861636b-2054-6865-2050-6c616e657421";
        let mounted_zpool = CoreZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(MOUNTED_INTERNAL).unwrap(),
        };
        let non_mounted_zpool = CoreZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(NOT_MOUNTED_INTERNAL).unwrap(),
        };
        let err_zpool = CoreZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(ERROR_INTERNAL).unwrap(),
        };
        const ZPOOL_MNT: &str = "/path/to/internal/zpool";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [
                    (
                        NOT_MOUNTED_INTERNAL,
                        [("mounted", Ok("no".to_string()))]
                            .into_iter()
                            .collect(),
                    ),
                    (
                        MOUNTED_INTERNAL,
                        [
                            ("mounted", Ok("yes".to_string())),
                            ("mountpoint", Ok(ZPOOL_MNT.to_string())),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    (
                        ERROR_INTERNAL,
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
        assert_eq!(
            worker.chosen_core_dir.as_ref().unwrap().0,
            Utf8PathBuf::from(ZPOOL_MNT).join(CRASH_DATASET)
        );

        // back to nothing if it becomes unavailable
        worker.update_disk_loadout(
            vec![],
            vec![],
            vec![non_mounted_zpool, err_zpool],
        );
        assert_eq!(worker.chosen_core_dir, None);
        logctx.cleanup_successful();
    }

    // we make these so illumos_utils::dumpadm::dump_flag_is_valid returns what we want
    fn populate_tempdir_with_fake_dumps(
        tempdir: &TempDir,
    ) -> (DumpSlicePath, DumpSlicePath) {
        let occupied = DumpSlicePath(
            Utf8PathBuf::from_path_buf(tempdir.path().join("occupied.bin"))
                .unwrap(),
        );
        let mut f = std::fs::File::create(occupied.as_ref()).unwrap();
        f.write_all(&[0u8; DUMP_OFFSET as usize]).unwrap();
        f.write_all(&DUMP_MAGIC.to_le_bytes()).unwrap();
        f.write_all(&DUMP_VERSION.to_le_bytes()).unwrap();
        f.write_all(&DF_VALID.to_le_bytes()).unwrap();
        drop(f);

        let vacant = DumpSlicePath(
            Utf8PathBuf::from_path_buf(tempdir.path().join("vacant.bin"))
                .unwrap(),
        );
        let mut f = std::fs::File::create(vacant.as_ref()).unwrap();
        f.write_all(&[0u8; DUMP_OFFSET as usize]).unwrap();
        f.write_all(&DUMP_MAGIC.to_le_bytes()).unwrap();
        f.write_all(&DUMP_VERSION.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        drop(f);

        (occupied, vacant)
    }

    // if we only have two filled dump slices and nowhere to evacuate them,
    // don't configure a dump slice at all.
    #[test]
    fn test_savecore_and_dumpadm_not_called_when_occupied_and_no_dir() {
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
        let tempdir = TempDir::new().unwrap();
        let (occupied, _) = populate_tempdir_with_fake_dumps(&tempdir);

        worker.update_disk_loadout(
            vec![occupied.clone(), occupied],
            vec![],
            vec![],
        );
        assert!(worker.chosen_dump_slice.is_none());
        logctx.cleanup_successful();
    }

    // if we have one dump slice that's free and one that's full,
    // and nowhere to savecore the full one,
    // we should always call dumpadm with the free one.
    #[test]
    fn test_dumpadm_called_when_vacant_slice_but_no_dir() {
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
        let tempdir = TempDir::new().unwrap();
        let (occupied, vacant) = populate_tempdir_with_fake_dumps(&tempdir);
        worker.update_disk_loadout(
            vec![occupied, vacant.clone()],
            vec![],
            vec![],
        );
        assert_eq!(worker.chosen_dump_slice.as_ref(), Some(&vacant));
        logctx.cleanup_successful();
    }

    // if we have two occupied dump slices,
    // but we also have somewhere to unload them,
    // call dumpadm and savecore.
    #[test]
    fn test_savecore_and_dumpadm_invoked_when_slices_occupied_and_dir_is_available(
    ) {
        let logctx = omicron_test_utils::dev::test_setup_log("test_savecore_and_dumpadm_invoked_when_slices_occupied_and_dir_is_available");
        const MOUNTED_EXTERNAL: &str =
            "oxp_446f6e74-4469-6557-6f6e-646572696e67";
        const ZPOOL_MNT: &str = "/path/to/external/zpool";
        let mut worker = DumpSetupWorker::new(
            Box::<FakeCoreDumpAdm>::default(),
            Box::new(FakeZfs {
                zpool_props: [(
                    MOUNTED_EXTERNAL,
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
        let tempdir = TempDir::new().unwrap();
        let (occupied, _) = populate_tempdir_with_fake_dumps(&tempdir);

        let mounted_zpool = DebugZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(MOUNTED_EXTERNAL).unwrap(),
        };
        worker.update_disk_loadout(
            vec![occupied.clone()],
            vec![mounted_zpool],
            vec![],
        );
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

        let tempdir = TempDir::new().unwrap();
        let core_dir = tempdir.path().join(CRASH_DATASET);
        let debug_dir = tempdir.path().join(DUMP_DATASET);
        let zone_logs = tempdir.path().join("root/var/svc/log");

        let tempdir_path = tempdir.path().to_str().unwrap().to_string();
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
                        MOUNTED_INTERNAL,
                        [
                            ("mounted", Ok("yes".to_string())),
                            ("mountpoint", Ok(tempdir_path.clone())),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    (
                        MOUNTED_EXTERNAL,
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

        std::fs::create_dir_all(&core_dir).unwrap();
        std::fs::create_dir_all(&debug_dir).unwrap();
        std::fs::create_dir_all(&zone_logs).unwrap();
        const LOG_NAME: &'static str = "foo.log.0";
        writeln!(
            std::fs::File::create(zone_logs.join(LOG_NAME)).unwrap(),
            "hello"
        )
        .unwrap();

        const CORE_NAME: &str = "core.myzone.myexe.123.1690540950";
        writeln!(
            std::fs::File::create(core_dir.join(CORE_NAME)).unwrap(),
            "crunch"
        )
        .unwrap();

        let mounted_core_zpool = CoreZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(MOUNTED_INTERNAL).unwrap(),
        };
        let mounted_debug_zpool = DebugZpool {
            mount_config: MountConfig::default(),
            name: ZpoolName::from_str(MOUNTED_EXTERNAL).unwrap(),
        };

        worker.update_disk_loadout(
            vec![],
            vec![mounted_debug_zpool],
            vec![mounted_core_zpool],
        );
        worker.archive_files().await.unwrap();

        // it'll be renamed to use an epoch timestamp instead of .0
        let log_glob =
            debug_dir.join(zone.name()).join(LOG_NAME.replace(".0", ".*"));
        assert_eq!(glob::glob(log_glob.to_str().unwrap()).unwrap().count(), 1);
        assert!(!zone_logs.join(LOG_NAME).is_file());
        assert!(debug_dir.join(CORE_NAME).is_file());
        assert!(!core_dir.join(CORE_NAME).is_file());
        logctx.cleanup_successful();
    }
}
