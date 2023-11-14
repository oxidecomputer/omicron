use camino::Utf8PathBuf;
use derive_more::{AsRef, Deref, From};
use illumos_utils::dumpadm::DumpAdmError;
use illumos_utils::zone::{AdmError, Zones};
use illumos_utils::zpool::{ZpoolHealth, ZpoolName};
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use sled_storage::dataset::{CRASH_DATASET, DUMP_DATASET};
use sled_storage::disk::Disk;
use sled_storage::pool::Pool;
use slog::Logger;
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

pub struct DumpSetup {
    worker: Arc<std::sync::Mutex<DumpSetupWorker>>,
    _poller: std::thread::JoinHandle<()>,
    log: Logger,
}

impl DumpSetup {
    pub fn new(log: &Logger) -> Self {
        let worker = Arc::new(std::sync::Mutex::new(DumpSetupWorker::new(
            log.new(o!("component" => "DumpSetup-worker")),
        )));
        let worker_weak = Arc::downgrade(&worker);
        let log_poll = log.new(o!("component" => "DumpSetup-archival"));
        let _poller = std::thread::spawn(move || {
            Self::poll_file_archival(worker_weak, log_poll)
        });
        let log = log.new(o!("component" => "DumpSetup"));
        Self { worker, _poller, log }
    }
}

// we sure are passing a lot of Utf8PathBufs around, let's be careful about it
#[derive(
    AsRef, Clone, Debug, Deref, Eq, From, Hash, Ord, PartialEq, PartialOrd,
)]
struct DumpSlicePath(Utf8PathBuf);
#[derive(
    AsRef, Clone, Debug, Deref, Eq, From, Hash, Ord, PartialEq, PartialOrd,
)]
struct DebugDataset(Utf8PathBuf);
#[derive(
    AsRef, Clone, Debug, Deref, Eq, From, Hash, Ord, PartialEq, PartialOrd,
)]
struct CoreDataset(Utf8PathBuf);

#[derive(Deref)]
struct CoreZpool(ZpoolName);
#[derive(Deref)]
struct DebugZpool(ZpoolName);

// only want to access these directories after they're mounted!
trait GetMountpoint: std::ops::Deref<Target = ZpoolName> {
    type NewType: From<Utf8PathBuf>;
    const MOUNTPOINT: &'static str;
    fn mountpoint(&self) -> Result<Option<Self::NewType>, ZfsGetError> {
        if zfs_get_prop(self.to_string(), "mounted")? == "yes" {
            Ok(Some(Self::NewType::from(
                self.dataset_mountpoint(Self::MOUNTPOINT),
            )))
        } else {
            Ok(None)
        }
    }
}
impl GetMountpoint for DebugZpool {
    type NewType = DebugDataset;
    const MOUNTPOINT: &'static str = DUMP_DATASET;
}
impl GetMountpoint for CoreZpool {
    type NewType = CoreDataset;
    const MOUNTPOINT: &'static str = CRASH_DATASET;
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
}

const ARCHIVAL_INTERVAL: Duration = Duration::from_secs(300);

impl DumpSetup {
    pub(crate) async fn update_dumpdev_setup(
        &self,
        disks: &BTreeMap<DiskIdentity, (Disk, Pool)>,
    ) {
        let log = &self.log;
        let mut m2_dump_slices = Vec::new();
        let mut u2_debug_datasets = Vec::new();
        let mut m2_core_datasets = Vec::new();
        for (_id, (disk, _)) in disks.iter() {
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
                            m2_core_datasets.push(CoreZpool(name.clone()));
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
                            u2_debug_datasets.push(DebugZpool(name.clone()));
                        } else {
                            warn!(log, "Zpool {name:?} not online, won't attempt to save kernel core dumps there");
                        }
                    }
                }
            }
        }

        let savecore_lock = self.worker.clone();
        let log_tmp = log.new(o!("component" => "DumpSetup-mutex"));
        tokio::task::spawn_blocking(move || match savecore_lock.lock() {
            Ok(mut guard) => {
                guard.update_disk_loadout(
                    m2_dump_slices,
                    u2_debug_datasets,
                    m2_core_datasets,
                );
            }
            Err(err) => {
                error!(log_tmp, "DumpSetup mutex poisoned: {err:?}");
            }
        });
    }

    fn poll_file_archival(
        worker: Weak<std::sync::Mutex<DumpSetupWorker>>,
        log: Logger,
    ) {
        info!(log, "DumpSetup poll loop started.");
        loop {
            if let Some(mutex) = worker.upgrade() {
                match mutex.lock() {
                    Ok(mut guard) => {
                        guard.reevaluate_choices();
                        if let Err(err) = guard.archive_files() {
                            error!(
                                log,
                                "Failed to archive debug/dump files: {err:?}"
                            );
                        }
                    }
                    Err(err) => {
                        error!(
                            log,
                            "DumpSetup mutex poisoned in poll thread: {err:?}"
                        );
                        break;
                    }
                }
            } else {
                info!(
                    log,
                    "DumpSetup weak pointer dropped, leaving poll loop."
                );
                break;
            }
            std::thread::sleep(ARCHIVAL_INTERVAL);
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

const ZFS_PROP_USED: &str = "used";
const ZFS_PROP_AVAILABLE: &str = "available";

fn zfs_get_integer(
    mountpoint_or_name: impl AsRef<str>,
    property: &str,
) -> Result<u64, ZfsGetError> {
    zfs_get_prop(mountpoint_or_name, property)?.parse().map_err(Into::into)
}

fn zfs_get_prop(
    mountpoint_or_name: impl AsRef<str> + Sized,
    property: &str,
) -> Result<String, ZfsGetError> {
    let mountpoint = mountpoint_or_name.as_ref();
    let mut cmd = std::process::Command::new(illumos_utils::zfs::ZFS);
    cmd.arg("get").arg("-Hpo").arg("value");
    cmd.arg(property);
    cmd.arg(mountpoint);
    let output = cmd.output()?;
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

const DATASET_USAGE_PERCENT_CHOICE: u64 = 70;
const DATASET_USAGE_PERCENT_CLEANUP: u64 = 80;

fn below_thresh(
    mountpoint: &Utf8PathBuf,
    percent: u64,
) -> Result<(bool, u64), ZfsGetError> {
    let used = zfs_get_integer(mountpoint, ZFS_PROP_USED)?;
    let available = zfs_get_integer(mountpoint, ZFS_PROP_AVAILABLE)?;
    let capacity = used + available;
    let below = (used * 100) / capacity < percent;
    Ok((below, used))
}

impl DumpSetupWorker {
    fn new(log: Logger) -> Self {
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

        self.reevaluate_choices();
    }

    // only allow mounted zfs datasets into 'known_*_dirs',
    // such that we don't render them non-auto-mountable by zfs
    fn update_mounted_dirs(&mut self) {
        self.known_debug_dirs = self
            .debug_dataset_names
            .iter()
            .flat_map(|ds| ds.mountpoint())
            .flatten()
            .collect();
        self.known_core_dirs = self
            .core_dataset_names
            .iter()
            .flat_map(|ds| ds.mountpoint())
            .flatten()
            .collect();
    }

    fn reevaluate_choices(&mut self) {
        self.update_mounted_dirs();

        self.known_dump_slices.sort();
        // sort key: prefer to choose a dataset where there's already other
        // dumps so we don't shotgun them across every U.2, but only if they're
        // below a certain usage threshold.
        self.known_debug_dirs.sort_by_cached_key(
            |mountpoint: &DebugDataset| {
                match below_thresh(mountpoint.as_ref(), DATASET_USAGE_PERCENT_CHOICE) {
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
            let available = zfs_get_integer(&**mnt, "available").unwrap_or(0);
            (u64::MAX - available, mnt.clone())
        });

        if let Some(x) = &self.chosen_debug_dir {
            if !self.known_debug_dirs.contains(x) {
                warn!(self.log, "Previously-chosen debug/dump dir {x:?} no longer exists in our view of reality");
                self.chosen_debug_dir = None;
            } else {
                match below_thresh(x.as_ref(), DATASET_USAGE_PERCENT_CLEANUP) {
                    Ok((true, _)) => {}
                    Ok((false, _)) => {
                        if self.known_debug_dirs.iter().any(|x| {
                            below_thresh(
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
                            if let Err(err) = self.cleanup() {
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
                match illumos_utils::coreadm::coreadm(core_dir) {
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
                        &dump_slice,
                    ) {
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
                    if let Ok(saved) = self.dumpadm_and_savecore(&dump_slice) {
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
                    match illumos_utils::dumpadm::dump_flag_is_valid(dump_slice)
                    {
                        Ok(false) => {
                            // Have dumpadm write the config for crash dumps to be
                            // on this slice, at least, until a U.2 comes along.
                            match illumos_utils::dumpadm::dumpadm(
                                dump_slice, None,
                            ) {
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
                    match self.dumpadm_and_savecore(&dump_slice) {
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
                    if let Err(err) =
                        illumos_utils::dumpadm::dumpadm(dump_slice, None)
                    {
                        error!(self.log, "Could not restore dump slice to {dump_slice:?}: {err:?}");
                    }
                }
            }
        }
    }

    fn archive_files(&self) -> std::io::Result<()> {
        if let Some(debug_dir) = &self.chosen_debug_dir {
            if self.known_core_dirs.is_empty() {
                info!(self.log, "No core dump locations yet known.");
            }
            for core_dir in &self.known_core_dirs {
                if let Ok(dir) = core_dir.read_dir() {
                    for entry in dir.flatten() {
                        if let Some(path) = entry.file_name().to_str() {
                            let dest = debug_dir.join(path);

                            if let Err(err) =
                                Self::copy_sync_and_remove(&entry.path(), &dest)
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

        if let Err(err) = self.archive_logs() {
            if !matches!(err, ArchiveLogsError::NoDebugDirYet) {
                error!(
                    self.log,
                    "Failure while trying to archive logs to debug dataset: {err:?}"
                );
            }
        }

        Ok(())
    }

    fn copy_sync_and_remove(
        source: impl AsRef<Path>,
        dest: impl AsRef<Path>,
    ) -> std::io::Result<()> {
        let source = source.as_ref();
        let dest = dest.as_ref();
        let mut dest_f = std::fs::File::create(&dest)?;
        let mut src_f = std::fs::File::open(&source)?;

        std::io::copy(&mut src_f, &mut dest_f)?;

        dest_f.sync_all()?;

        drop(src_f);
        drop(dest_f);

        std::fs::remove_file(source)?;
        Ok(())
    }

    fn archive_logs(&self) -> Result<(), ArchiveLogsError> {
        let debug_dir = self
            .chosen_debug_dir
            .as_ref()
            .ok_or(ArchiveLogsError::NoDebugDirYet)?;
        // zone crate's 'deprecated' functions collide if you try to enable
        // its 'sync' and 'async' features simultaneously :(
        let rt =
            tokio::runtime::Runtime::new().map_err(ArchiveLogsError::Tokio)?;
        let oxz_zones = rt.block_on(Zones::get())?;
        self.archive_logs_inner(
            debug_dir,
            PathBuf::from("/var/svc/log"),
            "global",
        )?;
        for zone in oxz_zones {
            let logdir = zone.path().join("root/var/svc/log");
            let zone_name = zone.name();
            self.archive_logs_inner(debug_dir, logdir, zone_name)?;
        }
        Ok(())
    }

    fn archive_logs_inner(
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
        let dest_dir = debug_dir.join(zone_name).into_std_path_buf();
        if !rotated_log_files.is_empty() {
            std::fs::create_dir_all(&dest_dir)?;
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
            if let Err(err) = Self::copy_sync_and_remove(&entry, dest) {
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
    fn dumpadm_and_savecore(
        &mut self,
        dump_slice: &DumpSlicePath,
    ) -> Result<Option<OsString>, DumpAdmError> {
        // TODO: untangle savecore from illumos_utils::dumpadm
        assert!(self.chosen_debug_dir.is_some());

        let savecore_dir = self.chosen_debug_dir.clone().unwrap().0;

        match illumos_utils::dumpadm::dumpadm(&dump_slice, Some(&savecore_dir))
        {
            Ok(saved) => {
                self.savecored_slices.insert(dump_slice.clone());
                Ok(saved)
            }
            Err(err) => Err(err),
        }
    }

    fn cleanup(&self) -> Result<(), CleanupError> {
        let mut dir_info = Vec::new();
        for dir in &self.known_debug_dirs {
            match Self::scope_dir_for_cleanup(dir) {
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
                if let Err(err) = std::fs::remove_file(&path) {
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

    fn scope_dir_for_cleanup(
        debug_dir: &DebugDataset,
    ) -> Result<CleanupDirInfo, CleanupError> {
        let used = zfs_get_integer(&**debug_dir, ZFS_PROP_USED)?;
        let available = zfs_get_integer(&**debug_dir, ZFS_PROP_AVAILABLE)?;
        let capacity = used + available;

        let target_used = capacity * DATASET_USAGE_PERCENT_CHOICE / 100;

        let mut file_list = Vec::new();
        // find all files in the debug dataset and sort by modified time
        for path in glob::glob(debug_dir.join("**/*").as_str())?.flatten() {
            let meta = std::fs::metadata(&path)?;
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
enum ArchiveLogsError {
    #[error("Couldn't make an async runtime to get zone info: {0}")]
    Tokio(std::io::Error),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Error calling zoneadm: {0}")]
    Zoneadm(#[from] AdmError),
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
    IoError(#[from] std::io::Error),
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
