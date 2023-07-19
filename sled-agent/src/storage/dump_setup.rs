use crate::storage_manager::DiskWrapper;
use camino::Utf8PathBuf;
use derive_more::{AsRef, Deref};
use illumos_utils::dumpadm::DumpAdmError;
use illumos_utils::zpool::ZpoolHealth;
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::sync::{Arc, Weak};
use tokio::sync::MutexGuard;

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
        let log_poll = log.new(o!("component" => "DumpSetup-rotation"));
        let _poller = std::thread::spawn(move || {
            Self::poll_file_rotation(worker_weak, log_poll)
        });
        let log = log.new(o!("component" => "DumpSetup"));
        Self { worker, _poller, log }
    }
}

// we sure are passing a lot of Utf8PathBufs around, let's be careful about it
#[derive(AsRef, Clone, Debug, Deref, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct DumpSlicePath(Utf8PathBuf);
#[derive(AsRef, Clone, Debug, Deref, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct DebugDirPath(Utf8PathBuf);
#[derive(AsRef, Clone, Debug, Deref, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CorePath(Utf8PathBuf);

struct DumpSetupWorker {
    chosen_dump_slice: Option<DumpSlicePath>,
    chosen_debug_dir: Option<DebugDirPath>,
    chosen_core_dir: Option<CorePath>,

    known_dump_slices: Vec<DumpSlicePath>,
    known_debug_dirs: Vec<DebugDirPath>,
    known_core_dirs: Vec<CorePath>,

    savecored_slices: HashSet<DumpSlicePath>,

    log: Logger,
}

const ROTATION_DURATION: std::time::Duration =
    std::time::Duration::from_secs(300);

impl DumpSetup {
    pub(crate) async fn update_dumpdev_setup(
        &self,
        disks: &mut MutexGuard<'_, HashMap<DiskIdentity, DiskWrapper>>,
    ) {
        let log = &self.log;
        let mut m2_dump_slices = Vec::new();
        let mut u2_debug_dirs = Vec::new();
        let mut m2_core_dirs = Vec::new();
        for (_id, disk_wrapper) in disks.iter() {
            match disk_wrapper {
                DiskWrapper::Real { disk, .. } => match disk.variant() {
                    DiskVariant::M2 => {
                        match disk.dump_device_devfs_path(false) {
                            Ok(path) => {
                                m2_dump_slices.push(DumpSlicePath(path))
                            }
                            Err(err) => {
                                warn!(log, "Error getting dump device devfs path: {err:?}");
                            }
                        }
                        let name = disk.zpool_name();
                        if let Ok(info) = illumos_utils::zpool::Zpool::get_info(
                            &name.to_string(),
                        ) {
                            if info.health() == ZpoolHealth::Online {
                                m2_core_dirs.push(CorePath(
                                    name.dataset_mountpoint(
                                        sled_hardware::disk::CRASH_DATASET,
                                    ),
                                ));
                            } else {
                                warn!(log, "Zpool {name:?} not online, won't attempt to save process core dumps there");
                            }
                        }
                    }
                    DiskVariant::U2 => {
                        let name = disk.zpool_name();
                        if let Ok(info) = illumos_utils::zpool::Zpool::get_info(
                            &name.to_string(),
                        ) {
                            if info.health() == ZpoolHealth::Online {
                                u2_debug_dirs.push(DebugDirPath(
                                    name.dataset_mountpoint(
                                        sled_hardware::disk::DUMP_DATASET,
                                    ),
                                ));
                            } else {
                                warn!(log, "Zpool {name:?} not online, won't attempt to save kernel core dumps there");
                            }
                        }
                    }
                },
                DiskWrapper::Synthetic { .. } => {}
            }
        }

        let savecore_lock = self.worker.clone();
        let log_tmp = log.new(o!("component" => "DumpSetup-mutex"));
        tokio::task::spawn_blocking(move || match savecore_lock.lock() {
            Ok(mut guard) => {
                guard.update_disk_loadout(
                    m2_dump_slices,
                    u2_debug_dirs,
                    m2_core_dirs,
                );
            }
            Err(err) => {
                error!(log_tmp, "DumpSetup mutex poisoned: {err:?}");
            }
        });
    }

    fn poll_file_rotation(
        worker: Weak<std::sync::Mutex<DumpSetupWorker>>,
        log: Logger,
    ) {
        info!(log, "DumpSetup poll loop started.");
        loop {
            if let Some(mutex) = worker.upgrade() {
                match mutex.lock() {
                    Ok(mut guard) => {
                        guard.reevaluate_choices();
                        if let Err(err) = guard.rotate_files(&log) {
                            error!(
                                log,
                                "Failed to rotate debug/dump files: {err:?}"
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
            std::thread::sleep(ROTATION_DURATION);
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

fn zfs_get_integer(
    mountpoint: impl AsRef<str>,
    property: &str,
) -> Result<u64, ZfsGetError> {
    let mountpoint = mountpoint.as_ref();
    let mut cmd = std::process::Command::new(illumos_utils::zfs::ZFS);
    cmd.arg("get").arg("-Hpo").arg("value");
    cmd.arg(property);
    cmd.arg(mountpoint);
    let output = cmd.output()?;
    String::from_utf8(output.stdout)?.trim().parse().map_err(Into::into)
}

const DATASET_USAGE_THRESHOLD_PERCENT: u64 = 70;
fn below_thresh(mountpoint: &Utf8PathBuf) -> Result<(bool, u64), ZfsGetError> {
    let used = zfs_get_integer(mountpoint, "used")?;
    let available = zfs_get_integer(mountpoint, "available")?;
    let capacity = used + available;
    let below = (used * 100) / capacity < DATASET_USAGE_THRESHOLD_PERCENT;
    Ok((below, used))
}

impl DumpSetupWorker {
    fn new(log: Logger) -> Self {
        Self {
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
        debug_dirs: Vec<DebugDirPath>,
        core_dirs: Vec<CorePath>,
    ) {
        self.known_dump_slices = dump_slices;
        self.known_debug_dirs = debug_dirs;
        self.known_core_dirs = core_dirs;

        self.reevaluate_choices();
    }

    fn reevaluate_choices(&mut self) {
        self.known_dump_slices.sort();
        // sort key: prefer to choose a dataset where there's already other
        // dumps so we don't shotgun them across every U.2, but only if they're
        // below a certain usage threshold.
        self.known_debug_dirs.sort_by_cached_key(
            |mountpoint: &DebugDirPath| {
                match below_thresh(mountpoint.as_ref()) {
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
            // these get rotated out periodically anyway, pick one with room
            let available = zfs_get_integer(&**mnt, "available").unwrap_or(0);
            (u64::MAX - available, mnt.clone())
        });

        if let Some(x) = &self.chosen_debug_dir {
            if !self.known_debug_dirs.contains(x) {
                warn!(self.log, "Previously-chosen debug/dump dir {x:?} no longer exists in our view of reality");
                self.chosen_debug_dir = None;
            } else {
                match below_thresh(x.as_ref()) {
                    Ok((true, _)) => {}
                    Ok((false, _)) => {
                        warn!(self.log, "Previously-chosen debug/dump dir {x:?} is over usage threshold, checking other disks for space");
                        self.chosen_debug_dir = None;
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

    fn rotate_files(&self, log: &Logger) -> Result<(), std::io::Error> {
        if let Some(debug_dir) = &self.chosen_debug_dir {
            if self.known_core_dirs.is_empty() {
                info!(log, "No core dump locations yet known.");
            }
            for core_dir in &self.known_core_dirs {
                if let Ok(dir) = core_dir.read_dir() {
                    for entry in dir.flatten() {
                        if let Some(path) = entry.file_name().to_str() {
                            let dest = debug_dir.join(path);

                            let mut dest_f = std::fs::File::create(&dest)?;
                            let mut src_f = std::fs::File::open(&entry.path())?;

                            std::io::copy(&mut src_f, &mut dest_f)?;
                            dest_f.sync_all()?;

                            drop(src_f);
                            drop(dest_f);

                            if let Err(err) = std::fs::remove_file(entry.path())
                            {
                                warn!(log, "Could not remove {entry:?} after copying it to {dest:?}: {err:?}");
                            } else {
                                info!(
                                    log,
                                    "Relocated core {entry:?} to {dest:?}"
                                );
                            }
                        } else {
                            error!(log, "Non-UTF8 path found while rotating core dumps: {entry:?}");
                        }
                    }
                }
            }
        } else {
            info!(log, "No rotation destination for crash dumps yet chosen.");
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
}
