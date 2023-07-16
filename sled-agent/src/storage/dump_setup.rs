use crate::storage_manager::DiskWrapper;
use camino::Utf8PathBuf;
use illumos_utils::zpool::ZpoolHealth;
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use slog::Logger;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::MutexGuard;

#[derive(Default)]
pub struct DumpSetup {
    // prevent us from running savecore concurrently.
    savecore_lock: Arc<std::sync::Mutex<()>>,
}

impl DumpSetup {
    pub(crate) async fn update_dumpdev_setup(
        &self,
        disks: &mut MutexGuard<'_, HashMap<DiskIdentity, DiskWrapper>>,
        log: Logger,
    ) {
        let mut dump_slices = Vec::new();
        let mut u2_dump_dirs = Vec::new();
        for (_id, disk_wrapper) in disks.iter() {
            match disk_wrapper {
                DiskWrapper::Real { disk, .. } => match disk.variant() {
                    DiskVariant::M2 => {
                        match disk.dump_device_devfs_path(false) {
                            Ok(path) => dump_slices.push(path),
                            Err(err) => {
                                warn!(log, "Error getting dump device devfs path: {err:?}");
                            }
                        }
                    }
                    DiskVariant::U2 => {
                        let name = disk.zpool_name();
                        if let Ok(info) = illumos_utils::zpool::Zpool::get_info(
                            &name.to_string(),
                        ) {
                            if info.health() == ZpoolHealth::Online {
                                u2_dump_dirs.push(name.dataset_mountpoint(
                                    sled_hardware::disk::DUMP_DATASET,
                                ));
                            } else {
                                warn!(log, "Zpool {name:?} not online, won't attempt to savecore dumps there");
                            }
                        }
                    }
                },
                DiskWrapper::Synthetic { .. } => {}
            }
        }

        dump_slices.sort();
        u2_dump_dirs.sort();

        let savecore_lock = self.savecore_lock.clone();
        tokio::task::spawn_blocking(move || {
            // TODO: a more reasonable way of deduplicating the effort.
            let _guard = savecore_lock.lock();
            Self::run_dumpadm_and_savecore(log, dump_slices, u2_dump_dirs);
        });
    }

    fn run_dumpadm_and_savecore(
        log: Logger,
        dump_slices: Vec<Utf8PathBuf>,
        u2_dump_dirs: Vec<Utf8PathBuf>,
    ) {
        for dump_slice in dump_slices {
            // NOTE: because of the need to have dumpadm change the global
            // state of which slice the system is using for dumps in order
            // for savecore to behave the way we want (i.e. clear the flag
            // after succeeding), we could hypothetically miss a dump if
            // the kernel crashes again while savecore is still running.
            if u2_dump_dirs.is_empty() {
                // Don't risk overwriting an existing dump if there's
                // already one there until we can attempt to savecore(8)
                // it away and clear the flag to make room.
                match illumos_utils::dumpadm::dump_flag_is_valid(&dump_slice) {
                    Ok(false) => {
                        // Have dumpadm write the config for crash dumps to be
                        // on this slice, at least, until a U.2 comes along.
                        match illumos_utils::dumpadm::dumpadm(&dump_slice, None)
                        {
                            Ok(_) => {
                                info!(log, "Using dump device {dump_slice:?} with no savecore destination (no U.2 debug zvol yet)");
                            }
                            Err(err) => {
                                warn!(log, "Could not configure {dump_slice:?} as dump device: {err:?}");
                            }
                        }
                    }
                    Ok(true) => {
                        warn!(log, "Not configuring {dump_slice:?} as it appears to contain a dump we cannot yet send to a U.2 debug zvol");
                    }
                    Err(err) => {
                        debug!(
                            log,
                            "Dump slice {dump_slice:?} appears to be unused : {err:?}",
                        );
                    }
                }
            } else {
                // Try each U.2 until we succeed once
                for mountpoint in &u2_dump_dirs {
                    // Let's try to see if it appears to have a dump already
                    match illumos_utils::dumpadm::dump_flag_is_valid(
                        &dump_slice,
                    ) {
                        Ok(true) => {
                            debug!(log, "Dump slice {dump_slice:?} appears to have a valid header; will attempt to savecore to {mountpoint:?}");
                        }
                        Ok(false) => {
                            info!(log, "Dump slice {dump_slice:?} appears to have already been saved");
                        }
                        Err(err) => {
                            debug!(log, "Dump slice {dump_slice:?} appears to be unused: {err:?}");
                        }
                    }
                    // Have dumpadm write the config for crash dumps to be
                    // on this slice, and invoke savecore(8) to save any
                    // dump that's already present there.
                    match illumos_utils::dumpadm::dumpadm(
                        &dump_slice,
                        Some(mountpoint),
                    ) {
                        Err(err) => {
                            warn!(log, "Could not configure {dump_slice:?} as dump device with {mountpoint:?} as savecore destination: {err:?}");
                        }
                        Ok(saved) => {
                            if let Some(stdout) = saved {
                                info!(
                                    log,
                                    "Saved dump from {dump_slice:?} to {mountpoint:?}: {stdout:?}"
                                );
                            } else {
                                info!(
                                    log,
                                    "Set {dump_slice:?} as system dump slice",
                                );
                            }
                            // If there was one present, we successfully
                            // compressed it onto a U.2's pool, no need to
                            // try others.
                            break;
                        }
                    }
                }
            }
        }
    }
}
