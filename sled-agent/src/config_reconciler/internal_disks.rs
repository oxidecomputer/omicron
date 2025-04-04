// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use futures::future;
use futures::future::Either;
use id_map::IdMap;
use omicron_common::backoff::Backoff;
use omicron_common::backoff::ExponentialBackoffBuilder;
use omicron_common::disk::DiskVariant;
use sled_storage::config::MountConfig;
use sled_storage::dataset::CONFIG_DATASET;
use sled_storage::disk::Disk;
use sled_storage::disk::DiskError;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::future::Future;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

/// A thin wrapper around a [`watch::Receiver`] that presents a similar API.
#[derive(Debug, Clone)]
pub struct InternalDisksReceiver {
    disks: watch::Receiver<Arc<IdMap<Disk>>>,
    mount_config: Arc<MountConfig>,
}

impl InternalDisksReceiver {
    pub fn borrow_and_update(&mut self) -> InternalDisks<'_> {
        InternalDisks {
            disks: self.disks.borrow_and_update(),
            mount_config: &self.mount_config,
        }
    }

    pub async fn changed(&mut self) -> Result<(), RecvError> {
        self.disks.changed().await
    }

    // Allow tests in other modules to construct this handle without having to
    // construct an entire `InternalDisksTask`.
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        disks: watch::Receiver<Arc<IdMap<Disk>>>,
        mount_config: MountConfig,
    ) -> Self {
        Self { disks, mount_config: Arc::new(mount_config) }
    }
}

pub struct InternalDisks<'a> {
    disks: watch::Ref<'a, Arc<IdMap<Disk>>>,
    mount_config: &'a MountConfig,
}

impl InternalDisks<'_> {
    /// Returns all `CONFIG_DATASET` paths within available M.2 disks.
    pub fn all_config_datasets(&self) -> Vec<Utf8PathBuf> {
        self.all_datasets(CONFIG_DATASET)
    }

    fn all_datasets(&self, dataset_name: &str) -> Vec<Utf8PathBuf> {
        self.disks
            .iter()
            .map(|disk| {
                disk.zpool_name()
                    .dataset_mountpoint(&self.mount_config.root, dataset_name)
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct InternalDisksTask {
    disks: watch::Sender<Arc<IdMap<Disk>>>,
    raw_disks: watch::Receiver<IdMap<RawDisk>>,
    mount_config: Arc<MountConfig>,
    log: Logger,
}

impl InternalDisksTask {
    pub fn spawn(
        mount_config: Arc<MountConfig>,
        raw_disks: watch::Receiver<IdMap<RawDisk>>,
        log: Logger,
    ) -> InternalDisksReceiver {
        Self::spawn_impl(mount_config, raw_disks, log, RealDiskAdopter)
    }

    // This method is private and used by tests to inject a fake disk adopter.
    fn spawn_impl<T: DiskAdopter + Send + 'static>(
        mount_config: Arc<MountConfig>,
        raw_disks: watch::Receiver<IdMap<RawDisk>>,
        log: Logger,
        mut disk_adopter: T,
    ) -> InternalDisksReceiver {
        let (disks_tx, disks_rx) = watch::channel(Arc::default());

        tokio::spawn({
            let task = Self {
                disks: disks_tx,
                raw_disks,
                mount_config: Arc::clone(&mount_config),
                log,
            };
            async move { task.run(&mut disk_adopter).await }
        });

        InternalDisksReceiver { disks: disks_rx, mount_config }
    }

    async fn run<T: DiskAdopter>(mut self, disk_adopter: &mut T) {
        // If disk adoption fails, the most likely cause is that the disk is not
        // formatted correctly, and we have no automated means to recover that.
        // However, it's always possible we could fail to adopt due to some
        // transient error. Construct an exponential backoff that scales up to
        // waiting a minute between attempts; that should let us handle any
        // short transient errors without constantly retrying a doomed
        // operation.
        let mut next_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None)
            .build();

        loop {
            let adoption_result = self.adopt_internal_disks(disk_adopter).await;

            // If any option failed, we'll retry; otherwise we'll wait for a
            // change in `raw_disks`.
            let retry_timeout = match adoption_result {
                AdoptionResult::AllSuccess => {
                    next_backoff.reset();
                    Either::Left(future::pending())
                }
                AdoptionResult::SomeAdoptionFailed => {
                    let timeout = next_backoff
                        .next_backoff()
                        .expect("backoff configured with no max elapsed time");
                    info!(
                        self.log,
                        "Will retry failed disk adoption after {:?}", timeout
                    );
                    Either::Right(tokio::time::sleep(timeout))
                }
            };

            // Wait until either we need to retry (if the above adoption failed)
            // or there's a change in the raw disks.
            tokio::select! {
                // Cancel-safe per docs on `sleep()`.
                _ = retry_timeout => {
                    continue;
                }
                // Cancel-safe per docs on `changed()`.
                result = self.raw_disks.changed() => {
                    match result {
                        Ok(()) => (),
                        Err(_) => {
                            // The `RawDisk` watch channel should never be
                            // closed in production, but could be in tests. All
                            // we can do here is exit; no further updates are
                            // coming.
                            error!(
                                self.log,
                                "InternalDisksTask exiting unexpectedly: \
                                 RawDisk watch channel closed by sender"
                            );
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn adopt_internal_disks<T: DiskAdopter>(
        &mut self,
        disk_adopter: &mut T,
    ) -> AdoptionResult {
        // Collect the internal disks into a Vec<_> to avoid holding the watch
        // channel lock while we attempt to adopt disks below.
        let internal_raw_disks = self
            .raw_disks
            .borrow_and_update()
            .iter()
            .filter(|disk| match disk.variant() {
                DiskVariant::U2 => false,
                DiskVariant::M2 => true,
            })
            .cloned()
            .collect::<IdMap<_>>();
        info!(
            self.log,
            "Attempting to ensure adoption of {} internal disks",
            internal_raw_disks.len(),
        );

        // Clone our current set of adopted disks. This allows us to do the
        // below calculations without holding the lock. If we need to make any
        // changes, we'll do so via `Arc::make_mut()` after we reacquire the
        // lock.
        let disks_snapshot = Arc::clone(&*self.disks.borrow());

        // Note for removal any disks we no longer have.
        let disks_to_remove = disks_snapshot
            .iter()
            .filter_map(|disk| {
                let identity = disk.identity();
                if !internal_raw_disks.contains_key(identity) {
                    Some(identity.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // Attempt to adopt any disks that are missing, or update properties of
        // any disks we've already adopted but that have changed.
        let mut disks_to_insert = Vec::new();
        let mut overall_result = AdoptionResult::AllSuccess;
        for raw_disk in internal_raw_disks {
            match disks_snapshot.get(raw_disk.identity()) {
                Some(existing_disk) => {
                    let existing_raw_disk =
                        RawDisk::from(existing_disk.clone());
                    if raw_disk != existing_raw_disk {
                        // TODO-correctness What if some property other than the
                        // disk firmware changed?
                        info!(
                            self.log, "Updating disk firmware metadata";
                            "old" => ?existing_disk.firmware(),
                            "new" => ?raw_disk.firmware(),
                            "identity" => ?existing_disk.identity(),
                        );
                        let mut existing_disk = existing_disk.clone();
                        existing_disk.update_firmware_metadata(&raw_disk);
                        disks_to_insert.push(existing_disk);
                    }
                }
                None => {
                    let identity = raw_disk.identity().clone();
                    let adopt_result = disk_adopter
                        .adopt_disk(raw_disk, &self.mount_config, &self.log)
                        .await;
                    let disk = match adopt_result {
                        Ok(disk) => disk,
                        Err(err) => {
                            // Disk adoption failed: there isn't much we can
                            // do other than retry later. Log an error, and
                            // tell `run()` that it should retry adoption in
                            // case the cause is transient.
                            warn!(
                                self.log, "Internal disk adoption failed";
                                "identity" => ?identity,
                                InlineErrorChain::new(&err),
                            );
                            overall_result = AdoptionResult::SomeAdoptionFailed;
                            continue;
                        }
                    };
                    info!(
                        self.log, "Adopted new internal disk";
                        "identity" => ?identity,
                    );
                    disks_to_insert.push(disk);
                }
            }
        }

        // Drop `disks_snapshot` now: we're done using to decide whether we have
        // any disks to remove or insert, and its liveness would guarantee the
        // `Arc::make_mut()` below would have to clone the full disks map. We
        // might still have to clone it if outside callers have a live clone,
        // but we can save the deep clone any time that isn't true.
        mem::drop(disks_snapshot);

        if !disks_to_remove.is_empty() || !disks_to_insert.is_empty() {
            self.disks.send_modify(|disks| {
                let disks = Arc::make_mut(disks);
                for disk in disks_to_remove {
                    disks.remove(&disk);
                }
                for disk in disks_to_insert {
                    disks.insert(disk);
                }
            });
        }

        overall_result
    }
}

enum AdoptionResult {
    AllSuccess,
    SomeAdoptionFailed,
}

/// Helper to allow unit tests to run without interacting with the real [`Disk`]
/// implementation. In production, the only implementor of this trait is
/// [`RealDiskAdopter`].
trait DiskAdopter {
    fn adopt_disk(
        &mut self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        log: &Logger,
    ) -> impl Future<Output = Result<Disk, DiskError>> + Send;
}

struct RealDiskAdopter;

impl DiskAdopter for RealDiskAdopter {
    async fn adopt_disk(
        &mut self,
        raw_disk: RawDisk,
        mount_config: &MountConfig,
        log: &Logger,
    ) -> Result<Disk, DiskError> {
        Disk::new(
            log,
            mount_config,
            raw_disk,
            None, // pool_id (control plane doesn't manage M.2s)
            None, // key_requester (M.2s are unencrypted)
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use illumos_utils::zpool::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_hardware::DiskFirmware;
    use sled_hardware::DiskPaths;
    use sled_hardware::PooledDisk;
    use sled_hardware::UnparsedDisk;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct TestDiskAdopter {
        requests: Mutex<Vec<RawDisk>>,
    }

    impl DiskAdopter for Arc<TestDiskAdopter> {
        async fn adopt_disk(
            &mut self,
            raw_disk: RawDisk,
            _mount_config: &MountConfig,
            _log: &Logger,
        ) -> Result<Disk, DiskError> {
            // InternalDisks should only adopt M2 disks
            assert_eq!(raw_disk.variant(), DiskVariant::M2);
            let disk = Disk::Real(PooledDisk {
                paths: DiskPaths {
                    devfs_path: "/fake-disk".into(),
                    dev_path: None,
                },
                slot: raw_disk.slot(),
                variant: raw_disk.variant(),
                identity: raw_disk.identity().clone(),
                is_boot_disk: raw_disk.is_boot_disk(),
                partitions: vec![],
                zpool_name: ZpoolName::new_internal(ZpoolUuid::new_v4()),
                firmware: raw_disk.firmware().clone(),
            });
            self.requests.lock().unwrap().push(raw_disk);
            Ok(disk)
        }
    }

    fn any_mount_config() -> MountConfig {
        MountConfig {
            root: "/sled-agent-tests".into(),
            synthetic_disk_root: "/sled-agent-tests".into(),
        }
    }

    fn new_raw_test_disk(variant: DiskVariant, serial: &str) -> RawDisk {
        RawDisk::Real(UnparsedDisk::new(
            "/test-devfs".into(),
            None,
            0,
            variant,
            omicron_common::disk::DiskIdentity {
                vendor: "test".into(),
                model: "test".into(),
                serial: serial.into(),
            },
            false,
            DiskFirmware::new(0, None, false, 1, vec![]),
        ))
    }

    #[tokio::test]
    async fn test_only_adopts_m2_disks() {
        let logctx = dev::test_setup_log("test_only_adopts_m2_disks");

        let (raw_disks_tx, raw_disks_rx) = watch::channel(IdMap::new());
        let disk_adopter = Arc::new(TestDiskAdopter::default());
        let mut disks_handle = InternalDisksTask::spawn_impl(
            Arc::new(any_mount_config()),
            raw_disks_rx,
            logctx.log.clone(),
            Arc::clone(&disk_adopter),
        );

        // There should be no disks to start.
        assert_eq!(*disks_handle.borrow_and_update().disks, Arc::default());

        // Add four disks: two M.2 and two U.2.
        raw_disks_tx.send_modify(|disks| {
            for disk in [
                new_raw_test_disk(DiskVariant::M2, "m2-0"),
                new_raw_test_disk(DiskVariant::U2, "u2-0"),
                new_raw_test_disk(DiskVariant::M2, "m2-1"),
                new_raw_test_disk(DiskVariant::U2, "u2-1"),
            ] {
                disks.insert(disk);
            }
        });

        // Wait for the adopted disks to change; this should happen nearly
        // immediately, but we'll put a timeout on to avoid hanging if something
        // is broken.
        tokio::time::timeout(Duration::from_secs(60), disks_handle.changed())
            .await
            .expect("disks changed before timeout")
            .expect("changed() succeeded");

        // We should see the two M.2s only.
        let adopted_disks = disks_handle.borrow_and_update().disks.clone();
        assert_eq!(adopted_disks.len(), 2);
        assert!(
            adopted_disks.iter().any(|disk| disk.identity().serial == "m2-0"),
            "found disk m2-0"
        );
        assert!(
            adopted_disks.iter().any(|disk| disk.identity().serial == "m2-1"),
            "found disk m2-1"
        );

        logctx.cleanup_successful();
    }

    // TODO-john more tests
    // * retry on adoption failure
    // * remove disks
}
