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
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

#[derive(Debug, Clone)]
pub struct InternalDisksHandle {
    disks: watch::Receiver<IdMap<Disk>>,
    mount_config: Arc<MountConfig>,
}

impl InternalDisksHandle {
    pub fn borrow_and_update(&mut self) -> InternalDisks<'_> {
        InternalDisks {
            disks: self.disks.borrow_and_update(),
            mount_config: &self.mount_config,
        }
    }

    pub async fn changed(&mut self) -> Result<(), RecvError> {
        self.disks.changed().await
    }
}

pub struct InternalDisks<'a> {
    disks: watch::Ref<'a, IdMap<Disk>>,
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
    disks: watch::Sender<IdMap<Disk>>,
    raw_disks: watch::Receiver<IdMap<RawDisk>>,
    mount_config: Arc<MountConfig>,
    log: Logger,
}

impl InternalDisksTask {
    pub fn spawn(
        mount_config: MountConfig,
        raw_disks: watch::Receiver<IdMap<RawDisk>>,
        log: Logger,
    ) -> InternalDisksHandle {
        let (disks_tx, disks_rx) = watch::channel(IdMap::default());

        let mount_config = Arc::new(mount_config);

        tokio::spawn({
            let task = Self {
                disks: disks_tx,
                raw_disks,
                mount_config: Arc::clone(&mount_config),
                log,
            };
            task.run()
        });

        InternalDisksHandle { disks: disks_rx, mount_config }
    }

    async fn run(mut self) {
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
            let retry_timeout = match self.adopt_internal_disks().await {
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

    async fn adopt_internal_disks(&mut self) -> AdoptionResult {
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
        // below calculations without holding the lock.
        let disks_snapshot = self.disks.borrow().clone();

        // Note for removal any disks we no longer have.
        let disks_to_remove = disks_snapshot
            .iter()
            .filter_map(|disk| {
                if !internal_raw_disks.contains_key(disk.identity()) {
                    Some(disk.identity())
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
                    let disk = match Disk::new(
                        &self.log,
                        &self.mount_config,
                        raw_disk,
                        None, // pool_id (control plane doesn't manage M.2s)
                        None, // key_requester (M.2s are unencrypted)
                    )
                    .await
                    {
                        Ok(disk) => disk,
                        Err(err) => {
                            // Disk adoption failed: there isn't much we can do
                            // other than retry later. Log an error, and tell
                            // `run()` that it should retry adoption in case the
                            // cause is transient.
                            error!(
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

        if !disks_to_remove.is_empty() || !disks_to_insert.is_empty() {
            self.disks.send_modify(|disks| {
                for disk in disks_to_remove {
                    disks.remove(disk);
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
