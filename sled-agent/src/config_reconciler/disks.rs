// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use id_map::Entry;
use id_map::IdMap;
use id_map::IdMappable;
use id_map::OccupiedEntry;
use id_map::VacantEntry;
use illumos_utils::zpool::ZpoolName;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::DiskVariant;
use sled_storage::config::MountConfig;
use sled_storage::dataset::CONFIG_DATASET;
use sled_storage::disk::Disk;
use sled_storage::disk::DiskError;
use sled_storage::disk::RawDisk;
use slog::Logger;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AllDisks {
    disks: IdMap<KnownDisk>,
    mount_config: MountConfig,
}

impl AllDisks {
    /// Add or update a disk.
    ///
    /// Returns `true` if any changes were made; false if we were asked to
    /// update a disk but none of its properties changed.
    pub async fn add_or_update_disk(
        &mut self,
        raw_disk: RawDisk,
        log: &Logger,
    ) -> Result<bool, DiskError> {
        let identity = raw_disk.id();
        let log = {
            let DiskIdentity { vendor, model, serial } = identity.clone();
            log.new(o!(
                "disk-vendor" => vendor,
                "disk-model" => model,
                "disk-serial" => serial,
            ))
        };
        info!(log, "Inserting or updating disk");

        let updated = match self.disks.entry(identity) {
            Entry::Vacant(entry) => {
                Self::add_new_disk(raw_disk, entry, &self.mount_config, &log)
                    .await?;
                true
            }
            Entry::Occupied(entry) => {
                Self::update_existing_disk(raw_disk, entry, &log)
            }
        };

        Ok(updated)
    }

    async fn add_new_disk(
        raw_disk: RawDisk,
        entry: VacantEntry<'_, KnownDisk>,
        mount_config: &MountConfig,
        log: &Logger,
    ) -> Result<(), DiskError> {
        match raw_disk.variant() {
            DiskVariant::U2 => {
                // Record this disk as an `UnmanagedU2`. We might start managing
                // it shortly, but the common cases are that we wouldn't have
                // enough information to do so now:
                //
                // 1. When booting, we add all the M.2s and U.2s in a batch, so
                //    we wouldn't have read the ledger yet to know whether we're
                //    supposed to manage this disk.
                // 2. When a new disk is added, it will show up physically (and
                //    we'll land here) before the control plane tells us to
                //    start managing it.
                //
                // There are less common cases where we could start managing
                // this immediately (e.g., the boot path somehow only found M.2s
                // first and then later found U.2s, or a disk we were previously
                // configured to manage has shown back up some time after boot).
                // But it doesn't seem important to optimize for these; our
                // parent reconciler will convert this from unmanaged to managed
                // soon enough.
                entry.insert(KnownDisk::UnmanagedU2(raw_disk));
                info!(log, "Inserted new U.2 disk (currently unmanaged)");
            }
            DiskVariant::M2 => {
                let disk = Disk::new(
                    log,
                    mount_config,
                    raw_disk,
                    None, // pool_id (control plane doesn't manage M.2s)
                    None, // key_requester (M.2s are unencrypted)
                )
                .await?;
                entry.insert(KnownDisk::ImplicitlyManagedM2(disk));
                info!(log, "Inserted new M.2 disk");
            }
        }
        Ok(())
    }

    fn update_existing_disk(
        raw_disk: RawDisk,
        mut entry: OccupiedEntry<'_, KnownDisk>,
        log: &Logger,
    ) -> bool {
        let mut updated = false;

        match &mut *entry.get_mut() {
            KnownDisk::ImplicitlyManagedM2(disk)
            | KnownDisk::ManagedU2(disk) => {
                let old = RawDisk::from(disk.clone());
                if old != raw_disk {
                    info!(
                        log, "Updating disk firmware metadata";
                        "old" => ?old.firmware(),
                        "new" => ?disk.firmware(),
                    );
                    disk.update_firmware_metadata(&raw_disk);
                    updated = true;
                }
            }
            KnownDisk::UnmanagedU2(old) => {
                if *old != raw_disk {
                    *old = raw_disk;
                    updated = true;
                }
            }
        }

        if !updated {
            info!(log, "Disk already exists and has no updates");
        }

        updated
    }

    /// Returns all mountpoints within all M.2s for a particular dataset.
    pub fn all_m2_mountpoints<'a>(
        &'a self,
        dataset: &'a str,
    ) -> impl Iterator<Item = Utf8PathBuf> + 'a {
        self.all_zpools(DiskVariant::M2).map(|zpool| {
            zpool.dataset_mountpoint(&self.mount_config.root, dataset)
        })
    }

    /// Returns all `CONFIG_DATASET` paths within available M.2 disks.
    pub fn all_m2_config_datasets(
        &self,
    ) -> impl Iterator<Item = Utf8PathBuf> + '_ {
        self.all_m2_mountpoints(CONFIG_DATASET)
    }

    // Returns all zpools of a particular variant.
    //
    // Only returns zpools from disks actively being managed.
    fn all_zpools(
        &self,
        variant: DiskVariant,
    ) -> impl Iterator<Item = &ZpoolName> {
        self.disks.iter().filter_map(move |disk| match disk {
            KnownDisk::ImplicitlyManagedM2(disk)
            | KnownDisk::ManagedU2(disk) => {
                if disk.variant() == variant {
                    Some(disk.zpool_name())
                } else {
                    None
                }
            }
            KnownDisk::UnmanagedU2(_) => None,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum KnownDisk {
    // An M.2 disk (implicitly managed by us).
    ImplicitlyManagedM2(Disk),

    // A U.2 disk explicitly managed by the control plane.
    ManagedU2(Disk),

    // A U.2 disk that is present, but we haven't started managing yet.
    //
    // This may be because the control plane hasn't told us to use it, or it may
    // be because we're still waiting on the key manager.
    UnmanagedU2(RawDisk),
}

impl IdMappable for KnownDisk {
    type Id = DiskIdentity;

    fn id(&self) -> Self::Id {
        // TODO-performance This clones three `String`s; maybe we should use
        // `Arc<DiskIdentity>` instead?
        let identity = match self {
            KnownDisk::ImplicitlyManagedM2(disk)
            | KnownDisk::ManagedU2(disk) => disk.identity(),
            KnownDisk::UnmanagedU2(raw_disk) => raw_disk.identity(),
        };
        identity.clone()
    }
}
