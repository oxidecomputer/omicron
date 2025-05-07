// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Discovered and usable disks and zpools

use crate::config::MountConfig;
use crate::dataset::{DatasetError, M2_DEBUG_DATASET};
use crate::disk::{Disk, DiskError, RawDisk};
use crate::error::Error;
use camino::Utf8PathBuf;
use cfg_if::cfg_if;
use illumos_utils::zpool::{PathInPool, ZpoolName, ZpoolOrRamdisk};
use key_manager::StorageKeyRequester;
use omicron_common::api::external::Generation;
use omicron_common::disk::{
    DiskIdentity, DiskManagementError, DiskManagementStatus, DiskVariant,
    DisksManagementResult, OmicronPhysicalDiskConfig,
    OmicronPhysicalDisksConfig,
};
use sled_hardware::DiskFirmware;
use slog::{Logger, error, info, o, warn};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;

// The directory within the debug dataset in which bundles are created.
const BUNDLE_DIRECTORY: &str = "bundle";

// The directory for zone bundles.
const ZONE_BUNDLE_DIRECTORY: &str = "zone";

// The Sled Agent is responsible for both observing disks and managing them at
// the request of the broader control plane. This enum encompasses that duality,
// by representing all disks that can exist, managed or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManagedDisk {
    // A disk explicitly managed by the control plane.
    //
    // This includes U.2s which Nexus has told us to format and use.
    ExplicitlyManaged(Disk),

    // A disk implicitly managed by the control plane.
    //
    // This includes M.2s which the sled agent auto-detects and uses.
    ImplicitlyManaged(Disk),

    // A disk which has been observed by the sled, but which is not yet being
    // managed by the control plane.
    //
    // This disk should be treated as "read-only" until we're explicitly told to
    // use it.
    Unmanaged(RawDisk),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AllDisksInner {
    values: BTreeMap<DiskIdentity, ManagedDisk>,
}

/// The disks, keyed by their identity, managed by the sled agent.
///
/// This state is owned by [`crate::manager::StorageManager`], through
/// [`crate::resources::StorageResources`]. Clones of this state can be
/// retrieved by requests to the `StorageManager` task from the
/// [`crate::manager::StorageHandle`]. This state is not `Sync`, and as such
/// does not require any mutexes. However, we do expect to share it relatively
/// frequently, and we want copies of it to be as cheaply made as possible. So
/// any large state is stored inside `Arc`s. On the other hand, we expect
/// infrequent updates to this state, and as such, we use
/// [`std::sync::Arc::make_mut`] to implement clone on write functionality
/// inside the `StorageManager` task if there are any outstanding copies.
/// Therefore, we only pay the cost to update infrequently, and no locks are
/// required by callers when operating on cloned data. The only contention here
/// is for the reference counters of the internal Arcs when `AllDisks`
/// gets cloned or dropped.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AllDisks {
    // This generation corresponds to the generation supplied in
    // [OmicronPhysicalDisksConfig].
    generation: Generation,
    inner: Arc<AllDisksInner>,
    mount_config: MountConfig,
}

impl AllDisks {
    /// Returns the latest generation number of this set of disks.
    pub fn generation(&self) -> &Generation {
        &self.generation
    }

    pub fn mount_config(&self) -> &MountConfig {
        &self.mount_config
    }

    /// Returns the identity of the boot disk.
    ///
    /// If this returns `None`, we have not processed the boot disk yet.
    pub fn boot_disk(&self) -> Option<(DiskIdentity, ZpoolName)> {
        for (id, disk) in self.inner.values.iter() {
            if let ManagedDisk::ImplicitlyManaged(disk) = disk {
                if disk.is_boot_disk() {
                    return Some((id.clone(), *disk.zpool_name()));
                }
            }
        }
        None
    }

    /// Returns all M.2 zpools
    pub fn all_m2_zpools(&self) -> Vec<ZpoolName> {
        self.all_zpools(DiskVariant::M2)
    }

    /// Returns all U.2 zpools
    pub fn all_u2_zpools(&self) -> Vec<ZpoolName> {
        self.all_zpools(DiskVariant::U2)
    }

    /// Returns all mountpoints within all M.2s for a particular dataset.
    pub fn all_m2_mountpoints(&self, dataset: &str) -> Vec<Utf8PathBuf> {
        self.all_m2_zpools()
            .iter()
            .map(|zpool| {
                zpool.dataset_mountpoint(&self.mount_config.root, dataset)
            })
            .collect()
    }

    /// Returns all mountpoints within all U.2s for a particular dataset.
    pub fn all_u2_mountpoints(&self, dataset: &str) -> Vec<PathInPool> {
        self.all_u2_zpools()
            .into_iter()
            .map(|pool| {
                let path =
                    pool.dataset_mountpoint(&self.mount_config.root, dataset);
                PathInPool { pool: ZpoolOrRamdisk::Zpool(pool), path }
            })
            .collect()
    }

    /// Returns all zpools managed by the control plane
    pub fn get_all_zpools(&self) -> Vec<(ZpoolName, DiskVariant)> {
        self.inner
            .values
            .values()
            .filter_map(|disk| match disk {
                ManagedDisk::ExplicitlyManaged(disk)
                | ManagedDisk::ImplicitlyManaged(disk) => {
                    Some((*disk.zpool_name(), disk.variant()))
                }
                ManagedDisk::Unmanaged(_) => None,
            })
            .collect()
    }

    // Returns all zpools of a particular variant.
    //
    // Only returns zpools from disks actively being managed.
    fn all_zpools(&self, variant: DiskVariant) -> Vec<ZpoolName> {
        self.inner
            .values
            .values()
            .filter_map(|disk| match disk {
                ManagedDisk::ExplicitlyManaged(disk)
                | ManagedDisk::ImplicitlyManaged(disk) => {
                    if disk.variant() == variant {
                        return Some(*disk.zpool_name());
                    }
                    None
                }
                ManagedDisk::Unmanaged(_) => None,
            })
            .collect()
    }

    /// Return the directories for storing zone service bundles.
    pub fn all_zone_bundle_directories(&self) -> Vec<Utf8PathBuf> {
        self.all_m2_mountpoints(M2_DEBUG_DATASET)
            .into_iter()
            .map(|p| p.join(BUNDLE_DIRECTORY).join(ZONE_BUNDLE_DIRECTORY))
            .collect()
    }

    /// Return the directories that can be used for temporary sled-diagnostics
    /// file storage.
    pub fn all_sled_diagnostics_directories(&self) -> Vec<Utf8PathBuf> {
        // These directories are currently used for tempfile storage when
        // zipping up zone logs before shuffling them off to a nexus collecting
        // a support bundle.
        self.all_m2_mountpoints(M2_DEBUG_DATASET).into_iter().collect()
    }

    /// Returns an iterator over all managed disks.
    pub fn iter_managed(&self) -> impl Iterator<Item = (&DiskIdentity, &Disk)> {
        self.inner.values.iter().filter_map(|(identity, disk)| match disk {
            ManagedDisk::ExplicitlyManaged(disk) => Some((identity, disk)),
            ManagedDisk::ImplicitlyManaged(disk) => Some((identity, disk)),
            _ => None,
        })
    }

    /// Returns an iterator over all disks, managed or not.
    pub fn iter_all(
        &self,
    ) -> impl Iterator<Item = (&DiskIdentity, DiskVariant, i64, &DiskFirmware)>
    {
        self.inner.values.iter().map(|(identity, disk)| match disk {
            ManagedDisk::ExplicitlyManaged(disk) => {
                (identity, disk.variant(), disk.slot(), disk.firmware())
            }
            ManagedDisk::ImplicitlyManaged(disk) => {
                (identity, disk.variant(), disk.slot(), disk.firmware())
            }
            ManagedDisk::Unmanaged(raw) => {
                (identity, raw.variant(), raw.slot(), raw.firmware())
            }
        })
    }
}

/// The intersection of "physical disks noticed by hardware" and "physical
/// disks requested by the control plane".
#[derive(Debug)]
pub struct StorageResources {
    log: Logger,

    key_requester: StorageKeyRequester,

    // All disks, real and synthetic, that exist within this sled
    disks: watch::Sender<AllDisks>,

    // The last set of disks the control plane explicitly told us to manage.
    //
    // Only includes external storage (U.2s).
    control_plane_config: ControlPlaneConfig,
}

impl StorageResources {
    pub fn new(
        log: &Logger,
        mount_config: MountConfig,
        key_requester: StorageKeyRequester,
    ) -> Self {
        let disks = AllDisks {
            generation: Generation::new(),
            inner: Arc::new(AllDisksInner { values: BTreeMap::new() }),
            mount_config,
        };
        Self {
            log: log.new(o!("component" => "StorageResources")),
            key_requester,
            disks: watch::Sender::new(disks),
            control_plane_config: ControlPlaneConfig::new(),
        }
    }

    /// Monitors the set of disks for any updates
    pub fn watch_disks(&self) -> watch::Receiver<AllDisks> {
        self.disks.subscribe()
    }

    /// Gets the set of all disks
    pub fn disks(&self) -> AllDisks {
        self.disks.borrow().clone()
    }

    /// Sets the "control plane disk" state, as last requested by Nexus.
    ///
    /// Does not attempt to manage any of the physical disks previously
    /// observed. To synchronize the "set of requested disks" with the "set of
    /// observed disks", call [Self::synchronize_disk_management].
    pub fn set_config(&mut self, config: &OmicronPhysicalDisksConfig) {
        if self.control_plane_config.generation > config.generation {
            return;
        }
        self.control_plane_config = ControlPlaneConfig::from_config(config);
    }

    pub fn get_config(
        &self,
    ) -> &BTreeMap<DiskIdentity, OmicronPhysicalDiskConfig> {
        &self.control_plane_config.disks
    }

    /// Attempts to "manage" all the U.2 disks requested by the control plane.
    ///
    /// If any requested physical disks have not been observed by the hardware
    /// monitor, they are ignored.
    /// If the hardware monitor has observed disks that are not requested, they
    /// are ignored.
    ///
    /// Attempts to manage all disks possible, and returns an error on partial
    /// failure, indicating "which disks have failed to be synchronized".
    pub async fn synchronize_disk_management(
        &mut self,
    ) -> DisksManagementResult {
        self.disks_send_if_modified(async |self_, all_disks| {
            Self::synchronize_disk_management_impl(
                all_disks,
                &mut self_.control_plane_config,
                &self_.key_requester,
                &self_.log,
            )
            .await
        })
        .await
    }

    async fn synchronize_disk_management_impl(
        all_disks: &mut AllDisks,
        control_plane_config: &mut ControlPlaneConfig,
        key_requester: &StorageKeyRequester,
        log: &Logger,
    ) -> DisksManagementResult {
        let disks = Arc::make_mut(&mut all_disks.inner);
        info!(log, "Synchronizing disk managment");

        if all_disks.generation < control_plane_config.generation {
            all_disks.generation = control_plane_config.generation;
        } else if all_disks.generation > control_plane_config.generation {
            // This should never happen; `set_config()` rejects updates to
            // `control_plane_config` that go backwards. This could probably be
            // an `assert!`, but there's enough action-at-a-distance between
            // `set_config` and this function that I'm worried we could actually
            // see this; we'll log an error and return an empty set of results.
            error!(
                log,
                "refusing to downgrade disk config generation";
                "in-memory generation" => %all_disks.generation,
                "incoming generation" => %control_plane_config.generation,
            );
            return DisksManagementResult::default();
        }

        // "Unmanage" all disks no longer requested by the control plane.
        //
        // This updates the reported sets of "managed" disks, and performs no
        // other modifications to the underlying storage.
        for (identity, managed_disk) in &mut disks.values {
            match managed_disk {
                // This leaves the presence of the disk still in "Self", but
                // downgrades the disk to an unmanaged status.
                ManagedDisk::ExplicitlyManaged(disk) => {
                    if !control_plane_config.disks.contains_key(identity) {
                        *managed_disk =
                            ManagedDisk::Unmanaged(RawDisk::from(disk.clone()));
                    }
                }
                _ => (),
            }
        }

        // "Manage" all disks that the control plane wants.
        //
        // If the disk can be successfully managed, and it's new, it will be
        // formatted with a zpool identified by the Nexus-specified
        // configuration.
        let mut result = DisksManagementResult::default();
        for (identity, config) in &control_plane_config.disks {
            let Some(managed_disk) = disks.values.get_mut(identity) else {
                warn!(
                    log,
                    "Control plane disk requested, but not detected within sled";
                    "disk_identity" => ?identity
                );
                result.status.push(DiskManagementStatus {
                    identity: identity.clone(),
                    err: Some(DiskManagementError::NotFound),
                });
                continue;
            };
            info!(log, "Managing disk"; "disk_identity" => ?identity);
            match managed_disk {
                // Disk is currently unmanaged. Try to adopt the disk, which may
                // involve formatting it, and emplacing the zpool.
                ManagedDisk::Unmanaged(raw_disk) => {
                    match Self::begin_disk_management(
                        &log,
                        &all_disks.mount_config,
                        raw_disk,
                        config,
                        Some(key_requester),
                    )
                    .await
                    {
                        Ok(disk) => {
                            info!(log, "Disk management started successfully"; "disk_identity" => ?identity);
                            *managed_disk = disk;
                        }
                        Err(err) => {
                            warn!(log, "Cannot parse disk"; "err" => ?err);
                            result.status.push(DiskManagementStatus {
                                identity: identity.clone(),
                                err: Some(err),
                            });
                            continue;
                        }
                    }
                }
                // Disk is already managed. Check that the configuration
                // matches what we expect.
                ManagedDisk::ExplicitlyManaged(disk) => {
                    let expected = config.pool_id;
                    let observed = disk.zpool_name().id();
                    if expected != observed {
                        warn!(
                            log,
                            "Observed an unexpected zpool uuid";
                            "expected" => ?expected, "observed" => ?observed
                        );
                        result.status.push(DiskManagementStatus {
                            identity: identity.clone(),
                            err: Some(DiskManagementError::ZpoolUuidMismatch {
                                expected,
                                observed,
                            }),
                        });
                        continue;
                    }
                    info!(log, "Disk already managed successfully"; "disk_identity" => ?identity);
                }
                // Skip disks that are managed implicitly
                ManagedDisk::ImplicitlyManaged(_) => continue,
            }

            result.status.push(DiskManagementStatus {
                identity: identity.clone(),
                err: None,
            });
        }
        result
    }

    // Helper function to help transition an "unmanaged" disk to a "managed"
    // disk.
    async fn begin_disk_management(
        log: &Logger,
        mount_config: &MountConfig,
        raw_disk: &RawDisk,
        config: &OmicronPhysicalDiskConfig,
        key_requester: Option<&StorageKeyRequester>,
    ) -> Result<ManagedDisk, DiskManagementError> {
        info!(log, "Invoking Disk::new on an unmanaged disk");
        let disk = Disk::new(
            &log,
            mount_config,
            raw_disk.clone(),
            Some(config.pool_id),
            key_requester,
        )
        .await
        .map_err(|err| {
            error!(log, "Disk::new failed"; "err" => ?err);
            match err {
                // We pick this error out and identify it separately because
                // it may be transient, and should sometimes be handled with
                // a retry.
                DiskError::Dataset(DatasetError::KeyManager(_)) => {
                    DiskManagementError::KeyManager(err.to_string())
                }
                err => DiskManagementError::Other(err.to_string()),
            }
        })?;
        info!(log, "Disk::new completed successfully"; "disk_identity" => ?raw_disk.identity());
        Ok(ManagedDisk::ExplicitlyManaged(disk))
    }

    /// Tracks a new disk, or updates an existing disk.
    ///
    /// For U.2s: Does not automatically attempt to manage disks -- for this,
    /// the caller will need to also invoke
    /// [`Self::synchronize_disk_management`].
    ///
    /// For M.2s: As no additional control plane guidance is necessary to adopt
    /// M.2s, these are automatically managed.
    pub(crate) async fn insert_or_update_disk(
        &mut self,
        disk: RawDisk,
    ) -> Result<(), Error> {
        self.disks_send_if_modified(async |self_, all_disks| {
            Self::insert_or_update_disk_impl(
                disk,
                all_disks,
                &self_.key_requester,
                &self_.log,
            )
            .await
        })
        .await
    }

    async fn insert_or_update_disk_impl(
        disk: RawDisk,
        all_disks: &mut AllDisks,
        key_requester: &StorageKeyRequester,
        log: &Logger,
    ) -> Result<(), Error> {
        let disk_identity = disk.identity().clone();
        info!(log, "Inserting disk"; "identity" => ?disk_identity);

        // This is a trade-off for simplicity even though we may be potentially
        // cloning data before we know if there is a write action to perform.
        let disks = Arc::make_mut(&mut all_disks.inner);

        // First check if there are any updates we need to apply to existing
        // managed disks.
        if let Some(managed) = disks.values.get_mut(&disk_identity) {
            let mut updated = false;
            match managed {
                ManagedDisk::ExplicitlyManaged(mdisk)
                | ManagedDisk::ImplicitlyManaged(mdisk) => {
                    let old = RawDisk::from(mdisk.clone());
                    if old != disk {
                        mdisk.update_firmware_metadata(&disk);
                        updated = true;
                    }
                }
                ManagedDisk::Unmanaged(raw) => {
                    if raw != &disk {
                        *raw = disk;
                        updated = true;
                    }
                }
            };

            if !updated {
                info!(log, "Disk already exists and has no updates";
                    "identity" => ?disk_identity);
            }

            return Ok(());
        }

        // If there's no update then we are inserting a new disk.
        match disk.variant() {
            DiskVariant::U2 => {
                disks
                    .values
                    .insert(disk_identity, ManagedDisk::Unmanaged(disk));
            }
            DiskVariant::M2 => {
                let managed_disk = Disk::new(
                    &log,
                    &all_disks.mount_config,
                    disk,
                    None,
                    Some(key_requester),
                )
                .await?;
                disks.values.insert(
                    disk_identity,
                    ManagedDisk::ImplicitlyManaged(managed_disk),
                );
            }
        }

        Ok(())
    }

    /// Delete a disk and its zpool
    ///
    /// Return true, if data was changed, false otherwise
    ///
    /// Note: We never allow removal of synthetic disks in production as they
    /// are only added once.
    pub(crate) fn remove_disk(&mut self, id: &DiskIdentity) {
        self.disks.send_if_modified(|all_disks| {
            Self::remove_disk_impl(id, all_disks, &self.log)
        });
    }

    pub(crate) fn remove_disk_impl(
        id: &DiskIdentity,
        all_disks: &mut AllDisks,
        log: &Logger,
    ) -> bool {
        info!(log, "Removing disk"; "identity" => ?id);
        let Some(entry) = all_disks.inner.values.get(id) else {
            info!(log, "Disk not found by id, exiting"; "identity" => ?id);
            return false;
        };

        let synthetic = match entry {
            ManagedDisk::ExplicitlyManaged(disk)
            | ManagedDisk::ImplicitlyManaged(disk) => disk.is_synthetic(),
            ManagedDisk::Unmanaged(raw) => raw.is_synthetic(),
        };

        cfg_if! {
            if #[cfg(test)] {
                // For testing purposes, we allow synthetic disks to be deleted.
                // Silence an unused variable warning.
                _ = synthetic;
            } else {
                // In production, we disallow removal of synthetic disks as they
                // are only added once.
                if synthetic {
                    info!(log, "Not removing synthetic disk"; "identity" => ?id);
                    return false;
                }
            }
        }

        // Safe to unwrap as we just checked the key existed above
        let disks = Arc::make_mut(&mut all_disks.inner);
        disks.values.remove(id).unwrap();
        true
    }

    // This is a wrapper around `self.disks.send_if_modified()` with two
    // nontrivial changes:
    //
    // 1. We take an async closure instead of a sync closure
    // 2. The closure does not have to return a `bool` indicating whether or not
    //    it made changes; instead, we compare the `AllDisks` value before and
    //    after the closure is called to see if any changes were made.
    async fn disks_send_if_modified<F, T>(&mut self, op: F) -> T
    where
        F: AsyncFnOnce(&mut StorageResources, &mut AllDisks) -> T,
    {
        // Make a clone of our disks (held in a watch channel). We'll do all our
        // modifications to that clone, then update the channel if needed.
        let mut all_disks = self.disks();
        let result = op(self, &mut all_disks).await;

        // Update the watch channel if `op` made any changes.
        self.disks.send_if_modified(|previous| {
            if *previous != all_disks {
                *previous = all_disks;
                true
            } else {
                false
            }
        });

        result
    }
}

#[derive(Debug)]
struct ControlPlaneConfig {
    generation: Generation,
    disks: BTreeMap<DiskIdentity, OmicronPhysicalDiskConfig>,
}

impl ControlPlaneConfig {
    fn new() -> Self {
        Self { generation: Generation::new(), disks: BTreeMap::new() }
    }

    fn from_config(config: &OmicronPhysicalDisksConfig) -> Self {
        Self {
            generation: config.generation,
            disks: config
                .disks
                .iter()
                .map(|disk| (disk.identity.clone(), disk.clone()))
                .collect(),
        }
    }
}
