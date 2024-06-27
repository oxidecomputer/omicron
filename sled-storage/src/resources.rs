// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Discovered and usable disks and zpools

use crate::config::MountConfig;
use crate::dataset::{DatasetError, M2_DEBUG_DATASET};
use crate::disk::{Disk, DiskError, OmicronPhysicalDiskConfig, RawDisk};
use crate::error::Error;
use camino::Utf8PathBuf;
use cfg_if::cfg_if;
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::api::external::Generation;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::DiskVariant;
use slog::{info, o, warn, Logger};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;

// The directory within the debug dataset in which bundles are created.
const BUNDLE_DIRECTORY: &str = "bundle";

// The directory for zone bundles.
const ZONE_BUNDLE_DIRECTORY: &str = "zone";

#[derive(Debug, thiserror::Error, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum DiskManagementError {
    #[error("Disk requested by control plane, but not found on device")]
    NotFound,

    #[error("Expected zpool UUID of {expected}, but saw {observed}")]
    ZpoolUuidMismatch { expected: ZpoolUuid, observed: ZpoolUuid },

    #[error("Failed to access keys necessary to unlock storage. This error may be transient.")]
    KeyManager(String),

    #[error("Other error starting disk management: {0}")]
    Other(String),
}

impl DiskManagementError {
    fn retryable(&self) -> bool {
        match self {
            DiskManagementError::KeyManager(_) => true,
            _ => false,
        }
    }
}

/// Identifies how a single disk management operation may have succeeded or
/// failed.
#[derive(Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DiskManagementStatus {
    pub identity: DiskIdentity,
    pub err: Option<DiskManagementError>,
}

/// The result from attempting to manage underlying disks.
///
/// This is more complex than a simple "Error" type because it's possible
/// for some disks to be initialized correctly, while others can fail.
///
/// This structure provides a mechanism for callers to learn about partial
/// failures, and handle them appropriately on a per-disk basis.
#[derive(Default, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[must_use = "this `DiskManagementResult` may contain errors, which should be handled"]
pub struct DisksManagementResult {
    pub status: Vec<DiskManagementStatus>,
}

impl DisksManagementResult {
    pub fn has_error(&self) -> bool {
        for status in &self.status {
            if status.err.is_some() {
                return true;
            }
        }
        false
    }

    pub fn has_retryable_error(&self) -> bool {
        for status in &self.status {
            if let Some(err) = &status.err {
                if err.retryable() {
                    return true;
                }
            }
        }
        false
    }
}

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
    // NOTE: When the set of "omicron physical disks" are set through the API,
    // a generation number is actually passed via [OmicronPhysicalDisksConfig].
    //
    // This is *not* that same generation number, but probably could be?
    //
    // Technically, this number also bumps up any time the set of
    // locally-detected disks has changed, but it's just treated as an opaque
    // value to track when changes have propagated throughout the system.
    generation: Generation,
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
    inner: Arc<AllDisksInner>,
    mount_config: MountConfig,
}

impl AllDisks {
    /// Returns the latest generation number of this set of disks.
    pub fn generation(&self) -> &Generation {
        &self.inner.generation
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
                    return Some((id.clone(), disk.zpool_name().clone()));
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
    pub fn all_u2_mountpoints(&self, dataset: &str) -> Vec<Utf8PathBuf> {
        self.all_u2_zpools()
            .iter()
            .map(|zpool| {
                zpool.dataset_mountpoint(&self.mount_config.root, dataset)
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
                    Some((disk.zpool_name().clone(), disk.variant()))
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
                        return Some(disk.zpool_name().clone());
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

    /// Returns an iterator over all managed disks.
    pub fn iter_managed(&self) -> impl Iterator<Item = (&DiskIdentity, &Disk)> {
        self.inner.values.iter().filter_map(|(identity, disk)| match disk {
            ManagedDisk::ExplicitlyManaged(disk) => Some((identity, disk)),
            ManagedDisk::ImplicitlyManaged(disk) => Some((identity, disk)),
            _ => None,
        })
    }

    /// Returns an iterator over all disks, managed or not.
    ///
    /// Additionally identifies details of "how the disk is managed".
    pub(crate) fn iter_all_inner(
        &self,
    ) -> impl Iterator<Item = (&DiskIdentity, &ManagedDisk)> {
        self.inner.values.iter()
    }

    /// Returns an iterator over all disks, managed or not.
    // Intentionally avoids returning the "ManagedDisk" structure to callers, to
    // hide the details of management.
    //
    // If this is important to the caller, they should use
    // [Self::iter_all_inner].
    pub fn iter_all(
        &self,
    ) -> impl Iterator<Item = (&DiskIdentity, DiskVariant, i64)> {
        self.inner.values.iter().map(|(identity, disk)| match disk {
            ManagedDisk::ExplicitlyManaged(disk) => {
                (identity, disk.variant(), disk.slot())
            }
            ManagedDisk::ImplicitlyManaged(disk) => {
                (identity, disk.variant(), disk.slot())
            }
            ManagedDisk::Unmanaged(raw) => {
                (identity, raw.variant(), raw.slot())
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
    disks: AllDisks,

    // The last set of disks the control plane explicitly told us to manage.
    //
    // Only includes external storage (U.2s).
    control_plane_disks: BTreeMap<DiskIdentity, OmicronPhysicalDiskConfig>,

    // Many clients are interested when changes in the set of [AllDisks]
    // might occur. This watch channel is updated once these disks get updated.
    disk_updates: watch::Sender<AllDisks>,
}

impl StorageResources {
    pub fn new(
        log: &Logger,
        mount_config: MountConfig,
        key_requester: StorageKeyRequester,
    ) -> Self {
        let disks = AllDisks {
            inner: Arc::new(AllDisksInner {
                generation: Generation::new(),
                values: BTreeMap::new(),
            }),
            mount_config,
        };
        Self {
            log: log.new(o!("component" => "StorageResources")),
            key_requester,
            disks: disks.clone(),
            control_plane_disks: BTreeMap::new(),
            disk_updates: watch::Sender::new(disks),
        }
    }

    /// Monitors the set of disks for any updates
    pub fn watch_disks(&self) -> watch::Receiver<AllDisks> {
        self.disk_updates.subscribe()
    }

    /// Gets the set of all disks
    pub fn disks(&self) -> &AllDisks {
        &self.disks
    }

    /// Sets the "control plane disk" state, as last requested by Nexus.
    ///
    /// Does not attempt to manage any of the physical disks previously
    /// observed. To synchronize the "set of requested disks" with the "set of
    /// observed disks", call [Self::synchronize_disk_management].
    pub fn set_config(&mut self, config: &Vec<OmicronPhysicalDiskConfig>) {
        self.control_plane_disks = config
            .iter()
            .map(|disk| (disk.identity.clone(), disk.clone()))
            .collect();
    }

    pub fn get_config(
        &self,
    ) -> &BTreeMap<DiskIdentity, OmicronPhysicalDiskConfig> {
        &self.control_plane_disks
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
        let mut updated = false;
        let disks = Arc::make_mut(&mut self.disks.inner);
        info!(self.log, "Synchronizing disk managment");

        // "Unmanage" all disks no longer requested by the control plane.
        //
        // This updates the reported sets of "managed" disks, and performs no
        // other modifications to the underlying storage.
        for (identity, managed_disk) in &mut disks.values {
            match managed_disk {
                // This leaves the presence of the disk still in "Self", but
                // downgrades the disk to an unmanaged status.
                ManagedDisk::ExplicitlyManaged(disk) => {
                    if !self.control_plane_disks.contains_key(identity) {
                        *managed_disk =
                            ManagedDisk::Unmanaged(RawDisk::from(disk.clone()));
                        updated = true;
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
        for (identity, config) in &self.control_plane_disks {
            let Some(managed_disk) = disks.values.get_mut(identity) else {
                warn!(
                    self.log,
                    "Control plane disk requested, but not detected within sled";
                    "disk_identity" => ?identity
                );
                result.status.push(DiskManagementStatus {
                    identity: identity.clone(),
                    err: Some(DiskManagementError::NotFound),
                });
                continue;
            };
            info!(self.log, "Managing disk"; "disk_identity" => ?identity);
            match managed_disk {
                // Disk is currently unmanaged. Try to adopt the disk, which may
                // involve formatting it, and emplacing the zpool.
                ManagedDisk::Unmanaged(raw_disk) => {
                    match Self::begin_disk_management(
                        &self.log,
                        &self.disks.mount_config,
                        raw_disk,
                        config,
                        Some(&self.key_requester),
                    )
                    .await
                    {
                        Ok(disk) => {
                            info!(self.log, "Disk management started successfully"; "disk_identity" => ?identity);
                            *managed_disk = disk;
                            updated = true;
                        }
                        Err(err) => {
                            warn!(self.log, "Cannot parse disk"; "err" => ?err);
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
                            self.log,
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
                    info!(self.log, "Disk already managed successfully"; "disk_identity" => ?identity);
                }
                // Skip disks that are managed implicitly
                ManagedDisk::ImplicitlyManaged(_) => continue,
            }

            result.status.push(DiskManagementStatus {
                identity: identity.clone(),
                err: None,
            });
        }

        if updated {
            disks.generation = disks.generation.next();
            self.disk_updates.send_replace(self.disks.clone());
        }

        return result;
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
            warn!(log, "Disk::new failed"; "err" => ?err);
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

    /// Tracks a new disk.
    ///
    /// For U.2s: Does not automatically attempt to manage disks -- for this,
    /// the caller will need to also invoke
    /// [`Self::synchronize_disk_management`].
    ///
    /// For M.2s: As no additional control plane guidance is necessary to adopt
    /// M.2s, these are automatically managed.
    pub(crate) async fn insert_disk(
        &mut self,
        disk: RawDisk,
    ) -> Result<(), Error> {
        let disk_identity = disk.identity().clone();
        info!(self.log, "Inserting disk"; "identity" => ?disk_identity);
        if self.disks.inner.values.contains_key(&disk_identity) {
            info!(self.log, "Disk already exists"; "identity" => ?disk_identity);
            return Ok(());
        }

        let disks = Arc::make_mut(&mut self.disks.inner);
        match disk.variant() {
            DiskVariant::U2 => {
                disks
                    .values
                    .insert(disk_identity, ManagedDisk::Unmanaged(disk));
            }
            DiskVariant::M2 => {
                let managed_disk = Disk::new(
                    &self.log,
                    &self.disks.mount_config,
                    disk,
                    None,
                    Some(&self.key_requester),
                )
                .await?;
                disks.values.insert(
                    disk_identity,
                    ManagedDisk::ImplicitlyManaged(managed_disk),
                );
            }
        }

        disks.generation = disks.generation.next();
        self.disk_updates.send_replace(self.disks.clone());

        Ok(())
    }

    /// Delete a disk and its zpool
    ///
    /// Return true, if data was changed, false otherwise
    ///
    /// Note: We never allow removal of synthetic disks in production as they
    /// are only added once.
    pub(crate) fn remove_disk(&mut self, id: &DiskIdentity) {
        info!(self.log, "Removing disk"; "identity" => ?id);
        let Some(entry) = self.disks.inner.values.get(id) else {
            info!(self.log, "Disk not found by id, exiting"; "identity" => ?id);
            return;
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
                    info!(self.log, "Not removing synthetic disk"; "identity" => ?id);
                    return;
                }
            }
        }

        // Safe to unwrap as we just checked the key existed above
        let disks = Arc::make_mut(&mut self.disks.inner);
        disks.values.remove(id).unwrap();

        disks.generation = disks.generation.next();
        self.disk_updates.send_replace(self.disks.clone());
    }
}
