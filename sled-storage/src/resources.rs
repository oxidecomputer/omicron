// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Discovered and usable disks and zpools

use crate::dataset::M2_DEBUG_DATASET;
use crate::disk::Disk;
use crate::error::Error;
use crate::pool::Pool;
use camino::Utf8PathBuf;
use cfg_if::cfg_if;
use illumos_utils::zpool::ZpoolName;
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use std::collections::BTreeMap;
use std::sync::Arc;

// The directory within the debug dataset in which bundles are created.
const BUNDLE_DIRECTORY: &str = "bundle";

// The directory for zone bundles.
const ZONE_BUNDLE_DIRECTORY: &str = "zone";

pub enum AddDiskResult {
    DiskInserted,
    DiskAlreadyInserted,
    DiskQueued,
}

impl AddDiskResult {
    pub fn disk_inserted(&self) -> bool {
        match self {
            AddDiskResult::DiskInserted => true,
            _ => false,
        }
    }
}

/// Storage related resources: disks and zpools
///
/// This state is internal to the [`crate::manager::StorageManager`] task. Clones
/// of this state can be retrieved by requests to the `StorageManager` task
/// from the [`crate::manager::StorageHandle`]. This state is not `Sync`, and
/// as such does not require any mutexes. However, we do expect to share it
/// relatively frequently, and we want copies of it to be as cheaply made
/// as possible. So any large state is stored inside `Arc`s. On the other
/// hand, we expect infrequent updates to this state, and as such, we use
/// [`std::sync::Arc::make_mut`] to implement clone on write functionality
/// inside the `StorageManager` task if there are any outstanding copies.
/// Therefore, we only pay the cost to update infrequently, and no locks are
/// required by callers when operating on cloned data. The only contention here
/// is for the reference counters of the internal Arcs when `StorageResources`
/// gets cloned or dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageResources {
    // All disks, real and synthetic, being managed by this sled
    disks: Arc<BTreeMap<DiskIdentity, (Disk, Pool)>>,
}

impl StorageResources {
    /// Return a reference to the current snapshot of disks
    pub fn disks(&self) -> &BTreeMap<DiskIdentity, (Disk, Pool)> {
        &self.disks
    }

    /// Insert a disk and its zpool
    ///
    /// If the disk passed in is new or modified, or its pool size or pool
    /// name changed, then insert the changed values and return `DiskInserted`.
    /// Otherwise, do not insert anything and return `DiskAlreadyInserted`.
    /// For instance, if only the pool health changes, because it is not one
    /// of the checked values, we will not insert the update and will return
    /// `DiskAlreadyInserted`.
    pub(crate) fn insert_disk(
        &mut self,
        disk: Disk,
    ) -> Result<AddDiskResult, Error> {
        let disk_id = disk.identity().clone();
        let zpool_name = disk.zpool_name().clone();
        let zpool = Pool::new(zpool_name, disk_id.clone())?;
        if let Some((stored_disk, stored_pool)) = self.disks.get(&disk_id) {
            if stored_disk == &disk
                && stored_pool.info.size() == zpool.info.size()
                && stored_pool.name == zpool.name
            {
                return Ok(AddDiskResult::DiskAlreadyInserted);
            }
        }
        // Either the disk or zpool changed
        Arc::make_mut(&mut self.disks).insert(disk_id, (disk, zpool));
        Ok(AddDiskResult::DiskInserted)
    }

    /// Insert a disk while creating a fake pool
    /// This is a workaround for current mock based testing strategies
    /// in the sled-agent.
    #[cfg(feature = "testing")]
    pub fn insert_fake_disk(&mut self, disk: Disk) -> AddDiskResult {
        let disk_id = disk.identity().clone();
        let zpool_name = disk.zpool_name().clone();
        let zpool = Pool::new_with_fake_info(zpool_name, disk_id.clone());
        if self.disks.contains_key(&disk_id) {
            return AddDiskResult::DiskAlreadyInserted;
        }
        // Either the disk or zpool changed
        Arc::make_mut(&mut self.disks).insert(disk_id, (disk, zpool));
        AddDiskResult::DiskInserted
    }

    /// Delete a disk and its zpool
    ///
    /// Return true, if data was changed, false otherwise
    ///
    /// Note: We never allow removal of synthetic disks in production as they
    /// are only added once.
    pub(crate) fn remove_disk(&mut self, id: &DiskIdentity) -> bool {
        let Some((disk, _)) = self.disks.get(id) else {
            return false;
        };

        cfg_if! {
            if #[cfg(test)] {
                // For testing purposes, we allow synthetic disks to be deleted.
                // Silence an unused variable warning.
                _ = disk;
            } else {
                // In production, we disallow removal of synthetic disks as they
                // are only added once.
                if disk.is_synthetic() {
                    return false;
                }
            }
        }

        // Safe to unwrap as we just checked the key existed above
        Arc::make_mut(&mut self.disks).remove(id).unwrap();
        true
    }

    /// Returns the identity of the boot disk.
    ///
    /// If this returns `None`, we have not processed the boot disk yet.
    pub fn boot_disk(&self) -> Option<(DiskIdentity, ZpoolName)> {
        for (id, (disk, _)) in self.disks.iter() {
            if disk.is_boot_disk() {
                return Some((id.clone(), disk.zpool_name().clone()));
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
            .map(|zpool| zpool.dataset_mountpoint(dataset))
            .collect()
    }

    /// Returns all mountpoints within all U.2s for a particular dataset.
    pub fn all_u2_mountpoints(&self, dataset: &str) -> Vec<Utf8PathBuf> {
        self.all_u2_zpools()
            .iter()
            .map(|zpool| zpool.dataset_mountpoint(dataset))
            .collect()
    }

    pub fn get_all_zpools(&self) -> Vec<(ZpoolName, DiskVariant)> {
        self.disks
            .values()
            .map(|(disk, _)| (disk.zpool_name().clone(), disk.variant()))
            .collect()
    }

    // Returns all zpools of a particular variant
    fn all_zpools(&self, variant: DiskVariant) -> Vec<ZpoolName> {
        self.disks
            .values()
            .filter_map(|(disk, _)| {
                if disk.variant() == variant {
                    return Some(disk.zpool_name().clone());
                }
                None
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
}
