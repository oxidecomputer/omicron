// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Discovered and usable disks and zpools

use crate::dataset::M2_DEBUG_DATASET;
use crate::disk::Disk;
use crate::error::Error;
use crate::pool::Pool;
use camino::Utf8PathBuf;
use illumos_utils::zpool::ZpoolName;
use omicron_common::disk::DiskIdentity;
use sled_hardware::DiskVariant;
use std::collections::BTreeMap;
use std::sync::Arc;

// The directory within the debug dataset in which bundles are created.
const BUNDLE_DIRECTORY: &str = "bundle";

// The directory for zone bundles.
const ZONE_BUNDLE_DIRECTORY: &str = "zone";

/// Storage related resources: disks and zpools
///
/// This state is internal to the [`crate::StorageManager`] task. Clones
/// of this state can be retrieved by requests to the `StorageManager` task
/// from the [`crate::StorageManagerHandle`]. This state is not `Sync`, and
/// as such does not require any mutexes. However, we do expect to share it
/// relatively frequently, and we want copies of it to be as cheaply made
/// as possible. So any large state is stored inside `Arc`s. On the other
/// hand, we expect infrequent updates to this state, and as such, we use
/// [`std::sync::Arc::make_mut`] to implement clone on write functionality
/// inside the `StorageManager` task if there are any outstanding copies.
/// Therefore, we only pay the cost to update infrequently, and no locks are
/// required by callers when operating on cloned data. The only contention here
/// is for the refrence counters of the internal Arcs when `StorageResources`
/// gets cloned or dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageResources {
    // All disks, real and synthetic, being managed by this sled
    pub disks: Arc<BTreeMap<DiskIdentity, (Disk, Pool)>>,
}

impl StorageResources {
    /// Insert a disk and its zpool
    ///
    /// Return true if data was changed, false otherwise
    ///
    /// This really should not be used outside this crate, except for testing
    pub fn insert_disk(&mut self, disk: Disk) -> Result<bool, Error> {
        let disk_id = disk.identity().clone();
        let zpool_name = disk.zpool_name().clone();
        let zpool = Pool::new(zpool_name, disk_id.clone())?;
        if let Some((stored_disk, stored_pool)) = self.disks.get(&disk_id) {
            if stored_disk == &disk
                && stored_pool.info.size() == zpool.info.size()
                && stored_pool.name == zpool.name
            {
                return Ok(false);
            }
        }
        // Either the disk or zpool changed
        Arc::make_mut(&mut self.disks).insert(disk_id, (disk, zpool));
        Ok(true)
    }

    /// Insert a disk while creating a fake pool
    /// This is a workaround for current mock based testing strategies
    /// in the sled-agent.
    ///
    /// Return true if data was changed, false otherwise
    #[cfg(feature = "testing")]
    pub fn insert_fake_disk(&mut self, disk: Disk) -> bool {
        let disk_id = disk.identity().clone();
        let zpool_name = disk.zpool_name().clone();
        let zpool = Pool::new_with_fake_info(zpool_name, disk_id.clone());
        if self.disks.contains_key(&disk_id) {
            return false;
        }
        // Either the disk or zpool changed
        Arc::make_mut(&mut self.disks).insert(disk_id, (disk, zpool));
        true
    }

    /// Delete a real disk and its zpool
    ///
    /// Return true, if data was changed, false otherwise
    pub(crate) fn remove_disk(&mut self, id: &DiskIdentity) -> bool {
        if !self.disks.contains_key(id) {
            return false;
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
            .cloned()
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
