// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Provides thin wrappers around a watch channel carrying the sled's disk
//! bays, as the hardware topology last described them.
//!
//! This is the counterpart of [`crate::raw_disks`] for the bays themselves:
//! where that channel holds the disks sled-agent may manage, this one holds
//! every U.2 bay and M.2 socket of the chassis, occupied or not, so
//! inventory can report an empty bay or one holding something that is not a
//! usable disk. Nothing in sled-agent acts on it; it exists to be reported.

use sled_agent_types::inventory::{InventoryDiskBay, InventoryDiskBayOccupant};
use sled_hardware::{DiskBay, DiskBayOccupant};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct DiskBaysSender(watch::Sender<Arc<Vec<DiskBay>>>);

impl DiskBaysSender {
    pub(crate) fn new() -> Self {
        // Nothing subscribes yet; the sender alone holds the current value.
        let (tx, _rx) = watch::channel(Arc::default());
        Self(tx)
    }

    /// Replace the set of disk bays with what the hardware last reported.
    pub fn set_disk_bays(&self, disk_bays: Vec<DiskBay>) {
        self.0.send_if_modified(|current| {
            if **current == disk_bays {
                false
            } else {
                *current = Arc::new(disk_bays);
                true
            }
        });
    }

    pub(crate) fn to_inventory(&self) -> Vec<InventoryDiskBay> {
        self.0
            .borrow()
            .iter()
            .map(|bay| InventoryDiskBay {
                location: bay.location.clone(),
                kind: bay.kind,
                occupant: match &bay.occupant {
                    DiskBayOccupant::Empty => InventoryDiskBayOccupant::Empty,
                    DiskBayOccupant::Disk { identity } => {
                        InventoryDiskBayOccupant::Disk {
                            identity: identity.clone(),
                        }
                    }
                    DiskBayOccupant::Device { driver, devfs_path } => {
                        InventoryDiskBayOccupant::Device {
                            driver: driver.clone(),
                            devfs_path: devfs_path.clone(),
                        }
                    }
                },
            })
            .collect()
    }
}
