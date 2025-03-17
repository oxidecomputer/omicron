// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler task to ensure the sled is configured to match the
//! most-recently-received `OmicronSledConfig` from Nexus.

// TODO-john remove
#![allow(dead_code)]

use id_map::Entry;
use id_map::IdMap;
use id_map::IdMappable as _;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::disk::DiskIdentity;
use sled_storage::disk::RawDisk;
use tokio::sync::watch;

mod disks;
mod ledger;

pub struct ConfigReconciler {
    current_config: watch::Sender<CurrentConfig>,
    raw_disks: watch::Sender<IdMap<RawDisk>>,
}

impl ConfigReconciler {
    pub fn new() -> Self {
        let (current_config, _current_config_rx) =
            watch::channel(CurrentConfig::WaitingForM2Disks);
        let (raw_disks, _raw_disks_rx) = watch::channel(IdMap::new());
        Self { current_config, raw_disks }
    }

    pub fn set_raw_disks<I>(&self, raw_disks: I)
    where
        I: Iterator<Item = RawDisk>,
    {
        let new_raw_disks = raw_disks.collect::<IdMap<_>>();
        self.raw_disks.send_if_modified(|prev_raw_disks| {
            if *prev_raw_disks == new_raw_disks {
                false
            } else {
                *prev_raw_disks = new_raw_disks;
                true
            }
        });
    }

    pub fn add_or_insert_raw_disk(&self, raw_disk: RawDisk) {
        self.raw_disks.send_if_modified(|raw_disks| {
            match raw_disks.entry(raw_disk.id()) {
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(raw_disk);
                    true
                }
                Entry::Occupied(mut occupied_entry) => {
                    if *occupied_entry.get() == raw_disk {
                        false
                    } else {
                        occupied_entry.insert(raw_disk);
                        true
                    }
                }
            }
        });
    }

    pub fn remove_raw_disk(&self, identity: &DiskIdentity) {
        self.raw_disks
            .send_if_modified(|raw_disks| raw_disks.remove(identity).is_some());
    }
}

enum CurrentConfig {
    // We're still waiting on the M.2 drives to be found: We don't yet know
    // whether we have a ledgered config, nor would we be able to write one.
    WaitingForM2Disks,
    // We have at least one M.2 drive, but there is no ledgered config: we're
    // waiting for rack setup to run.
    WaitingForRackSetup,
    // We have a ledgered config.
    Ledgered(OmicronSledConfig),
}
