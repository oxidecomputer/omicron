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
use key_manager::StorageKeyRequester;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::disk::DiskIdentity;
use sled_storage::disk::RawDisk;
use tokio::sync::watch;

mod external_disks;
mod internal_disks;
mod key_requester;
mod ledger;
mod raw_disks;

use self::key_requester::KeyManagerWaiter;

pub struct ConfigReconciler {
    current_config: watch::Sender<CurrentConfig>,
    raw_disks: watch::Sender<IdMap<RawDisk>>,
    key_manager_waiter: KeyManagerWaiter,
}

impl ConfigReconciler {
    pub fn new(key_requester: StorageKeyRequester) -> Self {
        let (current_config, _current_config_rx) =
            watch::channel(CurrentConfig::WaitingForInternalDisks);
        let (raw_disks, _raw_disks_rx) = watch::channel(IdMap::new());
        let (key_manager_waiter, _key_requester_rx) =
            KeyManagerWaiter::hold_requester_until_key_manager_ready(
                key_requester,
            );
        Self { current_config, raw_disks, key_manager_waiter }
    }

    pub fn notify_key_manager_ready(&self) {
        self.key_manager_waiter.notify_key_manager_ready();
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum CurrentConfig {
    // We're still waiting on the M.2 drives to be found: We don't yet know
    // whether we have a ledgered config, nor would we be able to write one.
    WaitingForInternalDisks,
    // We have at least one M.2 drive, but there is no ledgered config: we're
    // waiting for rack setup to run.
    WaitingForRackSetup,
    // We have a ledgered config.
    Ledgered(OmicronSledConfig),
}
