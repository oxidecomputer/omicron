// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use id_map::IdMap;
use omicron_common::disk::DiskIdentity;
use sled_storage::disk::RawDisk;
use tokio::sync::watch;

#[derive(Debug)]
pub struct AllRawDisks {
    disks: watch::Sender<IdMap<RawDisk>>,
}

impl AllRawDisks {
    pub fn new() -> (Self, watch::Receiver<IdMap<RawDisk>>) {
        let (disks, rx) = watch::channel(IdMap::default());
        (Self { disks }, rx)
    }

    pub fn set_raw_disks<I>(&self, raw_disks: I)
    where
        I: Iterator<Item = RawDisk>,
    {
        let new_disks = raw_disks.collect::<IdMap<_>>();
        self.disks.send_if_modified(|disks| {
            if *disks == new_disks {
                false
            } else {
                *disks = new_disks;
                true
            }
        });
    }

    pub fn add_or_update_disk(&self, disk: RawDisk) {
        self.disks.send_if_modified(|disks| {
            match disks.insert(disk.clone()) {
                // "added disk" case
                None => true,
                // "updated disk" case
                Some(prev) => {
                    // Only send a `changed` notification if the entry we just
                    // replaced was different in some way.
                    prev != disk
                }
            }
        });
    }

    pub fn remove_disk(&self, identity: &DiskIdentity) {
        self.disks.send_if_modified(|disks| disks.remove(identity).is_some());
    }
}
