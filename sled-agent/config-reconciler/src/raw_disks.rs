// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Provides thin wrappers around a watch channel managing the set of
//! [`RawDisk`]s sled-agent is aware of.

use id_map::IdMap;
use id_map::IdMappable;
use omicron_common::disk::DiskIdentity;
use sled_storage::disk::RawDisk;
use slog::Logger;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) fn new() -> (RawDisksSender, watch::Receiver<IdMap<RawDiskWithId>>) {
    let (tx, rx) = watch::channel(IdMap::default());
    (RawDisksSender(tx), rx)
}

#[derive(Debug, Clone)]
pub struct RawDisksSender(watch::Sender<IdMap<RawDiskWithId>>);

impl RawDisksSender {
    /// Set the complete set of raw disks visible to sled-agent.
    pub fn set_raw_disks<I>(&self, _raw_disks: I, _log: &Logger)
    where
        I: Iterator<Item = RawDisk>,
    {
        unimplemented!()
    }

    /// Add or update the properties of a raw disk visible to sled-agent.
    pub fn add_or_update_raw_disk(
        &self,
        _disk: RawDisk,
        _log: &Logger,
    ) -> bool {
        unimplemented!()
    }

    /// Remove a raw disk that is no longer visible to sled-agent.
    pub fn remove_raw_disk(
        &self,
        _identity: &DiskIdentity,
        _log: &Logger,
    ) -> bool {
        unimplemented!()
    }
}

// Adapter to store `RawDisk` in an `IdMap` with cheap key cloning.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawDiskWithId {
    identity: Arc<DiskIdentity>,
    disk: RawDisk,
}

impl IdMappable for RawDiskWithId {
    type Id = Arc<DiskIdentity>;

    fn id(&self) -> Self::Id {
        Arc::clone(&self.identity)
    }
}

impl From<RawDisk> for RawDiskWithId {
    fn from(disk: RawDisk) -> Self {
        Self { identity: Arc::new(disk.identity().clone()), disk }
    }
}

impl Deref for RawDiskWithId {
    type Target = RawDisk;

    fn deref(&self) -> &Self::Target {
        &self.disk
    }
}
