// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! "Currently in-service control-plane disks": the executed view from
//! the `physical_disk` and `zpool` DB tables.
//!
//! This is distinct from the planned view in `BlueprintPhysicalDiskConfig`:
//! a disk is in this set only after the control plane has actually committed
//! to managing it (`physical_disk.disk_policy = 'in_service'`), not while a
//! planner is merely proposing to expunge or adopt it. Consumers that need
//! the *committed* view of which disks are part of the rack (fault
//! management diagnosers in particular) should read this rather than the
//! target blueprint.

use crate::external_api::physical_disk::PhysicalDiskKind;
use iddqd::{IdOrdItem, id_upcast};
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;

/// One control-plane-managed physical disk, joined with its zpool and the
/// stable identity facts the DB already knows about it.
#[derive(Clone, Debug, PartialEq)]
pub struct InServiceDisk {
    pub physical_disk_id: PhysicalDiskUuid,
    pub zpool_id: ZpoolUuid,
    pub sled_id: SledUuid,
    pub vendor: String,
    pub serial: String,
    pub model: String,
    pub variant: PhysicalDiskKind,
}

impl IdOrdItem for InServiceDisk {
    type Key<'a> = PhysicalDiskUuid;
    fn key(&self) -> Self::Key<'_> {
        self.physical_disk_id
    }
    id_upcast!();
}
