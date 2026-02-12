// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ByteCount;
use crate::CollectionTypeProvisioned;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::physical_provisioning_collection;
use omicron_common::api::external;
use uuid::Uuid;

/// Describes physical_provisioning_collection for a collection.
///
/// Physical provisioning tracks actual physical bytes consumed, including
/// replication overhead, unlike virtual provisioning which tracks
/// user-visible (virtual) sizes.
#[derive(Clone, Selectable, Queryable, Debug)]
#[diesel(table_name = physical_provisioning_collection)]
pub struct PhysicalProvisioningCollection {
    pub id: Uuid,
    pub time_modified: DateTime<Utc>,
    pub collection_type: String,

    pub physical_writable_disk_bytes: ByteCount,
    pub physical_zfs_snapshot_bytes: ByteCount,
    pub physical_read_only_disk_bytes: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

impl PhysicalProvisioningCollection {
    pub fn is_empty(&self) -> bool {
        self.physical_writable_disk_bytes.to_bytes() == 0
            && self.physical_zfs_snapshot_bytes.to_bytes() == 0
            && self.physical_read_only_disk_bytes.to_bytes() == 0
            && self.cpus_provisioned == 0
            && self.ram_provisioned.to_bytes() == 0
    }
}

/// Insertable form of [`PhysicalProvisioningCollection`], omitting
/// DB-defaulted columns (`time_modified`).
#[derive(Clone, Insertable, Debug)]
#[diesel(table_name = physical_provisioning_collection)]
pub struct PhysicalProvisioningCollectionNew {
    pub id: Uuid,
    pub collection_type: String,

    pub physical_writable_disk_bytes: ByteCount,
    pub physical_zfs_snapshot_bytes: ByteCount,
    pub physical_read_only_disk_bytes: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

impl PhysicalProvisioningCollectionNew {
    pub fn new(id: Uuid, collection_type: CollectionTypeProvisioned) -> Self {
        Self {
            id,
            collection_type: collection_type.to_string(),
            physical_writable_disk_bytes: ByteCount(
                external::ByteCount::from(0),
            ),
            physical_zfs_snapshot_bytes: ByteCount(
                external::ByteCount::from(0),
            ),
            physical_read_only_disk_bytes: ByteCount(
                external::ByteCount::from(0),
            ),
            cpus_provisioned: 0,
            ram_provisioned: ByteCount(external::ByteCount::from(0)),
        }
    }
}
