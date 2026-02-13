// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ByteCount;
use crate::ResourceTypeProvisioned;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::physical_provisioning_resource;
use omicron_common::api::external;
use uuid::Uuid;

/// Describes physical_provisioning_resource for a resource.
///
/// Physical provisioning tracks actual physical bytes consumed, including
/// replication overhead.
#[derive(Clone, Selectable, Queryable, Debug)]
#[diesel(table_name = physical_provisioning_resource)]
pub struct PhysicalProvisioningResource {
    pub id: Uuid,
    pub time_modified: DateTime<Utc>,
    pub resource_type: String,

    pub physical_writable_disk_bytes: ByteCount,
    pub physical_zfs_snapshot_bytes: ByteCount,
    pub physical_read_only_disk_bytes: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

/// Insertable form of [`PhysicalProvisioningResource`], omitting
/// DB-defaulted columns (`time_modified`).
#[derive(Clone, Insertable, Debug)]
#[diesel(table_name = physical_provisioning_resource)]
pub struct PhysicalProvisioningResourceNew {
    pub id: Uuid,
    pub resource_type: String,

    pub physical_writable_disk_bytes: ByteCount,
    pub physical_zfs_snapshot_bytes: ByteCount,
    pub physical_read_only_disk_bytes: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

impl PhysicalProvisioningResourceNew {
    pub fn new(id: Uuid, resource_type: ResourceTypeProvisioned) -> Self {
        Self {
            id,
            resource_type: resource_type.to_string(),
            physical_writable_disk_bytes: ByteCount(external::ByteCount::from(
                0,
            )),
            physical_zfs_snapshot_bytes: ByteCount(external::ByteCount::from(
                0,
            )),
            physical_read_only_disk_bytes: ByteCount(
                external::ByteCount::from(0),
            ),
            cpus_provisioned: 0,
            ram_provisioned: ByteCount(external::ByteCount::from(0)),
        }
    }
}
