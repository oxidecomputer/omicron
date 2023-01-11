// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::virtual_provisioning_resource;
use crate::ByteCount;
use omicron_common::api::external;
use uuid::Uuid;

#[derive(Debug)]
pub enum ResourceTypeProvisioned {
    Instance,
    Disk,
    Snapshot,
}

impl std::fmt::Display for ResourceTypeProvisioned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceTypeProvisioned::Instance => write!(f, "instance"),
            ResourceTypeProvisioned::Disk => write!(f, "disk"),
            ResourceTypeProvisioned::Snapshot => write!(f, "snapshot"),
        }
    }
}

/// Describes virtual_provisioning_resource for a resource.
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = virtual_provisioning_resource)]
pub struct VirtualProvisioningResource {
    pub id: Uuid,
    pub resource_type: String,

    pub virtual_disk_bytes_provisioned: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

impl VirtualProvisioningResource {
    pub fn new(id: Uuid, resource_type: ResourceTypeProvisioned) -> Self {
        Self {
            id,
            resource_type: resource_type.to_string(),
            virtual_disk_bytes_provisioned: ByteCount(
                external::ByteCount::from(0),
            ),
            cpus_provisioned: 0,
            ram_provisioned: ByteCount(external::ByteCount::from(0)),
        }
    }
}
