// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::virtual_resource_provisioning;
use uuid::Uuid;

#[derive(Debug)]
pub enum CollectionType {
    Instance,
    Disk,

    Project,
    Organization,
    Silo,
    Fleet,
}

impl std::fmt::Display for CollectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectionType::Instance => write!(f, "instance"),
            CollectionType::Disk => write!(f, "disk"),
            CollectionType::Project => write!(f, "project"),
            CollectionType::Organization => write!(f, "organization"),
            CollectionType::Silo => write!(f, "silo"),
            CollectionType::Fleet => write!(f, "fleet"),
        }
    }
}

/// Describes virtual_resource_provisioning for a collection
#[derive(Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = virtual_resource_provisioning)]
pub struct VirtualResourceProvisioning {
    pub id: Uuid,
    pub collection_type: String,

    pub virtual_disk_bytes_provisioned: i64,
    pub cpus_provisioned: i64,
    pub ram_provisioned: i64,
}

impl VirtualResourceProvisioning {
    pub fn new(id: Uuid, collection_type: CollectionType) -> Self {
        Self {
            id,
            collection_type: collection_type.to_string(),
            virtual_disk_bytes_provisioned: 0,
            cpus_provisioned: 0,
            ram_provisioned: 0,
        }
    }
}
