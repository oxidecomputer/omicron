// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::virtual_provisioning_collection;
use uuid::Uuid;

#[derive(Debug)]
pub enum CollectionTypeProvisioned {
    Project,
    Organization,
    Silo,
    Fleet,
}

impl std::fmt::Display for CollectionTypeProvisioned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectionTypeProvisioned::Project => write!(f, "project"),
            CollectionTypeProvisioned::Organization => {
                write!(f, "organization")
            }
            CollectionTypeProvisioned::Silo => write!(f, "silo"),
            CollectionTypeProvisioned::Fleet => write!(f, "fleet"),
        }
    }
}

/// Describes virtual_provisioning_collection for a collection
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = virtual_provisioning_collection)]
pub struct VirtualProvisioningCollection {
    pub id: Uuid,
    pub collection_type: String,

    pub virtual_disk_bytes_provisioned: i64,
    pub cpus_provisioned: i64,
    pub ram_provisioned: i64,
}

impl VirtualProvisioningCollection {
    pub fn new(id: Uuid, collection_type: CollectionTypeProvisioned) -> Self {
        Self {
            id,
            collection_type: collection_type.to_string(),
            virtual_disk_bytes_provisioned: 0,
            cpus_provisioned: 0,
            ram_provisioned: 0,
        }
    }
}
