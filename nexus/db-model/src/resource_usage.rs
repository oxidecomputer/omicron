// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::resource_usage;
use uuid::Uuid;

/// Describes resource_usage for a collection
#[derive(Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = resource_usage)]
pub struct ResourceUsage {
    pub id: Uuid,

    pub physical_disk_bytes_provisioned: i64,
    pub cpus_provisioned: i64,
    pub ram_provisioned: i64,
}

impl ResourceUsage {
    pub fn new(id: Uuid) -> Self {
        Self {
            id,
            physical_disk_bytes_provisioned: 0,
            cpus_provisioned: 0,
            ram_provisioned: 0,
        }
    }
}
