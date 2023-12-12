// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used to describe "virtual provisioning collections".
//!
//! This means:
//! - **Virtual**: These resources correspond with the user-requested
//! sizes, rather than representing physical utilization. For example,
//! if a user requests 10 GiB of storage, that's 10 GiB of virtual disk
//! space, even if we happen to use 3x replication and actually use 30 GiB
//! of physical storage.
//! - **Provisioning**: These are user-requested resources that are
//! provisioned in response to externally-facing requsets.
//! - **Collections**: These objects are aggregates, containing individual
//! resources (and possibly other collections). For example, a user-requested
//! VM would be a "resource", but a project containing many VMs would be a
//! "collection".

use crate::schema::virtual_provisioning_collection;
use crate::ByteCount;
use chrono::{DateTime, Utc};
use omicron_common::api::external;
use parse_display::Display;
use uuid::Uuid;

#[derive(Debug, Display)]
pub enum CollectionTypeProvisioned {
    Project,
    Silo,
    Fleet,
}

/// Describes virtual_provisioning_collection for a collection
#[derive(Clone, Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = virtual_provisioning_collection)]
#[diesel(treat_none_as_default_value = true)]
pub struct VirtualProvisioningCollection {
    pub id: Uuid,
    pub time_modified: Option<DateTime<Utc>>,
    pub collection_type: String,

    pub virtual_disk_bytes_provisioned: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

impl VirtualProvisioningCollection {
    pub fn new(id: Uuid, collection_type: CollectionTypeProvisioned) -> Self {
        Self {
            id,
            time_modified: None,
            collection_type: collection_type.to_string(),
            virtual_disk_bytes_provisioned: ByteCount(
                external::ByteCount::from(0),
            ),
            cpus_provisioned: 0,
            ram_provisioned: ByteCount(external::ByteCount::from(0)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.virtual_disk_bytes_provisioned.to_bytes() == 0
            && self.cpus_provisioned == 0
            && self.ram_provisioned.to_bytes() == 0
    }
}
