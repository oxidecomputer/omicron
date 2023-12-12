// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used to describe "virtual provisioning resources".
//!
//! This means:
//! - **Virtual**: These resources correspond with the user-requested
//! sizes, rather than representing physical utilization. For example,
//! if a user requests 10 GiB of storage, that's 10 GiB of virtual disk
//! space, even if we happen to use 3x replication and actually use 30 GiB
//! of physical storage.
//! - **Provisioning**: These are user-requested resources that are
//! provisioned in response to externally-facing requsets.
//! - **Resources**: These objects are the individual objects which
//! consume some amount of the same physical asset. This is in contrast to
//! "collections", which are containers of many resources, and can be used
//! to provide aggregate information about resource usage.

use crate::schema::virtual_provisioning_resource;
use crate::ByteCount;
use chrono::{DateTime, Utc};
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
#[diesel(treat_none_as_default_value = true)]
pub struct VirtualProvisioningResource {
    pub id: Uuid,
    pub time_modified: Option<DateTime<Utc>>,
    pub resource_type: String,

    pub virtual_disk_bytes_provisioned: ByteCount,
    pub cpus_provisioned: i64,
    pub ram_provisioned: ByteCount,
}

impl VirtualProvisioningResource {
    pub fn new(id: Uuid, resource_type: ResourceTypeProvisioned) -> Self {
        Self {
            id,
            time_modified: None,
            resource_type: resource_type.to_string(),
            virtual_disk_bytes_provisioned: ByteCount(
                external::ByteCount::from(0),
            ),
            cpus_provisioned: 0,
            ram_provisioned: ByteCount(external::ByteCount::from(0)),
        }
    }
}
