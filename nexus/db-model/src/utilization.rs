// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ByteCount;
use crate::DbTypedUuid;
use crate::Name;
use nexus_db_schema::schema::silo_utilization;
use nexus_types::external_api::ip_pool;
use nexus_types::external_api::silo;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloKind;
use omicron_uuid_kinds::SiloUuid;
use serde::{Deserialize, Serialize};

#[derive(Queryable, Debug, Clone, Selectable, Serialize, Deserialize)]
#[diesel(table_name = silo_utilization)]
pub struct SiloUtilization {
    pub silo_id: DbTypedUuid<SiloKind>,
    pub silo_name: Name,
    pub silo_discoverable: bool,

    pub cpus_allocated: i64,
    pub memory_allocated: ByteCount,
    pub storage_allocated: ByteCount,

    pub cpus_provisioned: i64,
    pub memory_provisioned: ByteCount,
    pub storage_provisioned: ByteCount,
}

impl SiloUtilization {
    pub fn silo_id(&self) -> SiloUuid {
        self.silo_id.into()
    }
}

impl From<SiloUtilization> for silo::SiloUtilization {
    fn from(silo_utilization: SiloUtilization) -> Self {
        Self {
            silo_id: silo_utilization.silo_id.into_untyped_uuid(),
            silo_name: silo_utilization.silo_name.into(),
            provisioned: silo::VirtualResourceCounts {
                cpus: silo_utilization.cpus_provisioned,
                memory: silo_utilization.memory_provisioned.into(),
                storage: silo_utilization.storage_provisioned.into(),
            },
            allocated: silo::VirtualResourceCounts {
                cpus: silo_utilization.cpus_allocated,
                memory: silo_utilization.memory_allocated.into(),
                storage: silo_utilization.storage_allocated.into(),
            },
        }
    }
}

impl From<SiloUtilization> for silo::Utilization {
    fn from(silo_utilization: SiloUtilization) -> Self {
        Self {
            provisioned: silo::VirtualResourceCounts {
                cpus: silo_utilization.cpus_provisioned,
                memory: silo_utilization.memory_provisioned.into(),
                storage: silo_utilization.storage_provisioned.into(),
            },
            capacity: silo::VirtualResourceCounts {
                cpus: silo_utilization.cpus_allocated,
                memory: silo_utilization.memory_allocated.into(),
                storage: silo_utilization.storage_allocated.into(),
            },
        }
    }
}

// Not really a DB model, just the result of a datastore function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpPoolUtilization {
    pub remaining: f64,
    pub capacity: f64,
}

impl From<IpPoolUtilization> for ip_pool::IpPoolUtilization {
    fn from(util: IpPoolUtilization) -> Self {
        Self { remaining: util.remaining, capacity: util.capacity }
    }
}
