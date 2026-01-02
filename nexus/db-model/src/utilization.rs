use crate::ByteCount;
use crate::Name;
use nexus_db_schema::schema::silo_utilization;
use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Queryable, Debug, Clone, Selectable, Serialize, Deserialize)]
#[diesel(table_name = silo_utilization)]
pub struct SiloUtilization {
    pub silo_id: Uuid,
    pub silo_name: Name,
    pub silo_discoverable: bool,

    pub cpus_allocated: i64,
    pub memory_allocated: ByteCount,
    pub storage_allocated: ByteCount,

    pub cpus_provisioned: i64,
    pub memory_provisioned: ByteCount,
    pub storage_provisioned: ByteCount,
}

impl From<SiloUtilization> for views::SiloUtilization {
    fn from(silo_utilization: SiloUtilization) -> Self {
        Self {
            silo_id: silo_utilization.silo_id,
            silo_name: silo_utilization.silo_name.into(),
            provisioned: views::VirtualResourceCounts {
                cpus: silo_utilization.cpus_provisioned,
                memory: silo_utilization.memory_provisioned.into(),
                storage: silo_utilization.storage_provisioned.into(),
            },
            allocated: views::VirtualResourceCounts {
                cpus: silo_utilization.cpus_allocated,
                memory: silo_utilization.memory_allocated.into(),
                storage: silo_utilization.storage_allocated.into(),
            },
        }
    }
}

impl From<SiloUtilization> for views::Utilization {
    fn from(silo_utilization: SiloUtilization) -> Self {
        Self {
            provisioned: views::VirtualResourceCounts {
                cpus: silo_utilization.cpus_provisioned,
                memory: silo_utilization.memory_provisioned.into(),
                storage: silo_utilization.storage_provisioned.into(),
            },
            capacity: views::VirtualResourceCounts {
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

impl From<IpPoolUtilization> for views::IpPoolUtilization {
    fn from(util: IpPoolUtilization) -> Self {
        Self { remaining: util.remaining, capacity: util.capacity }
    }
}
