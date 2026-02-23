use crate::ByteCount;
use crate::Name;
use nexus_db_schema::schema::silo_utilization;
use nexus_types::external_api::ip_pool;
use nexus_types::external_api::silo;
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

    pub physical_disk_bytes_provisioned: i64,
    pub physical_storage_allocated: Option<i64>,
}

impl From<SiloUtilization> for silo::SiloUtilization {
    fn from(silo_utilization: SiloUtilization) -> Self {
        Self {
            silo_id: silo_utilization.silo_id,
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
            physical_disk_bytes_provisioned: silo_utilization
                .physical_disk_bytes_provisioned,
            physical_storage_allocated: silo_utilization
                .physical_storage_allocated,
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
            physical_disk_bytes_provisioned: silo_utilization
                .physical_disk_bytes_provisioned,
            physical_storage_allocated: silo_utilization
                .physical_storage_allocated,
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
