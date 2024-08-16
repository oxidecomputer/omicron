use crate::ByteCount;
use crate::{schema::silo_utilization, Name};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ipv4Utilization {
    pub allocated: u32,
    pub capacity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ipv6Utilization {
    pub allocated: u128,
    pub capacity: u128,
}

// Not really a DB model, just the result of a datastore function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpPoolUtilization {
    pub ipv4: Ipv4Utilization,
    pub ipv6: Ipv6Utilization,
}

impl From<Ipv4Utilization> for views::Ipv4Utilization {
    fn from(util: Ipv4Utilization) -> Self {
        Self { allocated: util.allocated, capacity: util.capacity }
    }
}

impl From<Ipv6Utilization> for views::Ipv6Utilization {
    fn from(util: Ipv6Utilization) -> Self {
        Self { allocated: util.allocated, capacity: util.capacity }
    }
}

impl From<IpPoolUtilization> for views::IpPoolUtilization {
    fn from(util: IpPoolUtilization) -> Self {
        Self { ipv4: util.ipv4.into(), ipv6: util.ipv6.into() }
    }
}
