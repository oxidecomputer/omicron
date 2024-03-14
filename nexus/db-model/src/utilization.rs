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

// Not really a DB model, just the result of a datastore function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpPoolUtilization {
    // has to be i64 because SQL counts from Diesel are BigInts
    pub allocated: i64,
    pub total: u128,
}

impl From<IpPoolUtilization> for views::IpPoolUtilization {
    fn from(util: IpPoolUtilization) -> Self {
        Self { allocated: util.allocated.try_into().ok(), total: util.total }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ip_pool_utilization_to_view() {
        let view: views::IpPoolUtilization =
            IpPoolUtilization { allocated: 40, total: 100 }.into();
        assert_eq!(view.allocated, Some(40));
        assert_eq!(view.total, 100);

        let view: views::IpPoolUtilization =
            IpPoolUtilization { allocated: i64::from(u32::MAX), total: 100 }
                .into();
        assert_eq!(view.allocated, Some(u32::MAX));
        assert_eq!(view.total, 100);

        let view: views::IpPoolUtilization = IpPoolUtilization {
            allocated: i64::from(u32::MAX) + 1,
            total: 100,
        }
        .into();
        assert_eq!(view.allocated, None);
        assert_eq!(view.total, 100);

        let view: views::IpPoolUtilization =
            IpPoolUtilization { allocated: 40, total: u128::from(u32::MAX) }
                .into();
        assert_eq!(view.allocated, Some(40));
        assert_eq!(view.total, u128::from(u32::MAX));

        let view: views::IpPoolUtilization = IpPoolUtilization {
            allocated: 40,
            total: u128::from(u32::MAX) + 1,
        }
        .into();
        assert_eq!(view.allocated, Some(40));
        assert_eq!(view.total, u128::from(u32::MAX) + 1);
    }
}
