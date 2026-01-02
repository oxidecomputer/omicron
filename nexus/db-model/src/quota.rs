use super::ByteCount;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::silo_quotas;
use nexus_types::external_api::{params, views};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(table_name = silo_quotas)]
pub struct SiloQuotas {
    pub silo_id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,

    /// The number of CPUs that this silo is allowed to use
    pub cpus: i64,

    /// The amount of memory (in bytes) that this silo is allowed to use
    #[diesel(column_name = memory_bytes)]
    pub memory: ByteCount,

    /// The amount of storage (in bytes) that this silo is allowed to use
    #[diesel(column_name = storage_bytes)]
    pub storage: ByteCount,
}

impl SiloQuotas {
    pub fn new(
        silo_id: Uuid,
        cpus: i64,
        memory: ByteCount,
        storage: ByteCount,
    ) -> Self {
        Self {
            silo_id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            cpus,
            memory,
            storage,
        }
    }

    pub fn arbitrarily_high_default(silo_id: Uuid) -> Self {
        let count = params::SiloQuotasCreate::arbitrarily_high_default();
        Self::new(
            silo_id,
            count.cpus,
            count.memory.into(),
            count.storage.into(),
        )
    }
}

impl From<SiloQuotas> for views::SiloQuotas {
    fn from(silo_quotas: SiloQuotas) -> Self {
        Self {
            silo_id: silo_quotas.silo_id,
            limits: views::VirtualResourceCounts {
                cpus: silo_quotas.cpus,
                memory: silo_quotas.memory.into(),
                storage: silo_quotas.storage.into(),
            },
        }
    }
}

// Describes a set of updates for the [`SiloQuotas`] model.
#[derive(AsChangeset)]
#[diesel(table_name = silo_quotas)]
pub struct SiloQuotasUpdate {
    pub cpus: Option<i64>,
    #[diesel(column_name = memory_bytes)]
    pub memory: Option<ByteCount>,
    #[diesel(column_name = storage_bytes)]
    pub storage: Option<ByteCount>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::SiloQuotasUpdate> for SiloQuotasUpdate {
    fn from(params: params::SiloQuotasUpdate) -> Self {
        Self {
            cpus: params.cpus,
            memory: params.memory.map(|f| f.into()),
            storage: params.storage.map(|f| f.into()),
            time_modified: Utc::now(),
        }
    }
}
