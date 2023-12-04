use crate::schema::silo_quotas;
use chrono::{DateTime, Utc};
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

    pub cpus: i64,
    pub memory: i64,
    pub storage: i64,
}

impl SiloQuotas {
    pub fn new(silo_id: Uuid, cpus: i64, memory: i64, storage: i64) -> Self {
        Self {
            silo_id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            cpus,
            memory,
            storage,
        }
    }
}

impl From<SiloQuotas> for views::SiloQuotas {
    fn from(silo_quotas: SiloQuotas) -> Self {
        Self {
            silo_id: silo_quotas.silo_id,
            cpus: silo_quotas.cpus,
            memory: silo_quotas.memory,
            storage: silo_quotas.storage,
        }
    }
}

impl From<views::SiloQuotas> for SiloQuotas {
    fn from(silo_quotas: views::SiloQuotas) -> Self {
        Self {
            silo_id: silo_quotas.silo_id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            cpus: silo_quotas.cpus,
            memory: silo_quotas.memory,
            storage: silo_quotas.storage,
        }
    }
}

// Describes a set of updates for the [`SiloQuotas`] model.
#[derive(AsChangeset)]
#[diesel(table_name = silo_quotas)]
pub struct SiloQuotasUpdate {
    pub cpus: Option<i64>,
    pub memory: Option<i64>,
    pub storage: Option<i64>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::SiloQuotasUpdate> for SiloQuotasUpdate {
    fn from(params: params::SiloQuotasUpdate) -> Self {
        Self {
            cpus: params.cpus,
            memory: params.memory,
            storage: params.storage,
            time_modified: Utc::now(),
        }
    }
}
