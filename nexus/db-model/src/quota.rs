use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::schema::silo_quotas;

#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
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
