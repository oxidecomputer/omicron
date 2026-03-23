use crate::Name;
use crate::VmmState;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::sled_instance;
use nexus_types::external_api::sled;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// An operator view of an instance as exposed by the sled API.
#[derive(Queryable, Debug, Selectable, Serialize, Deserialize)]
#[diesel(table_name = sled_instance)]
pub struct SledInstance {
    pub id: Uuid,
    pub name: Name,
    pub silo_name: Name,
    pub project_name: Name,
    pub active_sled_id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub migration_id: Option<Uuid>,
    pub ncpus: i64,
    pub memory: i64,
    pub state: VmmState,
}

impl From<SledInstance> for sled::SledInstance {
    fn from(sled_instance: SledInstance) -> Self {
        Self {
            identity: nexus_types::identity::Asset::identity(&sled_instance),
            name: sled_instance.name.into(),
            active_sled_id: sled_instance.active_sled_id,
            silo_name: sled_instance.silo_name.into(),
            project_name: sled_instance.project_name.into(),
            state: sled_instance.state.into(),
            migration_id: sled_instance.migration_id,
            ncpus: sled_instance.ncpus,
            memory: sled_instance.memory,
        }
    }
}

impl nexus_types::identity::Asset for SledInstance {
    type IdType = Uuid;
    fn id(&self) -> Uuid {
        self.id
    }
    fn time_created(&self) -> DateTime<Utc> {
        self.time_created
    }
    fn time_modified(&self) -> DateTime<Utc> {
        self.time_modified
    }
}

impl SledInstance {
    pub fn instance_id(&self) -> Uuid {
        self.id
    }
}
