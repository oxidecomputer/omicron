use crate::schema::sled_instance;
use crate::InstanceState;
use crate::Name;
use db_macros::Asset;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// An operator view of an instance as exposed by the sled API.
#[derive(Queryable, Debug, Selectable, Asset, Serialize, Deserialize)]
#[diesel(table_name = sled_instance)]
pub struct SledInstance {
    #[diesel(embed)]
    identity: SledInstanceIdentity,
    active_sled_id: Uuid,
    pub migration_id: Option<Uuid>,

    pub name: Name,
    pub silo_name: Name,
    pub project_name: Name,

    pub state: InstanceState,
    pub ncpus: i64,
    pub memory: i64,
}

impl From<SledInstance> for views::SledInstance {
    fn from(sled_instance: SledInstance) -> Self {
        Self {
            identity: sled_instance.identity(),
            name: sled_instance.name.into(),
            active_sled_id: sled_instance.active_sled_id,
            silo_name: sled_instance.silo_name.into(),
            project_name: sled_instance.project_name.into(),
            state: *sled_instance.state.state(),
            migration_id: sled_instance.migration_id,
            ncpus: sled_instance.ncpus,
            memory: sled_instance.memory,
        }
    }
}

impl SledInstance {
    pub fn instance_id(&self) -> Uuid {
        self.identity.id
    }
}
