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

    pub name: Name,

    pub silo_name: Name,
    pub project_name: Name,

    pub state: InstanceState,

    pub migration_id: Option<Uuid>,

    pub ncpus: i64,
    pub memory: i64,
}

impl From<SledInstance> for views::SledInstance {
    from (sled_instance: SledInstance) -> Self {
        Self {
            identity: sled_instance.identity(),
            name: sled_instance.name,
            silo_name: sled_instance.silo_name,
            project_name: sled_instance.project_name,
            state: sled_instance.state,
            migration_id: sled_instance.migration_id,
            ncpus: sled_instance.ncpus,
            memory: sled_instance.memory,
        }
    }
}