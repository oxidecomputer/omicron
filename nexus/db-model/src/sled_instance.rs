use db_macros::Resource;
use nexus_types::identity::Resource;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{schema::sled_instance, InstanceState, Name};

/// An operator view of an instance as exposed by the sled API.
#[derive(Queryable, Debug, Selectable, Resource, Serialize, Deserialize)]
#[diesel(table_name = sled_instance)]
pub struct SledInstance {
    #[diesel(embed)]
    identity: SledInstanceIdentity,

    pub silo_name: Name,
    pub project_name: Name,

    pub state: InstanceState,

    pub migration_id: Option<Uuid>,

    pub ncpus: i64,
    pub memory: i64,
}
