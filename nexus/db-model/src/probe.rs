use crate::schema::probe;
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = probe)]
pub struct Probe {
    #[diesel(embed)]
    pub identity: ProbeIdentity,

    pub project_id: Uuid,
    pub sled: Uuid,
}

impl Probe {
    pub fn from_create(p: &params::ProbeCreate, project_id: Uuid) -> Self {
        Self {
            identity: ProbeIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: p.identity.name.clone(),
                    description: p.identity.description.clone(),
                },
            ),
            project_id,
            sled: p.sled,
        }
    }
}

impl Into<external::Probe> for Probe {
    fn into(self) -> external::Probe {
        external::Probe { identity: self.identity().clone(), sled: self.sled }
    }
}
