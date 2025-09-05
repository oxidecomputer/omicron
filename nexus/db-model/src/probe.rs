// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DbTypedUuid;
use db_macros::Resource;
use nexus_db_schema::schema::probe;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
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
    pub sled: DbTypedUuid<SledKind>,
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
            sled: p.sled.into(),
        }
    }

    pub fn sled(&self) -> SledUuid {
        self.sled.into()
    }
}

impl Into<external::Probe> for Probe {
    fn into(self) -> external::Probe {
        external::Probe { identity: self.identity().clone(), sled: self.sled() }
    }
}
