// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::fleet;
use db_macros::Asset;
use nexus_types::external_api::views;
use uuid::Uuid;

/// Information about a fleet
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = fleet)]
pub struct Fleet {
    #[diesel(embed)]
    pub identity: FleetIdentity,
}

impl Fleet {
    pub fn new(id: Uuid) -> Self {
        Self { identity: FleetIdentity::new(id) }
    }
}

impl From<Fleet> for views::Fleet {
    fn from(fleet: Fleet) -> Self {
        Self { identity: views::AssetIdentityMetadata::from(&fleet) }
    }
}
