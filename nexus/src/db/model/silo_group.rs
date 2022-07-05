// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::{silo_group, silo_group_membership};
use db_macros::Resource;
use omicron_common::api::external::IdentityMetadataCreateParams;
use uuid::Uuid;

/// Describes a silo group within the database.
#[derive(Resource, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_group)]
pub struct SiloGroup {
    #[diesel(embed)]
    identity: SiloGroupIdentity,

    pub silo_id: Uuid,
}

impl SiloGroup {
    pub fn new(
        id: Uuid,
        identity: IdentityMetadataCreateParams,
        silo_id: Uuid,
    ) -> Self {
        Self { identity: SiloGroupIdentity::new(id, identity), silo_id }
    }
}

/// Describe which silo users belong to which silo groups
#[derive(Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_group_membership)]
pub struct SiloGroupMembership {
    pub silo_group_id: Uuid,
    pub silo_user_id: Uuid,
}

impl SiloGroupMembership {
    pub fn new(silo_group_id: Uuid, silo_user_id: Uuid) -> Self {
        Self { silo_group_id, silo_user_id }
    }
}
