// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{silo_group, silo_group_membership};
use db_macros::Asset;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use uuid::Uuid;

/// Describes a silo group within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_group)]
pub struct SiloGroup {
    #[diesel(embed)]
    identity: SiloGroupIdentity,

    pub silo_id: Uuid,

    /// The identity provider's name for this group.
    pub external_id: String,
}

impl SiloGroup {
    pub fn new(id: Uuid, silo_id: Uuid, external_id: String) -> Self {
        Self { identity: SiloGroupIdentity::new(id), silo_id, external_id }
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

impl From<SiloGroupMembership> for views::SiloGroupMembership {
    fn from(gm: SiloGroupMembership) -> Self {
        Self { silo_group_id: gm.silo_group_id }
    }
}

impl From<SiloGroup> for views::Group {
    fn from(group: SiloGroup) -> Self {
        Self {
            id: group.id(),
            // TODO the use of external_id as display_name is temporary
            display_name: group.external_id,
            silo_id: group.silo_id,
        }
    }
}
