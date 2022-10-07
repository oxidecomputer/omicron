// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::silo_user;
use db_macros::Asset;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use uuid::Uuid;

/// Describes a silo user within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_user)]
pub struct SiloUser {
    #[diesel(embed)]
    identity: SiloUserIdentity,

    pub silo_id: Uuid,

    /// The identity provider's ID for this user.
    pub external_id: String,
}

impl SiloUser {
    pub fn new(silo_id: Uuid, user_id: Uuid, external_id: String) -> Self {
        Self { identity: SiloUserIdentity::new(user_id), silo_id, external_id }
    }
}

impl From<SiloUser> for views::User {
    fn from(user: SiloUser) -> Self {
        Self {
            id: user.id(),
            // TODO the use of external_id as display_name is temporary
            display_name: user.external_id,
            silo_id: user.silo_id,
        }
    }
}
