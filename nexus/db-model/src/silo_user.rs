// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use db_macros::Asset;
use nexus_db_schema::schema::silo_user;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::SiloUserUuid;
use uuid::Uuid;

/// Describes a silo user within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_user)]
#[asset(uuid_kind = SiloUserKind)]
pub struct SiloUser {
    #[diesel(embed)]
    identity: SiloUserIdentity,

    pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
    pub silo_id: Uuid,

    /// The identity provider's ID for this user.
    pub external_id: String,
}

impl SiloUser {
    pub fn new(
        silo_id: Uuid,
        user_id: SiloUserUuid,
        external_id: String,
    ) -> Self {
        Self {
            identity: SiloUserIdentity::new(user_id),
            time_deleted: None,
            silo_id,
            external_id,
        }
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
