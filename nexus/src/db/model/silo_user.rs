// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::silo_user;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use uuid::Uuid;

/// Describes a silo user within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_user)]
pub struct SiloUser {
    #[diesel(embed)]
    identity: SiloUserIdentity,
    pub silo_id: Uuid,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl SiloUser {
    pub fn new(silo_id: Uuid, user_id: Uuid) -> Self {
        Self {
            identity: SiloUserIdentity::new(user_id),
            silo_id,
            time_deleted: None,
        }
    }
}
