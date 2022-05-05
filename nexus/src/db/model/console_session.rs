// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::console_session;
use chrono::{DateTime, Utc};
use uuid::Uuid;

// TODO: `struct SessionToken(String)` for session token

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = console_session)]
pub struct ConsoleSession {
    pub token: String,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
    pub silo_user_id: Uuid,
}

impl ConsoleSession {
    pub fn new(token: String, silo_user_id: Uuid) -> Self {
        let now = Utc::now();
        Self { token, silo_user_id, time_last_used: now, time_created: now }
    }

    pub fn id(&self) -> String {
        self.token.clone()
    }
}
