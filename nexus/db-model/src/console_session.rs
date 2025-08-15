// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use nexus_db_schema::schema::console_session;
use nexus_types::external_api::views;
use omicron_uuid_kinds::SiloUserKind;
use omicron_uuid_kinds::SiloUserUuid;
use omicron_uuid_kinds::{ConsoleSessionKind, ConsoleSessionUuid, GenericUuid};

use crate::to_db_typed_uuid;
use crate::typed_uuid::DbTypedUuid;

// TODO: `struct SessionToken(String)` for session token

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = console_session)]
pub struct ConsoleSession {
    pub id: DbTypedUuid<ConsoleSessionKind>,
    pub token: String,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
    silo_user_id: DbTypedUuid<SiloUserKind>,
}

impl ConsoleSession {
    pub fn new(token: String, silo_user_id: SiloUserUuid) -> Self {
        let now = Utc::now();
        Self::new_with_times(token, silo_user_id, now, now)
    }

    pub fn new_with_times(
        token: String,
        silo_user_id: SiloUserUuid,
        time_created: DateTime<Utc>,
        time_last_used: DateTime<Utc>,
    ) -> Self {
        Self {
            id: ConsoleSessionUuid::new_v4().into(),
            token,
            silo_user_id: to_db_typed_uuid(silo_user_id),
            time_created,
            time_last_used,
        }
    }

    pub fn id(&self) -> ConsoleSessionUuid {
        self.id.0
    }

    pub fn silo_user_id(&self) -> SiloUserUuid {
        self.silo_user_id.into()
    }
}

impl From<ConsoleSession> for views::ConsoleSession {
    fn from(session: ConsoleSession) -> Self {
        Self {
            id: session.id.into_untyped_uuid(),
            time_created: session.time_created,
            time_last_used: session.time_last_used,
        }
    }
}
