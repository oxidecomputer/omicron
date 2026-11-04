// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DbTypedUuid;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::silo_auth_settings;
use nexus_types::external_api::silo;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloKind;
use omicron_uuid_kinds::SiloUuid;
use serde::{Deserialize, Serialize};

#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(table_name = silo_auth_settings)]
pub struct SiloAuthSettings {
    pub silo_id: DbTypedUuid<SiloKind>,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,

    /// Max token lifetime in seconds. Null means no max: users can create
    /// tokens that never expire.
    pub device_token_max_ttl_seconds: Option<SqlU32>,
}

impl SiloAuthSettings {
    pub fn silo_id(&self) -> SiloUuid {
        self.silo_id.into()
    }

    pub fn new(silo_id: SiloUuid) -> Self {
        Self {
            silo_id: silo_id.into(),
            time_created: Utc::now(),
            time_modified: Utc::now(),
            device_token_max_ttl_seconds: None,
        }
    }
}

impl From<SiloAuthSettings> for silo::SiloAuthSettings {
    fn from(silo_auth_settings: SiloAuthSettings) -> Self {
        Self {
            silo_id: silo_auth_settings.silo_id.into_untyped_uuid(),
            device_token_max_ttl_seconds: silo_auth_settings
                .device_token_max_ttl_seconds
                .map(|ttl| ttl.0),
        }
    }
}

// Describes a set of updates for the [`SiloAuthSettings`] model.
#[derive(AsChangeset)]
#[diesel(table_name = silo_auth_settings)]
pub struct SiloAuthSettingsUpdate {
    // Needs to be double Option so we can set a value of null in the DB by
    // passing Some(None). None by itself is ignored by Diesel.
    pub device_token_max_ttl_seconds: Option<Option<i64>>,
    pub time_modified: DateTime<Utc>,
}

impl From<silo::SiloAuthSettingsUpdate> for SiloAuthSettingsUpdate {
    fn from(params: silo::SiloAuthSettingsUpdate) -> Self {
        Self {
            device_token_max_ttl_seconds: Some(
                params.device_token_max_ttl_seconds.map(|ttl| ttl.get().into()),
            ),
            time_modified: Utc::now(),
        }
    }
}
