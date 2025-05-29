use chrono::{DateTime, Utc};
use nexus_db_schema::schema::silo_settings;
use nexus_types::external_api::{params, views};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
#[diesel(table_name = silo_settings)]
pub struct SiloSettings {
    pub silo_id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,

    /// Max token lifetime in seconds. Null means no max: users can create
    /// tokens that never expire.
    pub device_token_max_ttl_seconds: Option<i64>,
}

impl SiloSettings {
    pub fn new(silo_id: Uuid) -> Self {
        Self {
            silo_id,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            device_token_max_ttl_seconds: None,
        }
    }
}

impl From<SiloSettings> for views::SiloSettings {
    fn from(silo_settings: SiloSettings) -> Self {
        Self {
            silo_id: silo_settings.silo_id,
            device_token_max_ttl_seconds: silo_settings
                .device_token_max_ttl_seconds,
        }
    }
}

// Describes a set of updates for the [`SiloSettings`] model.
#[derive(AsChangeset)]
#[diesel(table_name = silo_settings)]
pub struct SiloSettingsUpdate {
    // Needs to be double Option so we can set a value of null in the DB by
    // passing Some(None). None by itself is ignored by Diesel.
    pub device_token_max_ttl_seconds: Option<Option<i64>>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::SiloSettingsUpdate> for SiloSettingsUpdate {
    fn from(params: params::SiloSettingsUpdate) -> Self {
        Self {
            device_token_max_ttl_seconds: Some(
                params.device_token_max_ttl_seconds.map(|ttl| ttl.get().into()),
            ),
            time_modified: Utc::now(),
        }
    }
}
