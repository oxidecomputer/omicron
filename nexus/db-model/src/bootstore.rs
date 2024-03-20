use crate::schema::{bootstore_config, bootstore_keys};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const NETWORK_KEY: &str = "network_key";

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = bootstore_keys)]
pub struct BootstoreKeys {
    pub key: String,
    pub generation: i64,
}

/// BootstoreConfig is a key-value store for bootstrapping data.
/// We serialize the data as json because it is inherently polymorphic and it
/// is not intended to be queried directly.
#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = bootstore_config)]
pub struct BootstoreConfig {
    pub key: String,
    pub generation: i64,
    pub data: serde_json::Value,
    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}
