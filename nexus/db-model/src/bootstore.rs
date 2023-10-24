use crate::schema::bootstore_keys;
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
