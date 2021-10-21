use omicron_common::api::external::Name as ApiName;
use omicron_nexus::db::datastore::DataStore;
use omicron_nexus::db::model::Name as ModelName;
use omicron_nexus::db::{Config, Pool};
use omicron_test_utils::dev::db::CockroachInstance;
use std::convert::TryFrom;
use std::sync::Arc;

/// Create a DataStore for us to inspect the database state with
pub fn create_datastore(db: &CockroachInstance) -> Arc<DataStore> {
    let cfg = Config { url: db.pg_config().clone() };
    let pool = Arc::new(Pool::new(&cfg));
    Arc::new(DataStore::new(Arc::clone(&pool)))
}

/// Convert a &str to the Name types used by DataStore
pub fn make_name(name: &str) -> ModelName {
    ModelName(ApiName::try_from(name.to_string()).unwrap())
}
