/*!
 * Facilities for working with the Omicron database
 */

mod config;
mod datastore;
mod operations;
mod pagination;
mod pool;
mod saga_recovery;
mod saga_types;
mod sec_store;
mod sql_operations;
mod update_and_check;

pub mod diesel_schema;
pub mod model;
pub mod schema;
pub mod sql; /* public for examples only */

pub use config::Config;
pub use datastore::DataStore;
pub use pool::Pool;
pub use saga_recovery::recover;
pub use saga_types::SecId;
pub use sec_store::CockroachDbSecStore;

/* These are exposed only so that we can write examples that use them. */
pub use sql::where_cond;
pub use sql::SqlString;
pub use sql::SqlValueSet;
