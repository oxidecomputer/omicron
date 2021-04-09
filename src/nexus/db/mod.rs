/*!
 * Facilities for working with the Omicron database
 */

mod config;
mod conversions;
mod datastore;
mod operations;
mod pool;
mod schema;
pub mod sql; /* For examples only */

pub use config::Config;
pub use config::PostgresConfigWithUrl;
pub use datastore::DataStore;
pub use pool::Pool;

/* These are exposed only so that we can write examples that use them. */
pub use sql::where_cond;
pub use sql::SqlString;
