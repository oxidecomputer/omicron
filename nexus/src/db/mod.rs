/*!
 * Facilities for working with the Omicron database
 */

mod config;
mod datastore;
mod error;
mod pagination;
mod pool;
mod saga_recovery;
mod saga_types;
mod sec_store;
mod traced_pg_connection;
mod update_and_check;

#[cfg(test)]
mod test_utils;

pub mod identity;
pub mod model;
pub mod schema;

pub use config::Config;
pub use datastore::DataStore;
pub use pool::Pool;
pub use saga_recovery::{recover, RecoveryTask};
pub use saga_types::SecId;
pub use sec_store::CockroachDbSecStore;
pub(crate) use traced_pg_connection::TracedPgConnection;
