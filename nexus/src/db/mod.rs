// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Facilities for working with the Omicron database
 */

// This is not intended to be public, but this is necessary to use it from
// doctests
pub mod collection_insert;
mod config;

// This is marked public for use by the integration tests
pub mod datastore;
mod error;
mod pagination;
mod pool;
mod saga_recovery;
mod saga_types;
mod sec_store;
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
