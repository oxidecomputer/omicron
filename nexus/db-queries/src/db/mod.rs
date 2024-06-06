// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for working with the Omicron database

// This is not intended to be public, but this is necessary to use it from
// doctests
pub mod collection_attach;
pub mod collection_detach;
pub mod collection_detach_many;
pub mod collection_insert;
mod column_walker;
mod config;
mod cte_utils;
// This is marked public for use by the integration tests
pub mod datastore;
pub(crate) mod error;
mod explain;
pub mod lookup;
mod on_conflict_ext;
// Public for doctests.
pub mod pagination;
mod pool;
mod pool_connection;
// This is marked public because the error types are used elsewhere, e.g., in
// sagas.
pub mod queries;
mod raw_query_builder;
mod saga_recovery;
mod sec_store;
pub mod subquery;
pub(crate) mod true_or_cast_error;
mod update_and_check;

/// Batch statement to disable full table scans.
// This is `pub` so tests that don't go through our connection pool can disable
// full table scans the same way pooled connections do.
pub use pool_connection::DISALLOW_FULL_TABLE_SCAN_SQL;

#[cfg(test)]
mod test_utils;

pub use nexus_db_fixed_data as fixed_data;
pub use nexus_db_model as model;
use nexus_db_model::saga_types;
pub use nexus_db_model::schema;

pub use crate::db::error::TransactionError;
pub use config::Config;
pub use datastore::DataStore;
pub use on_conflict_ext::IncompleteOnConflictExt;
pub use pool::{DbConnection, Pool};
pub use saga_recovery::{recover, CompletionTask, RecoveryTask};
pub use saga_types::SecId;
pub use sec_store::CockroachDbSecStore;

pub use nexus_types::identity;
