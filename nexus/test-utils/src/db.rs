// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database testing facilities.

use omicron_test_utils::dev;
use slog::Logger;
use std::path::PathBuf;

/// Path to the "seed" CockroachDB directory.
///
/// Populating CockroachDB unfortunately isn't free - creation of
/// tables, indices, and users takes several seconds to complete.
///
/// By creating a "seed" version of the database, we can cut down
/// on the time spent performing this operation. Instead, we opt
/// to copy the database from this seed location.
fn seed_dir() -> PathBuf {
    PathBuf::from(concat!(env!("OUT_DIR"), "/crdb-base"))
}

/// Wrapper around [`dev::test_setup_database`] which uses a a
/// seed directory provided at build-time.
pub async fn test_setup_database(log: &Logger) -> dev::db::CockroachInstance {
    let dir = seed_dir();
    dev::test_setup_database(log, &dir).await
}

/// See the definition of this constant in nexus_db_queries.
///
/// Besides the cases mentioned there, it's also preferable for some ad hoc
/// test-only queries to do table scans rather than add indexes that are only
/// used for the test suite.
pub const ALLOW_FULL_TABLE_SCAN_SQL: &str =
    nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
