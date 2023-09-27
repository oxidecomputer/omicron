// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database testing facilities.

use camino::Utf8PathBuf;
use omicron_test_utils::dev;
use slog::Logger;

/// Path to the "seed" CockroachDB directory.
///
/// Populating CockroachDB unfortunately isn't free - creation of
/// tables, indices, and users takes several seconds to complete.
///
/// By creating a "seed" version of the database, we can cut down
/// on the time spent performing this operation. Instead, we opt
/// to copy the database from this seed location.
fn seed_dir() -> Utf8PathBuf {
    // The setup script should set this environment variable.
    let seed_dir = std::env::var("CRDB_SEED_DIR")
        .expect("CRDB_SEED_DIR missing -- are you running this test with `cargo nextest run`?");
    seed_dir.into()
}

/// Wrapper around [`dev::test_setup_database`] which uses a a
/// seed directory provided at build-time.
pub async fn test_setup_database(log: &Logger) -> dev::db::CockroachInstance {
    let dir = seed_dir();
    dev::test_setup_database(
        log,
        dev::StorageSource::CopyFromSeed { input_dir: dir },
    )
    .await
}

/// Creates a new database with no data populated.
///
/// Primarily used for schema change and migration testing.
pub async fn test_setup_database_empty(
    log: &Logger,
) -> dev::db::CockroachInstance {
    dev::test_setup_database(log, dev::StorageSource::DoNotPopulate).await
}

/// See the definition of this constant in nexus_db_queries.
///
/// Besides the cases mentioned there, it's also preferable for some ad hoc
/// test-only queries to do table scans rather than add indexes that are only
/// used for the test suite.
pub const ALLOW_FULL_TABLE_SCAN_SQL: &str =
    nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
