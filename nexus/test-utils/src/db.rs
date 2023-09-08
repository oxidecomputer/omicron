// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database testing facilities.

use camino::Utf8PathBuf;
use omicron_test_utils::dev;
use slog::Logger;

// Creates a string identifier for the current DB schema and version.
//
// The goal here is to allow to create different "seed" directories
// for each revision of the DB.
fn digest_unique_to_schema() -> String {
    let schema = include_str!("../../../schema/crdb/dbinit.sql");
    let crdb_version = include_str!("../../../tools/cockroachdb_version");
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(&schema.as_bytes());
    ctx.update(&crdb_version.as_bytes());
    let digest = ctx.finish();
    hex::encode(digest.as_ref())
}

// Seed directories will be created within:
//
// - /tmp/crdb-base/<digest unique to schema>
//
// However, the process for creating these seed directories is not atomic.
// We create a temporary directory within:
//
// - /tmp/crdb-base/...
//
// And rename it to the final "digest" location once it has been fully created.
async fn ensure_seed_directory_exists(log: &Logger) -> Utf8PathBuf {
    let base_seed_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("Not a UTF-8 path")
        .join("crdb-base");
    std::fs::create_dir_all(&base_seed_dir).unwrap();
    let desired_seed_dir = base_seed_dir.join(digest_unique_to_schema());

    // If the directory didn't exist when we started, try to create it.
    //
    // Note that this may be executed concurrently by many tests, so
    // we should consider it possible for another caller to create this
    // seed directory before we finish setting it up ourselves.
    if !desired_seed_dir.exists() {
        let tmp_seed_dir =
            camino_tempfile::Utf8TempDir::new_in(base_seed_dir).unwrap();
        dev::test_setup_database_seed(log, tmp_seed_dir.path()).await;

        // If we can successfully perform the rename, we made the seed directory
        // faster than other tests.
        //
        // If we couldn't perform the rename, the directory might already exist.
        // Check that this is the error we encountered -- otherwise, we're
        // struggling.
        if let Err(err) =
            std::fs::rename(tmp_seed_dir.path(), &desired_seed_dir)
        {
            if !desired_seed_dir.exists() {
                panic!("Cannot rename seed directory for CockroachDB: {err}");
            }
        }
    }

    desired_seed_dir
}

/// Wrapper around [`dev::test_setup_database`] which uses a a
/// seed directory that we construct if it does not already exist.
pub async fn test_setup_database(log: &Logger) -> dev::db::CockroachInstance {
    let dir = ensure_seed_directory_exists(log).await;
    dev::test_setup_database(
        log,
        dev::StorageSource::CopyFromSeed { input_dir: dir.into() },
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
