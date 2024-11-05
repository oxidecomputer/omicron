// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database testing facilities.

use camino::Utf8PathBuf;
use omicron_test_utils::dev;
use slog::Logger;

/// Path to the "seed" CockroachDB tarball.
///
/// Populating CockroachDB unfortunately isn't free - creation of
/// tables, indices, and users takes several seconds to complete.
///
/// By creating a "seed" version of the database, we can cut down
/// on the time spent performing this operation. Instead, we opt
/// to copy the database from this seed location.
fn seed_tar() -> Utf8PathBuf {
    // The setup script should set this environment variable.
    let seed_dir = std::env::var(dev::CRDB_SEED_TAR_ENV).unwrap_or_else(|_| {
        panic!(
            "{} missing -- are you running this test \
                 with `cargo nextest run`?",
            dev::CRDB_SEED_TAR_ENV,
        )
    });
    seed_dir.into()
}

/// Wrapper around [`dev::test_setup_database`] which uses a seed tarball
/// provided from the environment.
pub async fn test_setup_database(log: &Logger) -> dev::db::CockroachInstance {
    let input_tar = seed_tar();
    dev::test_setup_database(
        log,
        dev::StorageSource::CopyFromSeed { input_tar },
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

/// Wrapper around [`dev::test_setup_database`] which uses a seed tarball
/// provided as an argument.
pub async fn test_setup_database_from_seed(
    log: &Logger,
    input_tar: Utf8PathBuf,
) -> dev::db::CockroachInstance {
    dev::test_setup_database(
        log,
        dev::StorageSource::CopyFromSeed { input_tar },
    )
    .await
}
