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
        dev::StorageSource::CopyFromSeed {
            input_tar,
            store_dir: None,
            listen_port: None,
        },
    )
    .await
}

/// Creates a new database with no data populated.
///
/// Primarily used for schema change and migration testing.
pub async fn test_setup_database_empty(
    log: &Logger,
    store_dir: Option<Utf8PathBuf>,
) -> dev::db::CockroachInstance {
    test_setup_database_empty_with_port(log, store_dir, None).await
}

/// Creates a new database with no data populated, on a specific port.
pub async fn test_setup_database_empty_with_port(
    log: &Logger,
    store_dir: Option<Utf8PathBuf>,
    listen_port: Option<u16>,
) -> dev::db::CockroachInstance {
    dev::test_setup_database(
        log,
        dev::StorageSource::DoNotPopulate { store_dir, listen_port },
    )
    .await
}

/// Wrapper around [`dev::test_setup_database`] which uses a seed tarball
/// provided as an argument.
pub async fn test_setup_database_from_seed(
    log: &Logger,
    input_tar: Utf8PathBuf,
    store_dir: Option<Utf8PathBuf>,
) -> dev::db::CockroachInstance {
    test_setup_database_from_seed_with_port(log, input_tar, store_dir, None)
        .await
}

/// Wrapper around [`dev::test_setup_database`] which uses a seed tarball
/// provided as an argument, on a specific port.
pub async fn test_setup_database_from_seed_with_port(
    log: &Logger,
    input_tar: Utf8PathBuf,
    store_dir: Option<Utf8PathBuf>,
    listen_port: Option<u16>,
) -> dev::db::CockroachInstance {
    dev::test_setup_database(
        log,
        dev::StorageSource::CopyFromSeed { input_tar, store_dir, listen_port },
    )
    .await
}
