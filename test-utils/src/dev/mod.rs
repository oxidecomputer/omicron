// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Facilities intended for development tools and the test suite.  These should
 * not be used in production code.
 */

pub mod clickhouse;
pub mod db;
pub mod poll;
pub mod test_cmds;

use dropshot::test_util::LogContext;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingIfExists;
use dropshot::ConfigLoggingLevel;
use slog::Logger;
use std::path::Path;

/// Path to the "seed" CockroachDB directory.
///
/// Populating CockroachDB unfortunately isn't free - creation of
/// tables, indices, and users takes several seconds to complete.
///
/// By creating a "seed" version of the database, we can cut down
/// on the time spent performing this operation. Instead, we opt
/// to copy the database from this seed location.
pub const SEED_DB_DIR: &str = "/tmp/crdb-base";

// Helper for copying all the files in one directory to another.
fn copy_dir(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

/**
 * Set up a [`dropshot::test_util::LogContext`] appropriate for a test named
 * `test_name`
 *
 * This function is currently only used by unit tests.  (We want the dead code
 * warning if it's removed from unit tests, but not during a normal build.)
 */
pub fn test_setup_log(test_name: &str) -> LogContext {
    let log_config = ConfigLogging::File {
        level: ConfigLoggingLevel::Debug,
        path: String::from("UNUSED"),
        if_exists: ConfigLoggingIfExists::Fail,
    };

    LogContext::new(test_name, &log_config)
}

enum StorageSource {
    Populate,
    CopyFromSeed,
}

/// Creates a [`db::CockroachInstance`] with a populated storage directory.
///
/// This is intended to optimize subsequent calls to [`test_setup_database`]
/// by reducing the latency of populating the storage directory.
pub async fn test_setup_database_seed(log: &Logger) {
    std::fs::remove_dir_all(SEED_DB_DIR).unwrap();
    std::fs::create_dir_all(SEED_DB_DIR).unwrap();
    let mut db =
        setup_database(log, Some(SEED_DB_DIR), StorageSource::Populate).await;
    db.cleanup().await.unwrap();
}

/// Set up a [`db::CockroachInstance`] for running tests.
pub async fn test_setup_database(log: &Logger) -> db::CockroachInstance {
    setup_database(log, None, StorageSource::CopyFromSeed).await
}

async fn setup_database(
    log: &Logger,
    store_dir: Option<&str>,
    storage_source: StorageSource,
) -> db::CockroachInstance {
    let builder = db::CockroachStarterBuilder::new();
    let mut builder = if let Some(store_dir) = store_dir {
        builder.store_dir(store_dir)
    } else {
        builder
    };
    builder.redirect_stdio_to_files();
    let starter = builder.build().unwrap();
    info!(
        &log,
        "cockroach temporary directory: {}",
        starter.temp_dir().display()
    );

    // If we're going to copy the storage directory from the seed,
    // it is critical we do so before starting the DB.
    if matches!(storage_source, StorageSource::CopyFromSeed) {
        info!(&log, "cockroach: copying from seed directory");
        copy_dir(SEED_DB_DIR, starter.store_dir())
            .expect("Cannot copy storage from seed directory");
    }

    info!(&log, "cockroach command line: {}", starter.cmdline());
    let database = starter.start().await.unwrap();
    info!(&log, "cockroach pid: {}", database.pid());
    let db_url = database.pg_config();
    info!(&log, "cockroach listen URL: {}", db_url);

    // If we populate the storage directory by importing the '.sql'
    // file, we must do so after the DB has started.
    if matches!(storage_source, StorageSource::Populate) {
        info!(&log, "cockroach: populating");
        database.populate().await.expect("failed to populate database");
        info!(&log, "cockroach: populated");
    }
    database
}

/**
 * Returns whether the given process is currently running
 */
pub fn process_running(pid: u32) -> bool {
    /*
     * It should be okay to invoke this syscall with these arguments.  This
     * only checks whether the process is running.
     */
    0 == (unsafe { libc::kill(pid as libc::pid_t, 0) })
}
