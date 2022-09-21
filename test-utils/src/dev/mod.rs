// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities intended for development tools and the test suite.  These should
//! not be used in production code.

pub mod clickhouse;
pub mod db;
pub mod poll;
pub mod test_cmds;

use anyhow::Context;
pub use dropshot::test_util::LogContext;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingIfExists;
use dropshot::ConfigLoggingLevel;
use slog::Logger;
use std::path::Path;

// Helper for copying all the files in one directory to another.
fn copy_dir(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
) -> Result<(), anyhow::Error> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    std::fs::create_dir_all(&dst)
        .with_context(|| format!("Failed to create dst {}", dst.display()))?;
    for entry in std::fs::read_dir(src)
        .with_context(|| format!("Failed to read_dir {}", src.display()))?
    {
        let entry = entry.with_context(|| {
            format!("Failed to read entry in {}", src.display())
        })?;
        let ty = entry.file_type().context("Failed to access file type")?;
        let target = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir(entry.path(), &target).with_context(|| {
                format!(
                    "Failed to copy subdirectory {} to {}",
                    entry.path().display(),
                    target.display()
                )
            })?;
        } else {
            std::fs::copy(entry.path(), &target).with_context(|| {
                format!(
                    "Failed to copy file at {} to {}",
                    entry.path().display(),
                    target.display()
                )
            })?;
        }
    }
    Ok(())
}

/// Set up a [`dropshot::test_util::LogContext`] appropriate for a test named
/// `test_name`
///
/// This function is currently only used by unit tests.  (We want the dead code
/// warning if it's removed from unit tests, but not during a normal build.)
pub fn test_setup_log(test_name: &str) -> LogContext {
    let log_config = ConfigLogging::File {
        level: ConfigLoggingLevel::Trace,
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
pub async fn test_setup_database_seed(log: &Logger, dir: &Path) {
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = setup_database(log, dir, StorageSource::Populate).await;
    db.cleanup().await.unwrap();

    // See https://github.com/cockroachdb/cockroach/issues/74231 for context on
    // this. We use this assertion to check that our seed directory won't point
    // back to itself, even if it is copied elsewhere.
    assert_eq!(
        0,
        dir.join("temp-dirs-record.txt")
            .metadata()
            .expect("Cannot access metadata")
            .len(),
        "Temporary directory record should be empty after graceful shutdown",
    );
}

/// Set up a [`db::CockroachInstance`] for running tests.
pub async fn test_setup_database(
    log: &Logger,
    dir: &Path,
) -> db::CockroachInstance {
    usdt::register_probes().expect("Failed to register USDT DTrace probes");
    setup_database(log, dir, StorageSource::CopyFromSeed).await
}

async fn setup_database(
    log: &Logger,
    seed_dir: &Path,
    storage_source: StorageSource,
) -> db::CockroachInstance {
    let builder = db::CockroachStarterBuilder::new();
    let mut builder = if matches!(storage_source, StorageSource::Populate) {
        builder.store_dir(seed_dir)
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
        info!(&log,
            "cockroach: copying from seed directory ({}) to storage directory ({})",
            seed_dir.to_string_lossy(), starter.store_dir().to_string_lossy(),
        );
        copy_dir(seed_dir, starter.store_dir())
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

/// Returns whether the given process is currently running
pub fn process_running(pid: u32) -> bool {
    // It should be okay to invoke this syscall with these arguments.  This
    // only checks whether the process is running.
    0 == (unsafe { libc::kill(pid as libc::pid_t, 0) })
}
