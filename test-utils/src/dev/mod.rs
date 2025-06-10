// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities intended for development tools and the test suite.  These should
//! not be used in production code.

pub mod clickhouse;
pub mod db;
pub mod dendrite;
pub mod falcon;
pub mod maghemite;
pub mod poll;
#[cfg(feature = "seed-gen")]
pub mod seed;
pub mod test_cmds;

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingIfExists;
use dropshot::ConfigLoggingLevel;
pub use dropshot::test_util::LogContext;
use omicron_common::disk::DiskIdentity;
use slog::Logger;
use std::io::BufReader;

/// The environment variable via which the path to the seed tarball is passed.
pub static CRDB_SEED_TAR_ENV: &str = "CRDB_SEED_TAR";

/// Set up a [`dropshot::test_util::LogContext`] appropriate for a test named
/// `test_name`
///
/// This function is currently only used by unit tests.  (We want the dead code
/// warning if it's removed from unit tests, but not during a normal build.)
pub fn test_setup_log(test_name: &str) -> LogContext {
    let log_config = ConfigLogging::File {
        level: ConfigLoggingLevel::Trace,
        path: "UNUSED".into(),
        if_exists: ConfigLoggingIfExists::Fail,
    };

    LogContext::new(test_name, &log_config)
}

/// Describes how to populate the database under test.
pub enum StorageSource {
    /// Do not populate anything. This is primarily used for migration testing.
    DoNotPopulate,
    /// Populate the latest version of the database.
    PopulateLatest { output_dir: Utf8PathBuf },
    /// Copy the database from a seed tarball, which has previously
    /// been created with `PopulateLatest`.
    CopyFromSeed { input_tar: Utf8PathBuf },
}

/// Set up a [`db::CockroachInstance`] for running tests.
pub async fn test_setup_database(
    log: &Logger,
    source: StorageSource,
) -> db::CockroachInstance {
    usdt::register_probes().expect("Failed to register USDT DTrace probes");
    setup_database(log, source).await.unwrap()
}

// TODO: switch to anyhow entirely -- this function is currently a mishmash of
// anyhow and unwrap/expect calls.
async fn setup_database(
    log: &Logger,
    storage_source: StorageSource,
) -> Result<db::CockroachInstance> {
    let builder = db::CockroachStarterBuilder::new();
    let mut builder = match &storage_source {
        StorageSource::DoNotPopulate | StorageSource::CopyFromSeed { .. } => {
            builder
        }
        StorageSource::PopulateLatest { output_dir } => {
            builder.store_dir(output_dir)
        }
    };
    builder.redirect_stdio_to_files();
    let starter = builder.build().context("error building CockroachStarter")?;
    info!(
        &log,
        "cockroach temporary directory: {}",
        starter.temp_dir().display()
    );

    // If we're going to copy the storage directory from the seed,
    // it is critical we do so before starting the DB.
    match &storage_source {
        StorageSource::DoNotPopulate | StorageSource::PopulateLatest { .. } => {
        }
        StorageSource::CopyFromSeed { input_tar } => {
            info!(
                &log,
                "cockroach: copying from seed tarball ({}) to storage directory ({})",
                input_tar,
                starter.store_dir().to_string_lossy(),
            );
            let reader = std::fs::File::open(input_tar).with_context(|| {
                format!("cannot open input tar {}", input_tar)
            })?;
            let mut tar = tar::Archive::new(BufReader::new(reader));
            tar.unpack(starter.store_dir()).with_context(|| {
                format!(
                    "cannot unpack input tar {} into {}",
                    input_tar,
                    starter.store_dir().display()
                )
            })?;
        }
    }

    info!(&log, "cockroach command line: {}", starter.cmdline());
    info!(
        &log,
        "cockroach environment: {}",
        starter
            .environment()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let database = starter.start().await.unwrap_or_else(|error| {
        panic!("failed to start CockroachDB: {:#}", error);
    });
    info!(&log, "cockroach pid: {}", database.pid());
    let db_url = database.pg_config();
    info!(&log, "cockroach listen URL: {}", db_url);

    database.disable_synchronization().await.expect("Failed to disable fsync");

    // If we populate the storage directory by importing the '.sql'
    // file, we must do so after the DB has started.
    match &storage_source {
        StorageSource::DoNotPopulate | StorageSource::CopyFromSeed { .. } => {}
        StorageSource::PopulateLatest { .. } => {
            info!(&log, "cockroach: populating");
            database.populate().await.expect("failed to populate database");
            info!(&log, "cockroach: populated");
        }
    }

    Ok(database)
}

/// Returns whether the given process is currently running
pub fn process_running(pid: u32) -> bool {
    // It should be okay to invoke this syscall with these arguments.  This
    // only checks whether the process is running.
    0 == (unsafe { libc::kill(pid as libc::pid_t, 0) })
}

/// Returns a DiskIdentity that can be passed to ensure_partition_layout when
/// not operating on a real disk.
pub fn mock_disk_identity() -> DiskIdentity {
    DiskIdentity {
        vendor: "MockVendor".to_string(),
        serial: "MOCKSERIAL".to_string(),
        model: "MOCKMODEL".to_string(),
    }
}
