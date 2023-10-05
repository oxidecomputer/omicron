// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities intended for development tools and the test suite.  These should
//! not be used in production code.

pub mod clickhouse;
pub mod db;
pub mod dendrite;
pub mod poll;
pub mod test_cmds;

use anyhow::ensure;
use anyhow::{Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
pub use dropshot::test_util::LogContext;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingIfExists;
use dropshot::ConfigLoggingLevel;
use slog::Logger;
use std::io::{BufReader, BufWriter, Write};

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

/// Creates a [`db::CockroachInstance`] with a populated storage directory
/// inside a tarball.
///
/// This is intended to optimize subsequent calls to [`test_setup_database`] by
/// reducing the latency of populating the storage directory.
pub async fn test_setup_database_seed(
    log: &Logger,
    output_tar: &Utf8Path,
) -> Result<()> {
    let base_seed_dir = output_tar.parent().unwrap();
    let tmp_seed_dir = camino_tempfile::Utf8TempDir::new_in(base_seed_dir)
        .context("failed to create temporary seed directory")?;

    let mut db = setup_database(
        log,
        StorageSource::PopulateLatest {
            output_dir: tmp_seed_dir.path().to_owned(),
        },
    )
    .await
    .context("failed to setup database")?;
    db.cleanup().await.context("failed to cleanup database")?;

    // See https://github.com/cockroachdb/cockroach/issues/74231 for context on
    // this. We use this assertion to check that our seed directory won't point
    // back to itself, even if it is copied elsewhere.
    let dirs_record_path = tmp_seed_dir.path().join("temp-dirs-record.txt");
    let dirs_record_len = dirs_record_path
        .metadata()
        .with_context(|| {
            format!("cannot access metadata for {dirs_record_path}")
        })?
        .len();
    ensure!(
        dirs_record_len == 0,
        "Temporary directory record should be empty (was {dirs_record_len}) \
        after graceful shutdown",
    );

    // Tar up the directory -- this prevents issues where some but not all of
    // the files get cleaned up by /tmp cleaners. See
    // https://github.com/oxidecomputer/omicron/issues/4193.
    let atomic_file = atomicwrites::AtomicFile::new(
        &output_tar,
        // We don't expect this to exist, but if it does, we want to overwrite
        // it. That is because there's a remote possibility that multiple
        // instances of test_setup_database_seed are running simultaneously.
        atomicwrites::OverwriteBehavior::AllowOverwrite,
    );
    let res = atomic_file.write(|f| {
        // Tar up the directory here.
        let writer = BufWriter::new(f);
        let mut tar = tar::Builder::new(writer);
        tar.follow_symlinks(false);
        tar.append_dir_all(".", tmp_seed_dir.path()).with_context(|| {
            format!(
                "failed to append directory `{}` to tarball",
                tmp_seed_dir.path(),
            )
        })?;

        let mut writer =
            tar.into_inner().context("failed to finish writing tarball")?;
        writer.flush().context("failed to flush tarball")?;

        Ok::<_, anyhow::Error>(())
    });
    match res {
        Ok(()) => Ok(()),
        Err(atomicwrites::Error::Internal(error)) => {
            Err(error).with_context(|| {
                format!("failed to write seed tarball: `{}`", output_tar)
            })
        }
        Err(atomicwrites::Error::User(error)) => Err(error),
    }
}

/// Set up a [`db::CockroachInstance`] for running tests.
pub async fn test_setup_database(
    log: &Logger,
    source: StorageSource,
) -> db::CockroachInstance {
    usdt::register_probes().expect("Failed to register USDT DTrace probes");
    setup_database(log, source).await.unwrap()
}

// TODO: switch to anyhow entirely -- this function is currently a mishmash
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
            info!(&log,
                "cockroach: copying from seed tarball ({}) to storage directory ({})",
                input_tar, starter.store_dir().to_string_lossy(),
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
