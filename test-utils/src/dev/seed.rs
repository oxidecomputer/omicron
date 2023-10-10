// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::{BufWriter, Write};

use anyhow::{ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use filetime::FileTime;
use slog::Logger;

use super::CRDB_SEED_TAR_ENV;

/// Creates a string identifier for the current DB schema and version.
//
/// The goal here is to allow to create different "seed" tarballs
/// for each revision of the DB.
pub fn digest_unique_to_schema() -> String {
    let schema = include_str!("../../../schema/crdb/dbinit.sql");
    let crdb_version = include_str!("../../../tools/cockroachdb_version");
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(&schema.as_bytes());
    ctx.update(&crdb_version.as_bytes());
    let digest = ctx.finish();
    hex::encode(digest.as_ref())
}

/// Looks up the standard environment variable `CRDB_SEED_INVALIDATE` to check
/// if a seed should be invalidated. Returns a string to pass in as the
/// `why_invalidate` argument of [`ensure_seed_tarball_exists`].
pub fn should_invalidate_seed() -> Option<&'static str> {
    (std::env::var("CRDB_SEED_INVALIDATE").as_deref() == Ok("1"))
        .then_some("CRDB_SEED_INVALIDATE=1 set in environment")
}

/// The return value of [`ensure_seed_tarball_exists`].
#[derive(Clone, Copy, Debug)]
pub enum SeedTarballStatus {
    Created,
    Invalidated,
    Existing,
}

impl SeedTarballStatus {
    pub fn log(self, log: &Logger, seed_tar: &Utf8Path) {
        match self {
            SeedTarballStatus::Created => {
                info!(log, "Created CRDB seed tarball: `{seed_tar}`");
            }
            SeedTarballStatus::Invalidated => {
                info!(
                    log,
                    "Invalidated and created new CRDB seed tarball: `{seed_tar}`",
                );
            }
            SeedTarballStatus::Existing => {
                info!(log, "Using existing CRDB seed tarball: `{seed_tar}`");
            }
        }
    }
}

/// Ensures that a seed tarball corresponding to the schema returned by
/// [`digest_unique_to_schema`] exists, recreating it if necessary.
///
/// This used to create a directory rather than a tarball, but that was changed
/// due to [Omicron issue
/// #4193](https://github.com/oxidecomputer/omicron/issues/4193).
///
/// If `why_invalidate` is `Some`, then if the seed tarball exists, it will be
/// deleted before being recreated.
///
/// # Notes
///
/// This method should _not_ be used by tests. Instead, rely on the `crdb-seed`
/// setup script.
pub async fn ensure_seed_tarball_exists(
    log: &Logger,
    why_invalidate: Option<&str>,
) -> Result<(Utf8PathBuf, SeedTarballStatus)> {
    // If the CRDB_SEED_TAR_ENV variable is set, return an error.
    //
    // Even though this module is gated behind a feature flag, omicron-dev needs
    // this function -- and so, if you're doing a top-level `cargo nextest run`
    // like CI does, feature unification would mean this gets included in test
    // binaries anyway. So this acts as a belt-and-suspenders check.
    if let Ok(val) = std::env::var(CRDB_SEED_TAR_ENV) {
        anyhow::bail!(
            "{CRDB_SEED_TAR_ENV} is set to `{val}` -- implying that a test called \
             ensure_seed_tarball_exists. Instead, tests should rely on the `crdb-seed` \
             setup script."
        );
    }

    // XXX: we aren't considering cross-user permissions for this file. Might be
    // worth setting more restrictive permissions on it, or using a per-user
    // cache dir.
    let base_seed_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("Not a UTF-8 path")
        .join("crdb-base");
    std::fs::create_dir_all(&base_seed_dir).unwrap();
    let mut desired_seed_tar = base_seed_dir.join(digest_unique_to_schema());
    desired_seed_tar.set_extension("tar");

    let invalidated = match (desired_seed_tar.exists(), why_invalidate) {
        (true, Some(why)) => {
            slog::info!(
                log,
                "{why}: invalidating seed tarball: `{desired_seed_tar}`",
            );
            std::fs::remove_file(&desired_seed_tar)
                .context("failed to remove seed tarball")?;
            true
        }
        (true, None) => {
            // The tarball exists. Update its atime and mtime (i.e. `touch` it)
            // to ensure that it doesn't get deleted by a /tmp cleaner.
            let now = FileTime::now();
            filetime::set_file_times(&desired_seed_tar, now, now)
                .context("failed to update seed tarball atime and mtime")?;
            return Ok((desired_seed_tar, SeedTarballStatus::Existing));
        }
        (false, Some(why)) => {
            slog::info!(
                log,
                "{why}, but seed tarball does not exist: `{desired_seed_tar}`",
            );
            false
        }
        (false, None) => {
            // The tarball doesn't exist.
            false
        }
    };

    // At this point the tarball does not exist (either because it didn't exist
    // in the first place or because it was deleted above), so try to create it.
    //
    // Nextest will execute this function just once via the `crdb-seed` binary,
    // but it is possible for a user to start up multiple nextest processes to
    // be running at the same time. So we should consider it possible for
    // another caller to create this seed tarball before we finish setting it up
    // ourselves.
    test_setup_database_seed(log, &desired_seed_tar)
        .await
        .context("failed to setup seed tarball")?;

    let status = if invalidated {
        SeedTarballStatus::Invalidated
    } else {
        SeedTarballStatus::Created
    };
    Ok((desired_seed_tar, status))
}

/// Creates a seed file for a Cockroach database at the output tarball.
///
/// This is intended to optimize subsequent calls to
/// [`test_setup_database`](super::test_setup_database) by reducing the latency
/// of populating the storage directory.
pub async fn test_setup_database_seed(
    log: &Logger,
    output_tar: &Utf8Path,
) -> Result<()> {
    let base_seed_dir = output_tar.parent().unwrap();
    let tmp_seed_dir = camino_tempfile::Utf8TempDir::new_in(base_seed_dir)
        .context("failed to create temporary seed directory")?;

    let mut db = super::setup_database(
        log,
        super::StorageSource::PopulateLatest {
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

    let output_tar = output_tar.to_owned();

    tokio::task::spawn_blocking(move || {
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
            tar.append_dir_all(".", tmp_seed_dir.path()).with_context(
                || {
                    format!(
                        "failed to append directory `{}` to tarball",
                        tmp_seed_dir.path(),
                    )
                },
            )?;

            let mut writer =
                tar.into_inner().context("failed to finish writing tarball")?;
            writer.flush().context("failed to flush tarball")?;

            Ok::<_, anyhow::Error>(())
        });
        match res {
            Ok(()) => Ok(()),
            Err(atomicwrites::Error::Internal(error)) => Err(error)
                .with_context(|| {
                    format!("failed to write seed tarball: `{}`", output_tar)
                }),
            Err(atomicwrites::Error::User(error)) => Err(error),
        }
    })
    .await
    .context("error in task to tar up contents")?
}
