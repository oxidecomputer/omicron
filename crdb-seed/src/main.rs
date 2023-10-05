use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use dropshot::{test_util::LogContext, ConfigLogging, ConfigLoggingLevel};
use omicron_test_utils::dev;
use slog::Logger;
use std::io::Write;

// Creates a string identifier for the current DB schema and version.
//
// The goal here is to allow to create different "seed" tarballs
// for each revision of the DB.
fn digest_unique_to_schema() -> String {
    let schema = include_str!("../../schema/crdb/dbinit.sql");
    let crdb_version = include_str!("../../tools/cockroachdb_version");
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(&schema.as_bytes());
    ctx.update(&crdb_version.as_bytes());
    let digest = ctx.finish();
    hex::encode(digest.as_ref())
}

enum SeedTarballStatus {
    Created,
    Existing,
}

async fn ensure_seed_tarball_exists(
    log: &Logger,
) -> Result<(Utf8PathBuf, SeedTarballStatus)> {
    let base_seed_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("Not a UTF-8 path")
        .join("crdb-base");
    std::fs::create_dir_all(&base_seed_dir).unwrap();
    let mut desired_seed_tar = base_seed_dir.join(digest_unique_to_schema());
    desired_seed_tar.set_extension("tar");

    if desired_seed_tar.exists() {
        return Ok((desired_seed_tar, SeedTarballStatus::Existing));
    }

    // The tarball didn't exist when we started, so try to create it.
    //
    // Nextest will execute it just once, but it is possible for a user to start
    // up multiple nextest processes to be running at the same time. So we
    // should consider it possible for another caller to create this seed
    // tarball before we finish setting it up ourselves.
    dev::test_setup_database_seed(log, &desired_seed_tar)
        .await
        .context("failed to setup seed tarball")?;

    Ok((desired_seed_tar, SeedTarballStatus::Created))
}

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: dropshot is v heavyweight for this, we should be able to pull in a
    // smaller binary
    let logctx = LogContext::new(
        "crdb_seeding",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );
    let (tarball, status) = ensure_seed_tarball_exists(&logctx.log).await?;
    match status {
        SeedTarballStatus::Created => {
            slog::info!(logctx.log, "Created seed tarball: `{tarball}`");
        }
        SeedTarballStatus::Existing => {
            slog::info!(logctx.log, "Using existing seed tarball: `{tarball}`");
        }
    }
    if let Ok(env_path) = std::env::var("NEXTEST_ENV") {
        let mut file = std::fs::File::create(&env_path)
            .context("failed to open NEXTEST_ENV file")?;
        writeln!(file, "CRDB_SEED_TAR={tarball}")
            .context("failed to write to NEXTEST_ENV file")?;
    } else {
        slog::warn!(
            logctx.log,
            "NEXTEST_ENV not set (is this script running under nextest?)"
        );
    }

    Ok(())
}
