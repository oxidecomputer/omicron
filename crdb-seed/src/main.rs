use camino::Utf8PathBuf;
use dropshot::{test_util::LogContext, ConfigLogging, ConfigLoggingLevel};
use omicron_test_utils::dev;
use slog::Logger;
use std::io::Write;

// Creates a string identifier for the current DB schema and version.
//
// The goal here is to allow to create different "seed" directories
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

#[tokio::main]
async fn main() {
    // TODO: dropshot is v heavyweight for this, we should be able to pull in a
    // smaller binary
    let logctx = LogContext::new(
        "crdb_seeding",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );
    let dir = ensure_seed_directory_exists(&logctx.log).await;
    if let Ok(env_path) = std::env::var("NEXTEST_ENV") {
        let mut file = std::fs::File::create(&env_path)
            .expect("failed to open NEXTEST_ENV file");
        write!(file, "CRDB_SEED_DIR={}", dir.as_str())
            .expect("failed to write to NEXTEST_ENV file");
    }
}
