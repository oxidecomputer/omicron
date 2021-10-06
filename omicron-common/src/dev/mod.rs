/*!
 * Facilities intended for development tools and the test suite.  These should
 * generally not be used in production code.
 */

/*
 * Dead code warnings are not meaningful here unless we're building for tests.
 */
#![cfg_attr(not(test), allow(dead_code))]

pub mod clickhouse;
pub mod db;
pub mod poll;
pub mod test_cmds;

use dropshot::test_util::LogContext;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingIfExists;
use dropshot::ConfigLoggingLevel;
use slog::Logger;

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

/**
 * Set up a [`db::CockroachInstance`] for running tests against.
 */
pub async fn test_setup_database(log: &Logger) -> db::CockroachInstance {
    let mut builder = db::CockroachStarterBuilder::new();
    builder.redirect_stdio_to_files();
    let starter = builder.build().unwrap();
    info!(
        &log,
        "cockroach temporary directory: {}",
        starter.temp_dir().display()
    );
    info!(&log, "cockroach command line: {}", starter.cmdline());
    let database = starter.start().await.unwrap();
    info!(&log, "cockroach pid: {}", database.pid());
    let db_url = database.pg_config();
    info!(&log, "cockroach listen URL: {}", db_url);
    info!(&log, "cockroach: populating");
    database.populate().await.expect("failed to populate database");
    info!(&log, "cockroach: populated");
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
