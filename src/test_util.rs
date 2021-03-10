/*!
 * Shared automated testing facilities used by unit tests
 */

/*
 * Dead code warnings are not meaningful here unless we're building for tests.
 */
#![cfg_attr(not(test), allow(dead_code))]

use dropshot::test_util::LogContext;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingIfExists;
use dropshot::ConfigLoggingLevel;

/**
 * Set up a [`dropshot::test_util::LogContext`] appropriate for a test named
 * `test_name`
 *
 * This function is currently only used by unit tests.  (We want the dead code
 * warning if it's removed from unit tests, but not during a normal build.)
 */
pub async fn test_setup_log(test_name: &str) -> LogContext {
    let log_config = ConfigLogging::File {
        level: ConfigLoggingLevel::Debug,
        path: String::from("UNUSED"),
        if_exists: ConfigLoggingIfExists::Fail,
    };

    LogContext::new(test_name, &log_config)
}

pub fn process_running(pid: u32) -> bool {
    /*
     * It should be okay to invoke this syscall with these arguments.  This
     * only checks whether the process is running.
     */
    0 == (unsafe { libc::kill(pid as libc::pid_t, 0) })
}
