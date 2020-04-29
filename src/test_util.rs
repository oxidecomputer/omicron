/*!
 * Shared automated testing facilities
 */

/*
 * Dead code warnings are not meaningful here unless we're building for tests.
 */
#![cfg_attr(not(test), allow(dead_code))]

use dropshot::test_util::LogContext;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::ConfigLoggingIfExists;

/*
 * This function is currently only used by unit tests.  We want the dead code
 * warning if it's removed from unit tests, but not during a normal build.
 */
pub async fn test_setup_log(test_name: &str) -> LogContext {
    let log_config = ConfigLogging::File {
        level: ConfigLoggingLevel::Debug,
        path: String::from("UNUSED"),
        if_exists: ConfigLoggingIfExists::Fail,
    };

    LogContext::new(test_name, &log_config)
}
