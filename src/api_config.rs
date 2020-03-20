/*!
 * Interfaces for parsing configuration files and working with API server
 * configuration.
 */

use crate::api_error::InitError;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use slog::Drain;
use slog::Level;
use slog::Logger;

/**
 * Represents configuration for the whole API server.
 */
#[derive(Debug, Deserialize)]
pub struct ApiServerConfig {
    /** IP address and TCP port to which to bind for accepting connections. */
    pub bind_address: SocketAddr,
    /** Server-wide logging configuration. */
    pub log: ConfigLogging,
}

impl ApiServerConfig {
    /**
     * Load an `ApiServerConfig` from the given TOML file.  The format is
     * described in the README.  This config object can then be used to create a
     * new `ApiServer`.
     */
    pub fn from_file(path: &Path) -> Result<ApiServerConfig, String> {
        let file_read = std::fs::read_to_string(path);
        let file_contents = file_read.map_err(|error| {
            format!("read \"{}\": {}", path.display(), error)
        })?;
        let config_parsed: ApiServerConfig = toml::from_str(&file_contents)
            .map_err(|error| {
                format!("parse \"{}\": {}", path.display(), error)
            })?;
        Ok(config_parsed)
    }
}

#[cfg(test)]
mod test {
    use super::ApiServerConfig;
    use slog::Logger;
    use std::fs;
    use std::net::IpAddr;
    use std::path::Path;

    /*
     * Chunks of valid config file.  These are put together with invalid chunks
     * in the test suite to construct complete config files that will only fail
     * on the known invalid chunk.
     */
    const CONFIG_VALID_BIND_ADDRESS: &str = r##"
            bind_address = "127.0.0.1:1234"
        "##;
    const CONFIG_VALID_LOG: &str = r##"
            [log]
            level = "trace"
            mode = "stderr-terminal"
        "##;

    pub fn read_config(
        label: &str,
        contents: &str,
    ) -> Result<ApiServerConfig, String> {
        let arg0str = std::env::args().next().expect("expected process arg0");
        let arg0 = Path::new(&arg0str)
            .file_name()
            .expect("expected arg0 filename")
            .to_str()
            .expect("expected arg0 filename to be valid Unicode");
        let pid = std::process::id();
        let mut pathbuf = std::env::temp_dir();
        pathbuf.push(format!("{}.{}.{}", arg0, pid, label));
        let path = pathbuf.as_path();
        eprintln!("writing test config {}", path.display());
        fs::write(path, contents).expect("write to tempfile failed");

        let result = ApiServerConfig::from_file(path);
        fs::remove_file(path).expect("failed to remove temporary file");
        eprintln!("{:?}", result);
        result
    }

    /*
     * Totally bogus config files (nonexistent, bad TOML syntax)
     */

    #[test]
    pub fn test_config_nonexistent() {
        let error = ApiServerConfig::from_file(Path::new("/nonexistent"))
            .expect_err("expected config to fail from /nonexistent");
        assert!(error
            .starts_with("read \"/nonexistent\": No such file or directory"));
    }

    #[test]
    pub fn test_config_bad_toml() {
        let error =
            read_config("bad_toml", "foo =").expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": unexpected eof"));
    }

    /*
     * Empty config (special case of a missing required field, but worth calling
     * out explicitly)
     */

    #[test]
    pub fn test_config_empty() {
        let error = read_config("empty", "").expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field"));
    }

    /*
     * Bad values for "bind_address"
     */

    #[test]
    pub fn test_config_bad_bind_address_port_too_small() {
        let bad_config = format!("{}{}", r###"
            bind_address = "127.0.0.1:-3"
            "###, CONFIG_VALID_LOG);
        let error = read_config("bad_bind_address_port_too_small", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": invalid IP address syntax for key \
            `bind_address`"));
    }

    #[test]
    pub fn test_config_bad_bind_address_port_too_large() {
        let bad_config = format!("{}{}", r###"
            bind_address = "127.0.0.1:65536"
            "###, CONFIG_VALID_LOG);
        let error = read_config("bad_bind_address_port_too_large", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": invalid IP address syntax for key \
            `bind_address`"));
    }

    #[test]
    pub fn test_config_bad_bind_address_garbage() {
        let bad_config = format!("{}{}", r###"
            bind_address = "foobar"
            "###, CONFIG_VALID_LOG);
        let error = read_config("bad_bind_address_garbage", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": invalid IP address syntax for key \
            `bind_address`"));
    }

    /*
     * Bad value for "log_mode"
     */

    #[test]
    pub fn test_config_bad_log_mode() {
        let bad_config = format!("{}{}", CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "bonkers"
            "##);
        let error = read_config("bad_log_mode", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": unknown variant `bonkers`, expected one \
            of `stderr-terminal`, `file`, `test-suite` for key `log.mode`"));
    }

    /*
     * Bad "mode = stderr-terminal" config
     *
     * TODO-coverage: consider adding tests for all variants of missing or
     * invalid properties for all log modes
     */

    #[test]
    pub fn test_config_bad_terminal_no_level() {
        let bad_config = format!("{}{}", CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "stderr-terminal"
            "##);
        let error = read_config("bad_terminal_no_level", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field `level` for key `log`"));
    }

    #[test]
    pub fn test_config_bad_terminal_bad_level() {
        let bad_config = format!("{}{}", CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "stderr-terminal"
            level = "everything"
            "##);
        let error = read_config("bad_terminal_bad_level", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": unknown variant `everything`, expected one \
            of `trace`, `debug`, `info`"));
    }

    /*
     * Working "mode = stderr-terminal" config
     *
     * TODO-coverage: should use the logger and redirect stderr to a file and
     * test its contents.
     * TODO-coverage: other tests:
     * - failed to create file logger (filesystem failure of some kind)
     * - successful file logger, all three modes?
     */
    #[test]
    pub fn test_config_stderr_terminal() {
        let config = r##"
            bind_address = "127.1.2.3:4567"
            [log]
            mode = "stderr-terminal"
            level = "warn"
        "##;
        let config = read_config("stderr-terminal", &config)
            .expect("expected success");
        assert_eq!(config.bind_address.ip(), "127.1.2.3".parse::<IpAddr>().unwrap());
        assert_eq!(config.bind_address.port(), 4567);

        config.log.to_logger().expect("expected logger");
    }
}

/*
 * Logging configuration
 *
 * The following types and functions could be separated out into a much more
 * generic "logging configuration" module.
 */

/**
 * Represents the logging configuration for a server (the "log" top-level object
 * in the server configuration).
 */
#[derive(Debug, Deserialize)]
#[serde(tag = "mode")]
pub enum ConfigLogging {
    #[serde(rename = "stderr-terminal")]
    StderrTerminal { level: ConfigLoggingLevel },

    #[serde(rename = "file")]
    File {
        level: ConfigLoggingLevel,
        path: String,
        if_exists: ConfigLoggingIfExists,
    },

    /*
     * "test-suite" mode generates log files in a particular directory that are
     * named with both the program name and process id.  It would be nice to
     * allow some kinds of expansions in the "file" mode instead (e.g., for
     * `{program_name}` and `{pid}`).  Then we wouldn't need a special mode
     * here.  There's the `runtime-fmt` crate that could be used for this, but
     * it requires nightly rust.  For now, we punt -- and don't pretend that
     * this is any more generic than it is -- a mode for configuring logging for
     * the test suite.
     *
     * Note that neither of the other two modes is suitable for multiple
     * processes logging to the same file, even when setting `if_exists =
     * "append"`.
     */
    #[serde(rename = "test-suite")]
    TestSuite { level: ConfigLoggingLevel, directory: String },
}

#[derive(Debug, Deserialize)]
pub enum ConfigLoggingIfExists {
    #[serde(rename = "fail")]
    Fail,
    #[serde(rename = "truncate")]
    Truncate,
    #[serde(rename = "append")]
    Append,
}

#[derive(Debug, Deserialize)]
pub enum ConfigLoggingLevel {
    #[serde(rename = "trace")]
    Trace,
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "warn")]
    Warn,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "critical")]
    Critical,
}

impl From<&ConfigLoggingLevel> for Level {
    fn from(config_level: &ConfigLoggingLevel) -> Level {
        match config_level {
            ConfigLoggingLevel::Trace => Level::Trace,
            ConfigLoggingLevel::Debug => Level::Debug,
            ConfigLoggingLevel::Info => Level::Info,
            ConfigLoggingLevel::Warn => Level::Warning,
            ConfigLoggingLevel::Error => Level::Error,
            ConfigLoggingLevel::Critical => Level::Critical,
        }
    }
}

static TEST_SUITE_LOGGER_ID: AtomicU32 = AtomicU32::new(0);

impl ConfigLogging {
    /**
     * Create the root logger based on the requested configuration.
     */
    pub fn to_logger(&self) -> Result<Logger, InitError> {
        let pid = std::process::id();
        match self {
            ConfigLogging::StderrTerminal {
                level,
            } => {
                let decorator = slog_term::TermDecorator::new().build();
                let drain =
                    slog_term::FullFormat::new(decorator).build().fuse();
                Ok(async_root_logger(level, drain))
            }

            ConfigLogging::File {
                level,
                path,
                if_exists,
            } => {
                let mut open_options = std::fs::OpenOptions::new();
                open_options.write(true);
                open_options.create(true);

                match if_exists {
                    ConfigLoggingIfExists::Fail => {
                        open_options.create_new(true);
                    }
                    ConfigLoggingIfExists::Append => {
                        open_options.append(true);
                    }
                    ConfigLoggingIfExists::Truncate => {
                        open_options.truncate(true);
                    }
                }

                let drain = log_drain_for_file(&open_options, Path::new(path))?;
                Ok(async_root_logger(level, drain))
            }

            ConfigLogging::TestSuite {
                level,
                directory,
            } => {
                let mut open_options = std::fs::OpenOptions::new();
                open_options.write(true).create_new(true);

                let arg0path =
                    std::env::args().next().expect("expected process arg0");
                let arg0 = Path::new(&arg0path)
                    .file_name()
                    .expect("expected arg0 filename")
                    .to_str()
                    .expect("expected arg0 filename to be valid Unicode");
                let id = TEST_SUITE_LOGGER_ID.fetch_add(1, Ordering::SeqCst);
                let mut pathbuf = PathBuf::new();
                pathbuf.push(directory);
                pathbuf.push(format!("{}.{}.{}.log", arg0, pid, id));

                let path = pathbuf.as_path();
                let drain = log_drain_for_file(&open_options, path)?;
                Ok(async_root_logger(level, drain))
            }
        }
    }
}

/*
 * TODO-hardening
 * We use an async drain for the terminal logger to take care of
 * synchronization.  That's mainly because the other two options use a
 * std::sync::Mutex, which is not futures-aware and is likely to foul up
 * our executor.  However, we have not verified that the async
 * implementation behaves reasonably under backpressure.
 */
fn async_root_logger<T>(level: &ConfigLoggingLevel, drain: T) -> slog::Logger
where
    T: slog::Drain + Send + 'static,
    <T as slog::Drain>::Err: std::fmt::Debug,
{
    let pid = std::process::id();
    let level_drain = slog::LevelFilter(drain, Level::from(level)).fuse();
    let async_drain = slog_async::Async::new(level_drain).build().fuse();
    slog::Logger::root(async_drain, o!("pid" => pid))
}

fn log_drain_for_file(
    open_options: &OpenOptions,
    path: &Path,
) -> Result<slog::Fuse<slog_json::Json<std::fs::File>>, InitError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            let p = path.display();
            let message = format!("open log file \"{}\": {}", p, e);
            InitError(message)
        })?;
    }

    let file = open_options.open(path).map_err(|e| {
        let p = path.display();
        let message = format!("open log file \"{}\": {}", p, e);
        InitError(message)
    })?;

    /*
     * Record a message to the stderr so that a reader who doesn't already know
     * how logging is configured knows where the rest of the log messages went.
     */
    eprintln!("note: configured to log to \"{}\"", path.display());
    Ok(slog_bunyan::with_name("oxide-api", file).build().fuse())
}
