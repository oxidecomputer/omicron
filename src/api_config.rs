/*!
 * Interfaces for parsing configuration files and working with API server
 * configuration.
 */

use crate::api_error::InitError;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::Path;

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

impl ConfigLogging {
    /**
     * Create the root logger based on the requested configuration.
     */
    pub fn to_logger(&self) -> Result<Logger, InitError> {
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
    let level_drain = slog::LevelFilter(drain, Level::from(level)).fuse();
    let async_drain = slog_async::Async::new(level_drain).build().fuse();
    slog::Logger::root(async_drain, o!())
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

#[cfg(test)]
mod test {
    use super::super::test_common::read_bunyan_log;
    use super::super::test_common::verify_bunyan_records;
    use super::super::test_common::verify_bunyan_records_sequential;
    use super::super::test_common::BunyanLogRecordSpec;
    use super::ApiServerConfig;
    use std::fs;
    use std::net::IpAddr;
    use std::path::Path;
    use std::path::PathBuf;

    /*
     * Chunks of valid config file.  These are put together with invalid chunks
     * in the test suite to construct complete config files that will only fail
     * on the known invalid chunk.
     */
    const CONFIG_VALID_BIND_ADDRESS: &str = r##"
            bind_address = "127.0.0.1:12221"
        "##;
    const CONFIG_VALID_LOG: &str = r##"
            [log]
            level = "critical"
            mode = "stderr-terminal"
        "##;

    /**
     * Generates a temporary filesystem path unique for the given label.
     */
    fn temp_path(label: &str) -> PathBuf {
        let arg0str = std::env::args().next().expect("expected process arg0");
        let arg0 = Path::new(&arg0str)
            .file_name()
            .expect("expected arg0 filename")
            .to_str()
            .expect("expected arg0 filename to be valid Unicode");
        let pid = std::process::id();
        let mut pathbuf = std::env::temp_dir();
        pathbuf.push(format!("{}.{}.{}", arg0, pid, label));
        pathbuf
    }

    /**
     * Load an ApiServerConfig with the given string `contents`.  To exercise
     * the full path, this function writes the contents to a file first, then
     * loads the config from that file, then removes the file.  `label` is used
     * as a unique string for the filename and error messages.  It should be
     * unique for each test.
     */
    fn read_config(
        label: &str,
        contents: &str,
    ) -> Result<ApiServerConfig, String> {
        let pathbuf = temp_path(label);
        let path = pathbuf.as_path();
        eprintln!("writing test config {}", path.display());
        fs::write(path, contents).expect("write to tempfile failed");

        let result = ApiServerConfig::from_file(path);
        fs::remove_file(path).expect("failed to remove temporary file");
        eprintln!("{:?}", result);
        result
    }

    /**
     * `LogTest` and `LogTestCleanup` are used for the tests that create various
     * files on the filesystem to commonize code and make sure everything gets
     * cleaned up as expected.
     */
    struct LogTest {
        directory: PathBuf,
        cleanup_list: Vec<LogTestCleanup>,
    }

    #[derive(Debug)]
    enum LogTestCleanup {
        Directory(PathBuf),
        File(PathBuf),
    }

    impl LogTest {
        /**
         * The setup for a logger test creates a temporary directory with the
         * given label and returns a `LogTest` with that directory in the
         * cleanup list so that on teardown the temporary directory will be
         * removed.  The temporary directory must be empty by the time the
         * `LogTest` is torn down except for files and directories created with
         * `will_create_dir()` and `will_create_file()`.
         */
        fn setup(label: &str) -> LogTest {
            let directory_path = temp_path(label);

            if let Err(e) = fs::create_dir_all(&directory_path) {
                panic!(
                    "unexpected failure creating directories leading up to \
                     {}: {}",
                    directory_path.as_path().display(),
                    e
                );
            }

            LogTest {
                directory: directory_path.clone(),
                cleanup_list: vec![LogTestCleanup::Directory(directory_path)],
            }
        }

        /**
         * Records that the caller intends to create a directory with relative
         * path "path" underneath the root directory for this log test.  Returns
         * a String representing the path to this directory.  This directory
         * will be removed during teardown.  Directories and files must be
         * recorded in the order they would be created so that the order can be
         * reversed at teardown (without needing any kind of recursive removal).
         */
        fn will_create_dir(&mut self, path: &str) -> String {
            let mut pathbuf = self.directory.clone();
            pathbuf.push(path);
            let rv = pathbuf.as_path().display().to_string();
            self.cleanup_list.push(LogTestCleanup::Directory(pathbuf));
            rv
        }

        /**
         * Records that the caller intends to create a file with relative path
         * "path" underneath the root directory for this log test.  Returns a
         * String representing the path to this file.  This file will be removed
         * during teardown.  Directories and files must be recorded in the order
         * they would be created so that the order can be reversed at teardown
         * (without needing any kind of recursive removal).
         */
        fn will_create_file(&mut self, path: &str) -> String {
            let mut pathbuf = self.directory.clone();
            pathbuf.push(path);
            let rv = pathbuf.as_path().display().to_string();
            self.cleanup_list.push(LogTestCleanup::File(pathbuf));
            rv
        }
    }

    impl Drop for LogTest {
        fn drop(&mut self) {
            for path in self.cleanup_list.iter().rev() {
                let maybe_error = match path {
                    LogTestCleanup::Directory(p) => fs::remove_dir(p),
                    LogTestCleanup::File(p) => fs::remove_file(p),
                };

                if let Err(e) = maybe_error {
                    panic!("unexpected failure removing {:?}", e);
                }
            }
        }
    }

    /*
     * Totally bogus config files (nonexistent, bad TOML syntax)
     */

    #[test]
    fn test_config_nonexistent() {
        let error = ApiServerConfig::from_file(Path::new("/nonexistent"))
            .expect_err("expected config to fail from /nonexistent");
        assert!(error
            .starts_with("read \"/nonexistent\": No such file or directory"));
    }

    #[test]
    fn test_config_bad_toml() {
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
    fn test_config_empty() {
        let error = read_config("empty", "").expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field"));
    }

    /*
     * Bad values for "bind_address"
     */

    #[test]
    fn test_config_bad_bind_address_port_too_small() {
        let bad_config = format!(
            "{}{}",
            r###"
            bind_address = "127.0.0.1:-3"
            "###,
            CONFIG_VALID_LOG
        );
        let error = read_config("bad_bind_address_port_too_small", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error
            .contains("\": invalid IP address syntax for key `bind_address`"));
    }

    #[test]
    fn test_config_bad_bind_address_port_too_large() {
        let bad_config = format!(
            "{}{}",
            r###"
            bind_address = "127.0.0.1:65536"
            "###,
            CONFIG_VALID_LOG
        );
        let error = read_config("bad_bind_address_port_too_large", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error
            .contains("\": invalid IP address syntax for key `bind_address`"));
    }

    #[test]
    fn test_config_bad_bind_address_garbage() {
        let bad_config = format!(
            "{}{}",
            r###"
            bind_address = "foobar"
            "###,
            CONFIG_VALID_LOG
        );
        let error = read_config("bad_bind_address_garbage", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error
            .contains("\": invalid IP address syntax for key `bind_address`"));
    }

    #[tokio::test]
    async fn test_config_bind_address() {
        let client = hyper::Client::new();
        let bind_ip_str = "127.0.0.1";
        let bind_port: u16 = 12221;

        /*
         * This helper constructs a GET HTTP request to
         * http://$bind_ip_str:$port/, where $port is the argument to the
         * closure.
         */
        let cons_request = |port: u16| {
            let uri = hyper::Uri::builder()
                .scheme("http")
                .authority(format!("{}:{}", bind_ip_str, port).as_str())
                .path_and_query("/")
                .build()
                .unwrap();
            hyper::Request::builder()
                .method(http::method::Method::GET)
                .uri(&uri)
                .body(hyper::Body::empty())
                .unwrap()
        };

        /*
         * Make sure there is not currently a server running on our expected
         * port so that when we subsequently create a server and run it we know
         * we're getting the one we configured.
         */
        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());

        /*
         * Now start a server with our configuration and make the request again.
         * This should succeed in terms of making the request.  (The request
         * itself might fail with a 400-level or 500-level response code -- we
         * don't want to depend on too much from the ApiServer here -- but we
         * should have successfully made the request.)
         */
        let config_text = format!(
            "bind_address = \"{}:{}\"\n{}",
            bind_ip_str, bind_port, CONFIG_VALID_LOG
        );
        let config = read_config("bind_address", &config_text).unwrap();
        let mut server = super::super::ApiServer::new(&config, false).unwrap();
        let task = server.http_server.run();
        client.request(cons_request(bind_port)).await.unwrap();
        server.http_server.close();
        task.await.unwrap().unwrap();

        /*
         * Make another request to make sure it fails now that we've shut down
         * the server.
         */
        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());

        /*
         * Start a server on another TCP port and make sure we can reach that
         * one (and NOT the one we just shut down).
         */
        let config_text = format!(
            "bind_address = \"{}:{}\"\n{}",
            bind_ip_str,
            bind_port + 1,
            CONFIG_VALID_LOG
        );
        let config = read_config("bind_address", &config_text).unwrap();
        let mut server = super::super::ApiServer::new(&config, false).unwrap();
        let task = server.http_server.run();
        client.request(cons_request(bind_port + 1)).await.unwrap();
        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());
        server.http_server.close();
        task.await.unwrap().unwrap();

        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());
        let error =
            client.request(cons_request(bind_port + 1)).await.unwrap_err();
        assert!(error.is_connect());
    }

    /*
     * Bad value for "log_mode"
     */

    #[test]
    fn test_config_bad_log_mode() {
        let bad_config = format!(
            "{}{}",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "bonkers"
            "##
        );
        let error = read_config("bad_log_mode", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains(
            "\": unknown variant `bonkers`, expected `stderr-terminal` or \
             `file` for key `log.mode`"
        ));
    }

    /*
     * Bad "mode = stderr-terminal" config
     *
     * TODO-coverage: consider adding tests for all variants of missing or
     * invalid properties for all log modes
     */

    #[test]
    fn test_config_bad_terminal_no_level() {
        let bad_config = format!(
            "{}{}",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "stderr-terminal"
            "##
        );
        let error = read_config("bad_terminal_no_level", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field `level` for key `log`"));
    }

    #[test]
    fn test_config_bad_terminal_bad_level() {
        let bad_config = format!(
            "{}{}",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "stderr-terminal"
            level = "everything"
            "##
        );
        let error = read_config("bad_terminal_bad_level", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains(
            "\": unknown variant `everything`, expected one of `trace`, \
             `debug`, `info`"
        ));
    }

    /*
     * Working "mode = stderr-terminal" config
     *
     * TODO-coverage: It would be nice to redirect our own stderr to a file (or
     * something else we can collect) and then use the logger that we get below.
     * Then we could verify that it contains the content we expect.
     * Unfortunately, while Rust has private functions to redirect stdout and
     * stderr, there's no exposed function for doing that, nor is there a way to
     * provide a specific stream to a terminal logger.  (We could always
     * implement our own.)
     */
    #[test]
    fn test_config_stderr_terminal() {
        let config = r##"
            bind_address = "127.1.2.3:4567"
            [log]
            mode = "stderr-terminal"
            level = "warn"
        "##;
        let config =
            read_config("stderr-terminal", &config).expect("expected success");
        assert_eq!(
            config.bind_address.ip(),
            "127.1.2.3".parse::<IpAddr>().unwrap()
        );
        assert_eq!(config.bind_address.port(), 4567);

        config.log.to_logger().expect("expected logger");
    }

    /*
     * Bad "mode = file" configurations
     */

    #[test]
    fn test_config_bad_file_no_file() {
        let bad_config = format!(
            "{}{}",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            level = "warn"
            "##
        );
        let error = read_config("bad_file_no_file", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field `path` for key `log`"));
    }

    #[test]
    fn test_config_bad_file_no_level() {
        let bad_config = format!(
            "{}{}",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            path = "nonexistent"
            "##
        );
        let error = read_config("bad_file_no_level", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field `level` for key `log`"));
    }

    #[test]
    fn test_config_bad_file_bad_path_type() {
        /*
         * We create a path as a directory so that when we subsequently try to
         * use it a file, we won't be able to.
         */
        let mut logtest = LogTest::setup("bad_file_bad_path_type_dir");
        let path = logtest.will_create_dir("log_file_as_dir");
        fs::create_dir(&path).unwrap();

        let bad_config = format!(
            "{}{}\"{}\"\n",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            level = "warn"
            if_exists = "append"
            path = "##,
            &path
        );

        let config =
            read_config("bad_file_bad_path_type", &bad_config).unwrap();
        let error = config.log.to_logger().unwrap_err();
        let message = format!("{}", error);
        eprintln!("error message: {}", message);
        assert!(message.starts_with(&format!(
            "error creating API server: open log file \"{}\": Is a directory",
            &path
        )));
    }

    #[test]
    fn test_config_bad_file_path_exists_fail() {
        let mut logtest = LogTest::setup("bad_file_path_exists_fail_dir");
        let logpath = logtest.will_create_file("log.out");
        fs::write(&logpath, "").expect("writing empty file");

        let bad_config = format!(
            "{}{}\"{}\"\n",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            level = "warn"
            if_exists = "fail"
            path = "##,
            &logpath
        );

        let config = read_config("bad_file_bad_path_exists_fail", &bad_config)
            .expect("expected success");
        let error = config.log.to_logger().expect_err("expected failure");
        let message = format!("{}", error);
        eprintln!("error message: {}", message);
        assert!(message.starts_with(&format!(
            "error creating API server: open log file \"{}\": File exists",
            &logpath
        )));
    }

    /*
     * Working "mode = file" configuration.  The following test exercises
     * successful file-based configurations for all three values of "if_exists",
     * different log levels, and the bunyan log format.
     */

    #[test]
    fn test_config_file() {
        let mut logtest = LogTest::setup("file_dir");
        let logpath = logtest.will_create_file("log.out");
        let time_before = chrono::offset::Utc::now();

        /* The first attempt should succeed.  The log file doesn't exist yet. */
        let bad_config = format!(
            "{}{}\"{}\"\n",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            level = "warn"
            if_exists = "fail"
            path = "##,
            &logpath
        );

        let config =
            read_config("file", &bad_config).expect("expected success");
        {
            /*
             * Construct the logger in a block so that it's flushed by the time
             * we proceed.
             */
            let log = config.log.to_logger().expect("expected success");
            debug!(log, "message1_debug");
            warn!(log, "message1_warn");
            error!(log, "message1_error");
        }

        /* Try again with if_exists = "append".  This should also work. */
        let bad_config = format!(
            "{}{}\"{}\"\n",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            level = "warn"
            if_exists = "append"
            path = "##,
            &logpath
        );

        let config =
            read_config("file", &bad_config).expect("expected success");
        {
            /* See above. */
            let log = config.log.to_logger().expect("expected success");
            warn!(log, "message2");
        }

        let time_after = chrono::offset::Utc::now();
        let log_records = read_bunyan_log(&logpath);
        let expected_hostname = hostname::get().unwrap().into_string().unwrap();
        verify_bunyan_records(log_records.iter(), &BunyanLogRecordSpec {
            name: Some("oxide-api".to_string()),
            hostname: Some(expected_hostname.clone()),
            v: Some(0),
            pid: Some(std::process::id()),
        });
        verify_bunyan_records_sequential(
            log_records.iter(),
            Some(&time_before),
            Some(&time_after),
        );

        assert_eq!(log_records.len(), 3);
        assert_eq!(log_records[0].msg, "message1_warn");
        assert_eq!(log_records[1].msg, "message1_error");
        assert_eq!(log_records[2].msg, "message2");

        /*
         * Try again with if_exists = "truncate".  This should also work, but
         * remove the contents that's already there.
         */
        let time_before = time_after;
        let time_after = chrono::offset::Utc::now();
        let bad_config = format!(
            "{}{}\"{}\"\n",
            CONFIG_VALID_BIND_ADDRESS,
            r##"
            [log]
            mode = "file"
            level = "trace"
            if_exists = "truncate"
            path = "##,
            &logpath
        );

        let config = read_config("file", &bad_config).unwrap();
        {
            /* See above. */
            let log = config.log.to_logger().unwrap();
            debug!(log, "message3_debug");
            warn!(log, "message3_warn");
            error!(log, "message3_error");
        }

        let log_records = read_bunyan_log(&logpath);
        verify_bunyan_records(log_records.iter(), &BunyanLogRecordSpec {
            name: Some("oxide-api".to_string()),
            hostname: Some(expected_hostname),
            v: Some(0),
            pid: Some(std::process::id()),
        });
        verify_bunyan_records_sequential(
            log_records.iter(),
            Some(&time_before),
            Some(&time_after),
        );
        assert_eq!(log_records.len(), 3);
        assert_eq!(log_records[0].msg, "message3_debug");
        assert_eq!(log_records[1].msg, "message3_warn");
        assert_eq!(log_records[2].msg, "message3_error");
    }
}
