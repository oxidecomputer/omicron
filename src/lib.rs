/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 *
 * TODO-cleanup is there a better way to do this?
 */

pub mod api_error;
mod api_http_entrypoints;
pub mod api_model;
pub mod httpapi;
mod sim;

use httpapi::RequestContext;
pub use httpapi::HEADER_REQUEST_ID;
use serde::Deserialize;
use std::any::Any;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[macro_use]
extern crate slog;
use slog::Drain;
use slog::Logger;
use slog::Level;

/**
 * Represents configuration for the whole API server.
 */
#[derive(Deserialize)]
pub struct ApiServerConfig {
    pub bind_address: SocketAddr,
    pub log: ApiServerConfigLogging,
}

impl ApiServerConfig {
    /**
     * Load an `ApiServerConfig` from the given TOML file.  The format is
     * described in the README.  This config object can then be used to create a
     * new `ApiServer`.
     */
    pub fn from_file(path: &Path) -> Result<ApiServerConfig, String> {
        let config_contents =
            std::fs::read_to_string(path).map_err(|error| {
                format!("read \"{}\": {}", path.display(), error)
            })?;
        let config_parsed: ApiServerConfig = toml::from_str(&config_contents)
            .map_err(|error| {
            format!("parse \"{}\": {}", path.display(), error)
        })?;
        Ok(config_parsed)
    }
}

/**
 * Consumer handle for the API server.
 */
pub struct ApiServer {
    pub http_server: httpapi::HttpServer,
    pub log: Logger,
}

/**
 * Represents an error that can happen while initializing the server.  If
 * useful, this could become an enum with specific failure modes, but for now
 * the caller is just going to print the message and bail anyway.
 */
#[derive(Debug)]
pub struct ApiServerCreateError(String);

impl From<hyper::error::Error> for ApiServerCreateError {
    fn from(error: hyper::error::Error) -> ApiServerCreateError {
        ApiServerCreateError(format!("{}", error))
    }
}

impl std::fmt::Display for ApiServerCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "error creating API server: {}", self.0)
    }
}

impl ApiServer {
    pub fn new(
        config: &ApiServerConfig,
    ) -> Result<ApiServer, ApiServerCreateError> {
        let mut simbuilder = sim::SimulatorBuilder::new();
        simbuilder.project_create("simproject1");
        simbuilder.project_create("simproject2");
        simbuilder.project_create("simproject3");

        let api_state = Arc::new(ApiRequestContext {
            backend: Arc::new(simbuilder.build()),
        });

        let mut router = httpapi::HttpRouter::new();

        let log = create_logger(&config)?;
        api_http_entrypoints::api_register_entrypoints(&mut router);
        let http_server = httpapi::HttpServer::new(
            &config.bind_address,
            router,
            Box::new(api_state),
            log.new(slog::o!()),
        )?;

        Ok(ApiServer {
            http_server,
            log,
        })
    }
}

/**
 * API-specific state that we'll associate with the server and make available to
 * API request handler functions.  See `api_backend()`.
 */
pub struct ApiRequestContext {
    pub backend: Arc<dyn api_model::ApiBackend>,
}

/**
 * This function gets our implementation-specific backend out of the
 * generic RequestContext structure.  We make use of 'dyn Any' here and
 * downcast.  It should not be possible for this downcast to fail unless the
 * caller has passed us a RequestContext from a totally different HttpServer
 * created with a different type for its private data, which we do not expect.
 * TODO-cleanup: can we make this API statically type-safe?
 */
pub fn api_backend(
    rqctx: &Arc<RequestContext>,
) -> Arc<dyn api_model::ApiBackend> {
    let maybectx: &(dyn Any + Send + Sync) = rqctx.server.private.as_ref();
    let apictx = maybectx
        .downcast_ref::<Arc<ApiRequestContext>>()
        .expect("api_backend(): wrong type for private data");
    return Arc::clone(&apictx.backend);
}

/**
 * Represents the logging configuration for a server.
 */
#[derive(Deserialize)]
#[serde(tag = "mode")]
pub enum ApiServerConfigLogging {
    #[serde(rename = "stdout-terminal")]
    StdoutTerminal { level: ApiServerConfigLoggingLevel },
    #[serde(rename = "file")]
    File {
        level: ApiServerConfigLoggingLevel,
        path: String,
        if_exists: ApiServerConfigLoggingIfExists,
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
    TestSuite { level: ApiServerConfigLoggingLevel, directory: String },
}

#[derive(Deserialize)]
pub enum ApiServerConfigLoggingIfExists {
    #[serde(rename = "fail")]
    Fail,
    #[serde(rename = "truncate")]
    Truncate,
    #[serde(rename = "append")]
    Append,
}

#[derive(Deserialize)]
pub enum ApiServerConfigLoggingLevel {
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

impl From<&ApiServerConfigLoggingLevel> for Level {
    fn from(config_level: &ApiServerConfigLoggingLevel) -> Level {
        match config_level {
            ApiServerConfigLoggingLevel::Trace => Level::Trace,
            ApiServerConfigLoggingLevel::Debug => Level::Debug,
            ApiServerConfigLoggingLevel::Info => Level::Info,
            ApiServerConfigLoggingLevel::Warn => Level::Warning,
            ApiServerConfigLoggingLevel::Error => Level::Error,
            ApiServerConfigLoggingLevel::Critical => Level::Critical,
        }
    }
}

static TEST_SUITE_LOGGER_ID: AtomicU32 = AtomicU32::new(0);

/**
 * Create the root logger based on the requested configuration.
 */
fn create_logger(
    config: &ApiServerConfig,
) -> Result<Logger, ApiServerCreateError> {
    /*
     * TODO-hardening
     * We use an async drain for the terminal logger to take care of
     * synchronization.  That's mainly because the other two options use a
     * std::sync::Mutex, which is not futures-aware and is likely to foul up our
     * executor.  However, we have not verified that the async implementation
     * behaves reasonably under backpressure.
     * TODO-cleanup can we commonize the common lines below?
     */
    let pid = std::process::id();
    match &config.log {
        ApiServerConfigLogging::StdoutTerminal {
            level,
        } => {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let level_drain = slog::LevelFilter(drain, Level::from(level)).fuse();
            let async_drain = slog_async::Async::new(level_drain).build().fuse();
            Ok(slog::Logger::root(async_drain, o!("pid" => pid)))
        }

        ApiServerConfigLogging::File {
            level,
            path,
            if_exists,
        } => {
            let mut open_options = std::fs::OpenOptions::new();
            open_options.write(true);
            open_options.create(true);

            match if_exists {
                ApiServerConfigLoggingIfExists::Fail => {
                    open_options.create_new(true);
                }
                ApiServerConfigLoggingIfExists::Append => {
                    open_options.append(true);
                }
                ApiServerConfigLoggingIfExists::Truncate => {
                    open_options.truncate(true);
                }
            }

            let drain = log_drain_for_file(&open_options, Path::new(path))?;
            let level_drain = slog::LevelFilter(drain, Level::from(level)).fuse();
            let async_drain = slog_async::Async::new(level_drain).build().fuse();
            Ok(slog::Logger::root(async_drain, o!("pid" => pid)))
        }

        ApiServerConfigLogging::TestSuite {
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

            let drain = log_drain_for_file(&open_options, pathbuf.as_path())?;
            let level_drain = slog::LevelFilter(drain, Level::from(level)).fuse();
            let async_drain = slog_async::Async::new(level_drain).build().fuse();
            Ok(slog::Logger::root(async_drain, o!("pid" => pid)))
        }
    }
}

fn log_drain_for_file(
    open_options: &OpenOptions,
    path: &Path,
) -> Result<slog::Fuse<slog_json::Json<std::fs::File>>, ApiServerCreateError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            let p = path.display();
            let message = format!("open log file \"{}\": {}", p, e);
            ApiServerCreateError(message)
        })?;
    }

    let file = open_options.open(path).map_err(|e| {
        let p = path.display();
        let message = format!("open log file \"{}\": {}", p, e);
        ApiServerCreateError(message)
    })?;

    eprintln!("note: configured to log to \"{}\"", path.display());
    Ok(slog_bunyan::with_name("oxide-api", file).build().fuse())
}
