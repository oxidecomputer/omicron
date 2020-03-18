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
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

#[macro_use]
extern crate slog;
use slog::Drain;
use slog::Logger;

/**
 * Represents configuration for the whole API server.
 */
#[derive(Deserialize)]
pub struct ApiServerConfig {
    pub bind_address: SocketAddr,
    pub log: ApiServerConfigLogging,
}

pub fn api_load_config_from_file(
    path: &Path,
) -> Result<ApiServerConfig, String> {
    let config_contents = std::fs::read_to_string(path)
        .map_err(|error| format!("read \"{}\": {}", path.display(), error))?;
    let config_parsed: ApiServerConfig = toml::from_str(&config_contents)
        .map_err(|error| format!("parse \"{}\": {}", path.display(), error))?;
    Ok(config_parsed)
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
    StdoutTerminal,
    #[serde(rename = "file")]
    File { path: String, if_exists: ApiServerConfigLoggingIfExists },
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
    match &config.log {
        ApiServerConfigLogging::StdoutTerminal => {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let async_drain = slog_async::Async::new(drain).build().fuse();
            Ok(slog::Logger::root(async_drain, o!()))
        }
        ApiServerConfigLogging::File {
            path,
            if_exists,
        } => {
            let mut open_options = std::fs::OpenOptions::new();
            open_options.write(true);

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

            let file = match open_options.open(path) {
                Ok(file) => file,
                Err(e) => {
                    let message = format!("open log file \"{}\": {}", path, e);
                    return Err(ApiServerCreateError(message));
                }
            };
            let drain = slog_bunyan::with_name("oxide-api", file).build().fuse();
            let async_drain = slog_async::Async::new(drain).build().fuse();
            eprintln!("note: configured to log to \"{}\"", path);
            Ok(slog::Logger::root(async_drain, o!()))
        }
    }
}
