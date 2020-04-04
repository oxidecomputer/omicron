/*!
 * Root of the HTTP submodule within the Oxide API crate.  This is kept separate
 * for possible future extraction into its own crate for use in other
 * HTTP-serving Rust programs.
 */

mod api_description;
mod config;
mod error;
mod handler;
mod http_util;
mod logging;
mod router;
mod server;

pub mod test_util;

#[macro_use]
extern crate slog;

pub use api_description::ApiDescription;
pub use config::ConfigDropshot;
pub use error::HttpError;
pub use error::HttpErrorResponseBody;
pub use handler::Derived;
pub use handler::HttpResponseCreated;
pub use handler::HttpResponseDeleted;
pub use handler::HttpResponseOkObject;
pub use handler::HttpResponseOkObjectList;
pub use handler::HttpRouteHandler;
pub use handler::Json;
pub use handler::Query;
pub use handler::RequestContext;
pub use handler::RouteHandler;
pub use http_util::http_extract_path_param;
pub use http_util::http_extract_path_params;
pub use http_util::CONTENT_TYPE_JSON;
pub use http_util::CONTENT_TYPE_NDJSON;
pub use http_util::HEADER_REQUEST_ID;
pub use logging::ConfigLogging;
pub use logging::ConfigLoggingIfExists;
pub use logging::ConfigLoggingLevel;
pub use server::HttpServer;

/*
 * TODO-cleanup There's not a good reason to expose HttpRouter or
 * RouterLookupResult.  Right now, they're needed because there's detailed
 * documentation for HttpRouter with an example that uses them.  We should
 * either figure out how to let that doc example access private stuff, translate
 * it into something that makes sense to be exposed (if that's possible), or
 * remove it.
 */
pub use router::HttpRouter;
pub use router::RouterLookupResult;
