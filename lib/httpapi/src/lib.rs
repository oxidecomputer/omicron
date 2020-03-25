/*!
 * Root of the HTTP submodule within the Oxide API crate.  This is kept separate
 * for possible future extraction into its own crate for use in other
 * HTTP-serving Rust programs.
 */

mod error;
mod handler;
mod http_util;
mod router;
mod server;

pub mod test_util;

#[macro_use]
extern crate slog;

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
pub use http_util::http_extract_path_params;
pub use http_util::CONTENT_TYPE_JSON;
pub use http_util::CONTENT_TYPE_NDJSON;
pub use http_util::HEADER_REQUEST_ID;
pub use router::HttpRouter;
pub use router::RouterLookupResult;
pub use server::HttpServer;
pub use server::ServerConfig;
