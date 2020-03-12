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

pub use error::HttpError;
pub use error::HttpErrorResponseBody;
pub use handler::Derived;
pub use handler::HttpRouteHandler;
pub use handler::Json;
pub use handler::Query;
pub use handler::RequestContext;
pub use handler::RouteHandler;
pub use handler::HttpResponseWrap;
pub use handler::HttpResponse;
pub use handler::HttpResponseCreated;
pub use http_util::http_extract_path_params;
pub use http_util::CONTENT_TYPE_JSON;
pub use http_util::CONTENT_TYPE_NDJSON;
pub use router::HttpRouter;
pub use router::RouterLookupResult;
pub use server::test_endpoints;
pub use server::HttpServer;
pub use server::ServerConfig;
