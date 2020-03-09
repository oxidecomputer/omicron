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
pub use handler::HttpRouteHandler;
pub use handler::Json;
pub use handler::Query;
pub use handler::RequestContext;
pub use http_util::http_extract_path_params;
pub use router::HttpRouter;
pub use server::HttpServer;
pub use server::ServerConfig;
