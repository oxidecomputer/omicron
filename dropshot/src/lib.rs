/*!
 * Dropshot is a general-purpose crate for exposing REST APIs from a Rust
 * program.  Planned highlights include:
 *
 * * Suitability for production use on a largely untrusted network.
 *   Dropshot-based systems should be high-performing, reliable, debuggable, and
 *   secure against basic denial of service attacks (intentional or otherwise).
 *
 * * First-class OpenAPI support, in the form of precise OpenAPI specs generated
 *   directly from code.  This works because the functions that serve HTTP
 *   resources consume arguments and return values of specific types from which
 *   a schema can be statically generated.
 *
 * * Ease of integrating into a diverse team.  An important use case for
 *   Dropshot consumers is to have a team of engineers where individuals might
 *   add a few endpoints at a time to a complex server, and it should be
 *   relatively easy to do this.  Part of this means an emphasis on the
 *   principle of least surprise: like Rust itself, we may choose abstractions
 *   that require more time to learn up front in order to make it harder to
 *   accidentally build systems that will not perform, will crash in corner
 *   cases, etc.
 *
 * For a discussion of alternative crates considered, see Oxide RFD 10.
 *
 * We hope Dropshot will be fairly general-purpose, but it's primarily intended
 * to address the needs of the Oxide control plane.
 *
 *
 * ## Status
 *
 * Dropshot is a work in progress.  It remains inside the parent repo because
 * we're still making changes that span both repos reasonably often.  Once it's
 * mature, we'll separate it into a separate repo and potentially publish it.
 *
 *
 * ## Usage
 *
 * The bare minimum might look like this:
 *
 * ```no_run
 * use dropshot::ApiDescription;
 * use dropshot::ConfigDropshot;
 * use dropshot::ConfigLogging;
 * use dropshot::ConfigLoggingLevel;
 * use dropshot::HttpServer;
 * use std::sync::Arc;
 *
 * #[tokio::main]
 * async fn main() -> Result<(), String> {
 *     // Set up a logger.
 *     let log =
 *         ConfigLogging::StderrTerminal {
 *             level: ConfigLoggingLevel::Info,
 *         }
 *         .to_logger("minimal-example")?;
 *
 *     // Describe the API.
 *     let mut api = ApiDescription::new();
 *     // Register API functions -- see detailed example or ApiDescription docs.
 *
 *     // Start the server.
 *     let mut server =
 *         HttpServer::new(
 *             &ConfigDropshot {
 *                 bind_address: "127.0.0.1:0".parse().unwrap(),
 *             },
 *             api,
 *             Arc::new(()),
 *             &log,
 *         )
 *         .map_err(|error| format!("failed to start server: {}", error))?;
 *
 *     let server_task = server.run();
 *     server.wait_for_shutdown(server_task).await
 * }
 * ```
 *
 * This server returns a 404 for all resources because no API functions were
 * registered.  See `examples/basic.rs` for a simple, documented example that
 * provides a few resources using shared state.
 *
 * For a given `ApiDescription`, you can also print out an OpenAPI spec
 * describing the API.  See [`ApiDescription::print_openapi`].
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
pub use api_description::ApiEndpoint;
pub use api_description::ApiEndpointParameter;
pub use api_description::ApiEndpointParameterLocation;
pub use config::ConfigDropshot;
pub use error::HttpError;
pub use error::HttpErrorResponseBody;
pub use handler::ExtractedParameter;
pub use handler::Extractor;
pub use handler::HttpResponseAccepted;
pub use handler::HttpResponseCreated;
pub use handler::HttpResponseDeleted;
pub use handler::HttpResponseOkObject;
pub use handler::HttpResponseOkObjectList;
pub use handler::HttpResponseUpdatedNoContent;
pub use handler::Json;
pub use handler::Path;
pub use handler::Query;
pub use handler::RequestContext;
pub use http_util::CONTENT_TYPE_JSON;
pub use http_util::CONTENT_TYPE_NDJSON;
pub use http_util::HEADER_REQUEST_ID;
pub use logging::ConfigLogging;
pub use logging::ConfigLoggingIfExists;
pub use logging::ConfigLoggingLevel;
pub use server::HttpServer;

extern crate dropshot_endpoint;
pub use dropshot_endpoint::endpoint;
pub use dropshot_endpoint::ExtractedParameter;
