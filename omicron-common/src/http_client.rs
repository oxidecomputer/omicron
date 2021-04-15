/*!
 * HTTP client used for internal control plane interfaces
 */

use crate::error::ApiError;
use dropshot::HttpErrorResponseBody;
use http::Method;
use hyper::client::HttpConnector;
use hyper::Body;
use hyper::Client;
use hyper::Request;
use hyper::Response;
use hyper::Uri;
use serde::de::DeserializeOwned;
use slog::Logger;
use std::fmt::Display;
use std::net::SocketAddr;

/**
 * HTTP client used for internal control plane interfaces
 *
 * This is quite limited and intended as a temporary abstraction until we have
 * proper OpenAPI-generated clients for our own internal interfaces.
 */
pub struct HttpClient {
    /** label for this client, used for error messages */
    label: String,
    /** remote address of the endpoint to connect to */
    /*
     * TODO-robustness This will need to be replaced with a pool of connections
     * based on some service discovery mechanism (e.g., DNS).
     */
    server_addr: SocketAddr,
    /** debug log */
    log: Logger,
    /** hyper Client used to actually make requests */
    http_client: Client<HttpConnector>,
}

impl HttpClient {
    /** Create a new `HttpClient` connected to `server_addr`. */
    pub fn new<S: AsRef<str>>(
        label: S,
        server_addr: SocketAddr,
        log: Logger,
    ) -> HttpClient {
        let http_client = Client::new();
        HttpClient {
            label: String::from(label.as_ref()),
            server_addr,
            log,
            http_client,
        }
    }

    /**
     * Issue a request to the server having the given HTTP `method`, URI `path`,
     * and `body` contents
     *
     * A 200-level response will be returned as a successful
     * `Ok(Response<Body>)`.  Any other result (including failure to make the
     * request, a 400-level response, or a 500-level response) will result in an
     * `Err(ApiError)` describing the error.  When possible, if an error
     * contained in the response corresponds to an `ApiError` that we can
     * recognize (i.e., because the remote side is another control plane service
     * that also uses `ApiError` and it serialized the error with enough
     * information for us to recognize it), the server-side error will be
     * reconstituted as the returned error.
     */
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Body,
    ) -> Result<Response<Body>, ApiError> {
        let error_message_base = self.error_message_base(&method, path);

        debug!(self.log, "client request";
            "method" => %method,
            "uri" => %path,
            "body" => ?&body,
        );

        let uri = Uri::builder()
            .scheme("http")
            .authority(format!("{}", self.server_addr).as_str())
            .path_and_query(path)
            .build()
            .unwrap();
        let request =
            Request::builder().method(method).uri(uri).body(body).unwrap();
        let result = self.http_client.request(request).await.map_err(|error| {
            convert_error(&error_message_base, "making request", error)
        });

        debug!(self.log, "client response"; "result" => ?result);
        let mut response = result?;
        let status = response.status();

        if !status.is_client_error() && !status.is_server_error() {
            return Ok(response);
        }

        let error_body: HttpErrorResponseBody =
            self.read_json(&error_message_base, &mut response).await?;
        Err(ApiError::from_response(error_message_base, error_body))
    }

    /**
     * Returns an appropriate prefix for an error message associated with a
     * request using method `method` to URI path `path`
     */
    /*
     * TODO-cleanup This interface kind of sucks.  There's too much redundancy
     * in the caller.
     */
    pub fn error_message_base(&self, method: &Method, path: &str) -> String {
        format!(
            "client request to {} at {} ({} {})",
            self.label, self.server_addr, method, path
        )
    }

    /**
     * Reads the body of a response as a JSON object to be deserialized into
     * type `T`
     */
    /*
     * TODO-cleanup TODO-robustness commonize with dropshot read_json() and make
     * this more robust to operational errors
     */
    pub async fn read_json<T: DeserializeOwned>(
        &self,
        error_message_base: &str,
        response: &mut Response<Body>,
    ) -> Result<T, ApiError> {
        assert_eq!(
            dropshot::CONTENT_TYPE_JSON,
            response.headers().get(http::header::CONTENT_TYPE).unwrap()
        );
        let body_bytes = hyper::body::to_bytes(response.body_mut())
            .await
            .map_err(|error| {
                convert_error(error_message_base, "reading response", error)
            })?;
        serde_json::from_slice::<T>(body_bytes.as_ref()).map_err(|error| {
            convert_error(error_message_base, "parsing response", error)
        })
    }
}

/**
 * Produce a useful human-readable error message starting with
 * `error_message_base` for a request that failed with error `error` while
 * performing action `action`
 */
fn convert_error<E: Display>(
    error_message_base: &str,
    action: &str,
    error: E,
) -> ApiError {
    ApiError::ServiceUnavailable {
        message: format!("{}: {}: {}", error_message_base, action, error),
    }
}
