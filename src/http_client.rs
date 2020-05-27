/*!
 * General-purpose HTTP client, used for internal control plane interfaces.
 * This is quite limited and intended as a temporary abstraction until we have
 * proper OpenAPI-generated clients for our own internal interfaces.
 */

use crate::api_error::ApiError;
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

pub struct HttpClient {
    label: String,
    server_addr: SocketAddr,
    log: Logger,
    http_client: Client<HttpConnector>,
}

impl HttpClient {
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

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Body,
    ) -> Result<Response<Body>, ApiError> {
        let error_message_base = self.error_message_base(&method, path);

        info!(self.log, "client request";
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
            Request::builder().method(Method::PUT).uri(uri).body(body).unwrap();
        let result = self.http_client.request(request).await.map_err(|error| {
            convert_error(&error_message_base, "making request", error)
        });

        info!(self.log, "client response"; "result" => ?result);
        let mut response = result?;
        let status = response.status();

        if !status.is_client_error() && !status.is_server_error() {
            return Ok(response);
        }

        let error_body: HttpErrorResponseBody =
            self.read_json(&error_message_base, &mut response).await?;
        /* XXX Should this parse an error code out? */
        Err(ApiError::DependencyError {
            message: error_body.message,
        })
    }

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

fn convert_error<E: Display>(
    error_message_base: &str,
    action: &str,
    error: E,
) -> ApiError {
    ApiError::ResourceNotAvailable {
        message: format!("{}: {}: {}", error_message_base, action, error),
    }
}
