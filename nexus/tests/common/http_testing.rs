//! Facilities for testing HTTP servers

use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
use dropshot::test_util::ClientTestContext;
use std::convert::TryInto;
use std::fmt::Debug;

/// Convenient way to make an outgoing HTTP request and verify various
/// properties of the response for testing
//
// When testing an HTTP server, we make varying requests to the server and
// verify a bunch of properties about it's behavior.  A lot of things can go
// wrong along the way:
//
// - failed to serialize request body
// - failed to construct a valid request
// - failed to transmit the request or receive a well-formed response
// - server did something funny, like gave back unexpected or invalid headers
// - server returned a response different from what we expected, either in
//   status code or the format of the response body
//
// We want to make it easy for consumers to verify all these things by default
// while also making it possible to skip certain checks (e.g., don't check the
// status code, maybe because the caller may expect any number of several status
// codes).
//
// The design here enables consumers to set up the request (including
// expectations about the response, like the status code), execute it, get back
// an error if anything went wrong, and get back the expected body (parsed)
// otherwise.  The caller can specify:
//
// - HTTP request method (required)
// - HTTP request uri (required)
// - zero or more HTTP request headers
// - an HTTP request body (optional)
// - an expected status code (optional)
// - an expected response body type (optional)
//
// On top of all this, we want failures to be easy to debug.  For now, that
// means we always log the outgoing request and incoming response.  With a bit
// more work, we could also format the final error to include as much of the
// request and response information as we have, including the caller's
// expectations.
//
pub struct RequestBuilder<'a> {
    testctx: &'a dropshot::test_util::ClientTestContext,

    method: http::Method,
    uri: http::Uri,
    headers: http::HeaderMap<http::header::HeaderValue>,
    body: hyper::Body,
    error: Option<anyhow::Error>,

    expected_status: Option<http::StatusCode>,
    allowed_headers: Option<Vec<http::header::HeaderName>>,
}

impl<'a> RequestBuilder<'a> {
    /// Start building a request with the given `method` and `uri`
    pub fn new(
        testctx: &'a dropshot::test_util::ClientTestContext,
        method: http::Method,
        uri: &str,
    ) -> Self {
        let uri = testctx.url(uri);
        RequestBuilder {
            testctx,
            method,
            uri,
            headers: http::HeaderMap::new(),
            body: hyper::Body::empty(),
            expected_status: None,
            allowed_headers: Some(vec![
                http::header::CONTENT_LENGTH,
                http::header::CONTENT_TYPE,
                http::header::DATE,
                http::header::SET_COOKIE,
                http::header::LOCATION,
                http::header::HeaderName::from_static("x-request-id"),
            ]),
            error: None,
        }
    }

    /// Set header `name` to `value` in the outgoing request
    pub fn header<K, V, KE, VE>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<http::header::HeaderName, Error = KE> + Debug,
        V: TryInto<http::header::HeaderValue, Error = VE> + Debug,
        KE: std::error::Error + Send + Sync + 'static,
        VE: std::error::Error + Send + Sync + 'static,
    {
        let header_name_dbg = format!("{:?}", name);
        let header_value_dbg = format!("{:?}", value);
        let header_name: Result<http::header::HeaderName, _> =
            name.try_into().with_context(|| {
                format!("converting header name {}", header_name_dbg)
            });
        let header_value: Result<http::header::HeaderValue, _> =
            value.try_into().with_context(|| {
                format!(
                    "converting value for header {}: {}",
                    header_name_dbg, header_value_dbg
                )
            });
        match (header_name, header_value) {
            (Ok(name), Ok(value)) => {
                self.headers.append(name, value);
            }
            (Err(error), _) => {
                self.error = Some(error);
            }
            (_, Err(error)) => {
                self.error = Some(error);
            }
        };
        self
    }

    /// Set the outgoing request body to the result of serializing `body`
    ///
    /// If `body` is `None`, the request body will be empty.
    pub fn body<RequestBodyType: serde::Serialize>(
        mut self,
        body: Option<RequestBodyType>,
    ) -> Self {
        let new_body = body.map(|b| {
            serde_json::to_string(&b)
                .context("failed to serialize request body")
        });
        match new_body {
            Some(Err(error)) => self.error = Some(error),
            Some(Ok(new_body)) => self.body = hyper::Body::from(new_body),
            None => self.body = hyper::Body::empty(),
        };
        self
    }

    /// Record that we expect to get status code `expected_status` in the
    /// response
    ///
    /// If `expected_status` is not `None`, then [`execute()`] will check this
    /// and raise an error if a different status code is found.
    pub fn expect_status(
        mut self,
        expected_status: Option<http::StatusCode>,
    ) -> Self {
        self.expected_status = expected_status;
        self
    }

    /// Record a list of header names allowed in the response
    ///
    /// If this function is used, then [`execute()`] will check each header in
    /// the response against this list and raise an error if a header name is
    /// found that's not in this list.
    pub fn expect_allowed_headers<
        I: IntoIterator<Item = http::header::HeaderName>,
    >(
        mut self,
        allowed_headers: I,
    ) -> Self {
        self.allowed_headers = Some(allowed_headers.into_iter().collect());
        self
    }

    /// Make the HTTP request using the given `client`, verify the returned
    /// response, and make the response available to the caller
    ///
    /// This function checks the returned status code (if [`expect_status()`]
    /// was used), allowed headers (if [`allowed_headers()`] was used), and
    /// various other properties of the response.
    pub async fn execute(self) -> Result<TestResponse, anyhow::Error> {
        if let Some(error) = self.error {
            return Err(error);
        }

        let mut builder =
            http::Request::builder().method(self.method).uri(self.uri);
        for (header_name, header_value) in &self.headers {
            builder = builder.header(header_name, header_value);
        }
        let request =
            builder.body(self.body).context("failed to construct request")?;

        let time_before = chrono::offset::Utc::now().timestamp();
        slog::info!(self.testctx.client_log,
            "client request";
            "method" => %request.method(),
            "uri" => %request.uri(),
            "body" => ?&request.body(),
        );

        let mut response = self
            .testctx
            .client
            .request(request)
            .await
            .context("making request to server")?;

        // Check that we got the expected response code.
        let status = response.status();
        slog::info!(self.testctx.client_log,
            "client received response";
            "status" => ?status
        );

        if let Some(expected_status) = self.expected_status {
            ensure!(
                expected_status == status,
                "expected status code {}, found {}",
                expected_status,
                status
            );
        }

        // Check that we didn't have any unexpected headers.
        let headers = response.headers();
        if let Some(allowed_headers) = self.allowed_headers {
            for header_name in headers.keys() {
                ensure!(
                    allowed_headers.contains(header_name),
                    "response contained unexpected header {:?}",
                    header_name
                );
            }
        }

        // Sanity check the Date header in the response.  This check assumes
        // that the server is running on the same system as the client, which is
        // currently true because this is running in a test suite.  We may need
        // to relax this constraint if we want to use this code elsewhere.
        //
        // Even on a single system, this check will fail spuriously in the
        // unlikely event that the system clock is adjusted backwards in between
        // when we sent the request and when we received the response.  We
        // consider that case unlikely enough to be worth doing this check
        // anyway.  (We'll try to check for the clock reset condition, too, but
        // we cannot catch all cases that would cause the Date header check to
        // be incorrect.)
        //
        // Note that the Date header typically only has precision down to one
        // second, so we don't want to try to do a more precise comparison.
        let time_after = chrono::offset::Utc::now().timestamp();
        ensure!(
            time_before <= time_after,
            "time went backwards during the request"
        );
        let date_header = headers
            .get(http::header::DATE)
            .ok_or_else(|| anyhow!("missing Date header in response"))?
            .to_str()
            .context("converting Date header to string")?;
        let time_request = chrono::DateTime::parse_from_rfc2822(date_header)
            .context("parsing server's Date header")?;
        ensure!(
            time_request.timestamp() >= time_before - 1,
            "Date header was too early"
        );
        ensure!(
            time_request.timestamp() <= time_after + 1,
            "Date header was too late"
        );

        // Validate that we have a request id header.
        // TODO-coverage check that it's unique among requests we've issued
        let request_id_header = headers
            .get(dropshot::HEADER_REQUEST_ID)
            .ok_or_else(|| anyhow!("missing request id header"))?
            .to_str()
            .context("parsing request-id header as string")?
            .to_string();

        // Read the response.  Note that this is somewhat dangerous -- a broken
        // or malicious server could do damage by sending us an enormous
        // response here.  Since we only use this in a test suite, we ignore
        // that risk.
        let response_body = hyper::body::to_bytes(response.body_mut())
            .await
            .context("reading response body")?;

        // For "204 No Content" responses, validate that we got no content in
        // the body.
        if status == http::StatusCode::NO_CONTENT {
            ensure!(
                response_body.len() == 0,
                "expected empty response for 204 status code"
            )
        }

        // For errors of any kind, attempt to parse the body and make sure the
        // request id matches what we found in the header.
        let test_response = TestResponse {
            status,
            headers: response.headers().clone(),
            body: response_body,
        };
        if status.is_client_error() || status.is_server_error() {
            let error_body = test_response
                .parsed_body::<dropshot::HttpErrorResponseBody>()
                .context("parsing error body")?;
            ensure!(
                error_body.request_id == request_id_header,
                "expected error response body to have request id {:?} \
                (to match request-id header), bout found {:?}",
                request_id_header,
                error_body.request_id
            );
        }

        Ok(test_response)
    }
}

/// Represents a response from an HTTP server
pub struct TestResponse {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: bytes::Bytes,
}

impl TestResponse {
    /// Parse the response body as an instance of `R` and returns it
    ///
    /// Fails if the body could not be parsed as an `R`.
    pub fn parsed_body<R: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<R, anyhow::Error> {
        serde_json::from_slice(self.body.as_ref())
            .context("parsing response body")
    }
}

/// Specifies what user (if any) the caller wants to use for authenticating to
/// the server
pub enum AuthnMode {
    UnprivilegedUser,
    PrivilegedUser,
}

/// Helper for constructing requests to Nexus's external API
///
/// This is a thin wrapper around [`RequestBuilder`] that exists to allow
/// callers to specify authentication details (and, in the future, any other
/// application-level considerations).
pub struct NexusRequest<'a> {
    request_builder: RequestBuilder<'a>,
}

impl<'a> NexusRequest<'a> {
    /// Create a `NexusRequest` around `request_builder`
    ///
    /// Most callers should use [`objects_post`], [`object_get`],
    /// [`object_delete`], or the other wrapper constructors.  If you use this
    /// function directly, you should set up your `request_builder` first with
    /// whatever HTTP-level stuff you want (including any non-authentication
    /// headers), then call this function to get a `NexusRequest`, then use
    /// [`authn_as()`] to configure authentication.
    pub fn new(request_builder: RequestBuilder<'a>) -> Self {
        NexusRequest { request_builder }
    }

    /// Causes the request to authenticate to Nexus as a user specified by
    /// `mode`
    pub fn authn_as(mut self, mode: AuthnMode) -> Self {
        use omicron_nexus::authn;
        let header_value = match mode {
            AuthnMode::UnprivilegedUser => authn::TEST_USER_UUID_UNPRIVILEGED,
            AuthnMode::PrivilegedUser => authn::TEST_USER_UUID_PRIVILEGED,
        };

        self.request_builder = self.request_builder.header(
            http::header::HeaderName::from_static(
                authn::external::spoof::HTTP_HEADER_OXIDE_AUTHN_SPOOF,
            ),
            http::header::HeaderValue::from_static(header_value),
        );
        self
    }

    /// See [`RequestBuilder::execute()`].
    pub async fn execute(self) -> Result<TestResponse, anyhow::Error> {
        self.request_builder.execute().await
    }

    /// Returns a new `NexusRequest` suitable for `POST $uri` with the given
    /// `body`
    pub fn objects_post<BodyType: serde::Serialize>(
        testctx: &'a ClientTestContext,
        uri: &str,
        body: BodyType,
    ) -> Self {
        NexusRequest::new(
            RequestBuilder::new(testctx, http::Method::POST, uri)
                .body(Some(body))
                .expect_status(Some(http::StatusCode::CREATED)),
        )
    }

    /// Returns a new `NexusRequest` suitable for `GET $uri`
    pub fn object_get(testctx: &'a ClientTestContext, uri: &str) -> Self {
        NexusRequest::new(
            RequestBuilder::new(testctx, http::Method::GET, uri)
                .expect_status(Some(http::StatusCode::OK)),
        )
    }

    /// Returns a new `NexusRequest` suitable for `DELETE $uri`
    pub fn object_delete(testctx: &'a ClientTestContext, uri: &str) -> Self {
        NexusRequest::new(
            RequestBuilder::new(testctx, http::Method::GET, uri)
                .expect_status(Some(http::StatusCode::NO_CONTENT)),
        )
    }
}

/// Functions for compatibility with corresponding `dropshot::test_util`
/// functions.
pub mod dropshot_compat {
    use super::NexusRequest;
    use dropshot::{test_util::ClientTestContext, ResultsPage};
    use serde::{de::DeserializeOwned, Serialize};

    /// See [`dropshot::test_util::object_get`].
    pub async fn object_get<T>(
        testctx: &ClientTestContext,
        object_url: &str,
    ) -> T
    where
        T: DeserializeOwned,
    {
        NexusRequest::object_get(testctx, object_url)
            .execute()
            .await
            .unwrap()
            .parsed_body::<T>()
            .unwrap()
    }

    /// See [`dropshot::test_util::objects_list_page`].
    pub async fn objects_list_page<T>(
        testctx: &ClientTestContext,
        list_url: &str,
    ) -> dropshot::ResultsPage<T>
    where
        T: DeserializeOwned,
    {
        NexusRequest::object_get(testctx, list_url)
            .execute()
            .await
            .unwrap()
            .parsed_body::<ResultsPage<T>>()
            .unwrap()
    }

    /// See [`dropshot::test_util::objects_post`].
    pub async fn objects_post<S, T>(
        testctx: &ClientTestContext,
        collection_url: &str,
        input: S,
    ) -> T
    where
        S: Serialize + std::fmt::Debug,
        T: DeserializeOwned,
    {
        NexusRequest::objects_post(testctx, collection_url, input)
            .execute()
            .await
            .unwrap()
            .parsed_body::<T>()
            .unwrap()
    }
}
