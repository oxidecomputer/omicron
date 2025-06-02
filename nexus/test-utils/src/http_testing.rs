// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for testing HTTP servers

use anyhow::Context;
use anyhow::anyhow;
use anyhow::ensure;
use camino::Utf8Path;
use dropshot::ResultsPage;
use dropshot::test_util::ClientTestContext;
use futures::TryStreamExt;
use headers::authorization::Credentials;
use http_body_util::BodyExt;
use nexus_db_queries::authn::external::spoof;
use nexus_db_queries::db::identity::Asset;
use serde_urlencoded;
use std::convert::TryInto;
use std::fmt::Debug;

/// Convenient way to make an outgoing HTTP request and verify various
/// properties of the response for testing
// When testing an HTTP server, we make varying requests to the server and
// verify a bunch of properties about its behavior.  A lot of things can go
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
    body: dropshot::Body,
    error: Option<anyhow::Error>,
    allow_non_dropshot_errors: bool,

    expected_status: Option<http::StatusCode>,
    allowed_headers: Option<Vec<http::header::HeaderName>>,
    // if an entry's value is `None`, we verify the header exists in the
    // response, but we don't check the value
    expected_response_headers:
        http::HeaderMap<Option<http::header::HeaderValue>>,
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
            body: dropshot::Body::empty(),
            expected_status: None,
            allowed_headers: Some(vec![
                http::header::CONNECTION,
                http::header::CONTENT_ENCODING,
                http::header::CONTENT_LENGTH,
                http::header::CONTENT_TYPE,
                http::header::DATE,
                http::header::LOCATION,
                http::header::SET_COOKIE,
                http::header::HeaderName::from_static("x-request-id"),
            ]),
            expected_response_headers: http::HeaderMap::default(),
            error: None,
            allow_non_dropshot_errors: false,
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
        match parse_header_pair(name, value) {
            Err(error) => {
                self.error = Some(error);
            }
            Ok((name, value)) => {
                self.headers.append(name, value);
            }
        }
        self
    }

    /// Set the outgoing request body
    ///
    /// If `body` is `None`, the request body will be empty.
    pub fn raw_body(mut self, body: Option<String>) -> Self {
        match body {
            Some(body) => self.body = dropshot::Body::from(body),
            None => self.body = dropshot::Body::empty(),
        };
        self
    }

    /// Set the outgoing request body to the result of serializing `body`
    ///
    /// If `body` is `None`, the request body will be empty.
    pub fn body<RequestBodyType: serde::Serialize>(
        mut self,
        body: Option<&RequestBodyType>,
    ) -> Self {
        let new_body = body.map(|b| {
            serde_json::to_string(b).context("failed to serialize request body")
        });
        match new_body {
            Some(Err(error)) => self.error = Some(error),
            Some(Ok(new_body)) => self.body = dropshot::Body::from(new_body),
            None => self.body = dropshot::Body::empty(),
        };
        self
    }

    /// Set the outgoing request body to the contents of a file.
    ///
    /// A handle to the file will be kept open until the request is completed.
    ///
    /// If `path` is `None`, the request body will be empty.
    pub fn body_file(mut self, path: Option<&Utf8Path>) -> Self {
        match path {
            Some(path) => {
                // Turn the file into a stream. (Opening the file with
                // std::fs::File::open means that this method doesn't have to
                // be async.)
                let file = std::fs::File::open(path).with_context(|| {
                    format!("failed to open request body file at {path}")
                });
                match file {
                    Ok(file) => {
                        let stream = tokio_util::io::ReaderStream::new(
                            tokio::fs::File::from_std(file),
                        );
                        let body = http_body_util::StreamBody::new(
                            stream.map_ok(|b| hyper::body::Frame::data(b)),
                        );
                        self.body = dropshot::Body::wrap(body);
                    }
                    Err(error) => self.error = Some(error),
                }
            }
            None => self.body = dropshot::Body::empty(),
        };
        self
    }

    /// Set the outgoing request body using URL encoding
    /// and set the content type appropriately
    ///
    /// If `body` is `None`, the request body will be empty.
    pub fn body_urlencoded<RequestBodyType: serde::Serialize>(
        mut self,
        body: Option<&RequestBodyType>,
    ) -> Self {
        let new_body = body.map(|b| {
            serde_urlencoded::to_string(b)
                .context("failed to URL-encode request body")
        });
        match new_body {
            Some(Err(error)) => self.error = Some(error),
            Some(Ok(new_body)) => self.body = dropshot::Body::from(new_body),
            None => self.body = dropshot::Body::empty(),
        };
        self.header(
            http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
    }

    /// Record that we expect to get status code `expected_status` in the
    /// response
    ///
    /// If `expected_status` is not `None`, then [`Self::execute()`] will check this
    /// and raise an error if a different status code is found.
    pub fn expect_status(
        mut self,
        expected_status: Option<http::StatusCode>,
    ) -> Self {
        self.expected_status = expected_status;
        self
    }

    /// Add header and value to check for at execution time
    ///
    /// Behaves like header() in that it takes one header at a time rather than
    /// a whole set.
    pub fn expect_response_header<K, V, KE, VE>(
        mut self,
        name: K,
        value: V,
    ) -> Self
    where
        K: TryInto<http::header::HeaderName, Error = KE> + Debug,
        V: TryInto<http::header::HeaderValue, Error = VE> + Debug,
        KE: std::error::Error + Send + Sync + 'static,
        VE: std::error::Error + Send + Sync + 'static,
    {
        match parse_header_pair(name, value) {
            Err(error) => {
                self.error = Some(error);
            }
            Ok((name, value)) => {
                self.expected_response_headers.append(name, Some(value));
            }
        }
        self
    }

    /// Tells the requst to expect headers related to range requests
    pub fn expect_range_requestable(mut self) -> Self {
        self.allowed_headers.as_mut().unwrap().extend([
            http::header::CONTENT_LENGTH,
            http::header::CONTENT_RANGE,
            http::header::CONTENT_TYPE,
            http::header::ACCEPT_RANGES,
        ]);
        self.expect_response_header(
            http::header::CONTENT_TYPE,
            "application/zip",
        )
        .expect_response_header(http::header::ACCEPT_RANGES, "bytes")
    }

    /// Tells the request to initiate and expect a WebSocket upgrade handshake.
    /// This also sets the request method to GET.
    pub fn expect_websocket_handshake(mut self) -> Self {
        const TEST_WEBSOCKET_REQUEST_KEY: &str = "SEFDSyBUSEUgUExBTkVUIQ==";
        const TEST_WEBSOCKET_RESPONSE_KEY: &str =
            "YmFpRQ3F6A+bsseEHaonFBhSKhA=";
        self.allowed_headers.as_mut().unwrap().extend([
            http::header::CONNECTION,
            http::header::UPGRADE,
            http::header::SEC_WEBSOCKET_ACCEPT,
        ]);
        self.method = http::method::Method::GET;
        self.header(http::header::CONNECTION, "Upgrade")
            .header(http::header::UPGRADE, "websocket")
            .header(http::header::SEC_WEBSOCKET_VERSION, "13")
            .header(http::header::SEC_WEBSOCKET_KEY, TEST_WEBSOCKET_REQUEST_KEY)
            .expect_status(Some(http::StatusCode::SWITCHING_PROTOCOLS))
            .expect_response_header(http::header::CONNECTION, "Upgrade")
            .expect_response_header(http::header::UPGRADE, "websocket")
            .expect_response_header(
                http::header::SEC_WEBSOCKET_ACCEPT,
                TEST_WEBSOCKET_RESPONSE_KEY,
            )
    }

    /// Expect a successful console asset response.
    pub fn expect_console_asset(mut self) -> Self {
        let headers = [
            http::header::CACHE_CONTROL,
            http::header::CONTENT_SECURITY_POLICY,
            http::header::X_CONTENT_TYPE_OPTIONS,
            http::header::X_FRAME_OPTIONS,
        ];
        self.allowed_headers.as_mut().unwrap().extend(headers.clone());
        for header in headers {
            self.expected_response_headers.entry(header).or_insert(None);
        }
        self.expect_status(Some(http::StatusCode::OK))
    }

    /// Allow non-dropshot error responses, i.e., errors that are not compatible
    /// with `dropshot::HttpErrorResponseBody`.
    pub fn allow_non_dropshot_errors(mut self) -> Self {
        self.allow_non_dropshot_errors = true;
        self
    }

    /// Make the HTTP request using the given `client`, verify the returned
    /// response, and make the response available to the caller
    ///
    /// This function checks the returned status code (if [`Self::expect_status()`]
    /// was used), allowed headers (if [`Self::expect_websocket_handshake()`] or
    /// [`Self::expect_console_asset()`] was used), and various other properties
    /// of the response.
    pub async fn execute(self) -> Result<TestResponse, anyhow::Error> {
        if let Some(error) = self.error {
            return Err(error);
        }

        let mut builder =
            http::Request::builder().method(self.method.clone()).uri(self.uri);
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
                    allowed_headers.contains(header_name)
                        || (
                            // Dropshot adds `allow` headers to its 405 Method
                            // Not Allowed responses, per RFC 9110. If we expect
                            // a 405 we should also inherently expect `allow`.
                            self.expected_status
                                == Some(http::StatusCode::METHOD_NOT_ALLOWED)
                                && header_name == http::header::ALLOW
                        ),
                    "response contained unexpected header {:?}",
                    header_name
                );
            }
        }

        // Check that we do have all expected headers
        for (header_name, expected_value) in
            self.expected_response_headers.iter()
        {
            ensure!(
                headers.contains_key(header_name),
                "response did not contain expected header {:?}",
                header_name
            );
            if let Some(expected_value) = expected_value {
                let actual_value = headers.get(header_name).unwrap();
                ensure!(
                    actual_value == expected_value,
                    "response contained expected header {:?}, but with value \
                    {:?} instead of expected {:?}",
                    header_name,
                    actual_value,
                    expected_value,
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
        let response_body = response
            .body_mut()
            .collect()
            .await
            .context("reading response body")?
            .to_bytes();

        // For "204 No Content" responses, validate that we got no content in
        // the body.
        if status == http::StatusCode::NO_CONTENT {
            ensure!(
                response_body.is_empty(),
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
        if (status.is_client_error() || status.is_server_error())
            && !self.allow_non_dropshot_errors
            && self.method != http::Method::HEAD
        {
            let error_body = test_response
                .parsed_body::<dropshot::HttpErrorResponseBody>()
                .context("parsing error body")?;
            ensure!(
                error_body.request_id == request_id_header,
                "expected error response body to have request id {:?} \
                (to match request-id header), but found {:?}",
                request_id_header,
                error_body.request_id
            );
        }

        Ok(test_response)
    }
}

fn parse_header_pair<K, V, KE, VE>(
    name: K,
    value: V,
) -> Result<(http::header::HeaderName, http::header::HeaderValue), anyhow::Error>
where
    K: TryInto<http::header::HeaderName, Error = KE> + Debug,
    V: TryInto<http::header::HeaderValue, Error = VE> + Debug,
    KE: std::error::Error + Send + Sync + 'static,
    VE: std::error::Error + Send + Sync + 'static,
{
    let header_name_dbg = format!("{:?}", name);
    let header_value_dbg = format!("{:?}", value);

    let header_name = name.try_into().with_context(|| {
        format!("converting header name {}", header_name_dbg)
    })?;
    let header_value = value.try_into().with_context(|| {
        format!(
            "converting value for header {}: {}",
            header_name_dbg, header_value_dbg
        )
    })?;

    Ok((header_name, header_value))
}

/// Represents a response from an HTTP server
#[derive(Debug, Clone)]
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
#[derive(Clone, Debug)]
pub enum AuthnMode {
    UnprivilegedUser,
    PrivilegedUser,
    SiloUser(uuid::Uuid),
    Session(String),
}

impl AuthnMode {
    pub fn authn_header(
        &self,
    ) -> Result<
        (http::header::HeaderName, http::header::HeaderValue),
        anyhow::Error,
    > {
        use nexus_db_queries::authn;
        match self {
            AuthnMode::UnprivilegedUser => {
                let header_value = spoof::make_header_value(
                    authn::USER_TEST_UNPRIVILEGED.id(),
                );
                Ok((http::header::AUTHORIZATION, header_value.0.encode()))
            }
            AuthnMode::PrivilegedUser => {
                let header_value =
                    spoof::make_header_value(authn::USER_TEST_PRIVILEGED.id());
                Ok((http::header::AUTHORIZATION, header_value.0.encode()))
            }
            AuthnMode::SiloUser(silo_user_id) => {
                let header_value = spoof::make_header_value(*silo_user_id);
                Ok((http::header::AUTHORIZATION, header_value.0.encode()))
            }
            AuthnMode::Session(session_token) => {
                let header_value = format!("session={}", session_token);
                parse_header_pair(http::header::COOKIE, header_value)
            }
        }
    }
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
    /// Most callers should use [`Self::objects_post`], [`Self::object_get`],
    /// [`Self::object_delete`], or the other wrapper constructors.  If you use this
    /// function directly, you should set up your `request_builder` first with
    /// whatever HTTP-level stuff you want (including any non-authentication
    /// headers), then call this function to get a `NexusRequest`, then use
    /// [`Self::authn_as()`] to configure authentication.
    pub fn new(request_builder: RequestBuilder<'a>) -> Self {
        NexusRequest { request_builder }
    }

    /// Causes the request to authenticate to Nexus as a user specified by
    /// `mode`
    pub fn authn_as(mut self, mode: AuthnMode) -> Self {
        match mode.authn_header() {
            Ok((header_name, header_value)) => {
                self.request_builder =
                    self.request_builder.header(header_name, header_value);
            }
            Err(error) => {
                self.request_builder.error = Some(error);
            }
        }
        self
    }

    /// Tells the request to initiate and expect a WebSocket upgrade handshake.
    pub fn websocket_handshake(mut self) -> Self {
        self.request_builder =
            self.request_builder.expect_websocket_handshake();
        self
    }

    /// Tells the request builder to expect headers specific to console assets.
    pub fn console_asset(mut self) -> Self {
        self.request_builder = self.request_builder.expect_console_asset();
        self
    }

    /// See [`RequestBuilder::execute()`].
    pub async fn execute(self) -> Result<TestResponse, anyhow::Error> {
        self.request_builder.execute().await
    }

    /// Convenience function that executes the request, parses the body, and
    /// unwraps any `Result`s along the way.
    pub async fn execute_and_parse_unwrap<T: serde::de::DeserializeOwned>(
        self,
    ) -> T {
        self.execute().await.unwrap().parsed_body().unwrap()
    }

    /// Returns a new `NexusRequest` suitable for `POST $uri` with the given
    /// `body`
    pub fn objects_post<BodyType: serde::Serialize>(
        testctx: &'a ClientTestContext,
        uri: &str,
        body: &BodyType,
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
            RequestBuilder::new(testctx, http::Method::DELETE, uri)
                .expect_status(Some(http::StatusCode::NO_CONTENT)),
        )
    }

    /// Returns a new `NexusRequest` suitable for `PUT $uri`
    pub fn object_put<B>(
        testctx: &'a ClientTestContext,
        uri: &str,
        body: Option<&B>,
    ) -> Self
    where
        B: serde::Serialize,
    {
        NexusRequest::new(
            RequestBuilder::new(testctx, http::Method::PUT, uri)
                .body(body)
                .expect_status(Some(http::StatusCode::OK)),
        )
    }

    /// Convenience constructor for failure cases
    pub fn expect_failure(
        testctx: &'a ClientTestContext,
        expected_status: http::StatusCode,
        method: http::Method,
        uri: &str,
    ) -> Self {
        NexusRequest::new(
            RequestBuilder::new(testctx, method, uri)
                .expect_status(Some(expected_status)),
        )
    }

    pub fn expect_failure_with_body<B: serde::Serialize>(
        testctx: &'a ClientTestContext,
        expected_status: http::StatusCode,
        method: http::Method,
        uri: &str,
        body: &B,
    ) -> Self {
        NexusRequest::new(
            RequestBuilder::new(testctx, method, uri)
                .body(Some(body))
                .expect_status(Some(expected_status)),
        )
    }

    /// Iterates a collection (like `dropshot::test_util::iter_collection`)
    /// using authenticated requests.
    pub async fn iter_collection_authn<T>(
        testctx: &'a ClientTestContext,
        collection_url: &str,
        initial_params: &str,
        limit: Option<usize>,
    ) -> Result<Collection<T>, anyhow::Error>
    where
        T: Clone + serde::de::DeserializeOwned,
    {
        let mut npages = 0;
        let mut all_items = Vec::new();
        let mut next_token: Option<String> = None;
        const DEFAULT_PAGE_SIZE: usize = 10;
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
        let url_base = if collection_url.contains('?') {
            format!("{}&limit={}", collection_url, limit)
        } else {
            format!("{}?limit={}", collection_url, limit)
        };

        loop {
            let url = if let Some(next_token) = &next_token {
                format!("{}&page_token={}", url_base, next_token)
            } else if !initial_params.is_empty() {
                format!("{}&{}", url_base, initial_params)
            } else {
                url_base.clone()
            };

            let page = NexusRequest::object_get(testctx, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .with_context(|| format!("fetch page {}", npages + 1))?
                .parsed_body::<ResultsPage<T>>()
                .with_context(|| format!("parse page {}", npages + 1))?;
            ensure!(
                page.items.len() <= limit,
                "server sent more items than expected in page {} \
                (limit = {}, found = {})",
                npages + 1,
                limit,
                page.items.len()
            );
            all_items.extend_from_slice(&page.items);
            npages += 1;
            if let Some(token) = page.next_page {
                next_token = Some(token);
            } else {
                break;
            }
        }

        Ok(Collection { all_items, npages })
    }
}

/// Result of iterating an API collection (using multiple requests)
pub struct Collection<T> {
    /// all the items found in the collection
    pub all_items: Vec<T>,
    /// number of requests made to fetch all the items
    pub npages: usize,
}

/// Functions for compatibility with corresponding `dropshot::test_util`
/// functions.
pub mod dropshot_compat {
    use super::NexusRequest;
    use dropshot::{ResultsPage, test_util::ClientTestContext};
    use serde::{Serialize, de::DeserializeOwned};

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
        NexusRequest::objects_post(testctx, collection_url, &input)
            .execute()
            .await
            .unwrap()
            .parsed_body::<T>()
            .unwrap()
    }
}
