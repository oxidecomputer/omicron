/*!
 * Shared automated testing facilities
 */

use http::method::Method;
use hyper::body::to_bytes;
use hyper::client::HttpConnector;
use hyper::Body;
use hyper::Client;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use hyper::Uri;
use oxide_api_prototype::httpapi::HttpErrorResponseBody;
use oxide_api_prototype::ApiServer;
use oxide_api_prototype::ApiServerConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;
use slog::Logger;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::Path;
use tokio::task::JoinHandle;

/**
 * TestContext encapsulates several pieces needed for these basic tests.
 * Essentially, any common setup code (and components, like an HTTP server and
 * client) ought to be included in this struct.
 */
pub struct TestContext {
    /** actual bind address of the HTTP server under test */
    pub bind_address: SocketAddr,
    /** handle to the HTTP server under test */
    pub api_server: ApiServer,
    /** handle to the task that's running the HTTP server */
    pub api_server_task: JoinHandle<Result<(), hyper::error::Error>>,
    /** HTTP client, used for making requests against the test server */
    pub client: Client<HttpConnector>,
    /** logger for the test suite HTTP client */
    pub client_log: Logger,
}

impl TestContext {
    /**
     * Given the path for an API endpoint (e.g., "/projects"), return a Uri that
     * we can use to invoke this endpoint from the client.  This essentially
     * appends the path to a base URL constructed from the server's IP address
     * and port.
     */
    pub fn url(&self, path: &str) -> Uri {
        Uri::builder()
            .scheme("http")
            .authority(format!("{}", self.bind_address).as_str())
            .path_and_query(path)
            .build()
            .expect("attempted to construct invalid URI")
    }
}

/**
 * Set up a `TestContext` for running tests against the API server.  This binds
 * to a hardcoded IP address and port, starts the server, and instantiates a
 * client.  The results are encapsulated in the `TestContext` struct.
 */
pub fn test_setup() -> TestContext {
    /*
     * We load as much configuration as we can from the test suite configuration
     * file.  However, we override the TCP port for the server to 0, indicating
     * that we wish to bind to any available port.  This is necessary because
     * we'll run multiple servers concurrently, so there's no one port that we
     * could reasonably pick.
     */
    let config_file_path = Path::new("tests/config.test.toml");
    let mut config = ApiServerConfig::from_file(config_file_path)
        .expect("failed to load config.test.toml");
    config.bind_address.set_port(0);

    let mut server =
        ApiServer::new(&config, false).expect("failed to set up server");
    let task = server.http_server.run();

    let server_addr = server.http_server.local_addr();
    let client_log = server.log.new(o!("http_client" => "test suite"));

    TestContext {
        bind_address: server_addr,
        api_server: server,
        api_server_task: task,
        client: Client::new(),
        client_log: client_log,
    }
}

/**
 * Tear down facilities that were set up during the test.  Currently, this shuts
 * down the test HTTP server and waits for it to exit gracefully.
 */
pub async fn test_teardown(testctx: TestContext) {
    testctx.api_server.http_server.close();
    let join_result =
        testctx.api_server_task.await.expect("failed to join on test server");
    join_result.expect("server closed with an error");
}

/**
 * List of allowed HTTP headers in responses.  This is used to make sure we
 * don't leak headers unexpectedly.
 */
const ALLOWED_HEADER_NAMES: [&str; 4] =
    ["content-length", "content-type", "date", "x-request-id"];

/**
 * Execute an HTTP request against the test server and perform basic validation
 * of the result, including:
 *
 * - the expected status code
 * - the expected Date header (within reason)
 * - for error responses: the expected body content
 * - header names are in allowed list
 * - any other semantics that can be verified in general
 */
pub async fn make_request<RequestBodyType: Serialize + Debug>(
    testctx: &TestContext,
    method: Method,
    path: &str,
    request_body: Option<RequestBodyType>,
    expected_status: StatusCode,
) -> Result<Response<Body>, HttpErrorResponseBody> {
    let body: Body = match request_body {
        None => Body::empty(),
        Some(input) => serde_json::to_string(&input).unwrap().into(),
    };

    make_request_with_body(testctx, method, path, body, expected_status).await
}

pub async fn make_request_with_body(
    testctx: &TestContext,
    method: Method,
    path: &str,
    body: Body,
    expected_status: StatusCode,
) -> Result<Response<Body>, HttpErrorResponseBody> {
    let uri = testctx.url(path);

    let time_before = chrono::offset::Utc::now().timestamp();
    info!(testctx.client_log, "client request";
        "method" => %method,
        "uri" => %uri,
        "body" => ?&body,
    );

    let mut response = testctx
        .client
        .request(
            Request::builder()
                .method(method)
                .uri(testctx.url(path))
                .body(body)
                .expect("attempted to construct invalid request"),
        )
        .await
        .expect("failed to make request to server");

    /* Check that we got the expected response code. */
    let status = response.status();
    info!(testctx.client_log, "client received response"; "status" => ?status);
    assert_eq!(expected_status, status);

    /*
     * Check that we didn't have any unexpected headers.  This could be more
     * efficient by putting the allowed headers into a BTree or Hash, but right
     * now the structure is tiny and it's convenient to have it
     * statically-defined above.
     */
    let headers = response.headers();
    for header_name in headers.keys() {
        let mut okay = false;
        for allowed_name in ALLOWED_HEADER_NAMES.iter() {
            if header_name == allowed_name {
                okay = true;
                break;
            }
        }

        if !okay {
            panic!("header name not in allowed list: \"{}\"", header_name);
        }
    }

    /*
     * Sanity check the Date header in the response.  Note that this assertion
     * will fail spuriously in the unlikely event that the system clock is
     * adjusted backwards in between when we sent the request and when we
     * received the response, but we consider that case unlikely enough to be
     * worth doing this check anyway.  (We'll try to check for the clock reset
     * condition, too, but we cannot catch all cases that would cause the Date
     * header check to be incorrect.)
     *
     * Note that the Date header typically only has precision down to one
     * second, so we don't want to try to do a more precise comparison.
     */
    let time_after = chrono::offset::Utc::now().timestamp();
    let date_header = headers
        .get(http::header::DATE)
        .expect("missing Date header")
        .to_str()
        .expect("non-ASCII characters in Date header");
    let time_request = chrono::DateTime::parse_from_rfc2822(date_header)
        .expect("unable to parse server's Date header");
    assert!(
        time_before <= time_after,
        "time obviously went backwards during the test"
    );
    assert!(time_request.timestamp() >= time_before - 1);
    assert!(time_request.timestamp() <= time_after + 1);

    /*
     * Validate that we have a request id header.
     * TODO-coverage check that it's unique among requests we've issued
     */
    let request_id_header = headers
        .get(oxide_api_prototype::HEADER_REQUEST_ID)
        .expect("missing request id header")
        .to_str()
        .expect("non-ASCII characters in request id")
        .to_string();

    /*
     * For "204 No Content" responses, validate that we got no content in the
     * body.
     */
    if status == StatusCode::NO_CONTENT {
        let body_bytes =
            to_bytes(response.body_mut()).await.expect("error reading body");
        assert_eq!(0, body_bytes.len());
    }

    /*
     * If this was a successful response, there's nothing else to check here.
     * Return the response so the caller can validate the content if they want.
     */
    if !status.is_client_error() && !status.is_server_error() {
        return Ok(response);
    }

    /*
     * We got an error.  Parse the response body to make sure it's valid and
     * then return that.
     */
    let error_body: HttpErrorResponseBody = read_json(&mut response).await;
    info!(testctx.client_log, "client error"; "error_body" => ?error_body);
    assert_eq!(error_body.request_id, request_id_header);
    Err(error_body)
}

/**
 * Given a Hyper Response whose body is expected to represent newline-separated
 * JSON, each of which is expected to be parseable via Serde as type T,
 * asynchronously read the body of the response and parse it accordingly,
 * returning a vector of T.
 */
pub async fn read_ndjson<T: DeserializeOwned>(
    response: &mut Response<Body>,
) -> Vec<T> {
    let headers = response.headers();
    assert_eq!(
        oxide_api_prototype::httpapi::CONTENT_TYPE_NDJSON,
        headers.get(http::header::CONTENT_TYPE).expect("missing content-type")
    );
    let body_bytes =
        to_bytes(response.body_mut()).await.expect("error reading body");
    let body_string = String::from_utf8(body_bytes.as_ref().into())
        .expect("response contained non-UTF-8 bytes");

    /*
     * TODO-cleanup: should probably implement NDJSON-based Serde type?
     * TODO-correctness: even if not, this should split on (\r?\n)+ to be
     * NDJSON-compatible.
     */
    body_string
        .split("\n")
        .filter(|line| line.len() > 0)
        .map(|line| {
            serde_json::from_str(line)
                .expect("failed to parse server body as expected type")
        })
        .collect::<Vec<T>>()
}

/**
 * Given a Hyper response whose body is expected to be a JSON object that should
 * be parseable via Serde as type T, asynchronously read the body of the
 * response and parse it, returning an instance of T.
 */
pub async fn read_json<T: DeserializeOwned>(
    response: &mut Response<Body>,
) -> T {
    let headers = response.headers();
    assert_eq!(
        oxide_api_prototype::httpapi::CONTENT_TYPE_JSON,
        headers.get(http::header::CONTENT_TYPE).expect("missing content-type")
    );
    let body_bytes =
        to_bytes(response.body_mut()).await.expect("error reading body");
    serde_json::from_slice(body_bytes.as_ref())
        .expect("failed to parse server body as expected type")
}

/**
 * Given a Hyper Response whose body is expected to be a UTF-8-encoded string,
 * asynchronously read the body.
 */
pub async fn read_string(response: &mut Response<Body>) -> String {
    let body_bytes =
        to_bytes(response.body_mut()).await.expect("error reading body");
    String::from_utf8(body_bytes.as_ref().into())
        .expect("response contained non-UTF-8 bytes")
}
