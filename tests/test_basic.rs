/*!
 * Smoke tests against the API server.
 *
 * This file defines a very basic set of tests against the API.
 */

use http::method::Method;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use hyper::client::HttpConnector;
use oxide_api_prototype::api_error::ApiHttpErrorResponseBody;
use oxide_api_prototype::api_model::ApiProjectCreateParams;
use oxide_api_prototype::api_model::ApiProjectUpdateParams;
use oxide_api_prototype::api_model::ApiProjectView;
use oxide_api_prototype::api_server;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

/*
 * Common test facilities
 */

/**
 * TestContext encapsulates several pieces needed for these basic tests.
 * Essentially, any common setup code (and components, like an HTTP server and
 * client) ought to be included in this struct.
 */
struct TestContext {
    /** actual bind address of the HTTP server under test */
    bind_address: SocketAddr,
    /** handle to the HTTP server under test */
    api_server: api_server::ApiHttpServer,
    /** handle to the task that's running the HTTP server */
    api_server_task: JoinHandle<Result<(), hyper::error::Error>>,
    /** HTTP client, used for making requests against the test server */
    client: hyper::Client<HttpConnector>,
}

impl TestContext {
    /**
     * Given the path for an API endpoint (e.g., "/projects"), return a Uri that
     * we can use to invoke this endpoint from the client.  This essentially
     * appends the path to a base URL constructed from the server's IP address
     * and port.
     */
    fn url(&self, path: &str)
        -> hyper::Uri
    {
        hyper::Uri::builder()
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
fn test_setup()
    -> TestContext
{
    let bind_address: SocketAddr = "127.0.0.1:1478".parse().unwrap();
    let mut server = api_server::api_server_create(&bind_address).expect(
        "failed to set up server");
    let task = server.run();

    TestContext {
        bind_address: bind_address,
        api_server: server,
        api_server_task: task,
        client: hyper::Client::new()
    }
}

/**
 * Tear down facilities that were set up during the test.  Currently, this shuts
 * down the test HTTP server and waits for it to exit gracefully.
 */
async fn test_teardown(testctx: TestContext)
{
    testctx.api_server.close();
    let join_result = testctx.api_server_task.await.expect(
        "failed to join on test server");
    join_result.expect("server closed with an error");
}

/**
 * List of allowed HTTP headers in responses.  This is used to make sure we
 * don't leak headers unexpectedly.
 */
const ALLOWED_HEADER_NAMES: [&str; 3] = [
    "content-length",
    "content-type",
    "date",
];

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
async fn make_request<RequestBodyType: Serialize + Debug>(
    testctx: &TestContext,
    method: Method,
    path: &str,
    request_body: Option<RequestBodyType>,
    expected_status: StatusCode)
    -> Result<Response<Body>, ApiHttpErrorResponseBody>
{
    let uri = testctx.url(path);

    let time_before = chrono::offset::Utc::now().timestamp();
    eprintln!("client request: {} {}\nbody:\n{:?}", method, uri, &request_body);

    let body: Body = match request_body {
        None => Body::empty(),
        Some(input) => serde_json::to_string(&input).unwrap().into()
    };

    let mut response = testctx.client.request(
        Request::builder()
            .method(method)
            .uri(testctx.url(path))
            .body(body)
            .expect("attempted to construct invalid request"))
        .await
        .expect("failed to make request to server");

    /* Check that we got the expected response code. */
    let status = response.status();
    eprintln!("client received response: status {}", status);
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
        .get(http::header::DATE).expect("missing Date header")
        .to_str().expect("non-ASCII characters in Date header");
    let time_request = chrono::DateTime::parse_from_rfc2822(date_header)
        .expect("unable to parse server's Date header");
    assert!(time_before <= time_after,
        "time obviously went backwards during the test");
    assert!(time_request.timestamp() >= time_before - 1);
    assert!(time_request.timestamp() <= time_after + 1);

    /*
     * For "204 No Content" responses, validate that we got no content in the
     * body.
     */
    if status == StatusCode::NO_CONTENT {
        let body_bytes = hyper::body::to_bytes(response.body_mut()).await
            .expect("error reading body");
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
    let error_body: ApiHttpErrorResponseBody = read_json(&mut response).await;
    eprintln!("client error: {:?}", error_body);
    Err(error_body)
}


/**
 * Given a Hyper Response whose body is expected to represent newline-separated
 * JSON, each of which is expected to be parseable via Serde as type T,
 * asynchronously read the body of the response and parse it accordingly,
 * returning a vector of T.
 */
async fn read_ndjson<T: DeserializeOwned>(response: &mut Response<Body>)
    -> Vec<T>
{
    let headers = response.headers();
    assert_eq!(oxide_api_prototype::api_http_util::CONTENT_TYPE_NDJSON,
        headers.get(http::header::CONTENT_TYPE).expect("missing content-type"));
    let body_bytes = hyper::body::to_bytes(response.body_mut()).await
        .expect("error reading body");
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
        .map(|line| serde_json::from_str(line).expect(
            "failed to parse server body as expected type"))
        .collect::<Vec<T>>()
}

/**
 * Given a Hyper response whose body is expected to be a JSON object that should
 * be parseable via Serde as type T, asynchronously read the body of the
 * response and parse it, returning an instance of T.
 */
async fn read_json<T: DeserializeOwned>(response: &mut Response<Body>)
    -> T
{
    let headers = response.headers();
    assert_eq!(oxide_api_prototype::api_http_util::CONTENT_TYPE_JSON,
        headers.get(http::header::CONTENT_TYPE).expect("missing content-type"));
    let body_bytes = hyper::body::to_bytes(response.body_mut()).await
        .expect("error reading body");
    serde_json::from_slice(body_bytes.as_ref()).expect(
        "failed to parse server body as expected type")
}

/*
 * Tests
 */

/*
 * Most of our tests wind up in this one test function primarily because they
 * depend on each other and Rust is allowed to parallelize execution of separate
 * tests.  An example where tests depend on each other is that one test creates
 * a project, one test deletes a project, and another lists projects.  The
 * result of the "list projects" test depends on which of these other tests have
 * run.  We could have it deal with all possible valid cases, but that's not
 * very scalable.  We could demand that users run with RUST_TEST_THREADS=1, but
 * in the future we may have lots of tests that _can_ run in parallel, and it
 * would be nice to do so.  This way, the code reflects the real dependency.
 * (It would be ideal if Rust had a way to say that the tests within one file
 * must not be parallelized or something like that, but that doesn't seem to
 * exist.)
 *
 * TODO-perf: many of these really could be broken out (e.g., all the error
 * cases that are totally independent of everything else) if we allowed the test
 * server to bind to an arbitrary port.
 */
#[tokio::test]
async fn smoke_test()
{
    let testctx = test_setup();

    /*
     * Error case: GET /nonexistent (a path with no route at all)
     */
    let error = make_request(&testctx, Method::GET, "/nonexistent",
        None as Option<()>, StatusCode::NOT_FOUND)
        .await
        .expect_err("expected error");
    assert_eq!("Not Found", error.message);

    /*
     * Error case: GET /projects/nonexistent (a possible value that does not
     * exist inside a collection that does exist)
     */
    let error = make_request(&testctx, Method::GET, "/projects/nonexistent",
        None as Option<()>, StatusCode::NOT_FOUND)
        .await
        .expect_err("expected error");
    assert_eq!("not found: project \"nonexistent\"", error.message);

    /*
     * Error case: GET /projects/simproject1/nonexistent (a path that does not
     * exist beneath a resource that does exist)
     */
    let error = make_request(&testctx, Method::GET,
        "/projects/simproject1/nonexistent", None as Option<()>,
        StatusCode::NOT_FOUND)
        .await
        .expect_err("expected error");
    assert_eq!("Not Found", error.message);

    /*
     * Error case: PUT /projects
     */
    let error = make_request(&testctx, Method::PUT, "/projects",
        None as Option<()>, StatusCode::METHOD_NOT_ALLOWED)
        .await
        .expect_err("expected error");
    assert_eq!("Method Not Allowed", error.message);

    /*
     * Error case: DELETE /projects
     */
    let error = make_request(&testctx, Method::DELETE, "/projects",
        None as Option<()>, StatusCode::METHOD_NOT_ALLOWED)
        .await
        .expect_err("expected error");
    assert_eq!("Method Not Allowed", error.message);

    /*
     * Basic test of out-of-the-box GET /projects
     * TODO-coverage: pagination
     */
    let mut response = make_request(&testctx, Method::GET, "/projects",
        None as Option<()>, StatusCode::OK)
        .await
        .expect("expected success");
    let initial_projects: Vec<ApiProjectView> =
        read_ndjson(&mut response).await;
    assert_eq!(initial_projects.len(), 3);
    assert_eq!(initial_projects[0].id, "simproject1");
    assert_eq!(initial_projects[0].name, "simproject1");
    assert!(initial_projects[0].description.len() > 0);
    assert_eq!(initial_projects[1].id, "simproject2");
    assert_eq!(initial_projects[1].name, "simproject2");
    assert!(initial_projects[1].description.len() > 0);
    assert_eq!(initial_projects[2].id, "simproject3");
    assert_eq!(initial_projects[2].name, "simproject3");
    assert!(initial_projects[2].description.len() > 0);

    /*
     * Basic test of out-of-the-box GET /projects/simproject2
     */
    let mut response = make_request(&testctx, Method::GET,
        "/projects/simproject2", None as Option<()>, StatusCode::OK)
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    let expected = &initial_projects[1];
    assert_eq!(project.id, "simproject2");
    assert_eq!(project.id, expected.id);
    assert_eq!(project.name, expected.name);
    assert_eq!(project.description, expected.description);
    assert!(project.description.len() > 0);

    /*
     * Delete "simproject2".  We'll make sure that's reflected in the other
     * requests.
     */
    make_request(&testctx, Method::DELETE, "/projects/simproject2",
        None as Option<()>, StatusCode::NO_CONTENT)
        .await
        .expect("expected success");

    /*
     * Having deleted "simproject2", verify "GET", "PUT", and "DELETE" on
     * "/projects/simproject2".
     */
    make_request(&testctx, Method::GET, "/projects/simproject2",
        None as Option<()>, StatusCode::NOT_FOUND)
        .await
        .expect_err("expected failure");
    make_request(&testctx, Method::DELETE, "/projects/simproject2",
        None as Option<()>, StatusCode::NOT_FOUND)
        .await
        .expect_err("expected failure");
    make_request(&testctx, Method::PUT, "/projects/simproject2",
        Some(ApiProjectUpdateParams {
            name: None,
            description: None,
        }), StatusCode::NOT_FOUND)
        .await
        .expect_err("expected failure");

    /*
     * Similarly, verify "GET /projects"
     */
    let mut response = make_request(&testctx, Method::GET, "/projects",
        None as Option<()>, StatusCode::OK)
        .await
        .expect("expected success");
    let expected_projects: Vec<&ApiProjectView> = initial_projects
        .iter()
        .filter(|p| p.name != "simproject2")
        .collect();
    let new_projects: Vec<ApiProjectView> = read_ndjson(&mut response).await;
    assert_eq!(new_projects.len(), expected_projects.len());
    assert_eq!(new_projects[0].id, expected_projects[0].id);
    assert_eq!(new_projects[0].name, expected_projects[0].name);
    assert_eq!(new_projects[0].description, expected_projects[0].description);
    assert_eq!(new_projects[1].id, expected_projects[1].id);
    assert_eq!(new_projects[1].name, expected_projects[1].name);
    assert_eq!(new_projects[1].description, expected_projects[1].description);

    /*
     * Update "simproject3".  We'll make sure that's reflected in the other
     * requests.
     */
    let project_update = ApiProjectUpdateParams {
        name: None,
        description: Some("Li'l lightnin'".to_string())
    };
    let mut response = make_request(&testctx, Method::PUT,
        "/projects/simproject3", Some(project_update), StatusCode::OK)
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, "simproject3");
    assert_eq!(project.name, "simproject3");
    assert_eq!(project.description, "Li'l lightnin'");

    let mut response = make_request(&testctx, Method::GET,
        "/projects/simproject3", None as Option<()>,
        StatusCode::OK)
        .await
        .expect("expected success");
    let expected = project;
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, expected.id);
    assert_eq!(project.name, expected.name);
    assert_eq!(project.description, expected.description);
    assert_eq!(project.description, "Li'l lightnin'");

    /*
     * Update "simproject3" in a way that changes its name.  This is a deeper
     * operation under the hood.  This case also exercises changes to multiple
     * fields in one request.
     */
    let project_update = ApiProjectUpdateParams {
        name: Some("lil_lightnin".to_string()),
        description: Some("little lightning".to_string()),
    };
    let mut response = make_request(&testctx, Method::PUT,
        "/projects/simproject3", Some(project_update), StatusCode::OK)
        .await
        .expect("failed to make request to server");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, "simproject3");
    assert_eq!(project.name, "lil_lightnin");
    assert_eq!(project.description, "little lightning");

    make_request(&testctx, Method::GET,
        "/projects/simproject3", None as Option<()>, StatusCode::NOT_FOUND)
        .await
        .expect_err("expected failure");

    /*
     * Try to create a project with a name that conflicts with an existing one.
     */
    let project_create = ApiProjectCreateParams {
        name: "simproject1".to_string(),
        description: "a duplicate of simproject1".to_string()
    };
    let error = make_request(&testctx, Method::POST, "/projects",
        Some(project_create), StatusCode::BAD_REQUEST)
        .await
        .expect_err("expected failure");
    assert_eq!("already exists: project \"simproject1\"", error.message);

    /*
     * Now, really do create a new project.
     */
    let project_create = ApiProjectCreateParams {
        name: "honor roller".to_string(),
        description: "a soapbox racer".to_string(),
    };
    let mut response = make_request(&testctx, Method::POST, "/projects",
        Some(project_create), StatusCode::CREATED)
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, "honor roller");
    assert_eq!(project.name, "honor roller");
    assert_eq!(project.description, "a soapbox racer");

    /*
     * List projects again and verify all of our changes.  We should have:
     *
     * - "honor roller" with description "a soapbox racer"
     * - "lil_lightnin" with description "little lightning"
     * - "simproject1", same as out-of-the-box
     */
    let mut response = make_request(&testctx, Method::GET, "/projects",
        None as Option<()>, StatusCode::OK)
        .await
        .expect("expected success");
    let projects: Vec<ApiProjectView> = read_ndjson(&mut response).await;
    assert_eq!(projects.len(), 3);
    assert_eq!(projects[0].id, "honor roller");
    assert_eq!(projects[0].name, "honor roller");
    assert_eq!(projects[0].description, "a soapbox racer");
    assert_eq!(projects[1].id, "simproject3");
    assert_eq!(projects[1].name, "lil_lightnin");
    assert_eq!(projects[1].description, "little lightning");
    assert_eq!(projects[2].id, "simproject1");
    assert_eq!(projects[2].name, "simproject1");
    assert!(projects[2].description.len() > 0);

    test_teardown(testctx).await;
}
