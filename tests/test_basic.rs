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
use oxide_api_prototype::api_model::ApiProjectCreateParams;
use oxide_api_prototype::api_model::ApiProjectUpdateParams;
use oxide_api_prototype::api_model::ApiProjectView;
use oxide_api_prototype::api_server;
use serde::de::DeserializeOwned;
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
    let mut server = api_server::setup_server(&bind_address).expect(
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
 * Given a Hyper Response whose body is expected to represent newline-separated
 * JSON, each of which is expected to be parseable via Serde as type T,
 * asynchronously read the body of the response and parse it accordingly,
 * returning a vector of T.
 */
async fn read_ndjson<T: DeserializeOwned>(response: &mut Response<Body>)
    -> Vec<T>
{
    let body_bytes = hyper::body::to_bytes(response.body_mut()).await
        .expect("error reading body");
    let body_string = String::from_utf8(body_bytes.as_ref().into())
        .expect("response contained non-UTF-8 bytes");

    // TODO-cleanup: should probably implement NDJSON-based Serde type?
    // TODO-correctness: even if not, this should split on (\r?\n)+ to be
    // NDJSON-compatible.
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
 * TODO: many of these really could be broken out (e.g., all the error cases
 * that are totally independent of everything else) if we allowed the test
 * server to bind to an arbitrary port.
 */
#[tokio::test]
async fn smoke_test()
{
    let testctx = test_setup();

    /*
     * Error case: GET /nonexistent (a path with no route at all)
     */
    let response = testctx
        .client
        .get(testctx.url("/nonexistent"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NOT_FOUND, response.status());

    /*
     * Error case: GET /projects/nonexistent (a possible value that does not
     * exist inside a collection that does exist)
     */
    let response = testctx
        .client
        .get(testctx.url("/projects/nonexistent"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NOT_FOUND, response.status());

    /*
     * Error case: GET /projects/simproject1/nonexistent (a path that does not
     * exist beneath a resource that does exist)
     */
    let response = testctx
        .client
        .get(testctx.url("/projects/simproject1/nonexistent"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NOT_FOUND, response.status());

    /*
     * Error case: PUT /projects
     */
    let response = testctx.client.request(
        Request::builder()
            .method(Method::PUT)
            .uri(testctx.url("/projects"))
            .body(Body::empty())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::METHOD_NOT_ALLOWED, response.status());
    // TODO-coverage: verify error body

    /*
     * Error case: DELETE /projects
     */
    let response = testctx.client.request(
        Request::builder()
            .method(Method::DELETE)
            .uri(testctx.url("/projects"))
            .body(Body::empty())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::METHOD_NOT_ALLOWED, response.status());

    /*
     * Basic test of out-of-the-box GET /projects
     * TODO-coverage: pagination
     */
    let mut response = testctx
        .client
        .get(testctx.url("/projects"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());
    let headers = response.headers();
    assert_eq!("application/x-ndjson", // XXX-cleanup should be a constant
        headers.get(http::header::CONTENT_TYPE).expect("missing content-type"));
    // TODO-cleanup: have common code that logs request, makes request and
    // sanity-checks all the headers (see below two TODOs)?  Maybe reads the
    // body too.
    // TODO could even consider making this whole test-case table-driven.
    // TODO-coverage validate that no other headers are present.
    // TODO-coverage check Date header

    let projects: Vec<ApiProjectView> = read_ndjson(&mut response).await;
    assert_eq!(projects.len(), 3);
    assert_eq!(projects[0].id, "simproject1");
    assert_eq!(projects[0].name, "simproject1");
    assert!(projects[0].description.len() > 0);
    assert_eq!(projects[1].id, "simproject2");
    assert_eq!(projects[1].name, "simproject2");
    assert!(projects[1].description.len() > 0);
    assert_eq!(projects[2].id, "simproject3");
    assert_eq!(projects[2].name, "simproject3");
    assert!(projects[2].description.len() > 0);

    /*
     * Basic test of out-of-the-box GET /projects/simproject2
     */
    let mut response = testctx
        .client
        .get(testctx.url("/projects/simproject2"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());
    let headers = response.headers();
    assert_eq!("application/json", // XXX-cleanup should be a constant
        headers.get(http::header::CONTENT_TYPE).expect("missing content-type"));
    // TODO-coverage validate that no other headers are present.
    // TODO-coverage check Date header

    let project: ApiProjectView = read_json(&mut response).await;
    let expected = &projects[1];
    assert_eq!(project.id, "simproject2");
    assert_eq!(project.id, expected.id);
    assert_eq!(project.name, expected.name);
    assert_eq!(project.description, expected.description);
    assert!(project.description.len() > 0);

    /*
     * Delete "simproject2".  We'll make sure that's reflected in the other
     * requests.
     */
    let mut response = testctx.client.request(
        Request::builder()
            .method(Method::DELETE)
            .uri(testctx.url("/projects/simproject2"))
            .body(Body::empty())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NO_CONTENT, response.status());
    assert_eq!(hyper::body::to_bytes(response.body_mut()).await.expect(
        "error reading body").len(), 0);

    /*
     * Having deleted "simproject2", verify "GET", "PUT", and "DELETE" on
     * "/projects/simproject2".
     */
    let response = testctx
        .client
        .get(testctx.url("/projects/simproject2"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NOT_FOUND, response.status());
    // TODO-coverage: verify that the error response body looks right

    let fake_update = ApiProjectUpdateParams {
        name: None,
        description: None,
    };
    let response = testctx.client.request(
        Request::builder()
            .method(Method::PUT)
            .uri(testctx.url("/projects/simproject2"))
            .body(serde_json::to_string(&fake_update).unwrap().into())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NOT_FOUND, response.status());
    // TODO-coverage: verify that the error response body looks right

    let response = testctx.client.request(
        Request::builder()
            .method(Method::DELETE)
            .uri(testctx.url("/projects/simproject2"))
            .body(Body::empty())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::NOT_FOUND, response.status());
    // TODO-coverage: verify that the error response body looks right

    /*
     * Similarly, verify "GET /projects"
     */
    let mut response = testctx
        .client
        .get(testctx.url("/projects"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());

    let expected_projects: Vec<&ApiProjectView> = projects
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
    let mut response = testctx.client.request(
        Request::builder()
            .method(Method::PUT)
            .uri(testctx.url("/projects/simproject3"))
            .body(serde_json::to_string(&project_update).unwrap().into())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, "simproject3");
    assert_eq!(project.name, "simproject3");
    assert_eq!(project.description, "Li'l lightnin'");

    let mut response = testctx
        .client
        .get(testctx.url("/projects/simproject3"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());

    let expected = project;
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, expected.id);
    assert_eq!(project.name, expected.name);
    assert_eq!(project.description, expected.description);
    assert_eq!(project.description, "Li'l lightnin'");

    /*
     * Update "simproject3" in a way that changes its name.  This is a deeper
     * operation under the hood.  This case also exercises changes to multiple
     * fields.
     */
    let project_update = ApiProjectUpdateParams {
        name: Some("lil_lightnin".to_string()),
        description: Some("little lightning".to_string()),
    };
    let mut response = testctx.client.request(
        Request::builder()
            .method(Method::PUT)
            .uri(testctx.url("/projects/simproject3"))
            .body(serde_json::to_string(&project_update).unwrap().into())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.id, "simproject3");
    assert_eq!(project.name, "lil_lightnin");
    assert_eq!(project.description, "little lightning");

    /*
     * Try to create a new project with a name that overlaps with an existing
     * one.
     */
    let project_create = ApiProjectCreateParams {
        name: "simproject1".to_string(),
        description: "a duplicate of simproject1".to_string()
    };
    let response = testctx.client.request(
        Request::builder()
            .method(Method::POST)
            .uri(testctx.url("/projects"))
            .body(serde_json::to_string(&project_create).unwrap().into())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::BAD_REQUEST, response.status());
    // TODO-coverage verify error body

    /*
     * Now, really do create a new project.
     */
    let project_create = ApiProjectCreateParams {
        name: "honor roller".to_string(),
        description: "a soapbox racer".to_string(),
    };
    let mut response = testctx.client.request(
        Request::builder()
            .method(Method::POST)
            .uri(testctx.url("/projects"))
            .body(serde_json::to_string(&project_create).unwrap().into())
            .unwrap())
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::CREATED, response.status());
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
    let mut response = testctx
        .client
        .get(testctx.url("/projects"))
        .await
        .expect("failed to make request to server");
    eprintln!("response info: {:?}", response);
    assert_eq!(StatusCode::OK, response.status());

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
