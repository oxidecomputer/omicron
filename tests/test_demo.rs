/*!
 * Test cases for the "demo" handlers.  These handlers exercise various
 * supported configurations of the HTTP handler interface.  We exercise them
 * here to make sure that even if these aren't used at a given point, they still
 * work.
 *
 * Note that the purpose is mainly to exercise the various possible function
 * signatures that can be used to implement handler functions.  We don't need to
 * exercises very many cases (or error cases) of each one because the handlers
 * themselves are not important, but we need to exercise enough to validate that
 * the generic JSON and query parsing handles error cases.
 *
 * TODO-hardening: add test cases that exceed limits (e.g., query string length,
 * JSON body length)
 */

use http::StatusCode;
use hyper::Method;
use oxide_api_prototype::httpapi::test_endpoints::DemoJsonBody;
use oxide_api_prototype::httpapi::test_endpoints::DemoJsonAndQuery;

pub mod common;
use common::make_request;
use common::make_request_with_body;
use common::read_json;
use common::read_string;
use common::test_setup;
use common::test_teardown;

/*
 * The "demo1" handler consumes neither query nor JSON body parameters.  Here we
 * test that such handlers work.  There are no error cases for us to induce.
 */
#[tokio::test]
async fn test_demo1()
{
    let testctx = test_setup();
    let mut response = make_request(&testctx, Method::GET, "/testing/demo1",
        None as Option<()>, StatusCode::OK).await.expect("expected success");
    let body = read_string(&mut response).await;
    assert_eq!(body, "demo_handler_args_1\n");
    test_teardown(testctx).await;
}

/*
 * The "demo2query" handler consumes only query arguments.  Here we make sure
 * such handlers work and also exercise various error cases associated with bad
 * query string parsing.
 * TODO-hardening there are a lot more to check here, particularly around
 * encoded values.
 */
#[tokio::test]
async fn test_demo2query()
{
    let testctx = test_setup();

    /* Test case: optional field missing */
    let mut response = make_request(&testctx, Method::GET,
        "/testing/demo2query?test1=foo", None as Option<()>,
        StatusCode::OK).await.expect("expected success");
    let json: DemoJsonBody = read_json(&mut response).await;
    assert_eq!(json.test1, "foo");
    assert_eq!(json.test2, None);

    /* Test case: both fields specified */
    let mut response = make_request(&testctx, Method::GET,
        "/testing/demo2query?test1=foo&test2=10", None as Option<()>,
        StatusCode::OK).await.expect("expected success");
    let json: DemoJsonBody = read_json(&mut response).await;
    assert_eq!(json.test1, "foo");
    assert_eq!(json.test2, Some(10));

    /* Test case: required field missing */
    let error = make_request(&testctx, Method::GET,
        "/testing/demo2query", None as Option<()>, StatusCode::BAD_REQUEST)
        .await.expect_err("expected failure");
    assert_eq!(error.message,
        "unable to parse query string: missing field `test1`");

    /* Test case: typed field has bad value */
    let error = make_request(&testctx, Method::GET,
        "/testing/demo2query?test1=foo&test2=bar", None as Option<()>,
        StatusCode::BAD_REQUEST)
        .await.expect_err("expected failure");
    assert_eq!(error.message,
        "unable to parse query string: invalid digit found in string");

    /* Test case: duplicated field name */
    let error = make_request(&testctx, Method::GET,
        "/testing/demo2query?test1=foo&test1=bar", None as Option<()>,
        StatusCode::BAD_REQUEST).await.expect_err("expected failure");
    assert_eq!(error.message,
        "unable to parse query string: duplicate field `test1`");

    test_teardown(testctx).await;
}

/*
 * The "demo2json" handler consumes only a JSON object.  Here we make sure such
 * handlers work and also exercise various error cases associated with bad JSON
 * handling.
 */
#[tokio::test]
async fn test_demo2json()
{
    let testctx = test_setup();

    /* Test case: optional field */
    let input = DemoJsonBody {
        test1: "bar".to_string(),
        test2: None,
    };
    let mut response = make_request(&testctx, Method::GET,
        "/testing/demo2json", Some(input),
        StatusCode::OK).await.expect("expected success");
    let json: DemoJsonBody = read_json(&mut response).await;
    assert_eq!(json.test1, "bar");
    assert_eq!(json.test2, None);

    /* Test case: both fields populated */
    let input = DemoJsonBody {
        test1: "bar".to_string(),
        test2: Some(15),
    };
    let mut response = make_request(&testctx, Method::GET,
        "/testing/demo2json", Some(input),
        StatusCode::OK).await.expect("expected success");
    let json: DemoJsonBody = read_json(&mut response).await;
    assert_eq!(json.test1, "bar");
    assert_eq!(json.test2, Some(15));

    /* Test case: no input specified */
    let error = make_request(&testctx, Method::GET,
        "/testing/demo2json", None as Option<()>, StatusCode::BAD_REQUEST)
        .await.expect_err("expected failure");
    assert!(error.message.starts_with("unable to parse body JSON"));

    /* Test case: invalid JSON */
    let error = make_request_with_body(&testctx, Method::GET,
        "/testing/demo2json", "}".into(), StatusCode::BAD_REQUEST)
        .await.expect_err("expected failure");
    assert!(error.message.starts_with("unable to parse body JSON"));

    /* Test case: bad type */
    let json_bad_type = "{ \"test1\": \"oops\", \"test2\": \"oops\" }";
    let error = make_request_with_body(&testctx, Method::GET,
        "/testing/demo2json", json_bad_type.into(), StatusCode::BAD_REQUEST)
        .await.expect_err("expected failure");
    assert!(error.message.starts_with("unable to parse body JSON: \
        invalid type: string \"oops\", expected u32"));

    test_teardown(testctx).await;
}

/*
 * The "demo3" handler takes both query arguments and a JSON body.  This test
 * makes sure that both sets of parameters are received by the handler function
 * and at least one error case from each of those sources is exercised.  We
 * don't need exhaustively re-test the query and JSON error handling paths.
 */
#[tokio::test]
async fn test_demo3json()
{
    let testctx = test_setup();

    /* Test case: everything filled in. */
    let json_input = DemoJsonBody {
        test1: "bart".to_string(),
        test2: Some(0)
    };

    let mut response = make_request(&testctx, Method::GET,
        "/testing/demo3?test1=martin&test2=2", Some(json_input),
        StatusCode::OK).await.expect("expected success");
    let json: DemoJsonAndQuery = read_json(&mut response).await;
    assert_eq!(json.json.test1, "bart");
    assert_eq!(json.json.test2.unwrap(), 0);
    assert_eq!(json.query.test1, "martin");
    assert_eq!(json.query.test2.unwrap(), 2);

    /* Test case: error parsing query */
    let json_input = DemoJsonBody {
        test1: "bart".to_string(),
        test2: Some(0)
    };
    let error = make_request(&testctx, Method::GET,
        "/testing/demo3?test2=2", Some(json_input),
        StatusCode::BAD_REQUEST).await.expect_err("expected error");
    assert_eq!(error.message, "unable to parse query string: missing \
        field `test1`");

    /* Test case: error parsing body */
    let error = make_request_with_body(&testctx, Method::GET,
        "/testing/demo3?test1=martin&test2=2", "}".into(),
        StatusCode::BAD_REQUEST).await.expect_err("expected error");
    assert!(error.message.starts_with("unable to parse body JSON"));

    test_teardown(testctx).await;
}
