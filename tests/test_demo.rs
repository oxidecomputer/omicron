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
use oxide_api_prototype::api_server::test_endpoints::DemoJsonBody;

pub mod common;
use common::make_request;
use common::make_request_with_body;
use common::read_json;
use common::read_string;
use common::test_setup;
use common::test_teardown;

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

    test_teardown(testctx).await;
}
