pub mod common;
use common::test_setup;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::OrganizationCreateParams;

use http::{header, method::Method, StatusCode};
use hyper;

extern crate slog;

#[tokio::test]
async fn test_sessions() {
    let cptestctx = test_setup("test_sessions").await;
    let client = &cptestctx.console_client;
    let external_client = &cptestctx.external_client;

    // Set-Cookie responses only work if you make dropshot's test utils not
    // panic! when it sees the Set-Cookie header

    let resp = client
        .make_request_with_body(
            Method::POST,
            "/logout",
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();

    let session_cookie =
        resp.headers().get("set-cookie").unwrap().to_str().unwrap();
    assert_eq!(
        session_cookie,
        "session=\"\"; Secure; HttpOnly; SameSite=Lax; Max-Age=0"
    );

    let resp = client
        .make_request_with_body(
            Method::POST,
            "/login",
            "{ \"username\": \"\", \"password\": \"\"}".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();

    let session_cookie =
        resp.headers().get("set-cookie").unwrap().to_str().unwrap();
    let (session_token, rest) = session_cookie.split_once("; ").unwrap();

    assert!(session_token.starts_with("session="));
    assert_eq!(rest, "Secure; HttpOnly; SameSite=Lax; Max-Age=3600");

    let org_params = OrganizationCreateParams {
        identity: IdentityMetadataCreateParams {
            name: "my-org".parse().unwrap(),
            description: "an org".to_string(),
        },
    };

    // hitting auth-gated endpoint without session cookie 401s

    let _ = external_client
        .make_request_with_body(
            Method::POST,
            "/organizations",
            serde_json::to_string(&org_params).unwrap().into(),
            StatusCode::UNAUTHORIZED,
        )
        .await;

    // now make same request with cookie

    let request = hyper::Request::builder()
        .header(header::COOKIE, session_token)
        .method(Method::POST)
        .uri(external_client.url("/organizations"))
        .body(serde_json::to_string(&org_params).unwrap().into())
        .expect("attempted to construct invalid test request");
    external_client
        .make_request_with_request(request, StatusCode::CREATED)
        .await
        .expect("failed to make request");

    cptestctx.teardown().await;
}
