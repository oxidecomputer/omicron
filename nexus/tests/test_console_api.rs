use dropshot::test_util::read_string;
use http::{header, method::Method, StatusCode};
use hyper;

pub mod common;
use common::test_setup;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::external_api::params::OrganizationCreate;

extern crate slog;

#[tokio::test]
async fn test_sessions() {
    let cptestctx = test_setup("test_sessions").await;
    let console_client = &cptestctx.console_client;
    let external_client = &cptestctx.external_client;

    // TODO: responses with set-cookie in them won't work until this uses the
    // new test helpers that don't lock you into a particular set of allowed
    // headers. See https://github.com/oxidecomputer/omicron/pull/403

    let resp = console_client
        .make_request_with_body(
            Method::POST,
            "/logout",
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // logout always gives the same response whether you have a session or not
    let set_cookie_header =
        resp.headers().get("set-cookie").unwrap().to_str().unwrap();
    assert_eq!(
        set_cookie_header,
        "session=\"\"; Secure; HttpOnly; SameSite=Lax; Max-Age=0"
    );

    let resp = console_client
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

    let org_params = OrganizationCreate {
        identity: IdentityMetadataCreateParams {
            name: "my-org".parse().unwrap(),
            description: "an org".to_string(),
        },
    };

    // hitting auth-gated API endpoint without session cookie 401s
    let _ = external_client
        .make_request_with_body(
            Method::POST,
            "/organizations",
            serde_json::to_string(&org_params).unwrap().into(),
            StatusCode::UNAUTHORIZED,
        )
        .await;

    // console pages don't 401, they 302
    let _ = console_client
        .make_request_with_body(
            Method::GET,
            "/c/whatever",
            "".into(),
            StatusCode::FOUND,
        )
        .await;

    // now make same requests with cookie
    let create_org = hyper::Request::builder()
        .header(header::COOKIE, session_token)
        .method(Method::POST)
        .uri(external_client.url("/organizations"))
        .body(serde_json::to_string(&org_params).unwrap().into())
        .expect("attempted to construct invalid test request");
    external_client
        .make_request_with_request(create_org, StatusCode::CREATED)
        .await
        .expect("failed to make request");

    let get_console_page = hyper::Request::builder()
        .header(header::COOKIE, session_token)
        .method(Method::GET)
        .uri(console_client.url("/c/whatever"))
        .body("".into())
        .expect("attempted to construct invalid test request");
    console_client
        .make_request_with_request(get_console_page, StatusCode::OK)
        .await
        .expect("failed to make request");

    // logout with an actual session should delete the session in the db
    let logout_request = hyper::Request::builder()
        .header(header::COOKIE, session_token)
        .method(Method::POST)
        .uri(console_client.url("/logout"))
        .body("".into())
        .expect("attempted to construct invalid test request");
    let logout_resp = console_client
        .make_request_with_request(logout_request, StatusCode::OK)
        .await
        .unwrap();

    // logout clears the cookie client-side
    let set_cookie_header =
        logout_resp.headers().get("set-cookie").unwrap().to_str().unwrap();
    assert_eq!(
        set_cookie_header,
        "session=\"\"; Secure; HttpOnly; SameSite=Lax; Max-Age=0"
    );

    // now the same requests with the same session cookie should 401 because
    // logout also deletes the session server-side
    let request = hyper::Request::builder()
        .header(header::COOKIE, session_token)
        .method(Method::POST)
        .uri(external_client.url("/organizations"))
        .body(serde_json::to_string(&org_params).unwrap().into())
        .expect("attempted to construct invalid test request");
    let _ = external_client
        .make_request_with_request(request, StatusCode::UNAUTHORIZED)
        .await;

    let get_console_page = hyper::Request::builder()
        .header(header::COOKIE, session_token)
        .method(Method::GET)
        .uri(console_client.url("/c/whatever"))
        .body("".into())
        .expect("attempted to construct invalid test request");
    console_client
        .make_request_with_request(get_console_page, StatusCode::FOUND)
        .await
        .expect("failed to make request");

    cptestctx.teardown().await;
}

#[tokio::test]
async fn test_console_pages() {
    let cptestctx = test_setup("test_console_pages").await;
    let client = &cptestctx.console_client;

    // request to console page route without auth should redirect to IdP
    let unauthed_response = client
        .make_request_with_body(
            Method::GET,
            // 404s will be handled client-side unless we want to pull in the
            // entire route tree from the client (which we may well want to do)
            "/c/irrelevant-path",
            "".into(),
            StatusCode::FOUND,
        )
        .await
        .unwrap();

    let location_header =
        unauthed_response.headers().get("location").unwrap().to_str().unwrap();
    assert_eq!(location_header, "idp.com/login");

    // get session

    // hit console page with session, should get back HTML response

    cptestctx.teardown().await;
}

#[tokio::test]
async fn test_assets() {
    let cptestctx = test_setup("test_assets").await;
    let client = &cptestctx.console_client;

    // nonexistent file 404s
    let _ = client
        .make_request_with_body(
            Method::GET,
            "/assets/nonexistent.svg",
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await;

    // existing file is returned
    let mut response = client
        .make_request_with_body(
            Method::GET,
            "/assets/hello.txt",
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let file_contents = read_string(&mut response).await;
    assert_eq!(file_contents, "hello there".to_string());

    cptestctx.teardown().await;
}
