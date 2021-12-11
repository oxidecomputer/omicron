// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::header::HeaderName;
use http::{header, method::Method, StatusCode};
use std::env::current_dir;

use nexus_test_utils::http_testing::{RequestBuilder, TestResponse};
use nexus_test_utils::{
    load_test_config, test_setup, test_setup_with_config,
    ControlPlaneTestContext,
};
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::external_api::console_api::LoginParams;
use omicron_nexus::external_api::params::OrganizationCreate;

#[nexus_test]
async fn test_sessions(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // logout always gives the same response whether you have a session or not
    RequestBuilder::new(&testctx, Method::POST, "/logout")
        .expect_status(Some(StatusCode::NO_CONTENT))
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Secure; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .expect("failed to clear cookie and 204 on logout");

    // log in and pull the token out of the header so we can use it for authed requests
    let session_token = log_in_and_extract_token(&testctx).await;

    let org_params = OrganizationCreate {
        identity: IdentityMetadataCreateParams {
            name: "my-org".parse().unwrap(),
            description: "an org".to_string(),
        },
    };

    // hitting auth-gated API endpoint without session cookie 401s
    RequestBuilder::new(&testctx, Method::POST, "/organizations")
        .body(Some(&org_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed API request");

    // console pages don't 401, they 302
    RequestBuilder::new(&testctx, Method::GET, "/orgs/whatever")
        .expect_status(Some(StatusCode::FOUND))
        .execute()
        .await
        .expect("failed to 302 on unauthed console page request");

    // now make same requests with cookie
    RequestBuilder::new(&testctx, Method::POST, "/organizations")
        .header(header::COOKIE, &session_token)
        .body(Some(&org_params))
        // TODO: explicit expect_status not needed. decide whether to keep it anyway
        .expect_status(Some(StatusCode::CREATED))
        .execute()
        .await
        .expect("failed to create org with session cookie");

    RequestBuilder::new(&testctx, Method::GET, "/orgs/whatever")
        .header(header::COOKIE, &session_token)
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("failed to get console page with session cookie");

    // logout with an actual session should delete the session in the db
    RequestBuilder::new(&testctx, Method::POST, "/logout")
        .header(header::COOKIE, &session_token)
        .expect_status(Some(StatusCode::NO_CONTENT))
        // logout also clears the cookie client-side
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Secure; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .expect("failed to log out");

    // now the same requests with the same session cookie should 401/302 because
    // logout also deletes the session server-side
    RequestBuilder::new(&testctx, Method::POST, "/organizations")
        .header(header::COOKIE, &session_token)
        .body(Some(&org_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to get 401 for unauthed API request");

    RequestBuilder::new(&testctx, Method::GET, "/orgs/whatever")
        .header(header::COOKIE, &session_token)
        .expect_status(Some(StatusCode::FOUND))
        .execute()
        .await
        .expect("failed to get 302 for unauthed console request");
}

#[tokio::test]
async fn test_console_pages() {
    let cptestctx = test_setup("test_console_pages").await;
    let testctx = &cptestctx.external_client;

    // request to console page route without auth should redirect to IdP
    let _ = RequestBuilder::new(&testctx, Method::GET, "/orgs/irrelevant-path")
        .expect_status(Some(StatusCode::FOUND))
        .expect_response_header(header::LOCATION, "/login")
        .execute()
        .await
        .expect("failed to redirect to IdP on auth failure");

    let session_token = log_in_and_extract_token(&testctx).await;

    // hit console page with session, should get back HTML response
    let console_page =
        RequestBuilder::new(&testctx, Method::GET, "/orgs/irrelevant-path")
            .header(http::header::COOKIE, session_token)
            .expect_status(Some(StatusCode::OK))
            .expect_response_header(
                http::header::CONTENT_TYPE,
                "text/html; charset=UTF-8",
            )
            .execute()
            .await
            .expect("failed to get console index");

    assert_eq!(console_page.body, "<html></html>".as_bytes());
}

#[tokio::test]
async fn text_login_form() {
    let cptestctx = test_setup("test_login_form").await;
    let testctx = &cptestctx.external_client;

    // login route returns bundle too, but is not auth gated
    let console_page = RequestBuilder::new(&testctx, Method::GET, "/login")
        .expect_status(Some(StatusCode::OK))
        .expect_response_header(
            http::header::CONTENT_TYPE,
            "text/html; charset=UTF-8",
        )
        .execute()
        .await
        .expect("failed to get login form");

    assert_eq!(console_page.body, "<html></html>".as_bytes());
}

#[tokio::test]
async fn test_assets() {
    let cptestctx = test_setup("test_assets").await;
    let testctx = &cptestctx.external_client;

    // nonexistent file 404s
    let _ =
        RequestBuilder::new(&testctx, Method::GET, "/assets/nonexistent.svg")
            .expect_status(Some(StatusCode::NOT_FOUND))
            .execute()
            .await
            .expect("failed to 404 on nonexistent asset");

    // existing file with disallowed extension 404s
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/blocked.ext")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await
        .expect("failed to 404 on disallowed extension");

    // symlink 404s
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/a_symlink")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await
        .expect("failed to 404 on symlink");

    // existing file is returned
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());
}

#[tokio::test]
async fn test_absolute_static_dir() {
    let mut config = load_test_config();
    config.console.static_dir = current_dir().unwrap().join("tests/static");
    let cptestctx =
        test_setup_with_config("test_absolute_static_dir", &mut config).await;
    let testctx = &cptestctx.external_client;

    // existing file is returned
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());
}

fn get_header_value(resp: TestResponse, header_name: HeaderName) -> String {
    resp.headers.get(header_name).unwrap().to_str().unwrap().to_string()
}

async fn log_in_and_extract_token(testctx: &ClientTestContext) -> String {
    let login = RequestBuilder::new(&testctx, Method::POST, "/login")
        .body(Some(&LoginParams { username: "privileged".to_string() }))
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("failed to log in");

    let session_cookie = get_header_value(login, header::SET_COOKIE);
    let (session_token, rest) = session_cookie.split_once("; ").unwrap();

    assert!(session_token.starts_with("session="));
    assert_eq!(rest, "Secure; HttpOnly; SameSite=Lax; Max-Age=3600");

    session_token.to_string()
}
