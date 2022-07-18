// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::header::HeaderName;
use http::{header, method::Method, StatusCode};
use std::env::current_dir;

use nexus_test_utils::http_testing::{
    AuthnMode, NexusRequest, RequestBuilder, TestResponse,
};
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::{
    load_test_config, test_setup_with_config, ControlPlaneTestContext,
};
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::authz::SiloRole;
use omicron_nexus::db::fixed_data::silo::DEFAULT_SILO;
use omicron_nexus::db::fixed_data::silo_user::USER_TEST_PRIVILEGED;
use omicron_nexus::db::fixed_data::silo_user::USER_TEST_UNPRIVILEGED;
use omicron_nexus::db::identity::{Asset, Resource};
use omicron_nexus::external_api::console_api::SpoofLoginBody;
use omicron_nexus::external_api::params::OrganizationCreate;
use omicron_nexus::external_api::{shared, views};

#[nexus_test]
async fn test_sessions(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // logout always gives the same response whether you have a session or not
    RequestBuilder::new(&testctx, Method::POST, "/logout")
        .expect_status(Some(StatusCode::NO_CONTENT))
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
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

    // Our test uses the "unprivileged" user to make sure login/logout works
    // without other privileges.  However, they _do_ need the privilege to
    // create Organizations because we'll be testing that as a smoke test.
    // We'll remove that privilege afterwards.
    let silo_url = format!("/silos/{}", DEFAULT_SILO.identity().name);
    let policy_url = format!("{}/policy", silo_url);
    let initial_policy: shared::Policy<SiloRole> =
        NexusRequest::object_get(testctx, &policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch Silo policy")
            .parsed_body()
            .expect("failed to parse Silo policy");
    grant_iam(
        testctx,
        &silo_url,
        SiloRole::Collaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

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

    NexusRequest::object_put(testctx, &policy_url, Some(&initial_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to restore Silo policy");

    // logout with an actual session should delete the session in the db
    RequestBuilder::new(&testctx, Method::POST, "/logout")
        .header(header::COOKIE, &session_token)
        .expect_status(Some(StatusCode::NO_CONTENT))
        // logout also clears the cookie client-side
        .expect_response_header(
            header::SET_COOKIE,
            "session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
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

#[nexus_test]
async fn test_console_pages(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // request to console page route without auth should redirect to IdP
    let _ = RequestBuilder::new(&testctx, Method::GET, "/orgs/irrelevant-path")
        .expect_status(Some(StatusCode::FOUND))
        .expect_response_header(
            header::LOCATION,
            "/spoof_login?state=%2Forgs%2Firrelevant-path",
        )
        .execute()
        .await
        .expect("failed to redirect to IdP on auth failure");

    let session_token = log_in_and_extract_token(&testctx).await;

    // hit console pages with session, should get back HTML response
    let console_paths = &[
        "/",
        "/orgs/irrelevant-path",
        "/settings/irrelevant-path",
        "/device/success",
        "/device/verify",
    ];

    for path in console_paths {
        let console_page = RequestBuilder::new(&testctx, Method::GET, path)
            .header(http::header::COOKIE, session_token.clone())
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
}

#[nexus_test]
async fn test_login_form(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // login route returns bundle too, but is not auth gated
    let console_page =
        RequestBuilder::new(&testctx, Method::GET, "/spoof_login")
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

#[nexus_test]
async fn test_assets(cptestctx: &ControlPlaneTestContext) {
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
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);

    // file in a directory is returned
    let resp = RequestBuilder::new(
        &testctx,
        Method::GET,
        "/assets/a_directory/another_file.txt",
    )
    .expect_status(Some(StatusCode::OK))
    .execute()
    .await
    .expect("failed to get existing file");

    assert_eq!(resp.body, "some words".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);

    // file with only gzipped version 404s if request doesn't have accept-encoding: gzip
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-only.txt")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await
        .expect("failed to 404 on gzip file without accept-encoding: gzip");

    // file with only gzipped version is returned if request accepts gzip
    let resp =
        RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-only.txt")
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .expect_status(Some(StatusCode::OK))
            .expect_response_header(http::header::CONTENT_ENCODING, "gzip")
            .execute()
            .await
            .expect("failed to get existing file");

    assert_eq!(resp.body, "nothing but gzip".as_bytes());

    // file with both gzip and not returns gzipped if request accepts gzip
    let resp =
        RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-and-not.txt")
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .expect_status(Some(StatusCode::OK))
            .expect_response_header(http::header::CONTENT_ENCODING, "gzip")
            .execute()
            .await
            .expect("failed to get existing file");

    assert_eq!(resp.body, "pretend this is gzipped beep boop".as_bytes());

    // returns non-gzipped if request doesn't accept gzip
    let resp =
        RequestBuilder::new(&testctx, Method::GET, "/assets/gzip-and-not.txt")
            .expect_status(Some(StatusCode::OK))
            .execute()
            .await
            .expect("failed to get existing file");

    assert_eq!(resp.body, "not gzipped but I know a guy".as_bytes());
    // make sure we're not including the gzip header on non-gzipped files
    assert_eq!(resp.headers.get(http::header::CONTENT_ENCODING), None);
}

#[tokio::test]
async fn test_absolute_static_dir() {
    let mut config = load_test_config();
    config.pkg.console.static_dir = current_dir().unwrap().join("tests/static");
    let cptestctx =
        test_setup_with_config("test_absolute_static_dir", &mut config).await;
    let testctx = &cptestctx.external_client;

    // existing file is returned
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .execute()
        .await
        .expect("failed to get existing file");

    assert_eq!(resp.body, "hello there".as_bytes());

    cptestctx.teardown().await;
}

#[nexus_test]
async fn test_session_me(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // hitting /session/me without being logged in is a 401
    RequestBuilder::new(&testctx, Method::GET, "/session/me")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed request");

    // now make same request with auth
    let priv_user = NexusRequest::object_get(testctx, "/session/me")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get current user")
        .parsed_body::<views::User>()
        .unwrap();

    assert_eq!(
        priv_user,
        views::User {
            id: USER_TEST_PRIVILEGED.id(),
            display_name: USER_TEST_PRIVILEGED.external_id.clone()
        }
    );

    // make sure it returns different things for different users
    let unpriv_user = NexusRequest::object_get(testctx, "/session/me")
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .expect("failed to get current user")
        .parsed_body::<views::User>()
        .unwrap();

    assert_eq!(
        unpriv_user,
        views::User {
            id: USER_TEST_UNPRIVILEGED.id(),
            display_name: USER_TEST_UNPRIVILEGED.external_id.clone()
        }
    );
}

#[nexus_test]
async fn test_login_redirect(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    expect_redirect(testctx, "/login", "/spoof_login").await;

    // pass through state param to login redirect URL. keep it URL encoded, don't double encode
    // encoded path is /abc/def
    expect_redirect(
        testctx,
        "/login?state=%2Fabc%2Fdef",
        "/spoof_login?state=%2Fabc%2Fdef",
    )
    .await;

    // if state param comes in not URL encoded, we should still URL encode it
    expect_redirect(
        testctx,
        "/login?state=/abc/def",
        "/spoof_login?state=%2Fabc%2Fdef",
    )
    .await;

    // empty state param gets dropped
    expect_redirect(testctx, "/login?state=", "/spoof_login").await;
}

fn get_header_value(resp: TestResponse, header_name: HeaderName) -> String {
    resp.headers.get(header_name).unwrap().to_str().unwrap().to_string()
}

async fn log_in_and_extract_token(testctx: &ClientTestContext) -> String {
    let login = RequestBuilder::new(&testctx, Method::POST, "/login")
        .body(Some(&SpoofLoginBody { username: "unprivileged".to_string() }))
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .expect("failed to log in");

    let session_cookie = get_header_value(login, header::SET_COOKIE);
    let (session_token, rest) = session_cookie.split_once("; ").unwrap();

    assert!(session_token.starts_with("session="));
    assert_eq!(rest, "Path=/; HttpOnly; SameSite=Lax; Max-Age=3600");

    session_token.to_string()
}

async fn expect_redirect(testctx: &ClientTestContext, from: &str, to: &str) {
    let _ = RequestBuilder::new(&testctx, Method::GET, from)
        .expect_status(Some(StatusCode::FOUND))
        .expect_response_header(header::LOCATION, to)
        .execute()
        .await
        .expect("did not find expected redirect");
}
