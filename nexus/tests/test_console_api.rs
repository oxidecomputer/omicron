use http::header::HeaderName;
use http::{header, method::Method, StatusCode};

pub mod common;
use common::http_testing::{RequestBuilder, TestResponse};
use common::test_setup;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::external_api::console_api::LoginParams;
use omicron_nexus::external_api::params::OrganizationCreate;

extern crate slog;

#[tokio::test]
async fn test_sessions() {
    let cptestctx = test_setup("test_sessions").await;
    let testctx = &cptestctx.external_client;

    // logout always gives the same response whether you have a session or not
    let _ = RequestBuilder::new(&testctx, Method::POST, "/logout")
        .expect_status(Some(StatusCode::NO_CONTENT))
        .expect_response_header(
            header::SET_COOKIE,
            "session=\"\"; Secure; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .unwrap();

    // log in and pull the token out of the header so we can use it for authed requests
    let login = RequestBuilder::new(&testctx, Method::POST, "/login")
        .body(Some(LoginParams {
            username: "".parse().unwrap(),
            password: "".parse().unwrap(),
        }))
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await
        .unwrap();

    let session_cookie = get_header_value(login, header::SET_COOKIE);
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
    let _ = RequestBuilder::new(&testctx, Method::POST, "/organizations")
        .body(Some(org_params.clone()))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await;

    // console pages don't 401, they 302
    let _ = RequestBuilder::new(&testctx, Method::POST, "/c/whatever")
        .expect_status(Some(StatusCode::FOUND))
        .execute()
        .await;

    // now make same requests with cookie
    let _ = RequestBuilder::new(&testctx, Method::POST, "/organizations")
        .header(header::COOKIE, session_token)
        .body(Some(org_params.clone()))
        // TODO: explicit expect_status not needed. decide whether to keep it anyway
        .expect_status(Some(StatusCode::CREATED))
        .execute()
        .await;

    let _ = RequestBuilder::new(&testctx, Method::GET, "/c/whatever")
        .header(header::COOKIE, session_token)
        .expect_status(Some(StatusCode::OK))
        .execute()
        .await;
    // TODO: expect error here

    // logout with an actual session should delete the session in the db
    let _ = RequestBuilder::new(&testctx, Method::POST, "/logout")
        .header(header::COOKIE, session_token)
        .expect_status(Some(StatusCode::NO_CONTENT))
        // logout also clears the cookie client-side
        .expect_response_header(
            header::SET_COOKIE,
            "session=\"\"; Secure; HttpOnly; SameSite=Lax; Max-Age=0",
        )
        .execute()
        .await
        .unwrap();

    // now the same requests with the same session cookie should 401/302 because
    // logout also deletes the session server-side
    let _ = RequestBuilder::new(&testctx, Method::POST, "/organizations")
        .header(header::COOKIE, session_token)
        .body(Some(org_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await;

    let _ = RequestBuilder::new(&testctx, Method::GET, "/c/whatever")
        .header(header::COOKIE, session_token)
        .expect_status(Some(StatusCode::FOUND))
        .execute()
        .await;

    cptestctx.teardown().await;
}

#[tokio::test]
async fn test_console_pages() {
    let cptestctx = test_setup("test_console_pages").await;
    let testctx = &cptestctx.external_client;

    // request to console page route without auth should redirect to IdP
    let _ = RequestBuilder::new(&testctx, Method::GET, "/c/irrelevant-path")
        .expect_status(Some(StatusCode::FOUND))
        .expect_response_header(header::LOCATION, "https://idp.com/login")
        .execute()
        .await
        .unwrap();

    // get session

    // hit console page with session, should get back HTML response

    cptestctx.teardown().await;
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
            .await;

    // symlink 404s
    let _ = RequestBuilder::new(&testctx, Method::GET, "/assets/a_symlink")
        .expect_status(Some(StatusCode::NOT_FOUND))
        .execute()
        .await;

    // existing file is returned
    let resp = RequestBuilder::new(&testctx, Method::GET, "/assets/hello.txt")
        .execute()
        .await
        .unwrap();

    assert_eq!(resp.body, "hello there".as_bytes());

    cptestctx.teardown().await;
}

fn get_header_value(resp: TestResponse, header_name: HeaderName) -> String {
    resp.headers.get(header_name).unwrap().to_str().unwrap().to_string()
}
