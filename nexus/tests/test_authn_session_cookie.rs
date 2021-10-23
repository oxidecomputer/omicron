use chrono::{Duration, Utc};
use dropshot::{test_util::ClientTestContext, HttpErrorResponseBody};
use http::{Response, StatusCode};
use hyper::{Body, Method, Request};

pub mod common;
use common::{load_test_config, test_setup_with_config};
use omicron_nexus::{config::SchemeName, TestInterfaces};
use uuid::Uuid;

extern crate slog;

#[tokio::test]
async fn test_authn_session_cookie() {
    let mut config = load_test_config();
    config.authn_schemes_external.push(SchemeName::SessionCookie);
    let cptestctx =
        test_setup_with_config("test_authn_session_cookie", &mut config).await;
    let client = &cptestctx.external_client;

    let nexus = &cptestctx.server.apictx.nexus;

    // create 2 sessions in DB: one good, one expired
    let user1 = Uuid::new_v4();
    let in_5_minutes = Utc::now() + Duration::seconds(300);
    let _ = nexus.session_create_with("good".into(), user1, in_5_minutes).await;

    let user2 = Uuid::new_v4();
    let ago_5_minutes = Utc::now() - Duration::seconds(300);
    let _ =
        nexus.session_create_with("expired".into(), user2, ago_5_minutes).await;

    let _ =
        get_projects_with_cookie(&client, Some("session=good"), StatusCode::OK)
            .await;

    let _ = get_projects_with_cookie(
        &client,
        Some("session=expired"),
        StatusCode::UNAUTHORIZED,
    )
    .await;

    let _ = get_projects_with_cookie(
        &client,
        Some("session=other"),
        StatusCode::UNAUTHORIZED,
    )
    .await;

    // TODO: this passes with NotRequested but we probably need it to fail
    let _ = get_projects_with_cookie(&client, None, StatusCode::OK).await;

    cptestctx.teardown().await;
}

async fn get_projects_with_cookie(
    client: &ClientTestContext,
    cookie: Option<&str>,
    expected_status: StatusCode,
) -> Result<Response<Body>, HttpErrorResponseBody> {
    let mut request =
        Request::builder().method(Method::GET).uri(client.url("/projects"));

    if let Some(cookie) = cookie {
        request = request.header("Cookie", cookie);
    }

    client
        .make_request_with_request(
            request
                .body("".into())
                .expect("attempted to construct invalid request"),
            expected_status,
        )
        .await
}
