use chrono::{Duration, Utc};
use dropshot::{test_util::ClientTestContext, HttpErrorResponseBody};
use http::{Response, StatusCode};
use hyper::{Body, Method, Request};

pub mod common;
use common::{load_test_config, test_setup_with_config};
use omicron_nexus::{
    config::SchemeName, db::model::ConsoleSession, TestInterfaces,
};
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

    /*
     * Valid fake token "good"
     */
    let _ = nexus
        .session_create_with(ConsoleSession {
            token: "good".into(),
            user_id: Uuid::new_v4(),
            time_created: Utc::now(),
            time_last_used: Utc::now(),
        })
        .await;

    let _ = get_orgs_with_cookie(&client, Some("session=good"), StatusCode::OK)
        .await;

    /*
     * Expired fake token "expired"
     */
    let _ = nexus
        .session_create_with(ConsoleSession {
            token: "expired_idle".into(),
            user_id: Uuid::new_v4(),
            time_created: Utc::now() - Duration::hours(2),
            time_last_used: Utc::now() - Duration::hours(2),
        })
        .await;

    let _ = get_orgs_with_cookie(
        &client,
        Some("session=expired_idle"),
        StatusCode::UNAUTHORIZED,
    )
    .await;

    /*
     * Expired fake token "expired_absolute"
     */
    let _ = nexus
        .session_create_with(ConsoleSession {
            token: "expired_absolute".into(),
            user_id: Uuid::new_v4(),
            time_created: Utc::now() - Duration::hours(24),
            time_last_used: Utc::now(),
        })
        .await;

    let _ = get_orgs_with_cookie(
        &client,
        Some("session=expired_absolute"),
        StatusCode::UNAUTHORIZED,
    )
    .await;

    /*
     * Valid random token
     */
    let session = nexus.session_create(Uuid::new_v4()).await.unwrap();
    let cookie = format!("session={}", session.token);

    let _ = get_orgs_with_cookie(&client, Some(&cookie), StatusCode::OK).await;

    /*
     * Nonexistent token
     */
    let _ = get_orgs_with_cookie(
        &client,
        Some("session=other"),
        StatusCode::UNAUTHORIZED,
    )
    .await;

    // TODO: this passes with NotRequested but we probably need it to fail
    /*
     * No session cookie
     */
    let _ = get_orgs_with_cookie(&client, None, StatusCode::OK).await;

    cptestctx.teardown().await;
}

async fn get_orgs_with_cookie(
    client: &ClientTestContext,
    cookie: Option<&str>,
    expected_status: StatusCode,
) -> Result<Response<Body>, HttpErrorResponseBody> {
    let mut request = Request::builder()
        .method(Method::GET)
        .uri(client.url("/organizations"));

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
