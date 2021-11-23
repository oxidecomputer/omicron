//! Sanity-tests for built-in users

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;

pub mod common;
use common::http_testing::AuthnMode;
use common::http_testing::NexusRequest;
use common::http_testing::RequestBuilder;
use common::test_setup;
use omicron_nexus::external_api::views::User;

extern crate slog;

#[tokio::test]
async fn test_users_predefined() {
    let cptestctx = test_setup("test_users_predefined").await;
    let testctx = &cptestctx.external_client;

    RequestBuilder::new(testctx, Method::GET, "/users")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .unwrap();

    NexusRequest::new(
        RequestBuilder::new(testctx, Method::GET, "/users")
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    let users = NexusRequest::object_get(&testctx, "/users")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .response_body::<ResultsPage<User>>()
        .unwrap()
        .items;
    assert_eq!(
        users[0].identity.id.to_string(),
        omicron_nexus::authn::USER_UUID_DB_INIT
    );
    assert_eq!(users[0].identity.name, "db-init");
    assert_eq!(
        users[1].identity.id.to_string(),
        omicron_nexus::authn::TEST_USER_UUID_PRIVILEGED
    );
    assert_eq!(users[1].identity.name, "test-user-privileged");
    assert_eq!(
        users[2].identity.id.to_string(),
        omicron_nexus::authn::TEST_USER_UUID_UNPRIVILEGED
    );
    assert_eq!(users[2].identity.name, "test-user-unprivileged");
    cptestctx.teardown().await;
}
