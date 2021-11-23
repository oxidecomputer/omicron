//! Sanity-tests for built-in users

use http::Method;
use http::StatusCode;

pub mod common;
use common::http_testing;
use common::http_testing::AuthnMode;
use common::http_testing::NexusRequest;
use common::http_testing::RequestBuilder;
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_users_predefined() {
    let cptestctx = test_setup("test_users_predefined").await;
    let testctx = &cptestctx.external_client;

    let builder =
        RequestBuilder::new(testctx, Method::GET, "/users")
            .expect_status(Some(StatusCode::UNAUTHORIZED))
            .execute()
            .await
            .unwrap();

    let builder = NexusRequest::new(
        RequestBuilder::new(testctx, Method::GET, "/users")
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    // XXX
    let users = NexusRequest::object_get(&testctx, "/users")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    cptestctx.teardown().await;
}
