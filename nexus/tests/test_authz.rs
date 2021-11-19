//! Basic end-to-end tests for authorization
use common::http_testing::RequestBuilder;
use dropshot::HttpErrorResponseBody;

pub mod common;
use common::test_setup;
use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::authn::external::spoof::HTTP_HEADER_OXIDE_AUTHN_SPOOF;
use omicron_nexus::external_api::params;

extern crate slog;

// TODO-coverage It would be nice to have tests that attempt to hit every
// OpenAPI endpoint with valid arguments and:
// (a) missing credentials (should all fail with 401, 403, or 404)
// (b) bogus credentials (should all fail with a 400-level error)
// (c) valid credentials for a user with no access to anything (should fail with
//     403 or 404)
// For now, we manually test one endpoint that we know should be protected with
// authz so that we're at least testing the mechanism itself.  Testing this for
// all endpoints would ensure that we've applied the mechanism consistently and
// correctly for all endpoints.
#[tokio::test]
async fn test_authz_basic() {
    let cptestctx = test_setup("test_authz_basic").await;
    let client = &cptestctx.external_client;

    // With no credentials, we should get back a 401 "Unauthorized" response.
    let error =
        try_create_organization(client, None, StatusCode::UNAUTHORIZED).await;
    assert_eq!(error.error_code, Some(String::from("Unauthorized")));
    assert_eq!(error.message.as_str(), "credentials missing or invalid");

    // If we provide the valid credentials of an unprivileged user, we should
    // get back a 403 "Forbidden" response.
    let error = try_create_organization(
        client,
        Some(omicron_nexus::authn::TEST_USER_UUID_UNPRIVILEGED),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_eq!(error.error_code, Some(String::from("Forbidden")));
    assert_eq!(error.message.as_str(), "Forbidden");

    // If we provide invalid credentials altogether, we should get an error.
    // This is sort of duplicated by a test in test_authn_http() (which tests
    // the authentication system in general, outside the context of Nexus).
    // This one verifies that we've correctly integrated authn with Nexus.
    let error = try_create_organization(
        client,
        Some(omicron_nexus::authn::external::spoof::SPOOF_RESERVED_BAD_ACTOR),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_eq!(error.error_code, Some(String::from("Unauthorized")));
    assert_eq!(error.message.as_str(), "credentials missing or invalid");

    let error = try_create_organization(
        client,
        Some(omicron_nexus::authn::external::spoof::SPOOF_RESERVED_BAD_CREDS),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_eq!(error.error_code, Some(String::from("Unauthorized")));
    assert_eq!(error.message.as_str(), "credentials missing or invalid");

    cptestctx.teardown().await;
}

async fn try_create_organization(
    client: &dropshot::test_util::ClientTestContext,
    maybe_user_id: Option<&'static str>,
    expected_status: http::StatusCode,
) -> HttpErrorResponseBody {
    let input = params::OrganizationCreate {
        identity: IdentityMetadataCreateParams {
            name: "a-crime-family".parse().unwrap(),
            description: "an org".to_string(),
        },
    };

    let mut builder =
        RequestBuilder::new(client, Method::POST, "/organizations")
            .body(Some(input))
            .expect_status(Some(expected_status));
    if let Some(user_id) = maybe_user_id {
        let authn_header = http::HeaderValue::from_static(user_id);
        builder = builder.header(HTTP_HEADER_OXIDE_AUTHN_SPOOF, authn_header);
    }

    builder
        .execute()
        .await
        .expect("failed to make request")
        .response_body()
        .unwrap()
}
