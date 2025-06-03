// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::num::NonZeroU32;

use dropshot::test_util::ClientTestContext;
use nexus_auth::authn::USER_TEST_UNPRIVILEGED;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::resource_helpers::{
    object_get, object_put, object_put_error,
};
use nexus_test_utils::{
    http_testing::{AuthnMode, NexusRequest, RequestBuilder},
    resource_helpers::grant_iam,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use nexus_types::external_api::{
    params::{DeviceAccessTokenRequest, DeviceAuthRequest, DeviceAuthVerify},
    views::{
        DeviceAccessTokenGrant, DeviceAccessTokenType, DeviceAuthResponse,
    },
};

use http::{StatusCode, header, method::Method};
use oxide_client::types::SiloRole;
use serde::Deserialize;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[derive(Deserialize)]
struct OAuthError {
    error: String,
    //error_description: Option<String>,
}

#[nexus_test]
async fn test_device_auth_flow(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // Trying to authenticate without a `client_id` fails.
    RequestBuilder::new(testctx, Method::POST, "/device/auth")
        .allow_non_dropshot_errors()
        .expect_status(Some(StatusCode::BAD_REQUEST))
        .execute()
        .await
        .expect("failed to reject device auth start without client_id");

    let client_id = Uuid::new_v4();
    let authn_params = DeviceAuthRequest { client_id };

    // Using a JSON encoded body fails.
    RequestBuilder::new(testctx, Method::POST, "/device/auth")
        .allow_non_dropshot_errors()
        .body(Some(&authn_params))
        .expect_status(Some(StatusCode::BAD_REQUEST))
        .execute()
        .await
        .expect("failed to reject JSON encoded body");

    // Start a device authentication flow using a correctly encoded body.
    let auth_response: DeviceAuthResponse =
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&authn_params))
            .expect_status(Some(StatusCode::OK))
            .execute()
            .await
            .expect("failed to start client authentication flow")
            .parsed_body()
            .expect("client authentication response");

    // Sanity-check the response.
    let device_code = auth_response.device_code;
    let user_code = auth_response.user_code;
    assert!(auth_response.verification_uri.ends_with("/device/verify"));
    assert_eq!(auth_response.expires_in, 300);

    // Unauthenticated requests to the verification page redirect to login.
    RequestBuilder::new(testctx, Method::GET, "/device/verify")
        .expect_status(Some(StatusCode::FOUND))
        .expect_response_header(
            header::LOCATION,
            &format!(
                "/login/{}/local?redirect_uri=%2Fdevice%2Fverify",
                cptestctx.silo_name
            ),
        )
        .execute()
        .await
        .expect("failed to redirect to login on auth failure");

    // Authenticated requests get the console verification page.
    assert!(
        NexusRequest::object_get(testctx, "/device/verify")
            .console_asset()
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to get verification page")
            .body
            .starts_with(b"<html>")
    );

    let confirm_params = DeviceAuthVerify { user_code };

    // Confirmation must be authenticated.
    RequestBuilder::new(testctx, Method::POST, "/device/confirm")
        .allow_non_dropshot_errors()
        .body(Some(&confirm_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed confirmation");

    let token_params = DeviceAccessTokenRequest {
        grant_type: "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        device_code,
        client_id,
    };

    // A client polling for a token gets an OAuth error until confirmation.
    let error: OAuthError =
        RequestBuilder::new(testctx, Method::POST, "/device/token")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::BAD_REQUEST))
            .execute()
            .await
            .expect("failed to get OAuth error on unconfirmed token request")
            .parsed_body()
            .expect("failed to deserialize OAuth error");
    assert_eq!(&error.error, "authorization_pending");

    // Authenticated confirmation should succeed.
    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/confirm")
            .body(Some(&confirm_params))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to confirm");

    // Token should be granted after confirmation.
    let token: DeviceAccessTokenGrant = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/token")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to get token")
    .parsed_body()
    .expect("failed to deserialize token response");
    assert_eq!(token.token_type, DeviceAccessTokenType::Bearer);
    assert_eq!(token.access_token.len(), 52);
    assert!(token.access_token.starts_with("oxide-token-"));

    // now make a request with the token. it 403s because unpriv user has no
    // roles
    project_list(&testctx, &token.access_token, StatusCode::FORBIDDEN)
        .await
        .expect("projects list should 403 with no roles");

    // make sure it also fails with a nonsense token
    project_list(&testctx, "oxide-token-xyz", StatusCode::UNAUTHORIZED)
        .await
        .expect("projects list should 403 with nonsense token");

    // grant unprivileged user silo viewer so they can fetch the projects
    grant_iam(
        testctx,
        &format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name),
        SiloRole::Viewer,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // now make the request again and it should work
    project_list(&testctx, &token.access_token, StatusCode::OK)
        .await
        .expect("failed to get projects with token");
}

/// Helper to make the test cute. Goes through the whole flow, returns the token
/// as a string
async fn get_device_token(testctx: &ClientTestContext) -> String {
    let client_id = Uuid::new_v4();
    let authn_params = DeviceAuthRequest { client_id };

    // Start a device authentication flow
    let auth_response: DeviceAuthResponse =
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&authn_params))
            .expect_status(Some(StatusCode::OK))
            .execute()
            .await
            .expect("failed to start client authentication flow")
            .parsed_body()
            .expect("client authentication response");

    let device_code = auth_response.device_code;
    let user_code = auth_response.user_code;

    let confirm_params = DeviceAuthVerify { user_code };

    // Confirm the device authentication
    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/confirm")
            .body(Some(&confirm_params))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to confirm");

    let token_params = DeviceAccessTokenRequest {
        grant_type: "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        device_code,
        client_id,
    };

    // Get the token
    let token: DeviceAccessTokenGrant = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/token")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to get token")
    .parsed_body()
    .expect("failed to deserialize token response");

    token.access_token
}

#[nexus_test]
async fn test_device_token_expiration(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let settings: views::SiloAuthSettings =
        object_get(testctx, "/v1/auth-settings").await;
    assert_eq!(settings.device_token_max_ttl_seconds, None);

    // get a token for the privileged user. default silo max token expiration
    // is null, so tokens don't expire
    let initial_token = get_device_token(testctx).await;

    // test token works on project list
    project_list(&testctx, &initial_token, StatusCode::OK)
        .await
        .expect("initial token should work");

    //  passing negative or zero gives a 400
    for value in [-3, 0] {
        let error = object_put_error(
            testctx,
            "/v1/auth-settings",
            &serde_json::json!({ "device_token_max_ttl_seconds": value }),
            StatusCode::BAD_REQUEST,
        )
        .await;
        let msg = "unable to parse JSON body: \
                   device_token_max_ttl_seconds: invalid value";
        assert!(error.message.starts_with(&msg));
    }
    for value in [-3, 0] {
        let error = object_put_error(
            testctx,
            "/v1/auth-settings",
            &serde_json::json!({ "device_token_max_ttl_seconds": value }),
            StatusCode::BAD_REQUEST,
        )
        .await;
        let msg = "unable to parse JSON body: \
                   device_token_max_ttl_seconds: invalid value";
        assert!(error.message.starts_with(&msg));
    }

    // omitting the key is also a 400
    let error = object_put_error(
        testctx,
        "/v1/auth-settings",
        &serde_json::json!({}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let msg = "unable to parse JSON body: \
         missing field `device_token_max_ttl_seconds`";
    assert!(error.message.starts_with(&msg));

    // set token expiration on silo to 3 seconds
    let settings: views::SiloAuthSettings = object_put(
        testctx,
        "/v1/auth-settings",
        &params::SiloAuthSettingsUpdate {
            device_token_max_ttl_seconds: NonZeroU32::new(3).into(),
        },
    )
    .await;

    assert_eq!(settings.device_token_max_ttl_seconds, Some(3));

    // might as well test the get endpoint as well
    let settings: views::SiloAuthSettings =
        object_get(testctx, "/v1/auth-settings").await;
    assert_eq!(settings.device_token_max_ttl_seconds, Some(3));

    // create token again (this one will have the 3-second expiration)
    let expiring_token = get_device_token(testctx).await;

    // immediately use token, it should work
    project_list(&testctx, &expiring_token, StatusCode::OK)
        .await
        .expect("expiring token should work immediately");

    // wait 4 seconds to ensure token has expired
    sleep(Duration::from_secs(4)).await;

    // confirm token has expired
    project_list(&testctx, &expiring_token, StatusCode::UNAUTHORIZED)
        .await
        .expect("expiring token should fail after expiration");

    // original token should still work (created before the expiration setting)
    project_list(&testctx, &initial_token, StatusCode::OK)
        .await
        .expect("initial token should still work");

    // now test setting the silo max TTL back to null
    let settings: views::SiloAuthSettings = object_put(
        testctx,
        "/v1/auth-settings",
        &params::SiloAuthSettingsUpdate {
            device_token_max_ttl_seconds: None.into(),
        },
    )
    .await;
    assert_eq!(settings.device_token_max_ttl_seconds, None);

    let settings: views::SiloAuthSettings =
        object_get(testctx, "/v1/auth-settings").await;
    assert_eq!(settings.device_token_max_ttl_seconds, None);
}

async fn project_list(
    testctx: &ClientTestContext,
    token: &str,
    status: StatusCode,
) -> Result<TestResponse, anyhow::Error> {
    RequestBuilder::new(testctx, Method::GET, "/v1/projects")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .expect_status(Some(status))
        .execute()
        .await
}
