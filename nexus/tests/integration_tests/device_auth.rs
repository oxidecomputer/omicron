// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::external_api::device_auth::{
    DeviceAccessTokenRequestParams, DeviceAuthRequestParams,
    DeviceAuthVerifyParams,
};
use omicron_nexus::external_api::views::{
    DeviceAccessTokenGrant, DeviceAccessTokenType, DeviceAuthResponse,
};

use http::{header, method::Method, StatusCode};
use serde::Deserialize;
use uuid::Uuid;

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
        .expect_status(Some(StatusCode::BAD_REQUEST))
        .execute()
        .await
        .expect("client_id required to start client authentication flow");

    let client_id = Uuid::new_v4();
    let authn_params = DeviceAuthRequestParams { client_id };

    // Using a JSON encoded body fails.
    RequestBuilder::new(testctx, Method::POST, "/device/auth")
        .body(Some(&authn_params))
        .expect_status(Some(StatusCode::BAD_REQUEST))
        .execute()
        .await
        .expect("failed to reject JSON encoded body");

    // Start a device authentication flow using a correctly encoded body.
    let auth_response: DeviceAuthResponse =
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
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
    assert!(auth_response.verification_uri_complete.ends_with(&user_code));
    assert_eq!(auth_response.expires_in, 300);

    // Unauthenticated requests to the verification page redirect to login.
    RequestBuilder::new(
        testctx,
        Method::GET,
        &format!("/device/verify?user_code={}", &user_code),
    )
    .expect_status(Some(StatusCode::FOUND))
    .expect_response_header(
        header::LOCATION,
        &format!(
            "/spoof_login?state=%2Fdevice%2Fverify%3Fuser_code%3D{}",
            &user_code
        ),
    )
    .execute()
    .await
    .expect("failed to redirect to login on auth failure");

    // Authenticated requests get the console verification page.
    assert!(NexusRequest::object_get(
        testctx,
        &format!("/device/verify?user_code={}", &user_code),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to get verification page")
    .body
    .starts_with(b"<html>"));

    let confirm_params = DeviceAuthVerifyParams { user_code };

    // Confirmation must be authenticated.
    RequestBuilder::new(testctx, Method::POST, "/device/confirm")
        .body(Some(&confirm_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed confirmation");

    let token_params = DeviceAccessTokenRequestParams {
        grant_type: "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        device_code,
        client_id,
    };

    // A client polling for a token gets an OAuth error until confirmation.
    let error: OAuthError =
        RequestBuilder::new(testctx, Method::POST, "/device/token")
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
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to confirm");

    // Token should be granted after confirmation.
    let token: DeviceAccessTokenGrant = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/token")
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to get token")
    .parsed_body()
    .expect("failed to deserialize token response");
    assert_eq!(token.token_type, DeviceAccessTokenType::Bearer);
    assert_eq!(token.access_token.len(), 52);
    assert!(token.access_token.starts_with("oxide-token-"));
}
