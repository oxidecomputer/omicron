// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::external_api::client_api::{
    AuthenticateParams, ClientVerificationParams, TokenRequestParams,
};
use omicron_nexus::external_api::views::{
    ClientAuthentication, ClientTokenGrant, TokenType,
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
async fn test_client_authentication(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // Trying to authenticate without a `client_id` fails.
    RequestBuilder::new(testctx, Method::POST, "/client/authenticate")
        .expect_status(Some(StatusCode::BAD_REQUEST))
        .execute()
        .await
        .expect("client_id required to start client authentication flow");

    let client_id = Uuid::new_v4();
    let authn_params = AuthenticateParams { client_id };

    // Using a JSON encoded body fails.
    RequestBuilder::new(testctx, Method::POST, "/client/authenticate")
        .body(Some(&authn_params))
        .expect_status(Some(StatusCode::BAD_REQUEST))
        .execute()
        .await
        .expect("failed to reject JSON encoded body");

    // Start a client authentication flow using a correctly encoded body.
    let client_authn: ClientAuthentication =
        RequestBuilder::new(testctx, Method::POST, "/client/authenticate")
            .body_urlencoded(Some(&authn_params))
            .expect_status(Some(StatusCode::OK))
            .execute()
            .await
            .expect("failed to start client authentication flow")
            .parsed_body()
            .expect("client authentication response");

    // Sanity-check the response.
    let device_code = client_authn.device_code;
    let user_code = client_authn.user_code;
    assert!(client_authn.verification_uri.ends_with("/client/verify"));
    assert!(client_authn.verification_uri_complete.ends_with(&user_code));
    assert_eq!(client_authn.expires_in, 300);

    // Unauthenticated requests to the verification page redirect to login.
    RequestBuilder::new(
        testctx,
        Method::GET,
        &format!("/client/verify?user_code={}", &user_code),
    )
    .expect_status(Some(StatusCode::FOUND))
    .expect_response_header(
        header::LOCATION,
        &format!(
            "/spoof_login?state=%2Fclient%2Fverify%3Fuser_code%3D{}",
            &user_code
        ),
    )
    .execute()
    .await
    .expect("failed to redirect to login on auth failure");

    // Authenticated requests get the console verification page.
    assert!(NexusRequest::object_get(
        testctx,
        &format!("/client/verify?user_code={}", &user_code),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to get verification page")
    .body
    .starts_with(b"<html>"));

    let confirm_params = ClientVerificationParams { user_code };

    // Confirmation must be authenticated.
    RequestBuilder::new(testctx, Method::POST, "/client/confirm")
        .body(Some(&confirm_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("failed to 401 on unauthed confirmation");

    let token_params = TokenRequestParams {
        grant_type: "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        device_code,
        client_id,
    };

    // A client polling for a token gets an OAuth error until confirmation.
    let error: OAuthError =
        RequestBuilder::new(testctx, Method::POST, "/client/token")
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
        RequestBuilder::new(testctx, Method::POST, "/client/confirm")
            .body(Some(&confirm_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to confirm");

    // Token should be granted after confirmation.
    let token: ClientTokenGrant = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/client/token")
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to get token")
    .parsed_body()
    .expect("failed to deserialize token response");
    assert_eq!(token.token_type, TokenType::Bearer);
    assert_eq!(token.access_token.len(), 52);
    assert!(token.access_token.starts_with("oxide-token-"));
}
