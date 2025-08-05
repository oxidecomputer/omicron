// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::num::NonZeroU32;

use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use dropshot::{HttpErrorResponseBody, ResultsPage};
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use nexus_auth::authn::USER_TEST_UNPRIVILEGED;
use nexus_config::NexusConfig;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::identity::{Asset, Resource};
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::resource_helpers::{
    create_local_user, object_delete_error, object_get, object_put,
    object_put_error, test_params,
};
use nexus_test_utils::{
    http_testing::{AuthnMode, NexusRequest, RequestBuilder},
    resource_helpers::grant_iam,
};
use nexus_test_utils::{load_test_config, test_setup_with_config};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use nexus_types::external_api::{
    params::{DeviceAccessTokenRequest, DeviceAuthRequest, DeviceAuthVerify},
    views::{
        DeviceAccessTokenGrant, DeviceAccessTokenType, DeviceAuthResponse,
    },
};
use omicron_uuid_kinds::SiloUserUuid;

use http::{StatusCode, header, method::Method};
use omicron_sled_agent::sim;
use oxide_client::types::{FleetRole, SiloRole};
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
    // note that this exercises ttl_seconds being omitted from the body because
    // it's URL encoded, so None means it's omitted
    let authn_params = DeviceAuthRequest { client_id, ttl_seconds: None };

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

    // Check tokens before creating the device token
    assert_eq!(get_tokens_priv(testctx).await.len(), 0);
    assert_eq!(get_tokens_unpriv(testctx).await.len(), 0);

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

    // Check token list endpoints after creating the device token
    assert_eq!(get_tokens_priv(testctx).await.len(), 0);
    let tokens_unpriv_after = get_tokens_unpriv(testctx).await;
    assert_eq!(tokens_unpriv_after.len(), 1);
    assert_eq!(tokens_unpriv_after[0].id, token.token_id);
    assert_eq!(token.time_expires, None);
    assert_eq!(tokens_unpriv_after[0].time_expires, None);

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

    let token_id = tokens_unpriv_after[0].id;

    // Priv user cannot delete unpriv's token through this endpoint by ID
    let token_url = format!("/v1/me/access-tokens/{}", token_id);
    object_delete_error(testctx, &token_url, StatusCode::NOT_FOUND).await;

    // Test deleting the token as the owner
    NexusRequest::object_delete(testctx, &token_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .expect("failed to delete token");

    // Verify token is gone from the list
    assert_eq!(get_tokens_unpriv(testctx).await.len(), 0);

    // Token should no longer work for API calls
    project_list(&testctx, &token.access_token, StatusCode::UNAUTHORIZED)
        .await
        .expect("deleted token should be unauthorized");

    // Trying to delete the same token again should 404
    NexusRequest::new(
        RequestBuilder::new(testctx, Method::DELETE, &token_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("double delete should 404");
}

/// Helper to make the test cute. Goes through the whole flow, returns the token
/// as a string
async fn get_device_token(
    testctx: &ClientTestContext,
    authn_mode: AuthnMode,
) -> DeviceAccessTokenGrant {
    let client_id = Uuid::new_v4();
    let authn_params = DeviceAuthRequest { client_id, ttl_seconds: None };

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
    .authn_as(authn_mode.clone())
    .execute()
    .await
    .expect("failed to confirm");

    let token_params = DeviceAccessTokenRequest {
        grant_type: "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        device_code,
        client_id,
    };

    // Get the token and return it
    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/token")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(authn_mode)
    .execute()
    .await
    .expect("failed to get token")
    .parsed_body()
    .expect("failed to deserialize token response")
}

#[nexus_test]
async fn test_device_token_expiration(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    let settings: views::SiloAuthSettings =
        object_get(testctx, "/v1/auth-settings").await;
    assert_eq!(settings.device_token_max_ttl_seconds, None);

    // no tokens in the list
    assert_eq!(get_tokens_priv(testctx).await.len(), 0);

    // get a token for the privileged user. default silo max token expiration
    // is null, so tokens don't expire
    let initial_token_grant =
        get_device_token(testctx, AuthnMode::PrivilegedUser).await;
    let initial_token = initial_token_grant.access_token;

    // now there is a token in the list
    let tokens = get_tokens_priv(testctx).await;
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].time_expires, None);
    assert_eq!(tokens[0].id, initial_token_grant.token_id);

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
    let expiring_token_grant =
        get_device_token(testctx, AuthnMode::PrivilegedUser).await;

    // check that expiration time is there and in the right range
    let exp = expiring_token_grant
        .time_expires
        .expect("Expiring token should have an expiration time");
    let exp = (exp - Utc::now()).num_seconds();
    assert!(exp > 0 && exp < 5, "should be around 3 seconds from now");

    let expiring_token = expiring_token_grant.access_token;

    // use a block so we don't touch expiring_token
    {
        // now there are two tokens in the list
        let tokens = get_tokens_priv(testctx).await;
        assert_eq!(tokens.len(), 2);

        let permanent_token =
            tokens.iter().find(|t| t.time_expires.is_none()).unwrap();
        let expiring_token =
            tokens.iter().find(|t| t.time_expires.is_some()).unwrap();

        assert_eq!(permanent_token.time_expires, None);
        assert!(expiring_token.time_expires.unwrap() > Utc::now());
    }

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

    // back down to one non-expiring token
    let tokens = get_tokens_priv(testctx).await;
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].time_expires, None);

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

// lets me stick whatever I want in this thing to be URL-encoded
#[derive(serde::Serialize)]
struct BadAuthReq {
    client_id: String,
    ttl_seconds: String,
}

/// Test that 0 and negative values for ttl_seconds give immediate 400s
#[nexus_test]
async fn test_device_token_request_ttl_invalid(
    cptestctx: &ControlPlaneTestContext,
) {
    let testctx = &cptestctx.external_client;

    let auth_response = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&BadAuthReq {
                client_id: Uuid::new_v4().to_string(),
                ttl_seconds: "0".to_string(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .execute()
    // .execute_and_parse_unwrap::<DeviceAuthResponse>()
    .await
    .expect("expected an Ok(TestResponse)");

    let error_body: serde_json::Value =
        serde_json::from_slice(&auth_response.body).unwrap();
    assert_eq!(
        error_body.get("message").unwrap().to_string(),
        "\"unable to parse URL-encoded body: ttl_seconds: \
         invalid value: integer `0`, expected a nonzero u32\""
    );

    let auth_response = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&BadAuthReq {
                client_id: Uuid::new_v4().to_string(),
                ttl_seconds: "-3".to_string(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .execute()
    // .execute_and_parse_unwrap::<DeviceAuthResponse>()
    .await
    .expect("expected an Ok(TestResponse)");

    let error_body: serde_json::Value =
        serde_json::from_slice(&auth_response.body).unwrap();
    assert_eq!(
        error_body.get("message").unwrap().to_string(),
        "\"unable to parse URL-encoded body: ttl_seconds: \
         invalid digit found in string\""
    );
}

#[nexus_test]
async fn test_device_token_request_ttl(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // Set silo max TTL to 10 seconds
    let settings = params::SiloAuthSettingsUpdate {
        device_token_max_ttl_seconds: NonZeroU32::new(10).into(),
    };
    let _: views::SiloAuthSettings =
        object_put(testctx, "/v1/auth-settings", &settings).await;

    // Request TTL above the max should fail at verification time
    let invalid_ttl = DeviceAuthRequest {
        client_id: Uuid::new_v4(),
        ttl_seconds: NonZeroU32::new(20), // Above the 10 second max
    };

    let auth_response = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
            .body_urlencoded(Some(&invalid_ttl))
            .expect_status(Some(StatusCode::OK)),
    )
    .execute_and_parse_unwrap::<DeviceAuthResponse>()
    .await;

    let confirm_params =
        DeviceAuthVerify { user_code: auth_response.user_code };

    // Confirmation fails because requested TTL exceeds max
    let confirm_error = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/confirm")
            .body(Some(&confirm_params))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<HttpErrorResponseBody>()
    .await;

    // Check that the error message mentions TTL
    assert_eq!(confirm_error.error_code, Some("InvalidRequest".to_string()));
    assert_eq!(
        confirm_error.message,
        "Requested TTL 20 seconds exceeds maximum allowed TTL \
         for this silo of 10 seconds"
    );

    // Request TTL below the max should succeed and be used
    let valid_ttl = DeviceAuthRequest {
        client_id: Uuid::new_v4(),
        ttl_seconds: NonZeroU32::new(3), // Below the 10 second max
    };

    let auth_response = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/auth")
            .body_urlencoded(Some(&valid_ttl))
            .expect_status(Some(StatusCode::OK)),
    )
    .execute_and_parse_unwrap::<DeviceAuthResponse>()
    .await;

    let device_code = auth_response.device_code;
    let user_code = auth_response.user_code;
    let confirm_params = DeviceAuthVerify { user_code };

    // this time will be pretty close to the now() used on the server when
    // calculating expiration time
    let t0 = Utc::now();

    // Confirmation should succeed
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
        client_id: valid_ttl.client_id,
    };

    // Get the token
    let token_grant = NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, "/device/token")
            .allow_non_dropshot_errors()
            .body_urlencoded(Some(&token_params))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<DeviceAccessTokenGrant>()
    .await;

    // Verify the token has roughly the correct expiration time. One second
    // threshold is sufficient to confirm it's not getting the silo max of 10
    // seconds. Locally, I saw diffs as low as 14ms.
    let tokens = get_tokens_priv(testctx).await;
    let time_expires = tokens[0].time_expires.unwrap();
    let expected_expires = t0 + Duration::from_secs(3);
    let diff_ms = (time_expires - expected_expires).num_milliseconds().abs();
    assert!(diff_ms <= 1000, "time diff was {diff_ms} ms. should be near zero");

    // Token should work initially
    project_list(&testctx, &token_grant.access_token, StatusCode::OK)
        .await
        .expect("token should work initially");

    // Wait for token to expire
    sleep(Duration::from_secs(4)).await;

    // Token is expired
    project_list(&testctx, &token_grant.access_token, StatusCode::UNAUTHORIZED)
        .await
        .expect("token should be expired");
}

#[nexus_test]
async fn test_admin_logout_deletes_tokens_and_sessions(
    cptestctx: &ControlPlaneTestContext,
) {
    let testctx = &cptestctx.external_client;

    let silo_name = cptestctx.silo_name.as_str();
    // create users so we can have user IDs to pass to authn_as
    let silo_url = format!("/v1/system/silos/{}", silo_name);
    let test_suite_silo: views::Silo = object_get(testctx, &silo_url).await;
    let user1 = create_local_user(
        testctx,
        &test_suite_silo,
        &"user1".parse().unwrap(),
        test_params::UserPassword::Password("password1".to_string()),
    )
    .await;
    let user2 = create_local_user(
        testctx,
        &test_suite_silo,
        &"user2".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // no tokens or sessions for user 1 yet
    let tokens = list_user_tokens(testctx, user1.id).await;
    assert!(tokens.is_empty());
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert!(sessions.is_empty());

    // create a token and session for user1
    get_device_token(testctx, AuthnMode::SiloUser(user1.id)).await;
    create_session_for_user(testctx, silo_name, "user1", "password1").await;

    // now there is a token and session for user1
    let tokens = list_user_tokens(testctx, user1.id).await;
    assert_eq!(tokens.len(), 1);
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert_eq!(sessions.len(), 1);

    let logout_url = format!("/v1/users/{}/logout", user1.id);

    // user 2 cannot hit the logout endpoint for user 1
    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, &logout_url)
            .body(Some(&serde_json::json!({})))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(user2.id))
    .execute()
    .await
    .expect("User has no perms, can't delete another user's tokens");

    let tokens = list_user_tokens(testctx, user1.id).await;
    assert_eq!(tokens.len(), 1);
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert_eq!(sessions.len(), 1);

    // user 1 can hit the logout endpoint for themselves
    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, &logout_url)
            .body(Some(&serde_json::json!({})))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .expect("User 1 should be able to delete their own tokens");

    let tokens = list_user_tokens(testctx, user1.id).await;
    assert!(tokens.is_empty());
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert!(sessions.is_empty());

    // create another couple of tokens and sessions for user1
    get_device_token(testctx, AuthnMode::SiloUser(user1.id)).await;
    get_device_token(testctx, AuthnMode::SiloUser(user1.id)).await;
    create_session_for_user(testctx, silo_name, "user1", "password1").await;
    create_session_for_user(testctx, silo_name, "user1", "password1").await;

    let tokens = list_user_tokens(testctx, user1.id).await;
    assert_eq!(tokens.len(), 2);
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert_eq!(sessions.len(), 2);

    // make user 2 fleet admin to show that fleet admin does not inherit
    // the appropriate role due to being fleet admin alone
    grant_iam(
        testctx,
        "/v1/system",
        FleetRole::Admin,
        user2.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, &logout_url)
            .body(Some(&serde_json::json!({})))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(user2.id))
    .execute()
    .await
    .expect("Fleet admin is not sufficient to delete another user's tokens");

    let tokens = list_user_tokens(testctx, user1.id).await;
    assert_eq!(tokens.len(), 2);
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert_eq!(sessions.len(), 2);

    // make user 2 a silo admin so they can delete user 1's tokens
    grant_iam(
        testctx,
        &silo_url,
        SiloRole::Admin,
        user2.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    NexusRequest::new(
        RequestBuilder::new(testctx, Method::POST, &logout_url)
            .body(Some(&serde_json::json!({})))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .expect("Silo admin should be able to delete user 1's tokens");

    // they're gone!
    let tokens = list_user_tokens(testctx, user1.id).await;
    assert!(tokens.is_empty());
    let sessions = list_user_sessions(testctx, user1.id).await;
    assert!(sessions.is_empty());
}

// Strictly speaking, this is not about device auth at all, but we need the
// lovely helpers we've defined in this file to make things convenient. This
// suggests we could stand to reorganize or rename some test files.

#[tokio::test]
async fn test_session_list_excludes_expired() {
    // Test with default TTL - session should not be expired
    let mut config = load_test_config();
    test_session_list_with_config(&mut config, 1).await;

    // Test with idle TTL = 0 - session should be expired immediately
    let mut config = load_test_config();
    config.pkg.console.session_idle_timeout_minutes = 0;
    test_session_list_with_config(&mut config, 0).await;

    // Test with abs TTL = 0 - session should be expired immediately
    let mut config = load_test_config();
    config.pkg.console.session_absolute_timeout_minutes = 0;
    test_session_list_with_config(&mut config, 0).await;
}

/// Set up a test context with the given config, create a user in the test suite
/// silo, and create a session, and assert about the length of the session list
async fn test_session_list_with_config(
    config: &mut NexusConfig,
    expected_sessions: usize,
) {
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_session_list_excludes_expired",
        config,
        sim::SimMode::Explicit,
        None,
        0,
        DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await;
    let testctx = &cptestctx.external_client;

    let silo_name = cptestctx.silo_name.as_str();
    let silo_url = format!("/v1/system/silos/{}", silo_name);
    let test_suite_silo: views::Silo = object_get(testctx, &silo_url).await;
    let user1 = create_local_user(
        testctx,
        &test_suite_silo,
        &"user1".parse().unwrap(),
        test_params::UserPassword::Password("password1".to_string()),
    )
    .await;

    let sessions = list_user_sessions(testctx, user1.id).await;
    assert!(sessions.is_empty());

    create_session_for_user(testctx, silo_name, "user1", "password1").await;

    let sessions = list_user_sessions(&testctx, user1.id).await;

    assert_eq!(
        sessions.len(),
        expected_sessions,
        "Expected session list to have {expected_sessions} items"
    );

    cptestctx.teardown().await; // important!
}

async fn get_tokens_priv(
    testctx: &ClientTestContext,
) -> Vec<views::DeviceAccessToken> {
    NexusRequest::object_get(testctx, "/v1/me/access-tokens")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::DeviceAccessToken>>()
        .await
        .items
}

async fn list_user_tokens(
    testctx: &ClientTestContext,
    user_id: SiloUserUuid,
) -> Vec<views::DeviceAccessToken> {
    NexusRequest::object_get(testctx, "/v1/me/access-tokens")
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute_and_parse_unwrap::<ResultsPage<views::DeviceAccessToken>>()
        .await
        .items
}

async fn list_user_sessions(
    testctx: &ClientTestContext,
    user_id: SiloUserUuid,
) -> Vec<views::ConsoleSession> {
    let url = format!("/v1/users/{}/sessions", user_id);
    NexusRequest::object_get(testctx, &url)
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute_and_parse_unwrap::<ResultsPage<views::ConsoleSession>>()
        .await
        .items
}

async fn create_session_for_user(
    testctx: &ClientTestContext,
    silo_name: &str,
    username: &str,
    password: &str,
) {
    let url = format!("/v1/login/{}/local", silo_name);
    let credentials = test_params::UsernamePasswordCredentials {
        username: username.parse().unwrap(),
        password: password.to_string(),
    };
    let _login = RequestBuilder::new(&testctx, Method::POST, &url)
        .body(Some(&credentials))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("failed to log in");
    // We don't need to extract the token, just creating the session is enough
}

async fn get_tokens_unpriv(
    testctx: &ClientTestContext,
) -> Vec<views::DeviceAccessToken> {
    NexusRequest::object_get(testctx, "/v1/me/access-tokens")
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::DeviceAccessToken>>()
        .await
        .items
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
