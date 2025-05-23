// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::{StatusCode, header, method::Method};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils::resource_helpers::{create_local_user, create_silo};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::shared::{self, SiloRole};
use nexus_types::external_api::views;
use omicron_common::api::external::{Name, UserId};
use omicron_passwords::MIN_EXPECTED_PASSWORD_VERIFY_TIME;
use std::str::FromStr;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// TODO-coverage verify that deleting a Silo deletes all the users and their
// password hashes

// TODO-coverage A more rigorous test to verify there are no timing attack
// vulnerabilities here might be to construct a few kinds of logins (attempt for
// nonexistent user, attempt for user with no password set, successful attempt,
// and attempt for user with a password set that doesn't match what's provided).
// Then run each of these a a bunch of times and verify there's no statistically
// significant difference between them.

#[nexus_test]
async fn test_local_users(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let silo_name = Name::from_str("test-silo").unwrap();
    let silo = create_silo(
        client,
        silo_name.as_str(),
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;
    test_local_user_basic(client, &silo).await;
    test_local_user_with_no_initial_password(client, &silo).await;
    NexusRequest::object_delete(
        client,
        &format!("/v1/system/silos/{}", silo_name),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn test_local_user_basic(client: &ClientTestContext, silo: &views::Silo) {
    let silo_name = &silo.identity.name;

    // First, try logging in with a non-existent user.  This naturally should
    // fail.  It should also take as long as it would take for a valid user.
    // The timing is verified in expect_login_failure().
    expect_login_failure(
        client,
        &silo_name,
        UserId::from_str("bigfoot").unwrap(),
        "ahh".to_string(),
    )
    .await;

    // Create a test user with a known password.
    let test_user = UserId::from_str("abe-simpson").unwrap();
    let test_password = "let me in you idiot!";

    let created_user = create_local_user(
        client,
        silo,
        &test_user,
        test_params::UserPassword::Password(test_password.to_string()),
    )
    .await;

    // Try to log in with a bogus password.
    expect_login_failure(
        client,
        &silo_name,
        test_user.clone(),
        "something else".to_string(),
    )
    .await;

    // Then log in with the right password and use the session token to do
    // something.
    let session_token = expect_login_success(
        client,
        &silo_name,
        test_user.clone(),
        test_password.to_string(),
    )
    .await;
    let found_user = expect_session_valid(client, &session_token).await;
    assert_eq!(created_user, found_user.user);

    // While we're still logged in, change the password.
    let test_password2 = "as was the style at the time";
    let user_password_url = format!(
        "/v1/system/identity-providers/local/users/{}/set-password?silo={}",
        created_user.id, silo_name
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &user_password_url)
            .expect_status(Some(StatusCode::NO_CONTENT))
            .body(Some(&test_params::UserPassword::Password(
                test_password2.to_string(),
            ))),
    )
    .authn_as(AuthnMode::Session(session_token.to_string()))
    .execute()
    .await
    .unwrap();

    // The old password should no longer work.
    expect_login_failure(
        client,
        &silo_name,
        test_user.clone(),
        test_password.to_string(),
    )
    .await;

    // We should be able to login separately with the new password.
    let session_token2 = expect_login_success(
        client,
        &silo_name,
        test_user.clone(),
        test_password2.to_string(),
    )
    .await;

    // At this point, both session tokens should be valid.
    expect_session_valid(client, &session_token).await;
    expect_session_valid(client, &session_token2).await;

    // Log out of the first session.
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/v1/logout")
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::Session(session_token.to_string()))
    .execute()
    .await
    .expect("failed to log out");

    // The first session token should not be valid any more.
    expect_session_invalid(client, &session_token).await;

    // But the second session token should still be valid.
    expect_session_valid(client, &session_token2).await;

    // Now, let's create an admin user and verify that they can change this
    // user's password.
    let admin_user = UserId::from_str("comic-book-guy").unwrap();
    let admin_password = "toodle-ooh";
    let admin_user_obj = create_local_user(
        client,
        silo,
        &admin_user,
        test_params::UserPassword::Password(admin_password.to_string()),
    )
    .await;
    let admin_password_url = format!(
        "/v1/system/identity-providers/local/users/{}/set-password?silo={}",
        admin_user_obj.id, silo_name
    );

    let silo_url = format!("/v1/system/silos/{}", silo_name);
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        admin_user_obj.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let admin_session = expect_login_success(
        client,
        &silo_name,
        admin_user.clone(),
        admin_password.to_string(),
    )
    .await;

    let hijacked_password = "sarcasm detector";
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &user_password_url)
            .expect_status(Some(StatusCode::NO_CONTENT))
            .body(Some(&test_params::UserPassword::Password(
                hijacked_password.to_string(),
            ))),
    )
    .authn_as(AuthnMode::Session(admin_session.to_string()))
    .execute()
    .await
    .unwrap();

    // Just to be clear, we modified the test user's password.
    let _ = expect_login_success(
        client,
        &silo_name,
        test_user.clone(),
        hijacked_password.to_string(),
    )
    .await;
    expect_login_failure(
        client,
        &silo_name,
        test_user.clone(),
        test_password2.to_string(),
    )
    .await;

    // And we did not modify the admin user's password.
    let _ = expect_login_success(
        client,
        &silo_name,
        admin_user.clone(),
        admin_password.to_string(),
    )
    .await;
    expect_login_failure(
        client,
        &silo_name,
        admin_user.clone(),
        hijacked_password.to_string(),
    )
    .await;

    // The admin can also invalidate the user's password.
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &user_password_url)
            .expect_status(Some(StatusCode::NO_CONTENT))
            .body(Some(&test_params::UserPassword::LoginDisallowed)),
    )
    .authn_as(AuthnMode::Session(admin_session.to_string()))
    .execute()
    .await
    .unwrap();
    expect_login_failure(
        client,
        &silo_name,
        test_user.clone(),
        hijacked_password.to_string(),
    )
    .await;
    // And we did not modify the admin user's password.
    let _ = expect_login_success(
        client,
        &silo_name,
        admin_user.clone(),
        admin_password.to_string(),
    )
    .await;
    expect_login_failure(
        client,
        &silo_name,
        admin_user.clone(),
        hijacked_password.to_string(),
    )
    .await;

    // But the ordinary user can neither set or invalidate the admin user's
    // password.  (i.e., users cannot reset each other's passwords unless
    // they're administrators).
    expect_session_valid(client, &session_token2).await;
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::FORBIDDEN,
        Method::POST,
        &admin_password_url,
        &test_params::UserPassword::Password(test_password.to_string()),
    )
    .authn_as(AuthnMode::Session(session_token2.clone()))
    .execute()
    .await
    .unwrap();

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::FORBIDDEN,
        Method::POST,
        &admin_password_url,
        &test_params::UserPassword::LoginDisallowed,
    )
    .authn_as(AuthnMode::Session(session_token2.clone()))
    .execute()
    .await
    .unwrap();
}

async fn test_local_user_with_no_initial_password(
    client: &ClientTestContext,
    silo: &views::Silo,
) {
    let silo_name = &silo.identity.name;

    // Create a user with no initial password.
    let test_user = UserId::from_str("steven-falken").unwrap();
    let created_user = create_local_user(
        client,
        silo,
        &test_user,
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Logging in should not work.  (What password would we use, anyway?)
    expect_login_failure(client, &silo_name, test_user.clone(), "".to_string())
        .await;

    // Now, set a password.
    let test_password2 = "joshua";
    let user_password_url = format!(
        "/v1/system/identity-providers/local/users/{}/set-password?silo={}",
        created_user.id, silo_name,
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &user_password_url)
            .expect_status(Some(StatusCode::NO_CONTENT))
            .body(Some(&test_params::UserPassword::Password(
                test_password2.to_string(),
            ))),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Now, we should be able to log in and do things.
    let session_token = expect_login_success(
        client,
        &silo_name,
        test_user.clone(),
        test_password2.to_string(),
    )
    .await;
    let found_user = expect_session_valid(client, &session_token).await;
    assert_eq!(created_user, found_user.user);
}

async fn expect_session_valid(
    client: &ClientTestContext,
    session_token: &str,
) -> views::CurrentUser {
    NexusRequest::object_get(client, "/v1/me")
        .authn_as(AuthnMode::Session(session_token.to_string()))
        .execute_and_parse_unwrap::<views::CurrentUser>()
        .await
}

async fn expect_session_invalid(
    client: &ClientTestContext,
    session_token: &str,
) {
    NexusRequest::expect_failure(
        client,
        StatusCode::UNAUTHORIZED,
        Method::GET,
        "/v1/me",
    )
    .authn_as(AuthnMode::Session(session_token.to_string()))
    .execute()
    .await
    .expect(
        "expected request failure due to invalid session token, found success",
    );
}

async fn expect_login_failure(
    client: &ClientTestContext,
    silo_name: &Name,
    username: UserId,
    password: String,
) {
    let start = std::time::Instant::now();
    let login_url = format!("/v1/login/{}/local", silo_name);
    let error: dropshot::HttpErrorResponseBody =
        NexusRequest::expect_failure_with_body(
            client,
            StatusCode::UNAUTHORIZED,
            Method::POST,
            &login_url,
            &test_params::UsernamePasswordCredentials { username, password },
        )
        .execute()
        .await
        .expect("expected login failure, got success")
        .parsed_body()
        .expect("unexpected error format from login failure");
    let elapsed = start.elapsed();

    assert_eq!(error.message, "credentials missing or invalid");

    // Check that failed login attempts take at least as long as the minimum
    // verification time.  Otherwise, we might have a failure path that exposes a
    // timing attack.  (For example, suppose we returned quickly when you
    // attempted to log in as a user that does not exist.  An attacker could
    // learn whether or not a specific user exists based on how long it took for
    // a login attempt to fail.)
    if elapsed < MIN_EXPECTED_PASSWORD_VERIFY_TIME {
        panic!(
            "failed login attempt unexpectedly took less time ({:?}) than \
             minimum password verification time ({:?})",
            elapsed, MIN_EXPECTED_PASSWORD_VERIFY_TIME
        );
    }
}

async fn expect_login_success(
    client: &ClientTestContext,
    silo_name: &Name,
    username: UserId,
    password: String,
) -> String {
    let start = std::time::Instant::now();
    let login_url = format!("/v1/login/{}/local", silo_name);
    let response = RequestBuilder::new(client, Method::POST, &login_url)
        .body(Some(&test_params::UsernamePasswordCredentials {
            username,
            password,
        }))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("expected successful login, but it failed");
    let elapsed = start.elapsed();
    let cookie_header = response
        .headers
        .get(header::SET_COOKIE)
        .expect("session cookie: missing header")
        .to_str()
        .expect("session cookie: header value was not a string");
    let (token_cookie, rest) = cookie_header
        .split_once("; ")
        .expect("session cookie: bad cookie header value (missing semicolon)");
    assert!(token_cookie.starts_with("session="));
    assert_eq!(rest, "Path=/; HttpOnly; SameSite=Lax; Max-Age=86400");
    let (_, session_token) = token_cookie
        .split_once('=')
        .expect("session cookie: bad cookie header value (missing 'session=')");

    // It's not clear how a successful login could ever take less than the
    // minimum verification time, but we verify it here anyway.  (If we fail
    // here, it's possible that our hash parameters have gotten too weak for the
    // current hardware.  See the similar test in the omicron_passwords module.)
    if elapsed < MIN_EXPECTED_PASSWORD_VERIFY_TIME {
        panic!(
            "successful login unexpectedly took less time ({:?}) than \
             minimum password verification time ({:?})",
            elapsed, MIN_EXPECTED_PASSWORD_VERIFY_TIME
        );
    }

    session_token.to_string()
}
