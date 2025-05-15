// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for authz policy not covered in the set of unauthorized tests

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;

use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use omicron_common::api::external::IdentityMetadataCreateParams;

use dropshot::ResultsPage;
use nexus_test_utils::resource_helpers::{create_local_user, create_silo};

use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Test that a user cannot read other user's SSH keys
#[nexus_test]
async fn test_cannot_read_others_ssh_keys(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a silo with a two unprivileged users
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let user1 = create_local_user(
        client,
        &silo,
        &"user1".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;
    let user2 = create_local_user(
        client,
        &silo,
        &"user2".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    // Create a key for user1

    let name = "akey";
    let description = "authz test";
    let public_key = "AAAAAAAAAAAAAAA";

    // Create a key
    let _new_key: views::SshKey = NexusRequest::objects_post(
        client,
        "/v1/me/ssh-keys",
        &params::SshKeyCreate {
            identity: IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: description.to_string(),
            },
            public_key: public_key.to_string(),
        },
    )
    .authn_as(AuthnMode::SiloUser(user1))
    .execute()
    .await
    .expect("failed to make POST request")
    .parsed_body()
    .unwrap();

    // user1 can read that key
    let _fetched_key: views::SshKey =
        NexusRequest::object_get(client, &format!("/v1/me/ssh-keys/{}", name))
            .authn_as(AuthnMode::SiloUser(user1))
            .execute()
            .await
            .expect("failed to make GET request")
            .parsed_body()
            .unwrap();

    // user2 cannot - they should see 404, not 403
    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::GET,
            &format!("/v1/me/ssh-keys/{}", name),
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user2))
    .execute()
    .await
    .expect("GET request should have failed");

    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::DELETE,
            &format!("/v1/me/ssh-keys/{}", name),
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user2))
    .execute()
    .await
    .expect("GET request should have failed");

    // it also shouldn't show up in their list
    let user2_keys: ResultsPage<views::SshKey> =
        NexusRequest::object_get(client, &"/v1/me/ssh-keys")
            .authn_as(AuthnMode::SiloUser(user2))
            .execute()
            .await
            .expect("failed to make GET request")
            .parsed_body()
            .unwrap();

    assert!(user2_keys.items.is_empty());
}

// Test that an authenticated, unprivileged user can list their silo's users
#[nexus_test]
async fn test_list_silo_users_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &"unpriv".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    // Create another silo with another unprivileged user
    let silo = create_silo(
        &client,
        "other",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    create_local_user(
        client,
        &silo,
        &"otheruser".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Listing users should work
    let users: ResultsPage<views::User> =
        NexusRequest::object_get(client, &"/v1/users")
            .authn_as(AuthnMode::SiloUser(new_silo_user_id))
            .execute()
            .await
            .expect("failed to make GET request")
            .parsed_body()
            .unwrap();

    // And only show the first silo's user
    let user_ids: Vec<Uuid> = users.items.iter().map(|x| x.id).collect();
    assert_eq!(user_ids, vec![new_silo_user_id]);
}

// Test that an authenticated, unprivileged user can list their silo identity
// providers
#[nexus_test]
async fn test_list_silo_idps_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &"unpriv".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    let _users: ResultsPage<views::IdentityProvider> =
        NexusRequest::object_get(
            client,
            &"/v1/system/identity-providers?silo=authz",
        )
        .authn_as(AuthnMode::SiloUser(new_silo_user_id))
        .execute()
        .await
        .expect("failed to make GET request")
        .parsed_body()
        .unwrap();
}

// Test that an authenticated, unprivileged user can access /v1/me
#[nexus_test]
async fn test_session_me_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &"unpriv".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    let _session_user = NexusRequest::object_get(client, &"/v1/me")
        .authn_as(AuthnMode::SiloUser(new_silo_user_id))
        .execute()
        .await
        .expect("failed to make GET request");
}

// Test that an authenticated, unprivileged user can access their own silo
#[nexus_test]
async fn test_silo_read_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    let new_silo_user_id = create_local_user(
        client,
        &silo,
        &"unpriv".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await
    .id;

    // Create another silo
    let _silo = create_silo(
        &client,
        "other",
        true,
        shared::SiloIdentityMode::LocalOnly,
    )
    .await;

    // That user can access their own silo
    let _silo: views::Silo =
        NexusRequest::object_get(client, &"/v1/system/silos/authz")
            .authn_as(AuthnMode::SiloUser(new_silo_user_id))
            .execute()
            .await
            .expect("failed to make GET request")
            .parsed_body()
            .unwrap();

    // But not others
    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::GET,
            &"/v1/system/silos/other",
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("GET request should have failed");
}
