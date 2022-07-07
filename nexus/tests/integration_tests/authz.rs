// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for authz policy not covered in the set of unauthorized tests

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::views;
use omicron_nexus::TestInterfaces;

use dropshot::ResultsPage;
use nexus_test_utils::resource_helpers::create_silo;

use httptest::{matchers::*, responders::*, Expectation, ServerBuilder};

use uuid::Uuid;


// Test that an authenticated, unprivileged user has full CRUD access to their SSH keys
#[nexus_test]
async fn test_ssh_key_crud_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    let new_silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            "unpriv".into(),
        )
        .await
        .unwrap();

    let name = "akey";
    let description = "authz test";
    let public_key = "AAAAAAAAAAAAAAA";

    // Create a key
    let _new_key: views::SshKey = NexusRequest::objects_post(
        client,
        "/session/me/sshkeys",
        &params::SshKeyCreate {
            identity: IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: description.to_string(),
            },
            public_key: public_key.to_string(),
        },
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make POST request")
    .parsed_body()
    .unwrap();

    // Fetch that key
    let _fetched_key: views::SshKey = NexusRequest::object_get(
        client,
        &format!("/session/me/sshkeys/{}", name),
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();

    // List keys
    let _keys: ResultsPage<views::SshKey> = NexusRequest::object_get(
        client,
        &"/session/me/sshkeys",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();

    // Delete the key
    NexusRequest::object_delete(
        client,
        &format!("/session/me/sshkeys/{}", name),
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to DELETE key");
}

// Test that an authenticated, unprivileged user can list and read global images
#[nexus_test]
async fn test_global_image_read_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    let new_silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            "unpriv".into(),
        )
        .await
        .unwrap();

    // Create a global image
    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header(
                    "Content-Length",
                    format!("{}", 4096 * 1000),
                ),
            ),
    );

    let image_create_params = params::GlobalImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        distribution: params::Distribution {
            name: "alpine".parse().unwrap(),
            version: "edge".into(),
        },
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    NexusRequest::objects_post(client, "/images", &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // List images
    let _images: ResultsPage<views::GlobalImage> = NexusRequest::object_get(
        client,
        &"/images",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();

    // Read an image
    let _image: views::GlobalImage = NexusRequest::object_get(
        client,
        &"/images/alpine-edge",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();
}

// Test that an authenticated, unprivileged user can list silo users
#[nexus_test]
async fn test_list_silo_users_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    let new_silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            "unpriv".into(),
        )
        .await
        .unwrap();

    let _users: ResultsPage<views::User> = NexusRequest::object_get(
        client,
        &"/users",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();
}

// Test that an authenticated, unprivileged user can list silo identity
// providers
#[nexus_test]
async fn test_list_silo_idps_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    let new_silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            "unpriv".into(),
        )
        .await
        .unwrap();

    let _users: ResultsPage<views::IdentityProvider> = NexusRequest::object_get(
        client,
        &"/silos/authz/identity_providers",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();
}

// Test that an authenticated, unprivileged user can access /session/me
#[nexus_test]
async fn test_session_me_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    let new_silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            "unpriv".into(),
        )
        .await
        .unwrap();

    let _session_user: views::SessionUser = NexusRequest::object_get(
        client,
        &"/session/me",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();
}

// Test that an authenticated, unprivileged user can access their own silo
#[nexus_test]
async fn test_silo_read_for_unpriv(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;

    // Create a silo with an unprivileged user
    let silo = create_silo(
        &client,
        "authz",
        true,
        shared::UserProvisionType::Fixed,
    )
    .await;

    let new_silo_user_id = Uuid::new_v4();
    nexus
        .silo_user_create(
            silo.identity.id,
            new_silo_user_id,
            "unpriv".into(),
        )
        .await
        .unwrap();

    let _silo: views::Silo = NexusRequest::object_get(
        client,
        &"/silos/authz",
    )
    .authn_as(AuthnMode::SiloUser(new_silo_user_id))
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();
}
