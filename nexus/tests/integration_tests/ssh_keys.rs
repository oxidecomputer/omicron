//! Sanity-tests for public SSH keys

use http::{method::Method, StatusCode};

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_nexus::external_api::params::SshKeyCreate;
use omicron_nexus::external_api::views::SshKey;

// Note: we use UnprivilegedUser in this test because unlike most tests, all the
// endpoints here _can_ be accessed by that user and we want to explicitly
// verify that behavior.
#[nexus_test]
async fn test_ssh_keys(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Ensure we start with an empty list of SSH keys.
    let keys = objects_list_page_authz::<SshKey>(client, "/session/me/sshkeys")
        .await
        .items;
    assert_eq!(keys.len(), 0);

    // Ensure GET fails on non-existent keys.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/session/me/sshkeys/nonexistent",
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to make GET request");

    // Ensure we can POST new keys.
    let new_keys = vec![
        ("key1", "an SSH public key", "ssh-test AAAAAAAA"),
        ("key2", "another SSH public key", "ssh-test BBBBBBBB"),
        ("key3", "yet another public key", "ssh-test CCCCCCCC"),
    ];
    for (name, description, public_key) in &new_keys {
        let new_key: SshKey = NexusRequest::objects_post(
            client,
            "/session/me/sshkeys",
            &SshKeyCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: description.to_string(),
                },
                public_key: public_key.to_string(),
            },
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .expect("failed to make POST request")
        .parsed_body()
        .unwrap();
        assert_eq!(new_key.identity.name.as_str(), *name);
        assert_eq!(new_key.identity.description, *description);
        assert_eq!(new_key.public_key, *public_key);
    }

    // Verify what happens if we try to create one with a conflicting name.
    let error: dropshot::HttpErrorResponseBody =
        NexusRequest::expect_failure_with_body(
            client,
            http::StatusCode::BAD_REQUEST,
            http::Method::POST,
            "/session/me/sshkeys",
            &SshKeyCreate {
                identity: IdentityMetadataCreateParams {
                    name: "key1".parse().unwrap(),
                    description: String::from("a fourth public key"),
                },
                public_key: String::from("ssh-test DDDDDDDD"),
            },
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .expect(
            "unexpected failure trying to create ssh key with conflicting name",
        )
        .parsed_body()
        .unwrap();
    assert_eq!(error.error_code, Some(String::from("ObjectAlreadyExists")));
    assert_eq!(error.message, "already exists: ssh-key \"key1\"");

    // Ensure we can GET one of the keys we just posted.
    let key1: SshKey = NexusRequest::object_get(
        client,
        &format!("/session/me/sshkeys/{}", new_keys[0].0),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to make GET request")
    .parsed_body()
    .unwrap();
    assert_eq!(key1.identity.name.as_str(), new_keys[0].0);
    assert_eq!(key1.identity.description, new_keys[0].1);
    assert_eq!(key1.public_key, new_keys[0].2);

    // Ensure we can GET the list of keys we just posted.
    // TODO-coverage: pagination
    let keys: Vec<SshKey> = NexusRequest::object_get(
        client,
        "/session/me/sshkeys?sort_by=name_ascending",
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("fetching ssh keys")
    .parsed_body::<dropshot::ResultsPage<SshKey>>()
    .expect("parsing list of ssh keys")
    .items;
    assert_eq!(keys.len(), new_keys.len());
    for (key, (name, description, public_key)) in
        keys.iter().zip(new_keys.iter())
    {
        assert_eq!(key.identity.name.as_str(), *name);
        assert_eq!(key.identity.description, *description);
        assert_eq!(key.public_key, *public_key);
    }

    // Ensure we can DELETE a key.
    let deleted_key_name = new_keys[0].0;
    NexusRequest::object_delete(
        client,
        &format!("/session/me/sshkeys/{}", deleted_key_name),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to DELETE key");

    // Ensure that we can't GET the key we just deleted.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &format!("/session/me/sshkeys/{}", deleted_key_name),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to make GET request");
}
