// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_db_queries::authn::USER_TEST_UNPRIVILEGED;
use nexus_db_queries::db::identity::Asset;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::disk;
use nexus_types::external_api::policy;
use nexus_types::external_api::service_account::ServiceAccount;
use nexus_types::external_api::service_account::ServiceAccountCreate;
use nexus_types::external_api::service_account::ServiceAccountKey;
use nexus_types::external_api::service_account::ServiceAccountKeyCreate;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::IdentityMetadataCreateParams;
use oxide_http_signatures::Components;
use oxide_http_signatures::SigningKey;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

async fn signed_request(
    client: &ClientTestContext,
    key_id: Uuid,
    key: &SigningKey,
    method: reqwest::Method,
    path_and_query: &str,
    signed_body: Option<&str>,
    sent_body: Option<&str>,
) -> reqwest::Response {
    let authority = client.bind_address.to_string();
    let url =
        reqwest::Url::parse(&format!("http://{authority}{path_and_query}"))
            .unwrap();
    let digest = oxide_http_signatures::content_digest(
        signed_body.unwrap_or("").as_bytes(),
    );
    let headers = oxide_http_signatures::sign(
        key,
        &key_id.to_string(),
        Utc::now().timestamp(),
        None,
        &Components {
            method: method.as_str(),
            authority: &authority,
            path: url.path(),
            query: url.query(),
            content_digest: &digest,
        },
    );
    let mut request = reqwest::Client::new()
        .request(method, url.clone())
        .header(oxide_http_signatures::CONTENT_DIGEST_HEADER, digest)
        .header(
            oxide_http_signatures::SIGNATURE_INPUT_HEADER,
            headers.signature_input,
        )
        .header(oxide_http_signatures::SIGNATURE_HEADER, headers.signature);
    if let Some(body) = sent_body {
        request = request
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body.to_string());
    }
    request.send().await.unwrap()
}

#[nexus_test]
async fn test_service_account_request_signing(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project(client, "sa-project").await;
    create_project(client, "other-project").await;

    let service_account: ServiceAccount = NexusRequest::objects_post(
        client,
        "/v1/service-accounts?project=sa-project",
        &ServiceAccountCreate {
            identity: IdentityMetadataCreateParams {
                name: "ci".parse().unwrap(),
                description: String::from("ci bot"),
            },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;

    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let key: ServiceAccountKey = NexusRequest::objects_post(
        client,
        &format!("/v1/service-accounts/{}/keys", service_account.identity.id),
        &ServiceAccountKeyCreate {
            identity: IdentityMetadataCreateParams {
                name: "ci-key".parse().unwrap(),
                description: String::new(),
            },
            public_key: oxide_http_signatures::verifying_key_to_pem(
                &signing_key.verifying_key(),
            ),
            ttl_seconds: None,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;
    let key_id = key.identity.id;

    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::GET,
        "/v1/projects/sa-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let policy_url = "/v1/projects/sa-project/policy";
    let mut project_policy: policy::Policy<policy::ProjectRole> =
        NexusRequest::object_get(client, policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;
    project_policy.role_assignments.push(
        policy::RoleAssignment::for_service_account(
            service_account.identity.id,
            policy::ProjectRole::Collaborator,
        ),
    );
    NexusRequest::object_put(client, policy_url, Some(&project_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::GET,
        "/v1/projects/sa-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    for method in [Method::GET, Method::PUT] {
        let mut builder =
            RequestBuilder::new(client, method.clone(), policy_url)
                .header("api-version", "2026111000.0.0")
                .expect_status(Some(StatusCode::BAD_REQUEST));
        if method == Method::PUT {
            builder = builder.body(Some(&serde_json::json!({
                "role_assignments": []
            })));
        }
        NexusRequest::new(builder)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap();
    }
    let still_there: policy::Policy<policy::ProjectRole> =
        NexusRequest::object_get(client, policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;
    assert!(
        still_there.role_assignments.iter().any(|r| {
            r.identity_type == policy::IdentityType::ServiceAccount
        })
    );

    let body = r#"{"name":"example-name","description":"example-description","policy":"allow","failure_domain":"sled"}"#;
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::POST,
        "/v1/anti-affinity-groups?project=sa-project",
        Some(body),
        Some(body),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let tampered = body.replace("example-name", "tampered-name");
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::POST,
        "/v1/anti-affinity-groups?project=sa-project",
        Some(body),
        Some(&tampered),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let _disk: Disk = object_create(
        client,
        "/v1/disks?project=sa-project",
        &disk::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "import-disk".parse().unwrap(),
                description: String::new(),
            },
            disk_backend: disk::DiskBackend::Distributed {
                disk_source: disk::DiskSource::ImportingBlocks {
                    block_size: disk::BlockSize::try_from(512).unwrap(),
                },
            },
            size: ByteCount::from_gibibytes_u32(1),
        },
    )
    .await;
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::POST,
        "/v1/disks/import-disk/bulk-write-start?project=sa-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let chunk = serde_json::to_string(&disk::ImportBlocksBulkWrite {
        offset: 0,
        base64_encoded_data: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            vec![0x5a; 4096 * 1024],
        ),
    })
    .unwrap();
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::POST,
        "/v1/disks/import-disk/bulk-write?project=sa-project",
        Some(&chunk),
        Some(&chunk),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::POST,
        "/v1/disks/import-disk/bulk-write-stop?project=sa-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = signed_request(
        client,
        key_id,
        &SigningKey::from_bytes(&[8u8; 32]),
        reqwest::Method::GET,
        "/v1/projects/sa-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let mut project_policy: policy::Policy<policy::ProjectRole> =
        NexusRequest::object_get(client, policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;
    for r in &mut project_policy.role_assignments {
        if r.identity_id == service_account.identity.id {
            r.role_name = policy::ProjectRole::Admin;
        }
    }
    NexusRequest::object_put(client, policy_url, Some(&project_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    let key_create = serde_json::to_string(&ServiceAccountKeyCreate {
        identity: IdentityMetadataCreateParams {
            name: "minted-key".parse().unwrap(),
            description: String::new(),
        },
        public_key: oxide_http_signatures::verifying_key_to_pem(
            &SigningKey::from_bytes(&[9u8; 32]).verifying_key(),
        ),
        ttl_seconds: None,
    })
    .unwrap();
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::POST,
        &format!("/v1/service-accounts/{}/keys", service_account.identity.id),
        Some(&key_create),
        Some(&key_create),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    grant_iam(
        client,
        "/v1/projects/other-project",
        policy::ProjectRole::Admin,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;
    let other_policy_url = "/v1/projects/other-project/policy";
    let mut other_policy: policy::Policy<policy::ProjectRole> =
        NexusRequest::object_get(client, other_policy_url)
            .authn_as(AuthnMode::UnprivilegedUser)
            .execute_and_parse_unwrap()
            .await;
    other_policy.role_assignments.push(
        policy::RoleAssignment::for_service_account(
            service_account.identity.id,
            policy::ProjectRole::Viewer,
        ),
    );
    NexusRequest::object_put(client, other_policy_url, Some(&other_policy))
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap();
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::GET,
        "/v1/projects/other-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::GET,
        "/v1/system/silos",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let mut fleet_policy: policy::Policy<policy::FleetRole> =
        NexusRequest::object_get(client, "/v1/system/policy")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;
    fleet_policy.role_assignments.push(
        policy::RoleAssignment::for_service_account(
            service_account.identity.id,
            policy::FleetRole::Viewer,
        ),
    );
    NexusRequest::object_put(client, "/v1/system/policy", Some(&fleet_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::GET,
        "/v1/system/silos",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, "/v1/system/policy")
            .header("api-version", "2026111000.0.0")
            .body(Some(&serde_json::json!({ "role_assignments": [] })))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    NexusRequest::object_delete(
        client,
        &format!(
            "/v1/service-accounts/{}/keys/{}",
            service_account.identity.id, key_id
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    let response = signed_request(
        client,
        key_id,
        &signing_key,
        reqwest::Method::GET,
        "/v1/projects/sa-project",
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
