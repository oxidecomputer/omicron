// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ResultsPage;
use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{
    AuthnMode, NexusRequest, RequestBuilder, TestResponse,
};
use nexus_test_utils::resource_helpers::{
    create_local_user, create_silo, grant_iam, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::federation::{
    FederationIdentityProvider, FederationTrustPolicy,
};
use nexus_types::external_api::policy::SiloRole;
use nexus_types::external_api::silo::SiloIdentityMode;
use serde_json::{Value, json};
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
const URL: &str = "/v1/federation/inbound/trust-policies";
const IDPS: &str = "/v1/federation/inbound/identity-providers";
const POLICY: &str = "assume(claims) if claims.sub = \"builder\";";

async fn request(
    client: &ClientTestContext,
    authn: AuthnMode,
    method: Method,
    url: &str,
    body: Option<&Value>,
    status: StatusCode,
) -> TestResponse {
    NexusRequest::new(
        RequestBuilder::new(client, method, url)
            .body(body)
            .expect_status(Some(status)),
    )
    .authn_as(authn)
    .execute()
    .await
    .unwrap()
}

async fn silo_user(
    client: &ClientTestContext,
    name: &str,
    role: SiloRole,
) -> (Uuid, AuthnMode) {
    let silo = create_silo(client, name, SiloIdentityMode::LocalOnly).await;
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        &format!("/v1/system/silos/{name}"),
        role,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;
    (silo.identity.id, AuthnMode::SiloUser(user.id))
}

async fn idp(
    client: &ClientTestContext,
    authn: AuthnMode,
    name: &str,
) -> FederationIdentityProvider {
    request(client, authn, Method::POST, IDPS, Some(&json!({
        "name": name, "description": "test", "issuer": "https://issuer.example.com", "audience": "oxide",
        "verification_type": "oidc_discovery"
    })), StatusCode::CREATED).await.parsed_body().unwrap()
}

fn params(idp: &str, grants: Value) -> Value {
    json!({"name": "test-policy", "description": "test", "identity_provider": idp, "policy": POLICY, "grants": grants})
}

async fn get(
    client: &ClientTestContext,
    authn: AuthnMode,
    url: &str,
) -> FederationTrustPolicy {
    request(client, authn, Method::GET, url, None, StatusCode::OK)
        .await
        .parsed_body()
        .unwrap()
}

async fn patch(
    client: &ClientTestContext,
    authn: AuthnMode,
    url: &str,
    body: Value,
) -> FederationTrustPolicy {
    request(client, authn, Method::PATCH, url, Some(&body), StatusCode::OK)
        .await
        .parsed_body()
        .unwrap()
}

#[nexus_test]
async fn test_federation_trust_policy_crud(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let (silo_id, admin) =
        silo_user(client, "federation", SiloRole::Admin).await;
    let provider = idp(client, admin.clone(), "test-idp").await;
    let provider_url = format!("{IDPS}/{}", provider.identity.id);
    let project: Value = request(
        client,
        admin.clone(),
        Method::POST,
        "/v1/projects",
        Some(&json!({"name": "target", "description": "test", "defaults": {}})),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let grants = json!([
        {"resource_kind": "silo", "resource_id": silo_id, "role_name": "viewer"},
        {"resource_kind": "project", "resource_id": project["id"], "role_name": "collaborator"}
    ]);
    for policy in
        ["broken(", "check_claims(_claims);", "assume(_claims, _resource);"]
    {
        let mut invalid = params("test-idp", grants.clone());
        invalid["policy"] = json!(policy);
        request(
            client,
            admin.clone(),
            Method::POST,
            URL,
            Some(&invalid),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let create = params("test-idp", grants.clone());
    let created: FederationTrustPolicy = request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&create),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let url = format!("{URL}/{}", created.identity.id);
    assert_eq!(created.revision, 1);
    assert_eq!(created.idp_id, provider.identity.id);
    assert_eq!(created.grants.len(), 2);
    let fetched = get(client, admin.clone(), &url).await;
    assert_eq!(fetched.grants, created.grants);
    request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&create),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let updated = patch(
        client,
        admin.clone(),
        &url,
        json!({"name": "renamed", "description": "metadata"}),
    )
    .await;
    assert_eq!(updated.revision, 1);
    assert_eq!(updated.identity.time_created, created.identity.time_created);
    assert!(updated.identity.time_modified >= created.identity.time_modified);
    let reversed = json!([grants[1], grants[0]]);
    let unchanged = patch(client, admin.clone(), &url, json!({"grants": reversed, "policy": POLICY, "identity_provider": provider.identity.id})).await;
    assert_eq!(unchanged.revision, 1);
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &format!("/v1/projects/{}", project["id"].as_str().unwrap()),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    let after_delete = get(client, admin.clone(), &url).await;
    assert_eq!(after_delete.revision, 1);
    assert_eq!(after_delete.grants, created.grants);
    let recreated: Value = request(client, admin.clone(), Method::POST, "/v1/projects", Some(&json!({"name": "target", "description": "recreated", "defaults": {}})), StatusCode::CREATED).await.parsed_body().unwrap();
    assert_ne!(recreated["id"], project["id"]);
    assert_eq!(get(client, admin.clone(), &url).await.grants, created.grants);

    for bad in [
        json!({"policy": "broken("}),
        json!({"policy": "check_claims(_claims);"}),
        json!({"policy": "assume(_claims, _resource);"}),
        json!({"policy": "assume(_claims); ?= assume({});"}),
        json!({"grants": [grants[0], grants[0]]}),
        json!({"grants": [{"resource_kind": "fleet", "resource_id": silo_id, "role_name": "admin"}]}),
        json!({"grants": [{"resource_kind": "silo", "resource_id": silo_id, "role_name": "made_up"}]}),
        json!({"grants": [{"resource_kind": "project", "resource_id": Uuid::new_v4(), "role_name": "viewer"}]}),
        json!({"revision": 99}),
    ] {
        request(
            client,
            admin.clone(),
            Method::PATCH,
            &url,
            Some(&bad),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let unchanged = get(client, admin.clone(), &url).await;
    assert_eq!(unchanged.revision, 1);
    assert_eq!(unchanged.grants, created.grants);
    assert_eq!(unchanged.policy, POLICY);
    let updated =
        patch(client, admin.clone(), &url, json!({"grants": [grants[0]]}))
            .await;
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.grants.len(), 1);
    let updated = patch(
        client,
        admin.clone(),
        &url,
        json!({"policy": "assume(claims) if claims.sub = \"other\";"}),
    )
    .await;
    assert_eq!(updated.revision, 3);
    let replacement = idp(client, admin.clone(), "replacement").await;
    let updated = patch(
        client,
        admin.clone(),
        &url,
        json!({"identity_provider": replacement.identity.id}),
    )
    .await;
    assert_eq!(updated.revision, 4);
    assert_eq!(updated.idp_id, replacement.identity.id);
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &provider_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &url,
        Some(&json!({"identity_provider": provider.identity.id, "grants": []})),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(get(client, admin.clone(), &url).await.revision, 4);
    let updated =
        patch(client, admin.clone(), &url, json!({"grants": []})).await;
    assert_eq!(updated.revision, 5);
    assert!(updated.grants.is_empty());

    let second: FederationTrustPolicy = request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&params("replacement", json!([]))),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let page: ResultsPage<FederationTrustPolicy> = request(
        client,
        admin.clone(),
        Method::GET,
        &format!("{URL}?limit=1"),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].identity.id, created.identity.id);
    assert_eq!(page.items[0].revision, 5);
    let next: ResultsPage<FederationTrustPolicy> = request(
        client,
        admin.clone(),
        Method::GET,
        &format!("{URL}?page_token={}", page.next_page.unwrap()),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(next.items[0].identity.id, second.identity.id);
    let replacement_url = format!("{IDPS}/{}", replacement.identity.id);
    for policy_url in [&url, &format!("{URL}/{}", second.identity.id)] {
        request(
            client,
            admin.clone(),
            Method::DELETE,
            policy_url,
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
    }
    for (method, body) in [
        (Method::GET, None),
        (Method::PATCH, Some(json!({"description": "gone"}))),
        (Method::DELETE, None),
    ] {
        request(
            client,
            admin.clone(),
            method,
            &url,
            body.as_ref(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    let reused: FederationTrustPolicy = request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&params("replacement", json!([]))),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_ne!(reused.identity.id, second.identity.id);
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &format!("{URL}/{}", reused.identity.id),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    request(
        client,
        admin,
        Method::DELETE,
        &replacement_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
}

#[nexus_test]
async fn test_federation_trust_policy_silo_isolation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let (owner_id, owner) = silo_user(client, "owner", SiloRole::Admin).await;
    let (other_id, other) = silo_user(client, "other", SiloRole::Admin).await;
    let provider = idp(client, owner.clone(), "test-idp").await;
    let other_provider = idp(client, other.clone(), "test-idp").await;
    let create = params("test-idp", json!([]));
    let policy: FederationTrustPolicy = request(
        client,
        owner.clone(),
        Method::POST,
        URL,
        Some(&create),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let url = format!("{URL}/{}", policy.identity.id);
    for (method, body) in [
        (Method::GET, None),
        (Method::PATCH, Some(json!({"description": "stolen"}))),
        (Method::DELETE, None),
    ] {
        request(
            client,
            other.clone(),
            method,
            &url,
            body.as_ref(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    let page: ResultsPage<FederationTrustPolicy> =
        request(client, other.clone(), Method::GET, URL, None, StatusCode::OK)
            .await
            .parsed_body()
            .unwrap();
    assert!(page.items.is_empty());
    let other_project: Value = request(client, other.clone(), Method::POST, "/v1/projects", Some(&json!({"name": "foreign-project", "description": "test", "defaults": {}})), StatusCode::CREATED).await.parsed_body().unwrap();
    for bad_grant in [
        json!({"resource_kind": "silo", "resource_id": other_id, "role_name": "admin"}),
        json!({"resource_kind": "project", "resource_id": other_project["id"], "role_name": "admin"}),
    ] {
        request(
            client,
            owner.clone(),
            Method::POST,
            URL,
            Some(&params("test-idp", json!([bad_grant]))),
            StatusCode::BAD_REQUEST,
        )
        .await;
        request(
            client,
            owner.clone(),
            Method::PATCH,
            &url,
            Some(&json!({"grants": [bad_grant]})),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    request(
        client,
        other.clone(),
        Method::POST,
        URL,
        Some(&params(&provider.identity.id.to_string(), json!([]))),
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        client,
        owner.clone(),
        Method::PATCH,
        &url,
        Some(&json!({"identity_provider": other_provider.identity.id})),
        StatusCode::NOT_FOUND,
    )
    .await;
    let other_policy: FederationTrustPolicy = request(
        client,
        other.clone(),
        Method::POST,
        URL,
        Some(&create),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(other_policy.idp_id, other_provider.identity.id);
    let owner_policy = get(client, owner.clone(), &url).await;
    assert_eq!(owner_policy.idp_id, provider.identity.id);
    assert_eq!(owner_policy.revision, 1);
    assert!(owner_policy.grants.is_empty());
    for (name, role) in
        [("viewer", SiloRole::Viewer), ("collaborator", SiloRole::Collaborator)]
    {
        let (_, user) = silo_user(client, name, role).await;
        for (method, endpoint, body) in [
            (Method::GET, URL, None),
            (Method::POST, URL, Some(create.clone())),
            (Method::GET, url.as_str(), None),
            (
                Method::PATCH,
                url.as_str(),
                Some(json!({"description": "changed"})),
            ),
            (Method::DELETE, url.as_str(), None),
        ] {
            request(
                client,
                user.clone(),
                method,
                endpoint,
                body.as_ref(),
                StatusCode::FORBIDDEN,
            )
            .await;
        }
    }
    let grant = json!({"resource_kind": "silo", "resource_id": owner_id, "role_name": "limited_collaborator"});
    let updated = patch(client, owner, &url, json!({"grants": [grant]})).await;
    assert_eq!(updated.revision, 2);
}

#[nexus_test]
async fn test_federation_trust_policy_deleted_idp(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let (silo_id, admin) =
        silo_user(client, "deleted-idp", SiloRole::Admin).await;
    let provider = idp(client, admin.clone(), "test-idp").await;
    let grants = json!([{"resource_kind": "silo", "resource_id": silo_id, "role_name": "viewer"}]);
    let created: FederationTrustPolicy = request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&params("test-idp", grants.clone())),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let url = format!("{URL}/{}", created.identity.id);
    let provider_url = format!("{IDPS}/{}", provider.identity.id);
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &provider_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::GET,
        &provider_url,
        None,
        StatusCode::NOT_FOUND,
    )
    .await;
    let retained = get(client, admin.clone(), &url).await;
    assert_eq!(retained.idp_id, provider.identity.id);
    assert_eq!(retained.revision, created.revision);
    assert_eq!(retained.grants, created.grants);
    assert_eq!(retained.policy, created.policy);
    assert_eq!(retained.identity.time_modified, created.identity.time_modified);
    let mut invalid = params("test-idp", grants.clone());
    invalid["name"] = json!("another-policy");
    request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&invalid),
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &url,
        Some(&json!({"identity_provider": "test-idp"})),
        StatusCode::NOT_FOUND,
    )
    .await;

    let replacement = idp(client, admin.clone(), "test-idp").await;
    assert_ne!(replacement.identity.id, provider.identity.id);
    let retained = patch(
        client,
        admin.clone(),
        &url,
        json!({"description": "still editable"}),
    )
    .await;
    assert_eq!(retained.idp_id, provider.identity.id);
    assert_eq!(retained.revision, created.revision);
    assert_eq!(retained.grants, created.grants);
    invalid["identity_provider"] = json!(provider.identity.id);
    request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&invalid),
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &url,
        Some(&json!({"identity_provider": provider.identity.id})),
        StatusCode::NOT_FOUND,
    )
    .await;
    let updated =
        patch(client, admin, &url, json!({"identity_provider": "test-idp"}))
            .await;
    assert_eq!(updated.idp_id, replacement.identity.id);
    assert_eq!(updated.revision, created.revision + 1);
    assert_eq!(updated.grants, created.grants);
}
