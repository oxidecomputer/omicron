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
use nexus_types::external_api::federation::FederationIdentityProvider;
use nexus_types::external_api::policy::SiloRole;
use nexus_types::external_api::silo::SiloIdentityMode;
use serde_json::{Value, json};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
const URL: &str = "/v1/federation/inbound/identity-providers";

fn keys(kid: &str) -> Value {
    json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": kid,
        "x": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}]})
}

fn create_params() -> Value {
    json!({"name": "test-idp", "description": "external workloads",
        "issuer": "https://issuer.example.com", "audience": "oxide",
        "verification_type": "static_jwks", "signing_keys": keys("old")})
}

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
) -> AuthnMode {
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
    AuthnMode::SiloUser(user.id)
}

#[nexus_test]
async fn test_federation_identity_provider_crud(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let admin = silo_user(client, "federation", SiloRole::Admin).await;
    let provider: FederationIdentityProvider = request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&create_params()),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let url = format!("{URL}/{}", provider.identity.id);
    request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&create_params()),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let fetched: FederationIdentityProvider =
        request(client, admin.clone(), Method::GET, &url, None, StatusCode::OK)
            .await
            .parsed_body()
            .unwrap();
    assert_eq!(fetched.identity.id, provider.identity.id);

    let rotated: FederationIdentityProvider = request(client, admin.clone(), Method::PATCH, &url, Some(&json!({"name": "renamed", "description": "rotated", "signing_keys": keys("new")})), StatusCode::OK).await.parsed_body().unwrap();
    assert_eq!(rotated.identity.id, provider.identity.id);
    assert_eq!(rotated.identity.time_created, provider.identity.time_created);
    assert!(rotated.identity.time_modified >= provider.identity.time_modified);
    assert_eq!(rotated.issuer, provider.issuer);
    assert_eq!(rotated.audience, provider.audience);
    assert_eq!(rotated.signing_keys, Some(keys("new")));
    for field in
        ["issuer", "audience", "verification_type", "discovery_url", "silo_id"]
    {
        request(
            client,
            admin.clone(),
            Method::PATCH,
            &url,
            Some(&json!({field: "changed"})),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    let mut discovery = create_params();
    discovery["verification_type"] = json!("oidc_discovery");
    discovery["signing_keys"] = Value::Null;
    let discovered: FederationIdentityProvider = request(
        client,
        admin.clone(),
        Method::POST,
        URL,
        Some(&discovery),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let discovered_url = format!("{URL}/{}", discovered.identity.id);
    let error: dropshot::HttpErrorResponseBody = request(
        client,
        admin.clone(),
        Method::PATCH,
        &discovered_url,
        Some(&json!({"signing_keys": keys("new")})),
        StatusCode::BAD_REQUEST,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        "signing_keys can only be updated for static_jwks providers",
    );
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &discovered_url,
        Some(&json!({"name": "renamed"})),
        StatusCode::BAD_REQUEST,
    )
    .await;

    let page: ResultsPage<FederationIdentityProvider> = request(
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
    assert_eq!(page.items[0].identity.name.as_str(), "renamed");
    let next: ResultsPage<FederationIdentityProvider> = request(
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
    assert_eq!(next.items[0].identity.id, discovered.identity.id);

    request(
        client,
        admin.clone(),
        Method::DELETE,
        &url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::GET,
        &url,
        None,
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &url,
        Some(&json!({"description": "gone"})),
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &url,
        None,
        StatusCode::NOT_FOUND,
    )
    .await;
    let mut reused = create_params();
    reused["name"] = json!("renamed");
    let replacement: FederationIdentityProvider = request(
        client,
        admin,
        Method::POST,
        URL,
        Some(&reused),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_ne!(replacement.identity.id, provider.identity.id);
}

#[nexus_test]
async fn test_federation_identity_provider_silo_isolation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let owner = silo_user(client, "owner", SiloRole::Admin).await;
    let other = silo_user(client, "other", SiloRole::Admin).await;
    let provider: FederationIdentityProvider = request(
        client,
        owner,
        Method::POST,
        URL,
        Some(&create_params()),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let url = format!("{URL}/{}", provider.identity.id);
    for (method, body) in [
        (Method::GET, None),
        (Method::PATCH, Some(json!({"name": "stolen"}))),
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
    let page: ResultsPage<FederationIdentityProvider> =
        request(client, other.clone(), Method::GET, URL, None, StatusCode::OK)
            .await
            .parsed_body()
            .unwrap();
    assert!(page.items.is_empty());
    request(
        client,
        other,
        Method::POST,
        URL,
        Some(&create_params()),
        StatusCode::CREATED,
    )
    .await;
    for (name, role) in
        [("viewer", SiloRole::Viewer), ("collaborator", SiloRole::Collaborator)]
    {
        let user = silo_user(client, name, role).await;
        for (method, endpoint, body) in [
            (Method::POST, URL, Some(create_params())),
            (Method::GET, URL, None),
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
    NexusRequest::expect_failure(
        client,
        StatusCode::UNAUTHORIZED,
        Method::GET,
        URL,
    )
    .execute()
    .await
    .unwrap();
}
