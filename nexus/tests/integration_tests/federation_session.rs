// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::federation_trust_policy::{request, silo_user};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use nexus_db_model::FederationSession;
use nexus_db_schema::schema::{audit_log, federation_session};
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::federation::{
    FederationIdentityProvider, FederationToken, FederationTrustPolicy,
};
use nexus_types::external_api::policy::SiloRole;
use serde_json::{Value, json};
use uuid::Uuid;

pub const TOKEN_ENDPOINT: &str = "/v1/federation/inbound/token";
type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

fn signing_key() -> (EncodingKey, Value) {
    let pair = openssl::rsa::Rsa::generate(2048).unwrap();
    let key = EncodingKey::from_rsa_der(&pair.private_key_to_der().unwrap());
    let mut jwk = Jwk::from_encoding_key(&key, Algorithm::RS256).unwrap();
    jwk.common.key_id = Some("test-key".into());
    (key, serde_json::to_value(JwkSet { keys: vec![jwk] }).unwrap())
}

fn signed(key: &EncodingKey, claims: &Value) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-key".into());
    encode(&header, claims, key).unwrap()
}

async fn exchange(
    client: &ClientTestContext,
    host: &str,
    policy: Uuid,
    key: &EncodingKey,
) -> FederationToken {
    let now = Utc::now().timestamp();
    let jwt = signed(
        key,
        &json!({
            "iss": "https://issuer.example", "aud": "oxide", "sub": "builder",
            "iat": now, "exp": now + 300,
        }),
    );
    RequestBuilder::new(client, Method::POST, TOKEN_ENDPOINT)
        .header("host", host)
        .body(Some(&json!({"trust_policy": policy, "oidc_jwt": jwt})))
        .expect_status(Some(StatusCode::CREATED))
        .expect_response_header(http::header::CACHE_CONTROL, "no-store")
        .expect_response_header(http::header::PRAGMA, "no-cache")
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn bearer_request(
    client: &ClientTestContext,
    token: &str,
    method: Method,
    url: &str,
    body: Option<&Value>,
    status: StatusCode,
) -> TestResponse {
    RequestBuilder::new(client, method, url)
        .header("authorization", format!("Bearer {token}"))
        .body(body)
        .expect_status(Some(status))
        .execute()
        .await
        .unwrap()
}

#[nexus_test]
async fn test_federation_session_project_viewer(ctx: &ControlPlaneTestContext) {
    use nexus_types::external_api::project::Project;

    let client = &ctx.external_client;
    let (_, admin) =
        silo_user(client, "federation-viewer", SiloRole::Admin).await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("federation-viewer.sys.{}", ctx.external_dns_zone_name);
    let instance_params = json!({
        "name": "workload", "description": "test", "ncpus": 4,
        "memory": 1073741824, "hostname": "workload",
        "network_interfaces": {"type": "none"}, "start": false,
        "ssh_public_keys": [],
    });
    let project: Project = request(
        client,
        admin.clone(),
        Method::POST,
        "/v1/projects",
        Some(
            &json!({"name": "allowed", "description": "test", "defaults": {}}),
        ),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let project_id = project.identity.id;
    let instances_url = format!("/v1/instances?project={project_id}");
    let instance: Value = request(
        client,
        admin.clone(),
        Method::POST,
        &instances_url,
        Some(&instance_params),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let instance_url =
        format!("/v1/instances/{}", instance["id"].as_str().unwrap());
    let (key, keys) = signing_key();
    request(client, admin.clone(), Method::POST,
        "/v1/federation/inbound/identity-providers", Some(&json!({
            "name": "idp", "description": "test", "issuer": "https://issuer.example", "audience": "oxide",
            "verification_type": "static_jwks", "signing_keys": keys,
        })), StatusCode::CREATED).await;
    let policy: FederationTrustPolicy = request(client, admin, Method::POST,
        "/v1/federation/inbound/trust-policies", Some(&json!({
            "name": "reader", "description": "test", "identity_provider": "idp",
            "policy": "assume(claims) if claims.sub = \"builder\";",
            "grants": [{"resource_kind": "project", "resource_id": project_id, "role_name": "viewer"}],
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let token = exchange(client, &host, policy.identity.id, &key).await;

    let page: dropshot::ResultsPage<Value> = bearer_request(
        client,
        &token.token,
        Method::GET,
        &instances_url,
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0]["id"], instance["id"]);
    let item: Value = bearer_request(
        client,
        &token.token,
        Method::GET,
        &instance_url,
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(item["id"], instance["id"]);
}

#[nexus_test]
async fn test_federation_session_authentication(ctx: &ControlPlaneTestContext) {
    use nexus_db_schema::schema::{federation_identity_provider, silo};
    use nexus_types::external_api::audit::{AuditLogEntryActor, AuthMethod};
    use nexus_types::external_api::project::Project;
    use nexus_types::external_api::vpc::Vpc;

    let client = &ctx.external_client;
    let (silo_id, admin) =
        silo_user(client, "federation-auth", SiloRole::Admin).await;
    let (_, other_admin) =
        silo_user(client, "federation-other", SiloRole::Admin).await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("federation-auth.sys.{}", ctx.external_dns_zone_name);
    let mut projects = Vec::new();
    for (name, authn) in [
        ("allowed", admin.clone()),
        ("unrelated", admin.clone()),
        ("foreign", other_admin),
    ] {
        let project: Project = request(
            client,
            authn,
            Method::POST,
            "/v1/projects",
            Some(&json!({"name": name, "description": "test", "defaults": {}})),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        projects.push(project);
    }
    let project_id = projects[0].identity.id;
    let project_url = format!("/v1/projects/{project_id}");
    let (key, keys) = signing_key();
    let provider: FederationIdentityProvider = request(client, admin.clone(), Method::POST,
        "/v1/federation/inbound/identity-providers", Some(&json!({
            "name": "idp", "description": "test", "issuer": "https://issuer.example", "audience": "oxide",
            "verification_type": "static_jwks", "signing_keys": keys,
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let policy: FederationTrustPolicy = request(client, admin.clone(), Method::POST,
        "/v1/federation/inbound/trust-policies", Some(&json!({
            "name": "builder", "description": "test", "identity_provider": "idp",
            "policy": "assume(claims) if claims.sub = \"builder\";",
            "grants": [{"resource_kind": "project", "resource_id": project_id, "role_name": "collaborator"}],
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let policy_url =
        format!("/v1/federation/inbound/trust-policies/{}", policy.identity.id);
    let token = exchange(client, &host, policy.identity.id, &key).await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        "/v1/projects/allowed",
        None,
        StatusCode::OK,
    )
    .await;
    for project in &projects[1..] {
        bearer_request(
            client,
            &token.token,
            Method::GET,
            &format!("/v1/projects/{}", project.identity.id),
            None,
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    bearer_request(
        client,
        &token.token,
        Method::PUT,
        &project_url,
        Some(&json!({"description": "forbidden"})),
        StatusCode::FORBIDDEN,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        "/v1/system/silos",
        None,
        StatusCode::FORBIDDEN,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        "/v1/me",
        None,
        StatusCode::NOT_FOUND,
    )
    .await;
    bearer_request(
        client,
        "oxide-federation-invalid",
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;

    let response = bearer_request(client, &token.token, Method::POST, &format!("/v1/vpcs?project={project_id}"),
        Some(&json!({"name": "workload", "description": "test", "dns_name": "workload", "defaults": {}})), StatusCode::CREATED).await;
    let vpc: Vpc = response.parsed_body().unwrap();
    let vpc_url = format!("/v1/vpcs/{}", vpc.identity.id);
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &vpc_url,
        None,
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::DELETE,
        &vpc_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;

    let mut instance_params = json!({
        "name": "workload", "description": "test", "ncpus": 4,
        "memory": 1073741824, "hostname": "workload",
        "network_interfaces": {"type": "none"}, "start": false,
        "ssh_public_keys": [Uuid::new_v4()],
    });
    let instances_url = format!("/v1/instances?project={project_id}");
    bearer_request(
        client,
        &token.token,
        Method::POST,
        &instances_url,
        Some(&instance_params),
        StatusCode::BAD_REQUEST,
    )
    .await;
    instance_params.as_object_mut().unwrap().remove("ssh_public_keys");
    let instance: Value = bearer_request(
        client,
        &token.token,
        Method::POST,
        &instances_url,
        Some(&instance_params),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    bearer_request(
        client,
        &token.token,
        Method::DELETE,
        &format!("/v1/instances/{}", instance["id"].as_str().unwrap()),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;

    let store = ctx.server.server_context().nexus.datastore();
    let conn = store.pool_connection_for_tests().await.unwrap();
    let session: FederationSession = federation_session::table
        .filter(federation_session::token.eq(
            token.token.strip_prefix("oxide-federation-").unwrap().to_owned(),
        ))
        .select(FederationSession::as_select())
        .first_async(&*conn)
        .await
        .unwrap();
    assert!(session.time_last_used > session.time_created);
    let audit_page: dropshot::ResultsPage<
        nexus_types::external_api::audit::AuditLogEntry,
    > = request(
        client,
        AuthnMode::PrivilegedUser,
        Method::GET,
        &format!(
            "/v1/system/audit-log?limit=100&start_time={}",
            session
                .time_created
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        ),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    let entry = audit_page
        .items
        .iter()
        .find(|entry| {
            entry.request_id
                == response.headers["x-request-id"].to_str().unwrap()
        })
        .unwrap();
    assert_eq!(
        entry.actor,
        AuditLogEntryActor::Federated { session_id: session.id, silo_id },
    );
    assert_eq!(entry.auth_method, Some(AuthMethod::FederationToken));
    assert_eq!(entry.credential_id, Some(session.id));

    request(
        client,
        admin.clone(),
        Method::PUT,
        &project_url,
        Some(&json!({"name": "renamed"})),
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        "/v1/projects/renamed",
        None,
        StatusCode::OK,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &policy_url,
        Some(&json!({"description": "renamed metadata"})),
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::OK,
    )
    .await;
    let (_, rotated_keys) = signing_key();
    let provider_url = format!(
        "/v1/federation/inbound/identity-providers/{}",
        provider.identity.id
    );
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &provider_url,
        Some(&json!({"signing_keys": rotated_keys})),
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::OK,
    )
    .await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &provider_url,
        Some(&json!({"signing_keys": keys})),
        StatusCode::OK,
    )
    .await;

    request(client, admin.clone(), Method::PATCH, &policy_url, Some(&json!({"grants": [
        {"resource_kind": "silo", "resource_id": silo_id, "role_name": "viewer"}
    ]})), StatusCode::OK).await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let viewer = exchange(client, &host, policy.identity.id, &key).await;
    bearer_request(
        client,
        &viewer.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &viewer.token,
        Method::GET,
        &format!("/v1/projects/{}", projects[1].identity.id),
        None,
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &viewer.token,
        Method::GET,
        &format!("/v1/projects/{}", projects[2].identity.id),
        None,
        StatusCode::NOT_FOUND,
    )
    .await;
    bearer_request(
        client,
        &viewer.token,
        Method::PUT,
        &project_url,
        Some(&json!({"description": "forbidden"})),
        StatusCode::FORBIDDEN,
    )
    .await;

    diesel::update(silo::table.filter(silo::id.eq(silo_id)))
        .set(silo::mapped_fleet_roles.eq(json!({"viewer": ["viewer"]})))
        .execute_async(&*conn)
        .await
        .unwrap();
    bearer_request(
        client,
        &viewer.token,
        Method::GET,
        "/v1/system/silos",
        None,
        StatusCode::OK,
    )
    .await;
    diesel::update(silo::table.filter(silo::id.eq(silo_id)))
        .set(silo::mapped_fleet_roles.eq(json!({})))
        .execute_async(&*conn)
        .await
        .unwrap();
    bearer_request(
        client,
        &viewer.token,
        Method::GET,
        "/v1/system/silos",
        None,
        StatusCode::FORBIDDEN,
    )
    .await;

    diesel::update(federation_session::table.filter(
        federation_session::token.eq(
            viewer.token.strip_prefix("oxide-federation-").unwrap().to_owned(),
        ),
    ))
    .set(
        federation_session::time_expires
            .eq(Utc::now() - chrono::Duration::seconds(1)),
    )
    .execute_async(&*conn)
    .await
    .unwrap();
    bearer_request(
        client,
        &viewer.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let token = exchange(client, &host, policy.identity.id, &key).await;
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &policy_url,
        Some(&json!({"grants": []})),
        StatusCode::OK,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let empty = exchange(client, &host, policy.identity.id, &key).await;
    bearer_request(
        client,
        &empty.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::NOT_FOUND,
    )
    .await;

    request(client, admin.clone(), Method::PATCH, &policy_url, Some(&json!({"grants": [
        {"resource_kind": "silo", "resource_id": silo_id, "role_name": "viewer"}
    ]})), StatusCode::OK).await;
    let token = exchange(client, &host, policy.identity.id, &key).await;
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &provider_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    diesel::update(
        federation_identity_provider::table
            .filter(federation_identity_provider::id.eq(provider.identity.id)),
    )
    .set(
        federation_identity_provider::time_deleted
            .eq(None::<chrono::DateTime<Utc>>),
    )
    .execute_async(&*conn)
    .await
    .unwrap();
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::OK,
    )
    .await;
    diesel::update(silo::table.filter(silo::id.eq(silo_id)))
        .set(silo::time_deleted.eq(Utc::now()))
        .execute_async(&*conn)
        .await
        .unwrap();
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    diesel::update(silo::table.filter(silo::id.eq(silo_id)))
        .set(silo::time_deleted.eq(None::<chrono::DateTime<Utc>>))
        .execute_async(&*conn)
        .await
        .unwrap();
    request(
        client,
        admin,
        Method::DELETE,
        &policy_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    bearer_request(
        client,
        &token.token,
        Method::GET,
        &project_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
}

#[nexus_test]
async fn test_federation_token_exchange(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    let (silo_id, admin) =
        silo_user(client, "federation-token", SiloRole::Admin).await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("federation-token.sys.{}", ctx.external_dns_zone_name);
    let (key, keys) = signing_key();
    let provider: FederationIdentityProvider = request(client, admin.clone(), Method::POST,
        "/v1/federation/inbound/identity-providers", Some(&json!({
            "name": "idp", "description": "test", "issuer": "https://issuer.example", "audience": "oxide",
            "verification_type": "static_jwks", "signing_keys": keys,
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let policy: FederationTrustPolicy = request(client, admin.clone(), Method::POST,
        "/v1/federation/inbound/trust-policies", Some(&json!({
            "name": "builder", "description": "test", "identity_provider": "idp",
            "policy": "assume(claims) if claims.sub = \"builder\" and claims.my_idp.custom_claims.project_id = \"p123\";",
            "grants": [{"resource_kind": "silo", "resource_id": silo_id, "role_name": "viewer"}],
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let now = Utc::now().timestamp();
    let claims = json!({"iss": "https://issuer.example", "aud": "oxide", "sub": "builder", "iat": now, "exp": now + 300,
        "my_idp": {"custom_claims": {"project_id": "p123"}}});
    let jwt = signed(&key, &claims);
    let nexus = &ctx.server.server_context().nexus;
    let store = nexus.datastore();
    let conn = store.pool_connection_for_tests().await.unwrap();
    let mut tokens = Vec::new();
    for selector in [json!("builder"), json!(policy.identity.id)] {
        let before = Utc::now();
        let response =
            RequestBuilder::new(client, Method::POST, TOKEN_ENDPOINT)
                .header("host", &host)
                .body(Some(&json!({"trust_policy": selector, "oidc_jwt": jwt})))
                .expect_status(Some(StatusCode::CREATED))
                .expect_response_header(http::header::CACHE_CONTROL, "no-store")
                .expect_response_header(http::header::PRAGMA, "no-cache")
                .execute()
                .await
                .unwrap();
        let result: FederationToken = response.parsed_body().unwrap();
        assert_eq!(result.revision, policy.revision);
        assert!(result.expires_at >= before + chrono::Duration::minutes(5));
        assert!(result.expires_at <= Utc::now() + chrono::Duration::minutes(5));
        let secret = result.token.strip_prefix("oxide-federation-").unwrap();
        assert_eq!(secret.len(), 40);
        assert_eq!(hex::decode(secret).unwrap().len(), 20);
        let session = federation_session::table
            .filter(federation_session::token.eq(secret.to_owned()))
            .select(FederationSession::as_select())
            .first_async(&*conn)
            .await
            .unwrap();
        assert_eq!(session.jwt_claims, claims);
        assert_eq!(session.trust_policy_id, policy.identity.id);
        assert_eq!(session.trust_policy_revision, policy.revision);
        assert_eq!(session.time_created, session.time_last_used);
        assert_eq!(session.time_expires, result.expires_at);
        let (operation, status, request_id): (String, Option<i32>, String) =
            audit_log::table
                .filter(audit_log::id.eq(session.audit_log_id))
                .select((
                    audit_log::operation_id,
                    audit_log::http_status_code,
                    audit_log::request_id,
                ))
                .first_async(&*conn)
                .await
                .unwrap();
        assert_eq!(operation, "federation_token_create");
        assert_eq!(status, Some(201));
        assert_eq!(
            response.headers["x-request-id"].to_str().unwrap(),
            request_id
        );
        tokens.push(result.token);
    }
    assert_ne!(tokens[0], tokens[1]);
    for authn in [
        None,
        Some(AuthnMode::UnprivilegedUser),
        Some(AuthnMode::PrivilegedUser),
        Some(admin.clone()),
    ] {
        let builder = RequestBuilder::new(client, Method::POST, TOKEN_ENDPOINT)
            .header("host", &host)
            .body(Some(&json!({"trust_policy": policy.identity.id, "oidc_jwt": "invalid"})))
            .expect_status(Some(StatusCode::BAD_REQUEST));
        let response = match authn {
            None => builder.execute().await.unwrap(),
            Some(authn) => NexusRequest::new(builder)
                .authn_as(authn)
                .execute()
                .await
                .unwrap(),
        };
        let error: Value = response.parsed_body().unwrap();
        assert_eq!(error["error_code"], "InvalidRequest");
        assert_eq!(error["message"], "Malformed OIDC token");
        let (operation, status): (String, Option<i32>) = audit_log::table
            .filter(
                audit_log::time_completed
                    .gt(chrono::DateTime::from_timestamp(now, 0).unwrap()),
            )
            .filter(audit_log::request_id.eq(
                response.headers["x-request-id"].to_str().unwrap().to_owned(),
            ))
            .select((audit_log::operation_id, audit_log::http_status_code))
            .first_async(&*conn)
            .await
            .unwrap();
        assert_eq!(operation, "federation_token_create");
        assert_eq!(status, Some(400));
    }
    let (wrong_key, _) = signing_key();
    let mut denied = claims.clone();
    denied["sub"] = json!("other");
    for token in [signed(&wrong_key, &claims), signed(&key, &denied)] {
        RequestBuilder::new(client, Method::POST, TOKEN_ENDPOINT)
            .header("host", &host)
            .body(Some(&json!({"trust_policy": "builder", "oidc_jwt": token})))
            .expect_status(Some(StatusCode::FORBIDDEN))
            .execute()
            .await
            .unwrap();
    }
    let (_, other_admin) =
        silo_user(client, "other-federation-token", SiloRole::Admin).await;
    let _ = other_admin;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    RequestBuilder::new(client, Method::POST, TOKEN_ENDPOINT)
        .header(
            "host",
            format!(
                "other-federation-token.sys.{}",
                ctx.external_dns_zone_name
            ),
        )
        .body(Some(
            &json!({"trust_policy": policy.identity.id, "oidc_jwt": jwt}),
        ))
        .expect_status(Some(StatusCode::FORBIDDEN))
        .execute()
        .await
        .unwrap();

    let opctx = nexus.opctx_external_authn();
    let snapshot = store
        .federation_trust_policy_config(
            opctx,
            silo_id,
            &policy.identity.id.into(),
        )
        .await
        .unwrap();
    let policy_url =
        format!("/v1/federation/inbound/trust-policies/{}", policy.identity.id);
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &policy_url,
        Some(&json!({"policy": "assume(claims) if claims.sub = \"other\";"})),
        StatusCode::OK,
    )
    .await;
    assert!(
        store
            .federation_session_create(
                opctx,
                &snapshot,
                claims.clone(),
                Uuid::new_v4()
            )
            .await
            .is_err()
    );
    let snapshot = store
        .federation_trust_policy_config(
            opctx,
            silo_id,
            &policy.identity.id.into(),
        )
        .await
        .unwrap();
    let (_, rotated_keys) = signing_key();
    let provider_url = format!(
        "/v1/federation/inbound/identity-providers/{}",
        provider.identity.id
    );
    request(
        client,
        admin.clone(),
        Method::PATCH,
        &provider_url,
        Some(&json!({"signing_keys": rotated_keys})),
        StatusCode::OK,
    )
    .await;
    let session = store
        .federation_session_create(
            opctx,
            &snapshot,
            claims.clone(),
            Uuid::new_v4(),
        )
        .await
        .unwrap();
    assert_eq!(session.trust_policy_revision, snapshot.trust_policy.revision);
    let snapshot = store
        .federation_trust_policy_config(
            opctx,
            silo_id,
            &policy.identity.id.into(),
        )
        .await
        .unwrap();
    request(
        client,
        admin.clone(),
        Method::DELETE,
        &provider_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    let session = store
        .federation_session_create(
            opctx,
            &snapshot,
            claims.clone(),
            Uuid::new_v4(),
        )
        .await
        .unwrap();
    assert_eq!(session.trust_policy_revision, snapshot.trust_policy.revision);
    RequestBuilder::new(client, Method::POST, TOKEN_ENDPOINT)
        .header("host", &host)
        .body(Some(&json!({"trust_policy": "builder", "oidc_jwt": jwt})))
        .expect_status(Some(StatusCode::FORBIDDEN))
        .execute()
        .await
        .unwrap();
    request(
        client,
        admin,
        Method::DELETE,
        &policy_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    assert!(
        store
            .federation_session_create(opctx, &snapshot, claims, Uuid::new_v4())
            .await
            .is_err()
    );
}
