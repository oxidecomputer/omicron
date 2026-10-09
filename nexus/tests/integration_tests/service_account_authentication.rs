// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::service_account::{admin, project, request};
use super::service_account_federation::{
    claims, exchange, federation, provider, signed, signing_key,
};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{Duration, Utc};
use diesel::prelude::*;
use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use nexus_db_model::ServiceAccountToken;
use nexus_db_schema::schema::{
    federation_identity_provider, project as db_project, service_account_token,
    silo,
};
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::{AuthnMode, RequestBuilder, TestResponse};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::audit::{
    AuditLogEntry, AuditLogEntryActor, AuthMethod,
};
use nexus_types::external_api::service_account_token::ServiceAccountTokenGrant;
use serde_json::{Value, json};
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

async fn bearer(
    client: &ClientTestContext,
    token: &str,
    method: Method,
    url: &str,
    body: Option<&Value>,
    status: StatusCode,
) -> TestResponse {
    RequestBuilder::new(client, method, url)
        .header("Authorization", &format!("Bearer {token}"))
        .body(body)
        .expect_status(Some(status))
        .execute()
        .await
        .unwrap()
}

async fn issue(
    client: &ClientTestContext,
    owner: &AuthnMode,
    account_url: &str,
    body: Value,
) -> ServiceAccountTokenGrant {
    request(
        client,
        owner,
        Method::POST,
        &format!("{account_url}/token"),
        Some(&body),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap()
}

#[nexus_test]
async fn test_service_account_authentication(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    let (silo_id, owner) = admin(client, "service-auth").await;
    let (_, other_owner) = admin(client, "other-auth").await;
    let project_id = project(client, &owner, "allowed").await;
    let denied_id = project(client, &owner, "denied").await;
    let other_id = project(client, &other_owner, "other").await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("service-auth.sys.{}", ctx.external_dns_zone_name);
    let (key, keys) = signing_key();
    let idp_id = provider(client, &owner, keys).await;
    let conn = ctx
        .server
        .server_context()
        .nexus
        .datastore()
        .pool_connection_for_tests()
        .await
        .unwrap();
    for scope in ["silo", "project"] {
        let parent = if scope == "silo" { silo_id } else { project_id };
        let created: Value = request(client, &owner, Method::POST,
            &format!("/v1/service-accounts/{scope}?{scope}={parent}"), Some(&json!({
                "name": "reader", "description": "", "federation": federation(300),
                "grants": [{"resource_kind": "project", "resource_id": project_id, "role_name": "viewer"}],
            })), StatusCode::CREATED).await.parsed_body().unwrap();
        let account_id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
        let url = format!("/v1/service-accounts/{scope}/{account_id}");
        let direct = issue(client, &owner, &url, json!({})).await;
        let federated: ServiceAccountTokenGrant = exchange(
            client,
            &host,
            &format!("{url}/federation-token"),
            &json!({"jwt": signed(&key, &claims())}),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        let instances = format!("/v1/instances?project={project_id}");
        let vpcs = format!("/v1/vpcs?project={project_id}");
        for token in [&direct, &federated] {
            bearer(
                client,
                &token.token,
                Method::GET,
                &instances,
                None,
                StatusCode::OK,
            )
            .await;
            bearer(
                client,
                &token.token,
                Method::POST,
                &vpcs,
                Some(&json!({"name": "denied", "description": "", "dns_name": "denied", "defaults": {}})),
                StatusCode::FORBIDDEN,
            )
            .await;
            for denied in [denied_id, other_id] {
                bearer(
                    client,
                    &token.token,
                    Method::GET,
                    &format!("/v1/instances?project={denied}"),
                    None,
                    StatusCode::NOT_FOUND,
                )
                .await;
            }
            bearer(
                client,
                &token.token,
                Method::GET,
                "/v1/me",
                None,
                StatusCode::NOT_FOUND,
            )
            .await;
            let stored = service_account_token::table
                .filter(service_account_token::id.eq(token.id))
                .select(ServiceAccountToken::as_select())
                .first_async(&*conn)
                .await
                .unwrap();
            assert!(stored.time_last_used > stored.time_created);
        }
        request(client, &owner, Method::PATCH, &url, Some(&json!({
            "grants": [{"resource_kind": "project", "resource_id": project_id, "role_name": "collaborator"}],
        })), StatusCode::OK).await;
        for (index, token) in [&direct, &federated].into_iter().enumerate() {
            let response = bearer(client, &token.token, Method::POST, &vpcs,
                Some(&json!({"name": format!("created-{scope}-{index}"), "description": "", "dns_name": format!("created-{scope}-{index}"), "defaults": {}})), StatusCode::CREATED).await;
            let page: dropshot::ResultsPage<AuditLogEntry> = request(
                client,
                &AuthnMode::PrivilegedUser,
                Method::GET,
                &format!(
                    "/v1/system/audit-log?limit=100&start_time={}",
                    (Utc::now() - Duration::minutes(5))
                        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
                ),
                None,
                StatusCode::OK,
            )
            .await
            .parsed_body()
            .unwrap();
            let entry = page
                .items
                .iter()
                .find(|entry| {
                    entry.request_id
                        == response.headers["x-request-id"].to_str().unwrap()
                })
                .unwrap();
            assert_eq!(
                entry.actor,
                AuditLogEntryActor::ServiceAccount {
                    service_account_id: account_id,
                    silo_id,
                    federation: (token.id == federated.id).then(|| {
                        nexus_types::external_api::audit::FederationIdentity {
                            idp_id,
                            iss: "https://issuer.example".into(),
                            sub: "builder".into(),
                        }
                    }),
                }
            );
            assert_eq!(
                entry.auth_method,
                Some(AuthMethod::ServiceAccountToken)
            );
            assert_eq!(entry.credential_id, Some(token.id));
        }
        if scope == "project" {
            let mut params = json!({"name": "workload", "description": "", "ncpus": 4,
                "memory": 1073741824, "hostname": "workload", "network_interfaces": {"type": "none"},
                "start": false, "ssh_public_keys": [Uuid::new_v4()]});
            bearer(
                client,
                &direct.token,
                Method::POST,
                &instances,
                Some(&params),
                StatusCode::BAD_REQUEST,
            )
            .await;
            params.as_object_mut().unwrap().remove("ssh_public_keys");
            let instance: Value = bearer(
                client,
                &direct.token,
                Method::POST,
                &instances,
                Some(&params),
                StatusCode::CREATED,
            )
            .await
            .parsed_body()
            .unwrap();
            bearer(
                client,
                &direct.token,
                Method::DELETE,
                &format!("/v1/instances/{}", instance["id"].as_str().unwrap()),
                None,
                StatusCode::NO_CONTENT,
            )
            .await;
        }
        request(
            client,
            &owner,
            Method::PATCH,
            &url,
            Some(&json!({"grants": []})),
            StatusCode::OK,
        )
        .await;
        for token in [&direct, &federated] {
            bearer(
                client,
                &token.token,
                Method::GET,
                &instances,
                None,
                StatusCode::NOT_FOUND,
            )
            .await;
        }
        request(client, &owner, Method::PATCH, &url, Some(&json!({
            "grants": [{"resource_kind": "project", "resource_id": project_id, "role_name": "viewer"}],
        })), StatusCode::OK).await;
        request(
            client,
            &owner,
            Method::DELETE,
            &format!("{url}/tokens/{}", direct.id),
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
        bearer(
            client,
            &direct.token,
            Method::GET,
            &instances,
            None,
            StatusCode::UNAUTHORIZED,
        )
        .await;
        diesel::update(
            service_account_token::table
                .filter(service_account_token::id.eq(federated.id)),
        )
        .set(
            service_account_token::time_expires
                .eq(Utc::now() - Duration::seconds(1)),
        )
        .execute_async(&*conn)
        .await
        .unwrap();
        bearer(
            client,
            &federated.token,
            Method::GET,
            &instances,
            None,
            StatusCode::UNAUTHORIZED,
        )
        .await;
        let active = issue(client, &owner, &url, json!({})).await;
        if scope == "project" {
            diesel::update(
                db_project::table.filter(db_project::id.eq(project_id)),
            )
            .set(db_project::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .unwrap();
            bearer(
                client,
                &active.token,
                Method::GET,
                &instances,
                None,
                StatusCode::UNAUTHORIZED,
            )
            .await;
            diesel::update(
                db_project::table.filter(db_project::id.eq(project_id)),
            )
            .set(db_project::time_deleted.eq(None::<chrono::DateTime<Utc>>))
            .execute_async(&*conn)
            .await
            .unwrap();
        }
        diesel::update(silo::table.filter(silo::id.eq(silo_id)))
            .set(silo::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .unwrap();
        bearer(
            client,
            &active.token,
            Method::GET,
            &instances,
            None,
            StatusCode::UNAUTHORIZED,
        )
        .await;
        diesel::update(silo::table.filter(silo::id.eq(silo_id)))
            .set(silo::time_deleted.eq(None::<chrono::DateTime<Utc>>))
            .execute_async(&*conn)
            .await
            .unwrap();
        bearer(
            client,
            &format!("{}bad", active.token),
            Method::GET,
            &instances,
            None,
            StatusCode::UNAUTHORIZED,
        )
        .await;
        request(
            client,
            &owner,
            Method::DELETE,
            &url,
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
        bearer(
            client,
            &active.token,
            Method::GET,
            &instances,
            None,
            StatusCode::UNAUTHORIZED,
        )
        .await;
    }
}

#[nexus_test]
async fn test_service_account_authentication_federation_changes(
    ctx: &ControlPlaneTestContext,
) {
    let client = &ctx.external_client;
    let (silo_id, owner) = admin(client, "federation-auth").await;
    let (other_silo, _) = admin(client, "other-federation-auth").await;
    let project_id = project(client, &owner, "visible").await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("federation-auth.sys.{}", ctx.external_dns_zone_name);
    let (key, keys) = signing_key();
    let idp_id = provider(client, &owner, keys).await;
    let idp_url = format!("/v1/federation/inbound/identity-providers/{idp_id}");
    let created: Value = request(client, &owner, Method::POST,
        &format!("/v1/service-accounts/silo?silo={silo_id}"), Some(&json!({
            "name": "reader", "description": "", "federation": federation(300),
            "grants": [{"resource_kind": "silo", "resource_id": silo_id, "role_name": "viewer"}],
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let url = format!(
        "/v1/service-accounts/silo/{}",
        created["id"].as_str().unwrap()
    );
    let instances = format!("/v1/instances?project={project_id}");
    let direct = issue(client, &owner, &url, json!({})).await;
    let federated: ServiceAccountTokenGrant = exchange(
        client,
        &host,
        &format!("{url}/federation-token"),
        &json!({"jwt": signed(&key, &claims())}),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let (rotated_key, rotated_keys) = signing_key();
    request(
        client,
        &owner,
        Method::PATCH,
        &idp_url,
        Some(&json!({"signing_keys": rotated_keys})),
        StatusCode::OK,
    )
    .await;
    request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": federation(60)})),
        StatusCode::OK,
    )
    .await;
    for token in [&direct, &federated] {
        bearer(
            client,
            &token.token,
            Method::GET,
            &instances,
            None,
            StatusCode::OK,
        )
        .await;
    }
    let conn = ctx
        .server
        .server_context()
        .nexus
        .datastore()
        .pool_connection_for_tests()
        .await
        .unwrap();
    diesel::update(
        federation_identity_provider::table
            .filter(federation_identity_provider::id.eq(idp_id)),
    )
    .set(federation_identity_provider::silo_id.eq(other_silo))
    .execute_async(&*conn)
    .await
    .unwrap();
    bearer(
        client,
        &federated.token,
        Method::GET,
        &instances,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    bearer(
        client,
        &direct.token,
        Method::GET,
        &instances,
        None,
        StatusCode::OK,
    )
    .await;
    diesel::update(
        federation_identity_provider::table
            .filter(federation_identity_provider::id.eq(idp_id)),
    )
    .set(federation_identity_provider::silo_id.eq(silo_id))
    .execute_async(&*conn)
    .await
    .unwrap();
    let mut changed = federation(300);
    changed["policy"] = json!("assume(claims) if claims.sub = \"builder\";");
    request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": changed})),
        StatusCode::OK,
    )
    .await;
    bearer(
        client,
        &federated.token,
        Method::GET,
        &instances,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    bearer(
        client,
        &direct.token,
        Method::GET,
        &instances,
        None,
        StatusCode::OK,
    )
    .await;
    let federated: ServiceAccountTokenGrant = exchange(
        client,
        &host,
        &format!("{url}/federation-token"),
        &json!({"jwt": signed(&rotated_key, &claims())}),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    request(
        client,
        &owner,
        Method::DELETE,
        &idp_url,
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    bearer(
        client,
        &federated.token,
        Method::GET,
        &instances,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    bearer(
        client,
        &direct.token,
        Method::GET,
        &instances,
        None,
        StatusCode::OK,
    )
    .await;
    request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": null})),
        StatusCode::OK,
    )
    .await;
    bearer(
        client,
        &federated.token,
        Method::GET,
        &instances,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    bearer(
        client,
        &direct.token,
        Method::GET,
        &instances,
        None,
        StatusCode::OK,
    )
    .await;
}

#[nexus_test]
async fn test_service_account_authentication_token_expiration(
    ctx: &ControlPlaneTestContext,
) {
    let client = &ctx.external_client;
    let (silo_id, owner) = admin(client, "chaining").await;
    let project_id = project(client, &owner, "project").await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("chaining.sys.{}", ctx.external_dns_zone_name);
    let (key, keys) = signing_key();
    provider(client, &owner, keys).await;
    for scope in ["silo", "project"] {
        let parent = if scope == "silo" { silo_id } else { project_id };
        let created: Value = request(client, &owner, Method::POST,
            &format!("/v1/service-accounts/{scope}?{scope}={parent}"), Some(&json!({
                "name": "admin", "description": "", "federation": federation(300),
                "grants": [{"resource_kind": scope, "resource_id": parent, "role_name": "admin"}],
            })), StatusCode::CREATED).await.parsed_body().unwrap();
        let url = format!(
            "/v1/service-accounts/{scope}/{}",
            created["id"].as_str().unwrap()
        );
        let direct =
            issue(client, &owner, &url, json!({"ttl_seconds": 300})).await;
        let federated: ServiceAccountTokenGrant = exchange(
            client,
            &host,
            &format!("{url}/federation-token"),
            &json!({"jwt": signed(&key, &claims())}),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        for token in [&direct, &federated] {
            let authn = AuthnMode::DeviceToken(token.token.clone());
            let child = issue(client, &authn, &url, json!({})).await;
            assert_eq!(child.time_expires, token.time_expires);
            let grandchild = issue(
                client,
                &AuthnMode::DeviceToken(child.token),
                &url,
                json!({}),
            )
            .await;
            assert_eq!(grandchild.time_expires, token.time_expires);
            let short =
                issue(client, &authn, &url, json!({"ttl_seconds": 30})).await;
            assert!(short.time_expires.unwrap() < token.time_expires.unwrap());
            request(
                client,
                &authn,
                Method::POST,
                &format!("{url}/token"),
                Some(&json!({"ttl_seconds": 600})),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
    }
}
