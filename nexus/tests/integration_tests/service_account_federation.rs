// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::service_account::{admin, project, request};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use nexus_db_model::ServiceAccountToken;
use nexus_db_schema::schema::{
    audit_log, federation_identity_provider, service_account_token,
};
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::{
    AuthnMode, NexusRequest, RequestBuilder, TestResponse,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::service_account::{
    ServiceAccountParentSelector, ServiceAccountPath, ServiceAccountScope,
};
use nexus_types::external_api::service_account_token::ServiceAccountTokenGrant;
use omicron_common::api::external::Error;
use openidconnect::core::{
    CoreGenderClaim, CoreJsonWebKeySet, CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm, CoreRsaPrivateSigningKey,
};
use openidconnect::{
    AdditionalClaims, IdToken, JsonWebKeyId, PrivateSigningKey,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::num::NonZeroU32;
use uuid::Uuid;

pub const TOKEN_ENDPOINT: &str = "/v1/service-accounts/silo/00000000-0000-0000-0000-000000000000/federation-token";
const IDPS: &str = "/v1/federation/inbound/identity-providers";
const POLICY: &str = "assume(claims) if claims.sub = \"builder\" and claims.my_idp.custom_claims.project_id = \"build-project\";";
type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[derive(Debug, Deserialize, Serialize)]
struct TestClaims(serde_json::Map<String, Value>);
impl AdditionalClaims for TestClaims {}

pub(super) fn signing_key() -> (CoreRsaPrivateSigningKey, Value) {
    let pair = openssl::rsa::Rsa::generate(2048).unwrap();
    let pem = String::from_utf8(pair.private_key_to_pem().unwrap()).unwrap();
    let key = CoreRsaPrivateSigningKey::from_pem(
        &pem,
        Some(JsonWebKeyId::new("test-key".into())),
    )
    .unwrap();
    let keys = CoreJsonWebKeySet::new(vec![key.as_verification_key()]);
    (key, serde_json::to_value(keys).unwrap())
}

pub(super) fn signed(key: &CoreRsaPrivateSigningKey, claims: &Value) -> String {
    IdToken::<
        TestClaims,
        CoreGenderClaim,
        CoreJweContentEncryptionAlgorithm,
        CoreJwsSigningAlgorithm,
    >::new(
        serde_json::from_value(claims.clone()).unwrap(),
        key,
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        None,
        None,
    )
    .unwrap()
    .to_string()
}

pub(super) fn claims() -> Value {
    let now = Utc::now().timestamp();
    json!({"iss": "https://issuer.example", "aud": ["oxide"], "sub": "builder",
        "iat": now, "exp": now + 600,
        "my_idp": {"custom_claims": {"project_id": "build-project"}}})
}

pub(super) fn federation(ttl: u32) -> Value {
    json!({"identity_provider": "gcp", "policy": POLICY, "max_ttl_seconds": ttl})
}

pub(super) async fn provider(
    client: &ClientTestContext,
    owner: &AuthnMode,
    keys: Value,
) -> Uuid {
    let created: Value = request(client, owner, Method::POST, IDPS, Some(&json!({
        "name": "gcp", "description": "test", "issuer": "https://issuer.example", "audience": "oxide",
        "verification_type": "static_jwks", "signing_keys": keys,
    })), StatusCode::CREATED).await.parsed_body().unwrap();
    created["id"].as_str().unwrap().parse().unwrap()
}

pub(super) async fn exchange(
    client: &ClientTestContext,
    host: &str,
    url: &str,
    body: &Value,
    status: StatusCode,
) -> TestResponse {
    let builder = RequestBuilder::new(client, Method::POST, url)
        .header("host", host)
        .body(Some(body))
        .expect_status(Some(status));
    let builder = if status == StatusCode::CREATED {
        builder
            .expect_response_header(http::header::CACHE_CONTROL, "no-store")
            .expect_response_header(http::header::PRAGMA, "no-cache")
    } else {
        builder
    };
    builder.execute().await.unwrap()
}

#[nexus_test]
async fn test_service_account_federation_exchange(
    ctx: &ControlPlaneTestContext,
) {
    let client = &ctx.external_client;
    let started = Utc::now();
    let (silo_id, owner) = admin(client, "federation").await;
    let (other_silo, _) = admin(client, "other").await;
    let project_id = project(client, &owner, "build").await;
    activate_background_task(&ctx.lockstep_client, "external_endpoints").await;
    let host = format!("federation.sys.{}", ctx.external_dns_zone_name);
    let other_host = format!("other.sys.{}", ctx.external_dns_zone_name);
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
    let mut last_url = String::new();
    for (scope, parent) in [("silo", silo_id), ("project", project_id)] {
        let created: Value = request(client, &owner, Method::POST,
            &format!("/v1/service-accounts/{scope}?{scope}={parent}"), Some(&json!({
                "name": "builder", "description": "test", "grants": [], "federation": federation(120),
            })), StatusCode::CREATED).await.parsed_body().unwrap();
        let account_id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
        let account_url = format!("/v1/service-accounts/{scope}/{account_id}");
        let url = format!("{account_url}/federation-token");
        last_url = url.clone();
        let claims = claims();
        let jwt = signed(&key, &claims);
        let body = json!({"jwt": jwt});
        let mut issued = Vec::new();
        for selector in [
            url.clone(),
            format!(
                "/v1/service-accounts/{scope}/builder/federation-token?{scope}={parent}"
            ),
        ] {
            let before = Utc::now();
            let response =
                exchange(client, &host, &selector, &body, StatusCode::CREATED)
                    .await;
            let grant: ServiceAccountTokenGrant =
                response.parsed_body().unwrap();
            assert!(
                grant.time_expires.unwrap() >= before + Duration::seconds(119)
            );
            assert!(
                grant.time_expires.unwrap()
                    <= Utc::now() + Duration::seconds(120)
            );
            let row = service_account_token::table
                .filter(service_account_token::id.eq(grant.id))
                .select(ServiceAccountToken::as_select())
                .first_async(&*conn)
                .await
                .unwrap();
            assert_eq!(row.service_account_id, account_id);
            assert_eq!(row.idp_id, Some(idp_id));
            assert_eq!(row.federation_generation, Some(1));
            assert_eq!(row.federation_jwt_claims, Some(claims.clone()));
            assert_eq!(
                grant.token,
                format!("oxide-service-account-{}", row.token)
            );
            assert_eq!(grant.time_expires, row.time_expires);
            let status = audit_log::table
                .filter(audit_log::time_completed.gt(started))
                .filter(
                    audit_log::request_id.eq(response.headers["x-request-id"]
                        .to_str()
                        .unwrap()
                        .to_owned()),
                )
                .select(audit_log::http_status_code)
                .first_async::<Option<i32>>(&*conn)
                .await
                .unwrap();
            assert_eq!(status, Some(201));
            issued.push(grant.token);
        }
        assert_ne!(issued[0], issued[1]);
        exchange(
            client,
            &host,
            &url,
            &json!({"jwt": jwt, "ttl_seconds": 30}),
            StatusCode::CREATED,
        )
        .await;
        for ttl in [0, 121] {
            exchange(
                client,
                &host,
                &url,
                &json!({"jwt": jwt, "ttl_seconds": ttl}),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
        let mut short = claims.clone();
        short["exp"] = json!(Utc::now().timestamp() + 60);
        let short_jwt = signed(&key, &short);
        let short_grant: ServiceAccountTokenGrant = exchange(
            client,
            &host,
            &url,
            &json!({"jwt": short_jwt}),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        assert_eq!(
            short_grant.time_expires,
            DateTime::from_timestamp(short["exp"].as_i64().unwrap(), 0)
        );
        exchange(
            client,
            &host,
            &url,
            &json!({"jwt": short_jwt, "ttl_seconds": 120}),
            StatusCode::BAD_REQUEST,
        )
        .await;
        exchange(client, &other_host, &url, &body, StatusCode::FORBIDDEN).await;
        let wrong_scope = if scope == "silo" { "project" } else { "silo" };
        exchange(client, &host, &format!("/v1/service-accounts/{wrong_scope}/{account_id}/federation-token"), &body, StatusCode::FORBIDDEN).await;
        for authn in
            [None, Some(AuthnMode::UnprivilegedUser), Some(owner.clone())]
        {
            let builder = RequestBuilder::new(client, Method::POST, &url)
                .header("host", &host)
                .body(Some(&json!({"jwt": "invalid"})))
                .expect_status(Some(StatusCode::BAD_REQUEST));
            let response = match authn {
                None => builder.execute().await.unwrap(),
                Some(authn) => NexusRequest::new(builder)
                    .authn_as(authn)
                    .execute()
                    .await
                    .unwrap(),
            };
            let (operation, status): (String, Option<i32>) = audit_log::table
                .filter(audit_log::time_completed.gt(started))
                .filter(
                    audit_log::request_id.eq(response.headers["x-request-id"]
                        .to_str()
                        .unwrap()
                        .to_owned()),
                )
                .select((audit_log::operation_id, audit_log::http_status_code))
                .first_async(&*conn)
                .await
                .unwrap();
            assert_eq!(operation, "service_account_federation_token_create");
            assert_eq!(status, Some(400));
        }
        let (wrong_key, _) = signing_key();
        exchange(
            client,
            &host,
            &url,
            &json!({"jwt": signed(&wrong_key, &claims)}),
            StatusCode::FORBIDDEN,
        )
        .await;
        for (field, value) in [
            ("sub", json!("other")),
            ("aud", json!("other")),
            ("my_idp", json!({})),
            ("exp", json!(Utc::now().timestamp() - 1)),
        ] {
            let mut bad = claims.clone();
            bad[field] = value;
            exchange(
                client,
                &host,
                &url,
                &json!({"jwt": signed(&key, &bad)}),
                StatusCode::FORBIDDEN,
            )
            .await;
        }
        exchange(
            client,
            &host,
            &url,
            &json!({"jwt": "x".repeat(32 * 1024 + 1)}),
            StatusCode::BAD_REQUEST,
        )
        .await;
        request(
            client,
            &owner,
            Method::PATCH,
            &account_url,
            Some(&json!({"federation": null})),
            StatusCode::OK,
        )
        .await;
        exchange(client, &host, &url, &body, StatusCode::FORBIDDEN).await;
        request(
            client,
            &owner,
            Method::PATCH,
            &account_url,
            Some(&json!({"federation": federation(120)})),
            StatusCode::OK,
        )
        .await;
    }
    let body = json!({"jwt": signed(&key, &claims())});
    exchange(client, &host, TOKEN_ENDPOINT, &body, StatusCode::FORBIDDEN).await;
    diesel::update(
        federation_identity_provider::table
            .filter(federation_identity_provider::id.eq(idp_id)),
    )
    .set(federation_identity_provider::silo_id.eq(other_silo))
    .execute_async(&*conn)
    .await
    .unwrap();
    exchange(client, &host, &last_url, &body, StatusCode::FORBIDDEN).await;
    diesel::update(
        federation_identity_provider::table
            .filter(federation_identity_provider::id.eq(idp_id)),
    )
    .set(federation_identity_provider::silo_id.eq(silo_id))
    .execute_async(&*conn)
    .await
    .unwrap();
    request(
        client,
        &owner,
        Method::DELETE,
        &format!("{IDPS}/{idp_id}"),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    exchange(client, &host, &last_url, &body, StatusCode::FORBIDDEN).await;
}

#[nexus_test]
async fn test_service_account_federation_config_changes(
    ctx: &ControlPlaneTestContext,
) {
    let client = &ctx.external_client;
    let (silo_id, owner) = admin(client, "federation-races").await;
    let (_, keys) = signing_key();
    let idp_id = provider(client, &owner, keys).await;
    let created: Value = request(client, &owner, Method::POST,
        &format!("/v1/service-accounts/silo?silo={silo_id}"), Some(&json!({
            "name": "builder", "description": "test", "grants": [], "federation": federation(120),
        })), StatusCode::CREATED).await.parsed_body().unwrap();
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    let url = format!("/v1/service-accounts/silo/{id}");
    let nexus = &ctx.server.server_context().nexus;
    let store = nexus.datastore();
    let opctx = nexus.opctx_external_authn();
    let path = ServiceAccountPath {
        scope: ServiceAccountScope::Silo,
        service_account: id.into(),
    };
    let snapshot = store
        .service_account_federation_config(
            opctx,
            silo_id,
            path.clone(),
            ServiceAccountParentSelector::default(),
        )
        .await
        .unwrap();
    request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": federation(10)})),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(
        store
            .service_account_federation_token_create(
                opctx,
                &snapshot,
                claims(),
                NonZeroU32::new(30)
            )
            .await,
        Err(Error::InvalidRequest { .. })
    ));
    let token = store
        .service_account_federation_token_create(
            opctx,
            &snapshot,
            claims(),
            None,
        )
        .await
        .unwrap();
    assert!(token.time_expires.unwrap() <= Utc::now() + Duration::seconds(10));
    let mut changed = federation(120);
    changed["policy"] = json!("assume(claims) if claims.sub = \"other\";");
    request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": changed})),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(
        store
            .service_account_federation_token_create(
                opctx,
                &snapshot,
                claims(),
                None
            )
            .await,
        Err(Error::Forbidden)
    ));
    let snapshot = store
        .service_account_federation_config(
            opctx,
            silo_id,
            path.clone(),
            ServiceAccountParentSelector::default(),
        )
        .await
        .unwrap();
    let (_, rotated) = signing_key();
    request(
        client,
        &owner,
        Method::PATCH,
        &format!("{IDPS}/{idp_id}"),
        Some(&json!({"signing_keys": rotated})),
        StatusCode::OK,
    )
    .await;
    store
        .service_account_federation_token_create(
            opctx,
            &snapshot,
            claims(),
            None,
        )
        .await
        .unwrap();
    request(client, &owner, Method::DELETE, &url, None, StatusCode::NO_CONTENT)
        .await;
    assert!(matches!(
        store
            .service_account_federation_token_create(
                opctx,
                &snapshot,
                claims(),
                None
            )
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        store
            .service_account_federation_config(
                opctx,
                silo_id,
                path,
                ServiceAccountParentSelector::default()
            )
            .await,
        Err(Error::Forbidden)
    ));
}
