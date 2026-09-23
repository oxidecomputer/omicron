// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::federation_trust_policy::{request, silo_user};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use http::{Method, StatusCode};
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use nexus_db_model::FederationSession;
use nexus_db_schema::schema::{audit_log, federation_session};
use nexus_test_utils::background::activate_background_task;
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
            .expect_status(Some(StatusCode::FORBIDDEN));
        let response = match authn {
            None => builder.execute().await.unwrap(),
            Some(authn) => NexusRequest::new(builder)
                .authn_as(authn)
                .execute()
                .await
                .unwrap(),
        };
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
        assert_eq!(status, Some(403));
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
