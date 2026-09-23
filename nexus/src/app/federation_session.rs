// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::external_client::{ExternalClientBuilder, ExternalHttpClient};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use nexus_db_model::FederationIdentityProvider;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::federation::{
    FederationToken, FederationTokenRequest,
};
use omicron_common::api::external::Error;
use openidconnect::core::{
    CoreIdToken, CoreIdTokenVerifier, CoreJsonWebKeySet,
    CoreJwsSigningAlgorithm, CoreProviderMetadata,
};
use openidconnect::{ClientId, IssuerUrl, Nonce};
use oso::{Oso, PolarValue, ToPolar};
use serde_json::Value;
use std::time::Duration;
use url::Url;
use uuid::Uuid;

const MAX_JWT_BYTES: usize = 32 * 1024;
const MAX_METADATA_BYTES: usize = 1024 * 1024;

impl super::Nexus {
    pub(crate) async fn authenticate_federation_token(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> Result<nexus_auth::authn::Details, nexus_auth::authn::Reason> {
        use nexus_auth::{authn, authz};
        use nexus_db_model::DatabaseString;
        use nexus_types::external_api::federation::FederationRoleGrant;
        use omicron_common::api::external::ResourceType;

        let (session, silo_id, grants) = self
            .datastore()
            .federation_session_fetch_for_authn(opctx, token)
            .await
            .map_err(|source| authn::Reason::UnknownError { source })?
            .ok_or_else(|| authn::Reason::UnknownActor {
                actor: "federation session".to_owned(),
            })?;
        let mut roles = authz::RoleSet::new();
        for grant in grants {
            match FederationRoleGrant::try_from(grant)
                .map_err(|source| authn::Reason::UnknownError { source })?
            {
                FederationRoleGrant::Silo { resource_id, role_name } => {
                    roles.insert(
                        ResourceType::Silo,
                        resource_id,
                        &role_name.to_database_string(),
                    );
                }
                FederationRoleGrant::Project { resource_id, role_name } => {
                    roles.insert(
                        ResourceType::Project,
                        resource_id,
                        &role_name.to_database_string(),
                    );
                }
            }
        }
        Ok(authn::Details {
            actor: authn::Actor::Federated { session_id: session.id, silo_id },
            credential_id: Some(session.id),
            device_token_expiration: None,
            federation_roles: Some(roles),
        })
    }

    pub(crate) async fn federation_token_create(
        &self,
        opctx: &OpContext,
        silo_id: Uuid,
        params: FederationTokenRequest,
        audit_log_id: Uuid,
    ) -> Result<FederationToken, Error> {
        if params.oidc_jwt.len() > MAX_JWT_BYTES {
            return Err(Error::invalid_request("identity token is too large"));
        }
        let verified = self
            .db_datastore
            .federation_trust_policy_config(
                opctx,
                silo_id,
                &params.trust_policy,
            )
            .await?;
        let (keys, algorithms) = match verified
            .identity_provider
            .verification_type
            .as_str()
        {
            "static_jwks" => (
                verification_keys(
                    verified
                        .identity_provider
                        .signing_keys
                        .clone()
                        .ok_or(Error::Forbidden)?,
                )?,
                None,
            ),
            "oidc_discovery" => {
                let builder: ExternalClientBuilder =
                    reqwest::ClientBuilder::new()
                        .connect_timeout(Duration::from_secs(5))
                        .timeout(Duration::from_secs(10))
                        .into();
                let client = builder
                    .redirect(reqwest::redirect::Policy::none())
                    .build(
                        &self.external_http_client_config,
                        &self.external_resolver,
                    )
                    .map_err(|_| {
                        Error::internal_error("building federation HTTP client")
                    })?;
                let (keys, algorithms) =
                    discover_keys(&client, &verified.identity_provider).await?;
                (keys, Some(algorithms))
            }
            _ => return Err(Error::Forbidden),
        };
        let provider = verified.identity_provider.clone();
        let policy = verified.trust_policy.policy.clone();
        let claims = tokio::task::spawn_blocking(move || {
            let claims = verify_jwt(
                &params.oidc_jwt,
                &provider,
                &keys,
                algorithms.as_deref(),
            )?;
            evaluate_policy(&policy, &claims)?;
            Ok::<_, Error>(claims)
        })
        .await
        .map_err(|_| {
            Error::internal_error("evaluating federation request")
        })??;
        let session = self
            .db_datastore
            .federation_session_create(opctx, &verified, claims, audit_log_id)
            .await?;
        Ok(FederationToken {
            token: format!("oxide-federation-{}", session.token),
            expires_at: session.time_expires,
            revision: session.trust_policy_revision,
        })
    }
}

async fn discover_keys(
    client: &ExternalHttpClient,
    provider: &FederationIdentityProvider,
) -> Result<(CoreJsonWebKeySet, Vec<CoreJwsSigningAlgorithm>), Error> {
    let metadata: CoreProviderMetadata = serde_json::from_value(
        fetch_json(
            client,
            provider.discovery_url.as_deref().ok_or(Error::Forbidden)?,
        )
        .await?,
    )
    .map_err(|_| Error::Forbidden)?;
    if metadata.issuer().as_str() != provider.issuer {
        return Err(Error::Forbidden);
    }
    let keys = verification_keys(
        fetch_json(client, metadata.jwks_uri().as_str()).await?,
    )?;
    Ok((keys, metadata.id_token_signing_alg_values_supported().clone()))
}

fn verification_keys(mut jwks: Value) -> Result<CoreJsonWebKeySet, Error> {
    let keys = jwks
        .get_mut("keys")
        .and_then(Value::as_array_mut)
        .ok_or(Error::Forbidden)?;
    keys.retain(|key| {
        key.get("key_ops").is_none_or(|ops| {
            ops.as_array().is_some_and(|ops| {
                ops.len() == 1 && ops[0].as_str() == Some("verify")
            })
        })
    });
    serde_json::from_value(jwks).map_err(|_| Error::Forbidden)
}

async fn fetch_json(
    client: &ExternalHttpClient,
    value: &str,
) -> Result<Value, Error> {
    let url = Url::parse(value).map_err(|_| Error::Forbidden)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Forbidden);
    }
    let mut response = client
        .get(url)
        .map_err(|_| Error::Forbidden)?
        .send()
        .await
        .map_err(|_| Error::Forbidden)?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|n| n > MAX_METADATA_BYTES as u64)
    {
        return Err(Error::Forbidden);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) =
        response.chunk().await.map_err(|_| Error::Forbidden)?
    {
        if chunk.len() > MAX_METADATA_BYTES - bytes.len() {
            return Err(Error::Forbidden);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Forbidden)
}

fn verify_jwt(
    token: &str,
    provider: &FederationIdentityProvider,
    keys: &CoreJsonWebKeySet,
    advertised_algorithms: Option<&[CoreJwsSigningAlgorithm]>,
) -> Result<Value, Error> {
    let id_token: CoreIdToken = token.parse().map_err(|_| Error::Forbidden)?;
    let algorithm = id_token.signing_alg().map_err(|_| Error::Forbidden)?;
    if advertised_algorithms.is_some_and(|algs| !algs.contains(algorithm)) {
        return Err(Error::Forbidden);
    }
    let verifier = CoreIdTokenVerifier::new_public_client(
        ClientId::new(provider.audience.clone()),
        IssuerUrl::new(provider.issuer.clone())
            .map_err(|_| Error::Forbidden)?,
        keys.clone(),
    )
    .set_allowed_algs([CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256])
    .set_issue_time_verifier_fn(|issued| {
        if issued > Utc::now() + chrono::Duration::seconds(60) {
            Err("token issued in the future".to_owned())
        } else {
            Ok(())
        }
    });
    let registered = id_token
        .claims(&verifier, |_: Option<&Nonce>| Ok(()))
        .map_err(|_| Error::Forbidden)?;
    if registered.expiration() <= registered.issue_time()
        || registered.subject().as_str().is_empty()
        || registered.subject().as_str().len() > 255
        || !registered.subject().as_str().is_ascii()
    {
        return Err(Error::Forbidden);
    }
    let payload = URL_SAFE_NO_PAD
        .decode(token.split('.').nth(1).ok_or(Error::Forbidden)?)
        .map_err(|_| Error::Forbidden)?;
    let claims: Value =
        serde_json::from_slice(&payload).map_err(|_| Error::Forbidden)?;
    if let Some(nbf) = claims.get("nbf") {
        let nbf = nbf.as_i64().ok_or(Error::Forbidden)?;
        if nbf > Utc::now().timestamp() {
            return Err(Error::Forbidden);
        }
    }
    Ok(claims)
}

fn evaluate_policy(policy: &str, claims: &Value) -> Result<(), Error> {
    nexus_db_model::validate_federation_trust_policy(None, Some(policy), None)
        .map_err(|_| Error::Forbidden)?;
    let mut oso = Oso::new();
    oso.load_str(policy).map_err(|_| Error::Forbidden)?;
    let mut query = oso
        .query_rule("assume", (polar_claims(claims)?,))
        .map_err(|_| Error::Forbidden)?;
    match query.next() {
        Some(Ok(_)) => Ok(()),
        _ => Err(Error::Forbidden),
    }
}

fn polar_claims(value: &Value) -> Result<PolarValue, Error> {
    Ok(match value {
        Value::Null => Option::<PolarValue>::None.to_polar(),
        Value::Bool(v) => PolarValue::Boolean(*v),
        Value::String(v) => PolarValue::String(v.clone()),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                PolarValue::Integer(v)
            } else if v.is_f64() {
                PolarValue::Float(v.as_f64().ok_or(Error::Forbidden)?)
            } else {
                return Err(Error::Forbidden);
            }
        }
        Value::Array(v) => PolarValue::List(
            v.iter().map(polar_claims).collect::<Result<_, _>>()?,
        ),
        Value::Object(v) => PolarValue::Map(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), polar_claims(v)?)))
                .collect::<Result<_, Error>>()?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::jwk::{Jwk, JwkSet};
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use nexus_types::external_api::federation::{
        FederationIdentityProviderCreate, FederationVerificationType,
    };
    use serde_json::json;

    fn claims() -> Value {
        let now = Utc::now().timestamp();
        json!({"iss": "https://issuer.example", "aud": "oxide", "sub": "builder",
            "iat": now, "exp": now + 300, "azp": "requesting-service-account",
            "my_idp": {"custom_claims": {"project_id": "project-123"}},
            "groups": ["build", "deploy"], "optional": null})
    }

    #[test]
    fn federation_verifier_configuration() {
        let pair = openssl::rsa::Rsa::generate(2048).unwrap();
        let key =
            EncodingKey::from_rsa_der(&pair.private_key_to_der().unwrap());
        let mut jwk = Jwk::from_encoding_key(&key, Algorithm::RS256).unwrap();
        jwk.common.key_id = Some("test-key".into());
        let jwks = serde_json::to_value(JwkSet { keys: vec![jwk] }).unwrap();
        let provider = FederationIdentityProvider::new(
            Uuid::new_v4(),
            FederationIdentityProviderCreate {
                name: "test".parse().unwrap(),
                description: String::new(),
                issuer: "https://issuer.example".into(),
                audience: "oxide".into(),
                verification_type: FederationVerificationType::StaticJwks,
                discovery_url: None,
                signing_keys: Some(jwks.clone()),
            },
        )
        .unwrap();
        let keys = verification_keys(jwks.clone()).unwrap();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".into());
        let valid = claims();
        let token = encode(&header, &valid, &key).unwrap();
        assert_eq!(verify_jwt(&token, &provider, &keys, None).unwrap(), valid);
        assert!(verify_jwt(&token, &provider, &keys, Some(&[])).is_err());
        for (field, bad) in [
            ("iss", json!("https://other.example")),
            ("aud", json!("other")),
            ("nbf", json!(Utc::now().timestamp() + 300)),
            ("iat", json!(Utc::now().timestamp() + 300)),
        ] {
            let mut claims = valid.clone();
            claims[field] = bad;
            let token = encode(&header, &claims, &key).unwrap();
            assert!(
                verify_jwt(&token, &provider, &keys, None).is_err(),
                "{field}"
            );
        }
        let mut restricted_keys = jwks.clone();
        restricted_keys["keys"][0]["key_ops"] = json!(["sign"]);
        let restricted_keys = verification_keys(restricted_keys).unwrap();
        assert!(verify_jwt(&token, &provider, &restricted_keys, None).is_err());
        let mut other_algorithm_keys = jwks;
        other_algorithm_keys["keys"][0]["alg"] = json!("RS384");
        let other_algorithm_keys =
            verification_keys(other_algorithm_keys).unwrap();
        header.alg = Algorithm::RS384;
        let token = encode(&header, &valid, &key).unwrap();
        assert!(
            verify_jwt(&token, &provider, &other_algorithm_keys, None).is_err()
        );
    }

    #[test]
    fn federation_polar_claims() {
        let claims = claims();
        assert!(evaluate_policy(r#"assume(claims) if claims.sub = "builder" and claims.my_idp.custom_claims.project_id = "project-123" and "build" in claims.groups and claims.optional = nil;"#, &claims).is_ok());
        assert!(
            evaluate_policy(
                r#"assume(claims) if claims.sub = "other";"#,
                &claims
            )
            .is_err()
        );
        assert!(
            evaluate_policy(
                "assume(claims) if claims.missing = true;",
                &claims
            )
            .is_err()
        );
        assert!(
            evaluate_policy(
                "assume(claims) if missing_helper(claims);",
                &claims
            )
            .is_err()
        );
        assert!(
            evaluate_policy("assume(_claims); ?= assume({});", &claims)
                .is_err()
        );
        assert!(evaluate_policy("assume(", &claims).is_err());
        assert!(evaluate_policy("assume(_claims);", &claims).is_ok());
    }
}

#[cfg(test)]
mod discovery_tests {
    use super::super::{external_client::ExternalIpPolicy, external_dns};
    use super::*;
    use jsonwebtoken::Algorithm;
    use nexus_config::{ExternalHttpClientConfig, TreatLoopbackAsExternal};
    use omicron_common::address::{
        Ipv6Subnet, RACK_PREFIX_LENGTH, UnderlaySubnets,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{IpAddr, Ipv4Addr, TcpListener};
    use std::sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    };

    struct Server {
        url: String,
        cert: reqwest::Certificate,
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl Server {
        fn new(routes: impl FnOnce(&str) -> HashMap<String, String>) -> Self {
            let cert =
                rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])
                    .unwrap();
            let cert_der = cert.serialize_der().unwrap();
            let key = rustls::pki_types::PrivatePkcs8KeyDer::from(
                cert.serialize_private_key_der(),
            );
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone().into()], key.into())
            .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("https://{}", listener.local_addr().unwrap());
            let routes = routes(&url);
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = stop.clone();
            let config = Arc::new(config);
            let thread = std::thread::spawn(move || {
                while !stopped.load(Ordering::Relaxed) {
                    let (stream, _) = match listener.accept() {
                        Ok(pair) => pair,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            std::thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(e) => panic!("{e}"),
                    };
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let connection =
                        rustls::ServerConnection::new(config.clone()).unwrap();
                    let mut stream =
                        rustls::StreamOwned::new(connection, stream);
                    let mut request = Vec::new();
                    let mut byte = [0; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        if stream.read_exact(&mut byte).is_err() {
                            break;
                        }
                        request.push(byte[0]);
                    }
                    let text = String::from_utf8_lossy(&request);
                    if let Some(path) = text.split_whitespace().nth(1) {
                        if let Some(response) = routes.get(path) {
                            let _ = stream.write_all(response.as_bytes());
                            stream.conn.send_close_notify();
                            let _ = stream.flush();
                        }
                    }
                }
            });
            Self {
                url,
                cert: reqwest::Certificate::from_der(&cert_der).unwrap(),
                stop,
                thread: Some(thread),
            }
        }

        fn client(&self) -> ExternalHttpClient {
            let subnets =
                UnderlaySubnets::new(Ipv6Subnet::<RACK_PREFIX_LENGTH>::from(
                    nexus_test_utils::RACK_SUBNET
                        .parse::<ipnetwork::Ipv6Network>()
                        .unwrap(),
                ));
            let policy = ExternalIpPolicy::new(
                Arc::new(OnceLock::from(subnets)),
                TreatLoopbackAsExternal::YesForTestPurposesOnly,
            );
            let resolver = Arc::new(external_dns::Resolver::new(
                &[IpAddr::V4(Ipv4Addr::LOCALHOST)],
                policy,
            ));
            let builder: ExternalClientBuilder = reqwest::ClientBuilder::new()
                .add_root_certificate(self.cert.clone())
                .timeout(Duration::from_secs(3))
                .into();
            builder
                .redirect(reqwest::redirect::Policy::none())
                .build(
                    &ExternalHttpClientConfig {
                        interface: None,
                        treat_loopback_as_external:
                            TreatLoopbackAsExternal::YesForTestPurposesOnly,
                    },
                    &resolver,
                )
                .unwrap()
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.thread.take().unwrap().join().unwrap();
        }
    }

    fn response(body: Value) -> String {
        let body = body.to_string();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn metadata(issuer: &str, jwks_uri: String) -> Value {
        json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "jwks_uri": jwks_uri,
            "response_types_supported": ["id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        })
    }

    #[tokio::test]
    async fn federation_discovery_fetching() {
        let pair = openssl::rsa::Rsa::generate(2048).unwrap();
        let key = jsonwebtoken::EncodingKey::from_rsa_der(
            &pair.private_key_to_der().unwrap(),
        );
        let mut jwk =
            jsonwebtoken::jwk::Jwk::from_encoding_key(&key, Algorithm::RS256)
                .unwrap();
        jwk.common.key_id = Some("test".into());
        let server = Server::new(|url| {
            HashMap::from([
                (
                    "/discovery".into(),
                    response(metadata(
                        "https://issuer.example",
                        format!("{url}/keys"),
                    )),
                ),
                (
                    "/wrong-issuer".into(),
                    response(metadata(
                        "https://other.example",
                        format!("{url}/keys"),
                    )),
                ),
                (
                    "/redirect-keys".into(),
                    response(metadata(
                        "https://issuer.example",
                        format!("{url}/redirect"),
                    )),
                ),
                ("/keys".into(), response(json!({"keys": [jwk]}))),
                (
                    "/redirect".into(),
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: {url}/keys\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    ),
                ),
                (
                    "/oversized".into(),
                    response(json!({"data": "x".repeat(MAX_METADATA_BYTES)})),
                ),
                (
                    "/chunked-oversized".into(),
                    format!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                        MAX_METADATA_BYTES + 1,
                        "x".repeat(MAX_METADATA_BYTES + 1)
                    ),
                ),
            ])
        });
        let client = server.client();
        let mut provider = FederationIdentityProvider::new(Uuid::new_v4(), nexus_types::external_api::federation::FederationIdentityProviderCreate {
            name: "test".parse().unwrap(), description: String::new(), issuer: "https://issuer.example".into(), audience: "oxide".into(),
            verification_type: nexus_types::external_api::federation::FederationVerificationType::OidcDiscovery,
            discovery_url: Some(format!("{}/discovery", server.url)), signing_keys: None,
        }).unwrap();
        let (keys, algorithms) =
            discover_keys(&client, &provider).await.unwrap();
        let now = Utc::now().timestamp();
        let claims = json!({"iss": "https://issuer.example", "sub": "builder", "aud": "oxide", "iat": now, "exp": now + 60});
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some("test".into());
        let jwt = jsonwebtoken::encode(&header, &claims, &key).unwrap();
        assert_eq!(
            verify_jwt(&jwt, &provider, &keys, Some(&algorithms)).unwrap(),
            claims
        );
        for path in [
            "wrong-issuer",
            "redirect",
            "redirect-keys",
            "oversized",
            "chunked-oversized",
        ] {
            provider.discovery_url = Some(format!("{}/{path}", server.url));
            assert!(discover_keys(&client, &provider).await.is_err(), "{path}");
        }
        for url in [
            "http://127.0.0.1/keys",
            "https://user:password@127.0.0.1/keys",
            "https://127.0.0.1/keys#fragment",
            "https://[fd00::1]/keys",
        ] {
            assert!(fetch_json(&client, url).await.is_err(), "{url}");
        }
    }
}
