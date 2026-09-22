// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use db_macros::Resource;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::jwk::{
    AlgorithmParameters, JwkSet, KeyOperations, PublicKeyUse,
};
use nexus_db_schema::schema::federation_identity_provider;
use nexus_types::external_api::federation as api;
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
use serde_json::Value;
use std::collections::BTreeSet;
use uuid::Uuid;

#[derive(Queryable, Selectable, Insertable, Clone, Debug, Resource)]
#[diesel(table_name = federation_identity_provider)]
pub struct FederationIdentityProvider {
    #[diesel(embed)]
    pub identity: FederationIdentityProviderIdentity,
    pub silo_id: Uuid,
    pub audience: String,
    pub issuer: String,
    pub verification_type: String,
    pub signing_keys: Option<Value>,
}

impl FederationIdentityProvider {
    pub fn new(
        silo_id: Uuid,
        mut params: api::FederationIdentityProviderCreate,
    ) -> Result<Self, Error> {
        validate_description(&params.description)?;
        if params.issuer.trim().is_empty() || params.audience.trim().is_empty()
        {
            return Err(Error::invalid_request(
                "issuer and audience must be nonempty",
            ));
        }
        let verification_type = match params.verification_type {
            api::FederationVerificationType::OidcDiscovery => {
                if params.signing_keys.is_some() {
                    return Err(Error::invalid_request(
                        "oidc_discovery does not accept signing_keys",
                    ));
                }
                url::Url::parse(&params.issuer).map_err(|_| {
                    Error::invalid_request("invalid issuer URL")
                })?;
                "oidc_discovery"
            }
            api::FederationVerificationType::StaticJwks => {
                params.signing_keys = Some(parse_signing_keys(
                    params.signing_keys.take().ok_or_else(|| {
                        Error::invalid_request(
                            "static_jwks requires signing_keys",
                        )
                    })?,
                )?);
                "static_jwks"
            }
        };
        Ok(Self {
            identity: FederationIdentityProviderIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: params.name,
                    description: params.description,
                },
            ),
            silo_id,
            audience: params.audience,
            issuer: params.issuer,
            verification_type: verification_type.to_string(),
            signing_keys: params.signing_keys,
        })
    }
}

impl TryFrom<FederationIdentityProvider> for api::FederationIdentityProvider {
    type Error = Error;

    fn try_from(provider: FederationIdentityProvider) -> Result<Self, Error> {
        let verification_type = match provider.verification_type.as_str() {
            "oidc_discovery" => api::FederationVerificationType::OidcDiscovery,
            "static_jwks" => api::FederationVerificationType::StaticJwks,
            _ => {
                return Err(Error::internal_error(
                    "invalid stored federation verification type",
                ));
            }
        };
        Ok(Self {
            identity: provider.identity(),
            issuer: provider.issuer,
            audience: provider.audience,
            verification_type,
            signing_keys: provider.signing_keys,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = federation_identity_provider)]
pub struct FederationIdentityProviderUpdate {
    pub name: Option<crate::Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub signing_keys: Option<Value>,
}

impl TryFrom<api::FederationIdentityProviderUpdate>
    for FederationIdentityProviderUpdate
{
    type Error = Error;

    fn try_from(
        params: api::FederationIdentityProviderUpdate,
    ) -> Result<Self, Error> {
        if let Some(description) = &params.description {
            validate_description(description)?;
        }
        Ok(Self {
            name: params.name.map(crate::Name),
            description: params.description,
            time_modified: Utc::now(),
            signing_keys: params
                .signing_keys
                .map(parse_signing_keys)
                .transpose()?,
        })
    }
}

fn validate_description(description: &str) -> Result<(), Error> {
    if description.chars().count() > 512 {
        return Err(Error::invalid_request(
            "description must be at most 512 characters",
        ));
    }
    Ok(())
}

fn parse_signing_keys(value: Value) -> Result<Value, Error> {
    let invalid =
        || Error::invalid_request("signing_keys must be a valid public JWKS");
    let jwks: JwkSet = serde_json::from_value(value).map_err(|_| invalid())?;
    if jwks.keys.is_empty() {
        return Err(Error::invalid_request(
            "signing_keys must contain at least one key",
        ));
    }
    let mut kids = BTreeSet::new();
    for key in &jwks.keys {
        let kid = key.common.key_id.as_deref().ok_or_else(|| {
            Error::invalid_request("each signing key must have a kid")
        })?;
        if kid.is_empty() || !kids.insert(kid) {
            return Err(Error::invalid_request(
                "signing key IDs must be nonempty and unique",
            ));
        }
        if !matches!(
            key.algorithm,
            AlgorithmParameters::RSA(_)
                | AlgorithmParameters::EllipticCurve(_)
                | AlgorithmParameters::OctetKeyPair(_)
        ) {
            return Err(Error::invalid_request(
                "signing_keys must contain asymmetric public keys",
            ));
        }
        if key
            .common
            .public_key_use
            .as_ref()
            .is_some_and(|usage| *usage != PublicKeyUse::Signature)
        {
            return Err(Error::invalid_request("JWK use must be sig"));
        }
        if key
            .common
            .key_operations
            .as_ref()
            .is_some_and(|ops| ops.as_slice() != [KeyOperations::Verify])
        {
            return Err(Error::invalid_request("JWK key_ops must be [verify]"));
        }
        DecodingKey::try_from(key).map_err(|_| invalid())?;
    }
    serde_json::to_value(jwks).map_err(|e| {
        Error::internal_error(format!("serializing public JWKS: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn public_keys() -> Value {
        json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "test",
            "x": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}]})
    }

    fn discovery() -> api::FederationIdentityProviderCreate {
        api::FederationIdentityProviderCreate {
            name: "test-idp".parse().unwrap(),
            description: String::new(),
            issuer: "https://issuer.example.com".into(),
            audience: "oxide".into(),
            verification_type: api::FederationVerificationType::OidcDiscovery,
            signing_keys: None,
        }
    }

    #[test]
    fn federation_provider_configuration() {
        assert!(
            FederationIdentityProvider::new(Uuid::new_v4(), discovery())
                .is_ok()
        );
        let mut params = discovery();
        params.signing_keys = Some(public_keys());
        assert!(
            FederationIdentityProvider::new(Uuid::new_v4(), params.clone())
                .is_err()
        );
        params.verification_type = api::FederationVerificationType::StaticJwks;
        assert!(
            FederationIdentityProvider::new(Uuid::new_v4(), params.clone())
                .is_ok()
        );
        params.signing_keys = None;
        assert!(
            FederationIdentityProvider::new(Uuid::new_v4(), params).is_err()
        );
        let mut params = discovery();
        params.issuer = "not a URL".into();
        assert!(
            FederationIdentityProvider::new(Uuid::new_v4(), params).is_err()
        );
    }

    #[test]
    fn federation_signing_keys() {
        let valid = public_keys();
        assert!(parse_signing_keys(valid.clone()).is_ok());
        for invalid in [
            json!({}),
            json!({"keys": []}),
            json!({"keys": [{"kty": "oct", "kid": "test", "k": "c2VjcmV0"}]}),
            json!({"keys": [valid["keys"][0], valid["keys"][0]]}),
        ] {
            assert!(parse_signing_keys(invalid).is_err());
        }
        for (field, value) in [
            ("x", json!("invalid!")),
            ("kid", json!("")),
            ("use", json!("enc")),
            ("key_ops", json!(["sign"])),
        ] {
            let mut invalid = valid.clone();
            invalid["keys"][0][field] = value;
            assert!(parse_signing_keys(invalid).is_err(), "{field}");
        }
        let mut rotation = valid.clone();
        let mut next = valid["keys"][0].clone();
        next["kid"] = json!("next");
        rotation["keys"].as_array_mut().unwrap().push(next);
        assert!(parse_signing_keys(rotation).is_ok());
    }

    #[test]
    fn federation_stores_public_jwk_representation() {
        let mut keys = public_keys();
        keys["keys"][0]["d"] = json!("private material");
        keys["keys"][0]["custom_extension"] = json!("ignored");
        let mut params = discovery();
        params.verification_type = api::FederationVerificationType::StaticJwks;
        params.signing_keys = Some(keys.clone());
        let provider =
            FederationIdentityProvider::new(Uuid::new_v4(), params).unwrap();
        let update = FederationIdentityProviderUpdate::try_from(
            api::FederationIdentityProviderUpdate {
                name: None,
                description: None,
                signing_keys: Some(keys),
            },
        )
        .unwrap();
        for stored in
            [provider.signing_keys.unwrap(), update.signing_keys.unwrap()]
        {
            assert_eq!(stored["keys"][0]["kid"], "test");
            assert_eq!(stored["keys"][0]["x"], public_keys()["keys"][0]["x"]);
            assert!(stored["keys"][0].get("d").is_none());
            assert!(stored["keys"][0].get("custom_extension").is_none());
        }
    }

    #[test]
    fn federation_patch_rejects_immutable_fields() {
        for field in [
            "id",
            "silo_id",
            "issuer",
            "audience",
            "verification_type",
            "discovery_url",
            "time_deleted",
        ] {
            assert!(serde_json::from_value::<api::FederationIdentityProviderUpdate>(json!({field: "changed"})).is_err(), "{field}");
        }
        assert!(
            serde_json::from_value::<api::FederationIdentityProviderUpdate>(
                json!({"name": "renamed", "signing_keys": public_keys()})
            )
            .is_ok()
        );
    }
}
