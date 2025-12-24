// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Identity provider types for version INITIAL.

use api_identity::ObjectIdentity;
use base64::Engine;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams, NameOrId, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdentityProviderType {
    /// SAML identity provider
    Saml,
}

/// View of an Identity Provider
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IdentityProvider {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Identity provider type
    pub provider_type: IdentityProviderType,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SamlIdentityProvider {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// IdP's entity id
    pub idp_entity_id: String,

    /// SP's client id
    pub sp_client_id: String,

    /// Service provider endpoint where the response will be sent
    pub acs_url: String,

    /// Service provider endpoint where the idp should send log out requests
    pub slo_url: String,

    /// Customer's technical contact for saml configuration
    pub technical_contact_email: String,

    /// Optional request signing public certificate (base64 encoded der file)
    pub public_cert: Option<String>,

    /// If set, attributes with this name will be considered to denote a user's
    /// group membership, where the values will be the group names.
    pub group_attribute_name: Option<String>,
}

// Params

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SamlIdentityProviderSelector {
    /// Name or ID of the silo in which the SAML identity provider is associated
    pub silo: Option<NameOrId>,
    /// Name or ID of the SAML identity provider
    pub saml_identity_provider: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DerEncodedKeyPair {
    /// request signing public certificate (base64 encoded der file)
    #[serde(deserialize_with = "x509_cert_from_base64_encoded_der")]
    pub public_cert: String,

    /// request signing RSA private key in PKCS#1 format (base64 encoded der file)
    #[serde(deserialize_with = "key_from_base64_encoded_der")]
    pub private_key: String,
}

struct X509CertVisitor;

impl Visitor<'_> for X509CertVisitor {
    type Value = String;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str("a DER formatted X509 certificate as a string of base64 encoded bytes")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&value.as_bytes())
            .map_err(|e| {
                de::Error::custom(format!(
                    "could not base64 decode public_cert: {}",
                    e
                ))
            })?;
        let _parsed =
            openssl::x509::X509::from_der(&raw_bytes).map_err(|e| {
                de::Error::custom(format!(
                    "public_cert is not recognized as a X509 certificate: {}",
                    e
                ))
            })?;

        Ok(value.to_string())
    }
}

fn x509_cert_from_base64_encoded_der<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(X509CertVisitor)
}

struct KeyVisitor;

impl Visitor<'_> for KeyVisitor {
    type Value = String;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str(
            "a DER formatted key as a string of base64 encoded bytes",
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&value)
            .map_err(|e| {
                de::Error::custom(format!(
                    "could not base64 decode private_key: {}",
                    e
                ))
            })?;

        let parsed = openssl::rsa::Rsa::private_key_from_der(&raw_bytes)
            .map_err(|e| {
                de::Error::custom(format!(
                    "private_key is not recognized as a RSA private key: {}",
                    e
                ))
            })?;
        let _parsed = openssl::pkey::PKey::from_rsa(parsed).map_err(|e| {
            de::Error::custom(format!(
                "private_key is not recognized as a RSA private key: {}",
                e
            ))
        })?;

        Ok(value.to_string())
    }
}

fn key_from_base64_encoded_der<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(KeyVisitor)
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IdpMetadataSource {
    Url { url: String },
    Base64EncodedXml { data: String },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SamlIdentityProviderCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// the source of an identity provider metadata descriptor
    pub idp_metadata_source: IdpMetadataSource,

    /// idp's entity id
    pub idp_entity_id: String,

    /// sp's client id
    pub sp_client_id: String,

    /// service provider endpoint where the response will be sent
    pub acs_url: String,

    /// service provider endpoint where the idp should send log out requests
    pub slo_url: String,

    /// customer's technical contact for saml configuration
    pub technical_contact_email: String,

    /// request signing key pair
    #[serde(default)]
    #[serde(deserialize_with = "validate_key_pair")]
    pub signing_keypair: Option<DerEncodedKeyPair>,

    /// If set, SAML attributes with this name will be considered to denote a
    /// user's group membership, where the attribute value(s) should be a
    /// comma-separated list of group names.
    pub group_attribute_name: Option<String>,
}

/// sign some junk data and validate it with the key pair
fn sign_junk_data(key_pair: &DerEncodedKeyPair) -> Result<(), anyhow::Error> {
    let private_key = {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&key_pair.private_key)?;
        let parsed = openssl::rsa::Rsa::private_key_from_der(&raw_bytes)?;
        let parsed = openssl::pkey::PKey::from_rsa(parsed)?;
        parsed
    };

    let public_key = {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&key_pair.public_cert)?;
        let parsed = openssl::x509::X509::from_der(&raw_bytes)?;
        parsed.public_key()?
    };

    let mut signer = openssl::sign::Signer::new(
        openssl::hash::MessageDigest::sha256(),
        &private_key.as_ref(),
    )?;

    let some_junk_data = b"this is some junk data";

    signer.update(some_junk_data)?;
    let signature = signer.sign_to_vec()?;

    let mut verifier = openssl::sign::Verifier::new(
        openssl::hash::MessageDigest::sha256(),
        &public_key,
    )?;

    verifier.update(some_junk_data)?;

    if !verifier.verify(&signature)? {
        anyhow::bail!("signature validation failed!");
    }

    Ok(())
}

fn validate_key_pair<'de, D>(
    deserializer: D,
) -> Result<Option<DerEncodedKeyPair>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<DerEncodedKeyPair>::deserialize(deserializer)?;

    if let Some(ref key_pair) = v {
        if let Err(e) = sign_junk_data(&key_pair) {
            return Err(de::Error::custom(format!(
                "data signed with key not verified with certificate! {}",
                e
            )));
        }
    }

    Ok(v)
}
