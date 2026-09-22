// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, Name, ObjectIdentity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
/// How Nexus obtains verification keys.
pub enum FederationVerificationType {
    /// Fetch public keys using OIDC discovery.
    OidcDiscovery,
    /// Use configured public keys.
    StaticJwks,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FederationIdentityProviderCreate {
    /// Name of the identity provider within the current silo.
    pub name: Name,
    /// Human-readable description, limited to 512 characters.
    pub description: String,
    /// Exact expected JWT issuer. Immutable after creation.
    pub issuer: String,
    /// Required JWT audience. Immutable after creation.
    pub audience: String,
    /// Key source. Immutable after creation.
    pub verification_type: FederationVerificationType,
    /// HTTPS metadata URL, required only for `oidc_discovery`. Immutable after creation.
    pub discovery_url: Option<String>,
    /// Public JWKS, required only for `static_jwks`. Key IDs must be nonempty and unique.
    pub signing_keys: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FederationIdentityProviderUpdate {
    /// New name within the current silo.
    pub name: Option<Name>,
    /// New description, limited to 512 characters.
    pub description: Option<String>,
    /// Replacement public JWKS for a static provider; omitted or null leaves keys unchanged.
    pub signing_keys: Option<serde_json::Value>,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FederationIdentityProvider {
    #[serde(flatten)]
    /// Identifying metadata.
    pub identity: IdentityMetadata,
    /// Exact expected JWT issuer. Immutable after creation.
    pub issuer: String,
    /// Required JWT audience. Immutable after creation.
    pub audience: String,
    /// Key source. Immutable after creation.
    pub verification_type: FederationVerificationType,
    /// HTTPS metadata URL, required only for `oidc_discovery`. Immutable after creation.
    pub discovery_url: Option<String>,
    /// Public JWKS, required only for `static_jwks`. Key IDs must be nonempty and unique.
    pub signing_keys: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FederationIdentityProviderPath {
    /// Identity provider ID within the current silo.
    pub idp_id: Uuid,
}
