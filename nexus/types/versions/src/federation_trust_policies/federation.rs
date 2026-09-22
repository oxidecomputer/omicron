// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00::policy::{ProjectRole, SiloRole};
use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, Name, NameOrId, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "resource_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FederationRoleGrant {
    Silo {
        /// UUID of the current silo.
        resource_id: Uuid,
        /// Built-in silo role to grant.
        role_name: SiloRole,
    },
    Project {
        /// UUID of a project in the current silo.
        resource_id: Uuid,
        /// Built-in project role to grant.
        role_name: ProjectRole,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FederationTrustPolicyCreate {
    /// Name of the trust policy within the current silo.
    pub name: Name,
    /// Human-readable description, limited to 512 characters.
    pub description: String,
    /// Identity provider name or UUID within the current silo.
    pub identity_provider: NameOrId,
    /// Roles granted when the policy is assumed.
    pub grants: Vec<FederationRoleGrant>,
    /// Polar policy defining `assume` with one parameter for the verified JWT claims.
    pub policy: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FederationTrustPolicyUpdate {
    /// New name within the current silo.
    pub name: Option<Name>,
    /// New description, limited to 512 characters.
    pub description: Option<String>,
    /// Replacement identity provider name or UUID in the current silo.
    pub identity_provider: Option<NameOrId>,
    /// Replacement grants; an empty list removes all grants.
    pub grants: Option<Vec<FederationRoleGrant>>,
    /// Replacement Polar policy defining `assume` with one parameter for the verified JWT claims.
    pub policy: Option<String>,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FederationTrustPolicy {
    #[serde(flatten)]
    /// Identifying metadata.
    pub identity: IdentityMetadata,
    /// Revision of the identity provider, grants, and Polar rules.
    pub revision: i64,
    /// UUID of the identity provider.
    pub idp_id: Uuid,
    /// Roles granted when the policy is assumed.
    pub grants: Vec<FederationRoleGrant>,
    /// Polar policy defining `assume` with one parameter for the verified JWT claims.
    pub policy: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FederationTrustPolicyPath {
    /// UUID of the trust policy within the current silo.
    pub policy_id: Uuid,
}
