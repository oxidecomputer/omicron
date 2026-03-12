// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Affinity and anti-affinity group types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    AffinityPolicy, FailureDomain, IdentityMetadata,
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams, NameOrId,
    ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of an Affinity Group
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AffinityGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// View of an Anti-Affinity Group
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AntiAffinityGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// Create-time parameters for an `AffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AffinityGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// Updateable properties of an `AffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AffinityGroupUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// Create-time parameters for an `AntiAffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AntiAffinityGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// Updateable properties of an `AntiAffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AntiAffinityGroupUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct AffinityGroupSelector {
    /// Name or ID of the project, only required if `affinity_group` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the Affinity Group
    pub affinity_group: NameOrId,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct AntiAffinityGroupSelector {
    /// Name or ID of the project, only required if `anti_affinity_group` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the Anti Affinity Group
    pub anti_affinity_group: NameOrId,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AffinityInstanceGroupMemberPath {
    pub affinity_group: NameOrId,
    pub instance: NameOrId,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AntiAffinityInstanceGroupMemberPath {
    pub anti_affinity_group: NameOrId,
    pub instance: NameOrId,
}
