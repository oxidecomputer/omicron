// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Affinity and anti-affinity group types for version INITIAL.

use super::instance::InstanceState;

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, Name, NameOrId, ObjectIdentity,
};
use omicron_uuid_kinds::InstanceUuid;
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

/// Affinity policy used to describe "what to do when a request cannot be satisfied"
///
/// Used for both Affinity and Anti-Affinity Groups
#[derive(
    Clone, Copy, Debug, Deserialize, Hash, Eq, Serialize, PartialEq, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AffinityPolicy {
    /// If the affinity request cannot be satisfied, allow it anyway.
    ///
    /// This enables a "best-effort" attempt to satisfy the affinity policy.
    Allow,

    /// If the affinity request cannot be satisfied, fail explicitly.
    Fail,
}

/// Describes the scope of affinity for the purposes of co-location.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureDomain {
    /// Instances are considered co-located if they are on the same sled
    Sled,
}

/// A member of an Affinity Group
///
/// Membership in a group is not exclusive - members may belong to multiple
/// affinity / anti-affinity groups.
///
/// Affinity Groups can contain up to 32 members.
// See: AFFINITY_GROUP_MAX_MEMBERS
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum AffinityGroupMember {
    /// An instance belonging to this group
    ///
    /// Instances can belong to up to 16 affinity groups.
    // See: INSTANCE_MAX_AFFINITY_GROUPS
    Instance {
        #[schemars(with = "Uuid")]
        id: InstanceUuid,
        name: Name,
        run_state: InstanceState,
    },
}

/// A member of an Anti-Affinity Group
///
/// Membership in a group is not exclusive - members may belong to multiple
/// affinity / anti-affinity groups.
///
/// Anti-Affinity Groups can contain up to 32 members.
// See: ANTI_AFFINITY_GROUP_MAX_MEMBERS
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum AntiAffinityGroupMember {
    /// An instance belonging to this group
    ///
    /// Instances can belong to up to 16 anti-affinity groups.
    // See: INSTANCE_MAX_ANTI_AFFINITY_GROUPS
    Instance {
        #[schemars(with = "Uuid")]
        id: InstanceUuid,
        name: Name,
        run_state: InstanceState,
    },
}
