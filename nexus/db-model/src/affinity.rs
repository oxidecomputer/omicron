// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2025 Oxide Computer Company

//! Database representation of affinity and anti-affinity groups

use super::Name;
use super::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_db_schema::schema::affinity_group;
use nexus_db_schema::schema::affinity_group_instance_membership;
use nexus_db_schema::schema::anti_affinity_group;
use nexus_db_schema::schema::anti_affinity_group_instance_membership;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_uuid_kinds::AffinityGroupKind;
use omicron_uuid_kinds::AffinityGroupUuid;
use omicron_uuid_kinds::AntiAffinityGroupKind;
use omicron_uuid_kinds::AntiAffinityGroupUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceKind;
use omicron_uuid_kinds::InstanceUuid;
use uuid::Uuid;

impl_enum_type!(
    AffinityPolicyEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Ord, PartialOrd)]
    pub enum AffinityPolicy;

    // Enum values
    Fail => b"fail"
    Allow => b"allow"
);

impl From<AffinityPolicy> for external::AffinityPolicy {
    fn from(policy: AffinityPolicy) -> Self {
        match policy {
            AffinityPolicy::Fail => Self::Fail,
            AffinityPolicy::Allow => Self::Allow,
        }
    }
}

impl From<external::AffinityPolicy> for AffinityPolicy {
    fn from(policy: external::AffinityPolicy) -> Self {
        match policy {
            external::AffinityPolicy::Fail => Self::Fail,
            external::AffinityPolicy::Allow => Self::Allow,
        }
    }
}

impl_enum_type!(
    FailureDomainEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum FailureDomain;

    // Enum values
    Sled => b"sled"
);

impl From<FailureDomain> for external::FailureDomain {
    fn from(domain: FailureDomain) -> Self {
        match domain {
            FailureDomain::Sled => Self::Sled,
        }
    }
}

impl From<external::FailureDomain> for FailureDomain {
    fn from(domain: external::FailureDomain) -> Self {
        match domain {
            external::FailureDomain::Sled => Self::Sled,
        }
    }
}

#[derive(
    Queryable, Insertable, Clone, Debug, Resource, Selectable, PartialEq,
)]
#[diesel(table_name = affinity_group)]
pub struct AffinityGroup {
    #[diesel(embed)]
    pub identity: AffinityGroupIdentity,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

impl AffinityGroup {
    pub fn new(project_id: Uuid, params: params::AffinityGroupCreate) -> Self {
        Self {
            identity: AffinityGroupIdentity::new(
                Uuid::new_v4(),
                params.identity,
            ),
            project_id,
            policy: params.policy.into(),
            failure_domain: params.failure_domain.into(),
        }
    }
}

impl From<AffinityGroup> for views::AffinityGroup {
    fn from(group: AffinityGroup) -> Self {
        let identity = IdentityMetadata {
            id: group.identity.id,
            name: group.identity.name.into(),
            description: group.identity.description,
            time_created: group.identity.time_created,
            time_modified: group.identity.time_modified,
        };
        Self {
            identity,
            project_id: group.project_id,
            policy: group.policy.into(),
            failure_domain: group.failure_domain.into(),
        }
    }
}

/// Describes a set of updates for the [`AffinityGroup`] model.
#[derive(AsChangeset)]
#[diesel(table_name = affinity_group)]
pub struct AffinityGroupUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::AffinityGroupUpdate> for AffinityGroupUpdate {
    fn from(params: params::AffinityGroupUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

#[derive(
    Queryable, Insertable, Clone, Debug, Resource, Selectable, PartialEq,
)]
#[diesel(table_name = anti_affinity_group)]
pub struct AntiAffinityGroup {
    #[diesel(embed)]
    identity: AntiAffinityGroupIdentity,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

impl AntiAffinityGroup {
    pub fn new(
        project_id: Uuid,
        params: params::AntiAffinityGroupCreate,
    ) -> Self {
        Self {
            identity: AntiAffinityGroupIdentity::new(
                Uuid::new_v4(),
                params.identity,
            ),
            project_id,
            policy: params.policy.into(),
            failure_domain: params.failure_domain.into(),
        }
    }
}

impl From<AntiAffinityGroup> for views::AntiAffinityGroup {
    fn from(group: AntiAffinityGroup) -> Self {
        let identity = IdentityMetadata {
            id: group.identity.id,
            name: group.identity.name.into(),
            description: group.identity.description,
            time_created: group.identity.time_created,
            time_modified: group.identity.time_modified,
        };
        Self {
            identity,
            project_id: group.project_id,
            policy: group.policy.into(),
            failure_domain: group.failure_domain.into(),
        }
    }
}

/// Describes a set of updates for the [`AntiAffinityGroup`] model.
#[derive(AsChangeset)]
#[diesel(table_name = anti_affinity_group)]
pub struct AntiAffinityGroupUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::AntiAffinityGroupUpdate> for AntiAffinityGroupUpdate {
    fn from(params: params::AntiAffinityGroupUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = affinity_group_instance_membership)]
pub struct AffinityGroupInstanceMembership {
    pub group_id: DbTypedUuid<AffinityGroupKind>,
    pub instance_id: DbTypedUuid<InstanceKind>,
}

impl AffinityGroupInstanceMembership {
    pub fn new(group_id: AffinityGroupUuid, instance_id: InstanceUuid) -> Self {
        Self { group_id: group_id.into(), instance_id: instance_id.into() }
    }

    pub fn to_external(
        self,
        member_name: external::Name,
        run_state: external::InstanceState,
    ) -> external::AffinityGroupMember {
        external::AffinityGroupMember::Instance {
            id: self.instance_id.into_untyped_uuid(),
            name: member_name,
            run_state,
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = anti_affinity_group_instance_membership)]
pub struct AntiAffinityGroupInstanceMembership {
    pub group_id: DbTypedUuid<AntiAffinityGroupKind>,
    pub instance_id: DbTypedUuid<InstanceKind>,
}

impl AntiAffinityGroupInstanceMembership {
    pub fn new(
        group_id: AntiAffinityGroupUuid,
        instance_id: InstanceUuid,
    ) -> Self {
        Self { group_id: group_id.into(), instance_id: instance_id.into() }
    }

    pub fn to_external(
        self,
        member_name: external::Name,
        run_state: external::InstanceState,
    ) -> external::AntiAffinityGroupMember {
        external::AntiAffinityGroupMember::Instance {
            id: self.instance_id.into_untyped_uuid(),
            name: member_name,
            run_state,
        }
    }
}
