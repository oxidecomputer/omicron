// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2024 Oxide Computer Company

//! Database representation of affinity and anti-affinity groups

use super::impl_enum_type;
use crate::schema::affinity_group;
use crate::schema::affinity_group_instance_membership;
use crate::schema::anti_affinity_group;
use crate::schema::anti_affinity_group_instance_membership;
use crate::typed_uuid::DbTypedUuid;
use db_macros::Resource;
use nexus_types::external_api::views;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_uuid_kinds::AffinityGroupKind;
use omicron_uuid_kinds::AntiAffinityGroupKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceKind;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "affinity_policy", schema = "public"))]
    pub struct AffinityPolicyEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = AffinityPolicyEnum)]
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

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "failure_domain", schema = "public"))]
    pub struct FailureDomainEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = FailureDomainEnum)]
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

#[derive(Queryable, Insertable, Clone, Debug, Resource, Selectable)]
#[diesel(table_name = affinity_group)]
pub struct AffinityGroup {
    #[diesel(embed)]
    identity: AffinityGroupIdentity,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
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
            policy: group.policy.into(),
            failure_domain: group.failure_domain.into(),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Resource, Selectable)]
#[diesel(table_name = anti_affinity_group)]
pub struct AntiAffinityGroup {
    #[diesel(embed)]
    identity: AntiAffinityGroupIdentity,
    pub project_id: Uuid,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
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
            policy: group.policy.into(),
            failure_domain: group.failure_domain.into(),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = affinity_group_instance_membership)]
pub struct AffinityGroupInstanceMembership {
    pub group_id: DbTypedUuid<AffinityGroupKind>,
    pub instance_id: DbTypedUuid<InstanceKind>,
}

impl From<AffinityGroupInstanceMembership> for external::AffinityGroupMember {
    fn from(member: AffinityGroupInstanceMembership) -> Self {
        Self::Instance(member.instance_id.into_untyped_uuid())
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = anti_affinity_group_instance_membership)]
pub struct AntiAffinityGroupInstanceMembership {
    pub group_id: DbTypedUuid<AntiAffinityGroupKind>,
    pub instance_id: DbTypedUuid<InstanceKind>,
}

impl From<AntiAffinityGroupInstanceMembership>
    for external::AntiAffinityGroupMember
{
    fn from(member: AntiAffinityGroupInstanceMembership) -> Self {
        Self::Instance(member.instance_id.into_untyped_uuid())
    }
}
