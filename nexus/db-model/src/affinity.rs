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
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::AffinityGroupKind;
use omicron_uuid_kinds::AntiAffinityGroupKind;
use omicron_uuid_kinds::InstanceKind;

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

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = affinity_group)]
pub struct AffinityGroup {
    pub id: DbTypedUuid<AffinityGroupKind>,
    pub name: String,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = anti_affinity_group)]
pub struct AntiAffinityGroup {
    pub id: DbTypedUuid<AntiAffinityGroupKind>,
    pub name: String,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = affinity_group_instance_membership)]
pub struct AffinityGroupInstanceMembership {
    pub group_id: DbTypedUuid<AffinityGroupKind>,
    pub instance_id: DbTypedUuid<InstanceKind>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = anti_affinity_group_instance_membership)]
pub struct AntiAffinityGroupInstanceMembership {
    pub group_id: DbTypedUuid<AntiAffinityGroupKind>,
    pub instance_id: DbTypedUuid<InstanceKind>,
}
