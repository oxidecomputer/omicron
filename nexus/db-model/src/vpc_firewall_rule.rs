// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_wrapper, L4PortRange, SqlU16};
use crate::schema::vpc_firewall_rule;
use db_macros::Resource;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::HashSet;
use std::io::Write;
use uuid::Uuid;

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_status", schema = "public"))]
    pub struct VpcFirewallRuleStatusEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VpcFirewallRuleStatusEnum)]
    pub struct VpcFirewallRuleStatus(pub external::VpcFirewallRuleStatus);

    Disabled => b"disabled"
    Enabled => b"enabled"
);
NewtypeFrom! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }
NewtypeDeref! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_direction", schema = "public"))]
    pub struct VpcFirewallRuleDirectionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VpcFirewallRuleDirectionEnum)]
    pub struct VpcFirewallRuleDirection(pub external::VpcFirewallRuleDirection);

    Inbound => b"inbound"
    Outbound => b"outbound"
);
NewtypeFrom! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }
NewtypeDeref! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_action", schema = "public"))]
    pub struct VpcFirewallRuleActionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VpcFirewallRuleActionEnum)]
    pub struct VpcFirewallRuleAction(pub external::VpcFirewallRuleAction);

    Allow => b"allow"
    Deny => b"deny"
);
NewtypeFrom! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }
NewtypeDeref! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_protocol", schema = "public"))]
    pub struct VpcFirewallRuleProtocolEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VpcFirewallRuleProtocolEnum)]
    pub struct VpcFirewallRuleProtocol(pub external::VpcFirewallRuleProtocol);

    Tcp => b"TCP"
    Udp => b"UDP"
    Icmp => b"ICMP"
);
NewtypeFrom! { () pub struct VpcFirewallRuleProtocol(external::VpcFirewallRuleProtocol); }
NewtypeDeref! { () pub struct VpcFirewallRuleProtocol(external::VpcFirewallRuleProtocol); }

/// Newtype wrapper around [`external::VpcFirewallRuleTarget`] so we can derive
/// diesel traits for it
#[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct VpcFirewallRuleTarget(pub external::VpcFirewallRuleTarget);
NewtypeFrom! { () pub struct VpcFirewallRuleTarget(external::VpcFirewallRuleTarget); }
NewtypeDeref! { () pub struct VpcFirewallRuleTarget(external::VpcFirewallRuleTarget); }

impl ToSql<sql_types::Text, Pg> for VpcFirewallRuleTarget {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

// Deserialize the "VpcFirewallRuleTarget" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for VpcFirewallRuleTarget
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(VpcFirewallRuleTarget(
            String::from_sql(bytes)?
                .parse::<external::VpcFirewallRuleTarget>()?,
        ))
    }
}

/// Newtype wrapper around [`external::VpcFirewallRuleHostFilter`] so we can derive
/// diesel traits for it
#[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct VpcFirewallRuleHostFilter(pub external::VpcFirewallRuleHostFilter);
NewtypeFrom! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }
NewtypeDeref! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }

impl ToSql<sql_types::Text, Pg> for VpcFirewallRuleHostFilter {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

// Deserialize the "VpcFirewallRuleHostFilter" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for VpcFirewallRuleHostFilter
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(VpcFirewallRuleHostFilter(
            String::from_sql(bytes)?
                .parse::<external::VpcFirewallRuleHostFilter>()?,
        ))
    }
}

/// Newtype wrapper around [`external::VpcFirewallRulePriority`] so we can derive
/// diesel traits for it
#[derive(
    Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize,
)]
#[repr(transparent)]
#[diesel(sql_type = sql_types::Int4)]
pub struct VpcFirewallRulePriority(pub external::VpcFirewallRulePriority);
NewtypeFrom! { () pub struct VpcFirewallRulePriority(external::VpcFirewallRulePriority); }
NewtypeDeref! { () pub struct VpcFirewallRulePriority(external::VpcFirewallRulePriority); }

impl ToSql<sql_types::Int4, Pg> for VpcFirewallRulePriority {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <SqlU16 as ToSql<sql_types::Int4, Pg>>::to_sql(
            &SqlU16(self.0 .0),
            &mut out.reborrow(),
        )
    }
}

// Deserialize the "VpcFirewallRulePriority" object from SQL TEXT.
impl<DB> FromSql<sql_types::Int4, DB> for VpcFirewallRulePriority
where
    DB: Backend,
    SqlU16: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(VpcFirewallRulePriority(external::VpcFirewallRulePriority(
            *SqlU16::from_sql(bytes)?,
        )))
    }
}

#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = vpc_firewall_rule)]
pub struct VpcFirewallRule {
    #[diesel(embed)]
    pub identity: VpcFirewallRuleIdentity,

    pub vpc_id: Uuid,
    pub status: VpcFirewallRuleStatus,
    pub direction: VpcFirewallRuleDirection,
    pub targets: Vec<VpcFirewallRuleTarget>,
    pub filter_hosts: Option<Vec<VpcFirewallRuleHostFilter>>,
    pub filter_ports: Option<Vec<L4PortRange>>,
    pub filter_protocols: Option<Vec<VpcFirewallRuleProtocol>>,
    pub action: VpcFirewallRuleAction,
    pub priority: VpcFirewallRulePriority,
}

impl VpcFirewallRule {
    pub fn new(
        rule_id: Uuid,
        vpc_id: Uuid,
        rule: &external::VpcFirewallRuleUpdate,
    ) -> Self {
        let identity = VpcFirewallRuleIdentity::new(
            rule_id,
            external::IdentityMetadataCreateParams {
                name: rule.name.clone(),
                description: rule.description.clone(),
            },
        );
        Self {
            identity,
            vpc_id,
            status: rule.status.into(),
            direction: rule.direction.into(),
            targets: rule
                .targets
                .iter()
                .map(|target| target.clone().into())
                .collect(),
            filter_hosts: rule.filters.hosts.as_ref().map(|hosts| {
                hosts
                    .iter()
                    .map(|target| VpcFirewallRuleHostFilter(target.clone()))
                    .collect()
            }),
            filter_ports: rule.filters.ports.as_ref().map(|ports| {
                ports.iter().map(|range| L4PortRange(*range)).collect()
            }),
            filter_protocols: rule.filters.protocols.as_ref().map(|protos| {
                protos.iter().map(|proto| (*proto).into()).collect()
            }),
            action: rule.action.into(),
            priority: rule.priority.into(),
        }
    }

    pub fn vec_from_params(
        vpc_id: Uuid,
        params: external::VpcFirewallRuleUpdateParams,
    ) -> Result<Vec<VpcFirewallRule>, external::Error> {
        ensure_no_duplicates(&params)?;
        Ok(params
            .rules
            .iter()
            .map(|rule| VpcFirewallRule::new(Uuid::new_v4(), vpc_id, rule))
            .collect())
    }
}

fn ensure_no_duplicates(
    params: &external::VpcFirewallRuleUpdateParams,
) -> Result<(), external::Error> {
    // we could do this by comparing set(names).len() to names.len(), but this
    // way we can say what the duplicate names are, and that's nice!
    let mut names = HashSet::new();
    let mut dupes = HashSet::new();
    for r in params.rules.iter() {
        if !names.insert(r.name.clone()) {
            // insert returns false if already present
            dupes.insert(r.name.clone());
        }
    }

    if !dupes.is_empty() {
        return Err(external::Error::invalid_value(
            "rules",
            format!("Rule names must be unique. Duplicates: {}", json!(dupes)),
        ));
    }

    Ok(())
}

impl Into<external::VpcFirewallRule> for VpcFirewallRule {
    fn into(self) -> external::VpcFirewallRule {
        external::VpcFirewallRule {
            identity: self.identity(),
            status: self.status.into(),
            direction: self.direction.into(),
            targets: self
                .targets
                .iter()
                .map(|target| target.clone().into())
                .collect(),
            filters: external::VpcFirewallRuleFilter {
                hosts: self.filter_hosts.map(|hosts| {
                    hosts.iter().map(|host| host.0.clone()).collect()
                }),
                ports: self
                    .filter_ports
                    .map(|ports| ports.iter().map(|range| range.0).collect()),
                protocols: self.filter_protocols.map(|protocols| {
                    protocols.iter().map(|protocol| protocol.0).collect()
                }),
            },
            action: self.action.into(),
            priority: self.priority.into(),
            vpc_id: self.vpc_id,
        }
    }
}
