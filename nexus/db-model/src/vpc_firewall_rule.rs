// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    DatabaseString, L4PortRange, SqlU16, impl_enum_wrapper, impl_from_sql_text,
};
use db_macros::Resource;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use nexus_db_schema::schema::vpc_firewall_rule;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::io::Write;
use std::str::FromStr;
use uuid::Uuid;

impl_enum_wrapper!(
    VpcFirewallRuleStatusEnum:

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    pub struct VpcFirewallRuleStatus(pub external::VpcFirewallRuleStatus);

    Disabled => b"disabled"
    Enabled => b"enabled"
);
NewtypeFrom! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }
NewtypeDeref! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }

impl_enum_wrapper!(
    VpcFirewallRuleDirectionEnum:

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    pub struct VpcFirewallRuleDirection(pub external::VpcFirewallRuleDirection);

    Inbound => b"inbound"
    Outbound => b"outbound"
);
NewtypeFrom! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }
NewtypeDeref! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }

impl_enum_wrapper!(
    VpcFirewallRuleActionEnum:

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
    pub struct VpcFirewallRuleAction(pub external::VpcFirewallRuleAction);

    Allow => b"allow"
    Deny => b"deny"
);
NewtypeFrom! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }
NewtypeDeref! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }

impl_enum_wrapper!(
    VpcFirewallRuleProtocolEnum:

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
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

impl DatabaseString for VpcFirewallRuleTarget {
    type Error = <external::VpcFirewallRuleTarget as FromStr>::Err;

    fn to_database_string(&self) -> Cow<str> {
        self.0.to_string().into()
    }

    fn from_database_string(s: &str) -> Result<Self, Self::Error> {
        s.parse::<external::VpcFirewallRuleTarget>().map(Self)
    }
}

impl_from_sql_text!(VpcFirewallRuleTarget);

/// Newtype wrapper around [`external::VpcFirewallRuleHostFilter`] so we can derive
/// diesel traits for it
#[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize)]
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct VpcFirewallRuleHostFilter(pub external::VpcFirewallRuleHostFilter);
NewtypeFrom! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }
NewtypeDeref! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }

impl DatabaseString for VpcFirewallRuleHostFilter {
    type Error = <external::VpcFirewallRuleHostFilter as FromStr>::Err;

    fn to_database_string(&self) -> Cow<str> {
        self.0.to_string().into()
    }

    fn from_database_string(s: &str) -> Result<Self, Self::Error> {
        s.parse::<external::VpcFirewallRuleHostFilter>().map(Self)
    }
}

impl_from_sql_text!(VpcFirewallRuleHostFilter);

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
            &SqlU16(self.0.0),
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

/// Cap on the number of rules in a VPC
///
/// The choice of value is somewhat arbitrary, but the goal is to have a
/// large number that customers are unlikely to actually hit, but which still
/// meaningfully limits the ability to overload the DB with a single request.
const MAX_FW_RULES_PER_VPC: usize = 1024;

/// Cap on targets and on each type of filter
const MAX_FW_RULE_PARTS: usize = 256;

fn ensure_max_len<T>(
    items: &Vec<T>,
    label: &str,
    max: usize,
) -> Result<(), external::Error> {
    if items.len() > max {
        let msg = format!("max length {}", max);
        return Err(external::Error::invalid_value(label, msg));
    }
    Ok(())
}

impl VpcFirewallRule {
    pub fn new(
        rule_id: Uuid,
        vpc_id: Uuid,
        rule: &external::VpcFirewallRuleUpdate,
    ) -> Result<Self, external::Error> {
        let identity = VpcFirewallRuleIdentity::new(
            rule_id,
            external::IdentityMetadataCreateParams {
                name: rule.name.clone(),
                description: rule.description.clone(),
            },
        );

        ensure_max_len(&rule.targets, "targets", MAX_FW_RULE_PARTS)?;

        if let Some(hosts) = rule.filters.hosts.as_ref() {
            ensure_max_len(&hosts, "filters.hosts", MAX_FW_RULE_PARTS)?;
        }
        if let Some(ports) = rule.filters.ports.as_ref() {
            ensure_max_len(&ports, "filters.ports", MAX_FW_RULE_PARTS)?;
        }
        if let Some(protocols) = rule.filters.protocols.as_ref() {
            ensure_max_len(&protocols, "filters.protocols", MAX_FW_RULE_PARTS)?;
        }

        Ok(Self {
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
        })
    }

    pub fn vec_from_params(
        vpc_id: Uuid,
        params: external::VpcFirewallRuleUpdateParams,
    ) -> Result<Vec<VpcFirewallRule>, external::Error> {
        ensure_no_duplicates(&params)?;
        ensure_max_len(&params.rules, "rules", MAX_FW_RULES_PER_VPC)?;
        params
            .rules
            .into_iter()
            .map(|rule| VpcFirewallRule::new(Uuid::new_v4(), vpc_id, &rule))
            .collect()
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

    if dupes.is_empty() {
        return Ok(());
    }

    let dupes_str =
        dupes.iter().map(|d| format!("\"{d}\"")).collect::<Vec<_>>().join(", ");
    return Err(external::Error::invalid_value(
        "rules",
        format!("Rule names must be unique. Duplicates: [{}]", dupes_str),
    ));
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
