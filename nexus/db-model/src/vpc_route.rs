// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_wrapper, Name};
use crate::schema::router_route;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use std::io::Write;
use uuid::Uuid;

impl_enum_wrapper!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "router_route_kind", schema = "public"))]
    pub struct RouterRouteKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = RouterRouteKindEnum)]
    pub struct RouterRouteKind(pub external::RouterRouteKind);

    // Enum values
    Default => b"default"
    VpcSubnet => b"vpc_subnet"
    VpcPeering => b"vpc_peering"
    Custom => b"custom"
);

#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Text)]
pub struct RouteTarget(pub external::RouteTarget);

impl ToSql<sql_types::Text, Pg> for RouteTarget {
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

impl<DB> FromSql<sql_types::Text, DB> for RouteTarget
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(RouteTarget(
            String::from_sql(bytes)?.parse::<external::RouteTarget>()?,
        ))
    }
}

#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Text)]
pub struct RouteDestination(pub external::RouteDestination);

impl RouteDestination {
    pub fn new(state: external::RouteDestination) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::RouteDestination {
        &self.0
    }
}

impl ToSql<sql_types::Text, Pg> for RouteDestination {
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

impl<DB> FromSql<sql_types::Text, DB> for RouteDestination
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(RouteDestination::new(
            String::from_sql(bytes)?.parse::<external::RouteDestination>()?,
        ))
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = router_route)]
pub struct RouterRoute {
    #[diesel(embed)]
    identity: RouterRouteIdentity,

    pub kind: RouterRouteKind,
    pub vpc_router_id: Uuid,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

impl RouterRoute {
    pub fn new(
        route_id: Uuid,
        vpc_router_id: Uuid,
        kind: external::RouterRouteKind,
        params: params::RouterRouteCreate,
    ) -> Self {
        let identity = RouterRouteIdentity::new(route_id, params.identity);
        Self {
            identity,
            vpc_router_id,
            kind: RouterRouteKind(kind),
            target: RouteTarget(params.target),
            destination: RouteDestination::new(params.destination),
        }
    }

    pub fn for_subnet(
        route_id: Uuid,
        system_router_id: Uuid,
        subnet: Name,
    ) -> Result<Self, ()> {
        let name = format!("sn-{}", subnet).parse().map_err(|_| ())?;
        Ok(Self::new(
            route_id,
            system_router_id,
            external::RouterRouteKind::VpcSubnet,
            params::RouterRouteCreate {
                identity: external::IdentityMetadataCreateParams {
                    name,
                    description: format!("VPC Subnet route for '{subnet}'"),
                },
                target: external::RouteTarget::Subnet(subnet.0.clone()),
                destination: external::RouteDestination::Subnet(subnet.0),
            },
        ))
    }
}

impl Into<external::RouterRoute> for RouterRoute {
    fn into(self) -> external::RouterRoute {
        external::RouterRoute {
            identity: self.identity(),
            vpc_router_id: self.vpc_router_id,
            kind: self.kind.0,
            target: self.target.0.clone(),
            destination: self.destination.state().clone(),
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = router_route)]
pub struct RouterRouteUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

impl From<params::RouterRouteUpdate> for RouterRouteUpdate {
    fn from(params: params::RouterRouteUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
            target: RouteTarget(params.target),
            destination: RouteDestination::new(params.destination),
        }
    }
}
