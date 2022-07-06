// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, Generation, Name, RouterRoute};
use crate::db::collection_insert::DatastoreCollection;
use crate::db::schema::{router_route, vpc_router};
use crate::external_api::params;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_router_kind"))]
    pub struct VpcRouterKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = VpcRouterKindEnum)]
    pub enum VpcRouterKind;

    // Enum values
    System => b"system"
    Custom => b"custom"
);

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = vpc_router)]
pub struct VpcRouter {
    #[diesel(embed)]
    identity: VpcRouterIdentity,

    pub vpc_id: Uuid,
    pub kind: VpcRouterKind,
    pub rcgen: Generation,
}

impl VpcRouter {
    pub fn new(
        router_id: Uuid,
        vpc_id: Uuid,
        kind: VpcRouterKind,
        params: params::VpcRouterCreate,
    ) -> Self {
        let identity = VpcRouterIdentity::new(router_id, params.identity);
        Self { identity, vpc_id, kind, rcgen: Generation::new() }
    }
}

impl DatastoreCollection<RouterRoute> for VpcRouter {
    type CollectionId = Uuid;
    type GenerationNumberColumn = vpc_router::dsl::rcgen;
    type CollectionTimeDeletedColumn = vpc_router::dsl::time_deleted;
    type CollectionIdColumn = router_route::dsl::vpc_router_id;
}

#[derive(AsChangeset)]
#[diesel(table_name = vpc_router)]
pub struct VpcRouterUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::VpcRouterUpdate> for VpcRouterUpdate {
    fn from(params: params::VpcRouterUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}
