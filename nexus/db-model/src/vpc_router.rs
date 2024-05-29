// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, Generation, Name, RouterRoute};
use crate::collection::DatastoreCollectionConfig;
use crate::schema::{router_route, vpc_router, vpc_subnet};
use crate::{DatastoreAttachTargetConfig, VpcSubnet};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_router_kind", schema = "public"))]
    pub struct VpcRouterKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = VpcRouterKindEnum)]
    pub enum VpcRouterKind;

    // Enum values
    System => b"system"
    Custom => b"custom"
);

impl From<VpcRouterKind> for views::VpcRouterKind {
    fn from(kind: VpcRouterKind) -> Self {
        match kind {
            VpcRouterKind::Custom => Self::Custom,
            VpcRouterKind::System => Self::System,
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = vpc_router)]
pub struct VpcRouter {
    #[diesel(embed)]
    identity: VpcRouterIdentity,

    pub kind: VpcRouterKind,
    pub vpc_id: Uuid,
    pub rcgen: Generation,
    pub resolved_version: i64,
}

impl VpcRouter {
    pub fn new(
        router_id: Uuid,
        vpc_id: Uuid,
        kind: VpcRouterKind,
        params: params::VpcRouterCreate,
    ) -> Self {
        let identity = VpcRouterIdentity::new(router_id, params.identity);
        Self {
            identity,
            vpc_id,
            kind,
            rcgen: Generation::new(),
            resolved_version: 0,
        }
    }
}

impl From<VpcRouter> for views::VpcRouter {
    fn from(router: VpcRouter) -> Self {
        Self {
            identity: router.identity(),
            vpc_id: router.vpc_id,
            kind: router.kind.into(),
        }
    }
}

impl DatastoreCollectionConfig<RouterRoute> for VpcRouter {
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

impl DatastoreAttachTargetConfig<VpcSubnet> for VpcRouter {
    type Id = Uuid;

    type CollectionIdColumn = vpc_router::dsl::id;
    type CollectionTimeDeletedColumn = vpc_router::dsl::time_deleted;

    type ResourceIdColumn = vpc_subnet::dsl::id;
    type ResourceCollectionIdColumn = vpc_subnet::dsl::custom_router_id;
    type ResourceTimeDeletedColumn = vpc_subnet::dsl::time_deleted;

    const ALLOW_FROM_ATTACHED: bool = true;
}
