// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC routers and routes

use crate::app::vpc::Vpc;
use crate::external_api::params;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::RouterRoute;
use nexus_db_queries::db::model::VpcRouterKind;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::UpdateResult;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on VPC routers
pub struct VpcRouter {
    datastore: Arc<db::DataStore>,
    vpc: Vpc,
}

impl VpcRouter {
    pub fn new(datastore: Arc<db::DataStore>, vpc: Vpc) -> VpcRouter {
        VpcRouter { datastore, vpc }
    }

    pub fn lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        router_selector: params::RouterSelector,
    ) -> LookupResult<lookup::VpcRouter<'a>> {
        match router_selector {
            params::RouterSelector {
                router: NameOrId::Id(id),
                vpc: None,
                project: None
            } => {
                let router = LookupPath::new(opctx, &self.datastore)
                    .vpc_router_id(id);
                Ok(router)
            }
            params::RouterSelector {
                router: NameOrId::Name(name),
                vpc: Some(vpc),
                project
            } => {
                let router = self.vpc
                    .lookup(opctx, params::VpcSelector { project, vpc })?
                    .vpc_router_name_owned(name.into());
                Ok(router)
            }
            params::RouterSelector {
                router: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing router as an ID vpc and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "router should either be an ID or vpc should be specified",
            )),
        }
    }

    pub(crate) async fn create(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        kind: &VpcRouterKind,
        params: &params::VpcRouterCreate,
    ) -> CreateResult<db::model::VpcRouter> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::CreateChild).await?;
        let id = Uuid::new_v4();
        let router = db::model::VpcRouter::new(
            id,
            authz_vpc.id(),
            *kind,
            params.clone(),
        );
        let (_, router) = self
            .datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        Ok(router)
    }

    pub(crate) async fn list(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::VpcRouter> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::ListChildren).await?;
        let routers = self
            .datastore
            .vpc_router_list(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(routers)
    }

    pub(crate) async fn update(
        &self,
        opctx: &OpContext,
        vpc_router_lookup: &lookup::VpcRouter<'_>,
        params: &params::VpcRouterUpdate,
    ) -> UpdateResult<db::model::VpcRouter> {
        let (.., authz_router) =
            vpc_router_lookup.lookup_for(authz::Action::Modify).await?;
        self.datastore
            .vpc_update_router(opctx, &authz_router, params.clone().into())
            .await
    }

    // TODO: When a router is deleted all its routes should be deleted
    // TODO: When a router is deleted it should be unassociated w/ any subnets it may be associated with
    //       or trigger an error
    pub(crate) async fn delete(
        &self,
        opctx: &OpContext,
        vpc_router_lookup: &lookup::VpcRouter<'_>,
    ) -> DeleteResult {
        let (.., authz_router, db_router) =
            vpc_router_lookup.fetch_for(authz::Action::Delete).await?;
        // TODO-performance shouldn't this check be part of the "update"
        // database query?  This shouldn't affect correctness, assuming that a
        // router kind cannot be changed, but it might be able to save us a
        // database round-trip.
        if db_router.kind == VpcRouterKind::System {
            return Err(Error::invalid_request("Cannot delete system router"));
        }
        self.datastore.vpc_delete_router(opctx, &authz_router).await
    }

    // Routes

    pub fn route_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        route_selector: params::RouteSelector,
    ) -> LookupResult<lookup::RouterRoute<'a>> {
        match route_selector {
            params::RouteSelector {
                route: NameOrId::Id(id),
                router: None,
                vpc: None,
                project: None,
            } => {
                let route = LookupPath::new(opctx, &self.datastore)
                    .router_route_id(id);
                Ok(route)
            }
            params::RouteSelector {
                route: NameOrId::Name(name),
                router: Some(router),
                vpc,
                project,
            } => {
                let route = self
                    .lookup(
                        opctx,
                        params::RouterSelector { project, vpc, router },
                    )?
                    .router_route_name_owned(name.into());
                Ok(route)
            }
            params::RouteSelector {
                route: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing route as an ID router, subnet, vpc, and project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "route should either be an ID or router should be specified",
            )),
        }
    }

    pub(crate) async fn create_route(
        &self,
        opctx: &OpContext,
        router_lookup: &lookup::VpcRouter<'_>,
        kind: &RouterRouteKind,
        params: &params::RouterRouteCreate,
    ) -> CreateResult<db::model::RouterRoute> {
        let (.., authz_router) =
            router_lookup.lookup_for(authz::Action::CreateChild).await?;
        let id = Uuid::new_v4();
        let route = db::model::RouterRoute::new(
            id,
            authz_router.id(),
            *kind,
            params.clone(),
        );
        let route = self
            .datastore
            .router_create_route(&opctx, &authz_router, route)
            .await?;
        Ok(route)
    }

    pub(crate) async fn route_list(
        &self,
        opctx: &OpContext,
        vpc_router_lookup: &lookup::VpcRouter<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::RouterRoute> {
        let (.., authz_router) =
            vpc_router_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.datastore
            .vpc_router_route_list(opctx, &authz_router, pagparams)
            .await
    }

    pub(crate) async fn update_route(
        &self,
        opctx: &OpContext,
        route_lookup: &lookup::RouterRoute<'_>,
        params: &params::RouterRouteUpdate,
    ) -> UpdateResult<RouterRoute> {
        let (.., vpc, _, authz_route, db_route) =
            route_lookup.fetch_for(authz::Action::Modify).await?;
        // TODO: Write a test for this once there's a way to test it (i.e.
        // subnets automatically register to the system router table)
        match db_route.kind.0 {
            RouterRouteKind::Custom | RouterRouteKind::Default => (),
            _ => {
                return Err(Error::invalid_request(format!(
                    "routes of type {} from the system table of VPC {:?} \
                        are not modifiable",
                    db_route.kind.0,
                    vpc.id()
                )));
            }
        }
        self.datastore
            .router_update_route(&opctx, &authz_route, params.clone().into())
            .await
    }

    pub(crate) async fn delete_route(
        &self,
        opctx: &OpContext,
        route_lookup: &lookup::RouterRoute<'_>,
    ) -> DeleteResult {
        let (.., authz_route, db_route) =
            route_lookup.fetch_for(authz::Action::Delete).await?;

        // Only custom routes can be deleted
        // TODO Shouldn't this constraint be checked by the database query?
        if db_route.kind.0 != RouterRouteKind::Custom {
            return Err(Error::invalid_request(
                "DELETE not allowed on system routes",
            ));
        }
        self.datastore.router_delete_route(opctx, &authz_route).await
    }
}
