// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC routers and routes

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::RouterRoute;
use crate::db::model::VpcRouter;
use crate::db::model::VpcRouterKind;
use crate::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::RouterRouteUpdateParams;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    // Routers

    pub async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        kind: &VpcRouterKind,
        params: &params::VpcRouterCreate,
    ) -> CreateResult<db::model::VpcRouter> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let id = Uuid::new_v4();
        let router = db::model::VpcRouter::new(
            id,
            authz_vpc.id(),
            *kind,
            params.clone(),
        );
        let (_, router) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        Ok(router)
    }

    pub async fn vpc_list_routers(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcRouter> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        let routers = self
            .db_datastore
            .vpc_list_routers(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(routers)
    }

    pub async fn vpc_router_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> LookupResult<db::model::VpcRouter> {
        let (.., db_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .fetch()
            .await?;
        Ok(db_router)
    }

    pub async fn vpc_router_fetch_by_id(&self, opctx: &OpContext, vpc_router_id: Uuid) -> LookupResult<db::model::VpcRouter> {
        let (.., db_router) = LookupPath::new(opctx, &self.db_datastore)
            .vpc_router_id(vpc_router_id)
            .fetch()
            .await?;
        Ok(db_router)
    }

    pub async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        params: &params::VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .vpc_update_router(opctx, &authz_router, params.clone().into())
            .await
    }

    // TODO: When a router is deleted all its routes should be deleted
    // TODO: When a router is deleted it should be unassociated w/ any subnets it may be associated with
    //       or trigger an error
    pub async fn vpc_delete_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> DeleteResult {
        let (.., authz_router, db_router) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .fetch()
                .await?;
        // TODO-performance shouldn't this check be part of the "update"
        // database query?  This shouldn't affect correctness, assuming that a
        // router kind cannot be changed, but it might be able to save us a
        // database round-trip.
        if db_router.kind == VpcRouterKind::System {
            return Err(Error::MethodNotAllowed {
                internal_message: "Cannot delete system router".to_string(),
            });
        }
        self.db_datastore.vpc_delete_router(opctx, &authz_router).await
    }

    // Routes

    #[allow(clippy::too_many_arguments)]
    pub async fn router_create_route(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        kind: &RouterRouteKind,
        params: &RouterRouteCreateParams,
    ) -> CreateResult<db::model::RouterRoute> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let id = Uuid::new_v4();
        let route = db::model::RouterRoute::new(
            id,
            authz_router.id(),
            *kind,
            params.clone(),
        );
        let route = self
            .db_datastore
            .router_create_route(&opctx, &authz_router, route)
            .await?;
        Ok(route)
    }

    pub async fn router_list_routes(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::RouterRoute> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .router_list_routes(opctx, &authz_router, pagparams)
            .await
    }

    pub async fn route_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
    ) -> LookupResult<db::model::RouterRoute> {
        let (.., db_route) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .router_route_name(route_name)
            .fetch()
            .await?;
        Ok(db_route)
    }

    pub async fn route_fetch_by_id(&self, opctx: &OpContext, route_id: Uuid) -> LookupResult<db::model::RouterRoute> {
        let (.., db_route) = LookupPath::new(opctx, &self.db_datastore)
            .router_route_id(route_id)
            .fetch()
            .await?;
        Ok(db_route)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn router_update_route(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
        params: &RouterRouteUpdateParams,
    ) -> UpdateResult<RouterRoute> {
        let (.., authz_route, db_route) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .router_route_name(route_name)
                .fetch()
                .await?;
        // TODO: Write a test for this once there's a way to test it (i.e.
        // subnets automatically register to the system router table)
        match db_route.kind.0 {
            RouterRouteKind::Custom | RouterRouteKind::Default => (),
            _ => {
                return Err(Error::MethodNotAllowed {
                    internal_message: format!(
                        "routes of type {} from the system table of VPC {:?} \
                        are not modifiable",
                        db_route.kind.0, vpc_name
                    ),
                })
            }
        }
        self.db_datastore
            .router_update_route(&opctx, &authz_route, params.clone().into())
            .await
    }

    pub async fn router_delete_route(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
    ) -> DeleteResult {
        let (.., authz_route, db_route) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .router_route_name(route_name)
                .fetch()
                .await?;

        // Only custom routes can be deleted
        // TODO Shouldn't this constraint be checked by the database query?
        if db_route.kind.0 != RouterRouteKind::Custom {
            return Err(Error::MethodNotAllowed {
                internal_message: "DELETE not allowed on system routes"
                    .to_string(),
            });
        }
        self.db_datastore.router_delete_route(opctx, &authz_route).await
    }
}
