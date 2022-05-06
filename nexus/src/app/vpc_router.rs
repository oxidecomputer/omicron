// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::model::RouterRoute;
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
}
