// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC routers and routes

use crate::external_api::params;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::RouterRoute;
use nexus_db_queries::db::model::VpcRouter;
use nexus_db_queries::db::model::VpcRouterKind;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::UpdateResult;
use std::net::IpAddr;
use uuid::Uuid;

impl super::Nexus {
    // Routers
    pub fn vpc_router_lookup<'a>(
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
                let router = LookupPath::new(opctx, &self.db_datastore)
                    .vpc_router_id(id);
                Ok(router)
            }
            params::RouterSelector {
                router: NameOrId::Name(name),
                vpc: Some(vpc),
                project
            } => {
                let router = self
                    .vpc_lookup(opctx, params::VpcSelector { project, vpc })?
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

    pub(crate) async fn vpc_create_router(
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
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;

        // Note: we don't trigger the route RPW here as it's impossible
        //       for the router to be bound to a subnet at this point.

        Ok(router)
    }

    pub(crate) async fn vpc_router_list(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::VpcRouter> {
        let (.., authz_vpc) =
            vpc_lookup.lookup_for(authz::Action::ListChildren).await?;
        let routers = self
            .db_datastore
            .vpc_router_list(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(routers)
    }

    pub(crate) async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        vpc_router_lookup: &lookup::VpcRouter<'_>,
        params: &params::VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        let (.., authz_router) =
            vpc_router_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .vpc_update_router(opctx, &authz_router, params.clone().into())
            .await
    }

    pub(crate) async fn vpc_delete_router(
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
        let out =
            self.db_datastore.vpc_delete_router(opctx, &authz_router).await?;

        self.vpc_needed_notify_sleds();

        Ok(out)
    }

    // Routes

    pub fn vpc_router_route_lookup<'a>(
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
                let route = LookupPath::new(opctx, &self.db_datastore)
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
                    .vpc_router_lookup(
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

    pub(crate) async fn router_create_route(
        &self,
        opctx: &OpContext,
        router_lookup: &lookup::VpcRouter<'_>,
        kind: &RouterRouteKind,
        params: &params::RouterRouteCreate,
    ) -> CreateResult<db::model::RouterRoute> {
        let (.., authz_router, db_router) =
            router_lookup.fetch_for(authz::Action::CreateChild).await?;

        if db_router.kind == VpcRouterKind::System {
            return Err(Error::invalid_request(
                "user-provided routes cannot be added to a system router",
            ));
        }

        // Validate route destinations/targets at this stage:
        // - mixed explicit v4 and v6 are disallowed.
        // - users cannot specify 'Vpc' as a custom router dest/target.
        // - users cannot specify 'Subnet' as a custom router target.
        // - the only internet gateway we support today is 'outbound'.
        match (&params.destination, &params.target) {
            (RouteDestination::Ip(IpAddr::V4(_)), RouteTarget::Ip(IpAddr::V4(_)))
            | (RouteDestination::Ip(IpAddr::V6(_)), RouteTarget::Ip(IpAddr::V6(_)))
            | (RouteDestination::IpNet(IpNet::V4(_)), RouteTarget::Ip(IpAddr::V4(_)))
            | (RouteDestination::IpNet(IpNet::V6(_)), RouteTarget::Ip(IpAddr::V6(_))) => {},

            (RouteDestination::Ip(_), RouteTarget::Ip(_))
            | (RouteDestination::IpNet(_), RouteTarget::Ip(_))
                => return Err(Error::invalid_request(
                    "cannot mix explicit IPv4 and IPv6 addresses between destination and target"
                )),

            (RouteDestination::Vpc(_), _) | (_, RouteTarget::Vpc(_)) => return Err(Error::invalid_request(
                "VPCs cannot be used as a destination or target in custom routers"
                )),

            (_, RouteTarget::Subnet(_)) => return Err(Error::invalid_request(
                "subnets cannot be used as a target in custom routers"
                )),

            (_, RouteTarget::InternetGateway(n)) if n.as_str() != "outbound" => return Err(Error::invalid_request(
                "'outbound' is currently the only valid internet gateway"
                )),

            _ => {},
        };

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

        self.vpc_router_increment_rpw_version(opctx, &authz_router).await?;

        Ok(route)
    }

    pub(crate) async fn vpc_router_route_list(
        &self,
        opctx: &OpContext,
        vpc_router_lookup: &lookup::VpcRouter<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::RouterRoute> {
        let (.., authz_router) =
            vpc_router_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .vpc_router_route_list(opctx, &authz_router, pagparams)
            .await
    }

    pub(crate) async fn router_update_route(
        &self,
        opctx: &OpContext,
        route_lookup: &lookup::RouterRoute<'_>,
        params: &params::RouterRouteUpdate,
    ) -> UpdateResult<RouterRoute> {
        let (.., vpc, authz_router, authz_route, db_route) =
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
        let out = self
            .db_datastore
            .router_update_route(&opctx, &authz_route, params.clone().into())
            .await?;

        self.vpc_router_increment_rpw_version(opctx, &authz_router).await?;

        Ok(out)
    }

    pub(crate) async fn router_delete_route(
        &self,
        opctx: &OpContext,
        route_lookup: &lookup::RouterRoute<'_>,
    ) -> DeleteResult {
        let (.., authz_router, authz_route, db_route) =
            route_lookup.fetch_for(authz::Action::Delete).await?;

        // Only custom routes can be deleted
        // TODO Shouldn't this constraint be checked by the database query?
        if db_route.kind.0 != RouterRouteKind::Custom {
            return Err(Error::invalid_request(
                "DELETE not allowed on system routes",
            ));
        }
        let out =
            self.db_datastore.router_delete_route(opctx, &authz_route).await?;

        self.vpc_router_increment_rpw_version(opctx, &authz_router).await?;

        Ok(out)
    }

    /// Trigger the VPC routing RPW in repsonse to a state change
    /// or a new possible listener (e.g., instance/probe start, NIC
    /// create).
    pub(crate) fn vpc_needed_notify_sleds(&self) {
        self.background_tasks
            .activate(&self.background_tasks.task_vpc_route_manager)
    }

    /// Trigger an RPW version bump on a single VPC router in response
    /// to CRUD operations on individual routes.
    ///
    /// This will also awaken the VPC Router RPW.
    pub(crate) async fn vpc_router_increment_rpw_version(
        &self,
        opctx: &OpContext,
        authz_router: &authz::VpcRouter,
    ) -> UpdateResult<()> {
        self.datastore()
            .vpc_router_increment_rpw_version(opctx, authz_router.id())
            .await?;

        self.vpc_needed_notify_sleds();

        Ok(())
    }
}
