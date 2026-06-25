// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC routers and routes

use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::model::RouterRoute;
use nexus_db_queries::db::model::VpcRouter;
use nexus_db_queries::db::model::VpcRouterKind;
use nexus_types::external_api::vpc;
use nexus_types::identity::Resource;
// Oldest API version whose update bodies still allow omitting fields.
use nexus_types_versions::v2025_11_20_00 as update_compat;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use oxnet::IpNet;
use std::net::IpAddr;
use uuid::Uuid;

impl super::Nexus {
    // Routers
    pub fn vpc_router_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        router_selector: vpc::RouterSelector,
    ) -> LookupResult<lookup::VpcRouter<'a>> {
        match router_selector {
            vpc::RouterSelector {
                router: NameOrId::Id(id),
                vpc: None,
                project: None,
            } => {
                let router = LookupPath::new(opctx, &self.db_datastore)
                    .vpc_router_id(id);
                Ok(router)
            }
            vpc::RouterSelector {
                router: NameOrId::Name(name),
                vpc: Some(vpc),
                project,
            } => {
                let router = self
                    .vpc_lookup(opctx, vpc::VpcSelector { project, vpc })?
                    .vpc_router_name_owned(name.into());
                Ok(router)
            }
            vpc::RouterSelector { router: NameOrId::Id(_), .. } => {
                Err(Error::invalid_request(
                    "when providing router as an ID vpc and project should not be specified",
                ))
            }
            _ => Err(Error::invalid_request(
                "router should either be an ID or vpc should be specified",
            )),
        }
    }

    /// Lookup a (custom) router for attaching to a VPC subnet, when
    /// we have already determined which VPC the subnet exists within.
    pub(crate) async fn vpc_router_lookup_for_attach(
        &self,
        opctx: &OpContext,
        router: &NameOrId,
        authz_vpc: &authz::Vpc,
    ) -> LookupResult<authz::VpcRouter> {
        let (.., vpc, rtr) = (match router {
            key @ NameOrId::Name(_) => self.vpc_router_lookup(
                opctx,
                vpc::RouterSelector {
                    project: None,
                    vpc: Some(NameOrId::Id(authz_vpc.id())),
                    router: key.clone(),
                },
            )?,
            key @ NameOrId::Id(_) => self.vpc_router_lookup(
                opctx,
                vpc::RouterSelector {
                    project: None,
                    vpc: None,
                    router: key.clone(),
                },
            )?,
        })
        .lookup_for(authz::Action::Read)
        .await?;

        if vpc.id() != authz_vpc.id() {
            return Err(Error::invalid_request(
                "a router can only be attached to a subnet when both \
                belong to the same VPC",
            ));
        }

        Ok(rtr)
    }

    pub(crate) async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        vpc_lookup: &lookup::Vpc<'_>,
        kind: &VpcRouterKind,
        params: &vpc::VpcRouterCreate,
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

    /// Update a VPC router. Both API versions of this endpoint pass through
    /// here. The newer body is strict — `name` and `description` are required —
    /// while the older (`update_compat`) one is lax: either may be omitted,
    /// leaving it unchanged. We take the lax type because it can hold a body
    /// from either version. A strict body converts into it by wrapping each
    /// field in `Some`; the reverse is impossible, since there's no value to
    /// supply for a field the lax body omitted.
    pub(crate) async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        vpc_router_lookup: &lookup::VpcRouter<'_>,
        params: update_compat::vpc::VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        let (.., authz_router) =
            vpc_router_lookup.lookup_for(authz::Action::Modify).await?;
        let updates = db::model::VpcRouterUpdate {
            name: params.identity.name.map(db::model::Name),
            description: params.identity.description,
            time_modified: chrono::Utc::now(),
        };
        self.db_datastore.vpc_update_router(opctx, &authz_router, updates).await
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
            return Err(Error::invalid_request("cannot delete system router"));
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
        route_selector: vpc::RouteSelector,
    ) -> LookupResult<lookup::RouterRoute<'a>> {
        match route_selector {
            vpc::RouteSelector {
                route: NameOrId::Id(id),
                router: None,
                vpc: None,
                project: None,
            } => {
                let route = LookupPath::new(opctx, &self.db_datastore)
                    .router_route_id(id);
                Ok(route)
            }
            vpc::RouteSelector {
                route: NameOrId::Name(name),
                router: Some(router),
                vpc,
                project,
            } => {
                let route = self
                    .vpc_router_lookup(
                        opctx,
                        vpc::RouterSelector { project, vpc, router },
                    )?
                    .router_route_name_owned(name.into());
                Ok(route)
            }
            vpc::RouteSelector { route: NameOrId::Id(_), .. } => {
                Err(Error::invalid_request(
                    "when providing route as an ID router, subnet, vpc, and project should not be specified",
                ))
            }
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
        params: &vpc::RouterRouteCreate,
    ) -> CreateResult<db::model::RouterRoute> {
        let (.., authz_router, db_router) =
            router_lookup.fetch_for(authz::Action::CreateChild).await?;

        if db_router.kind == VpcRouterKind::System {
            return Err(Error::invalid_request(
                "user-provided routes cannot be added to a system router",
            ));
        }

        validate_user_route_targets(
            &params.destination,
            &params.target,
            RouterRouteKind::Custom,
        )?;

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

    /// Update a VPC router route. Both API versions of this endpoint pass
    /// through here. The newer body is strict — `name`, `description`,
    /// `target`, and `destination` are all required — while the older
    /// (`update_compat`) one is lax: `name`/`description` may be omitted
    /// (leaving them unchanged). We take the lax type because it can hold a
    /// body from either version. A strict body converts into it by wrapping
    /// `name`/`description` in `Some`; the reverse is impossible, since there's
    /// no value to supply for a field the lax body omitted.
    pub(crate) async fn router_update_route(
        &self,
        opctx: &OpContext,
        route_lookup: &lookup::RouterRoute<'_>,
        params: update_compat::vpc::RouterRouteUpdate,
    ) -> UpdateResult<RouterRoute> {
        let (.., authz_router, authz_route, db_route) =
            route_lookup.fetch_for(authz::Action::Modify).await?;

        // Default routes allow a constrained form of modification: only the
        // target may change. Under strict bodies `name`/`description` are
        // always present, so compare against the existing values rather than
        // checking for presence. An omitted field (lax body) resolves to the
        // existing value, so this is a no-op for callers who leave it unchanged.
        let new_name = params
            .identity
            .name
            .clone()
            .unwrap_or_else(|| db_route.name().clone());
        let new_description = params
            .identity
            .description
            .clone()
            .unwrap_or_else(|| db_route.description().to_string());

        match db_route.kind.0 {
            RouterRouteKind::Default
                if new_name != *db_route.name()
                    || new_description.as_str() != db_route.description()
                    || params.destination != db_route.destination.0 =>
            {
                return Err(Error::invalid_request(
                    "the destination and metadata of a Default route cannot be changed",
                ));
            }

            RouterRouteKind::Custom | RouterRouteKind::Default => {}

            _ => {
                return Err(Error::invalid_request(format!(
                    "routes of type {} within the system router \
                        are not modifiable",
                    db_route.kind.0,
                )));
            }
        }

        validate_user_route_targets(
            &params.destination,
            &params.target,
            db_route.kind.0,
        )?;

        let update = db::model::RouterRouteUpdate {
            name: params.identity.name.map(db::model::Name),
            description: params.identity.description,
            time_modified: chrono::Utc::now(),
            target: db::model::RouteTarget(params.target),
            destination: db::model::RouteDestination::new(params.destination),
        };
        let out = self
            .db_datastore
            .router_update_route(&opctx, &authz_route, update)
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

/// Validate route destinations/targets for a user-specified route:
/// - mixed explicit v4 and v6 are disallowed.
/// - users cannot specify 'Vpc' as a custom/default router dest/target.
/// - users cannot specify 'Subnet' as a custom/default router target.
fn validate_user_route_targets(
    dest: &RouteDestination,
    target: &RouteTarget,
    route_type: RouterRouteKind,
) -> Result<(), Error> {
    match (dest, target) {
        (
            RouteDestination::Ip(IpAddr::V4(_)),
            RouteTarget::Ip(IpAddr::V4(_)),
        )
        | (
            RouteDestination::Ip(IpAddr::V6(_)),
            RouteTarget::Ip(IpAddr::V6(_)),
        )
        | (
            RouteDestination::IpNet(IpNet::V4(_)),
            RouteTarget::Ip(IpAddr::V4(_)),
        )
        | (
            RouteDestination::IpNet(IpNet::V6(_)),
            RouteTarget::Ip(IpAddr::V6(_)),
        ) => {}

        (RouteDestination::Ip(_), RouteTarget::Ip(_))
        | (RouteDestination::IpNet(_), RouteTarget::Ip(_)) => {
            return Err(Error::invalid_request(
                "cannot mix explicit IPv4 and IPv6 addresses between destination and target",
            ));
        }

        (RouteDestination::Vpc(_), _) | (_, RouteTarget::Vpc(_)) => {
            return Err(Error::invalid_request(format!(
                "users cannot specify VPCs as a destination or target in {route_type} routes"
            )));
        }

        (_, RouteTarget::Subnet(_)) => {
            return Err(Error::invalid_request(format!(
                "subnets cannot be used as a target in {route_type} routers"
            )));
        }

        _ => {}
    };

    Ok(())
}
