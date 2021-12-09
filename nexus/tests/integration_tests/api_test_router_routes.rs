// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::common;
use std::net::{IpAddr, Ipv4Addr};

use common::test_setup;
use dropshot::test_util::{
    object_delete, object_get, objects_list_page, objects_post,
};
use dropshot::Method;
use http::StatusCode;
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams, Name,
    RouteDestination, RouteTarget, RouterRoute, RouterRouteCreateParams,
    RouterRouteKind, RouterRouteUpdateParams,
};

use crate::common::resource_helpers::{
    create_organization, create_project, create_router, create_vpc,
};

#[tokio::test]
async fn test_router_routes() {
    let cptestctx = test_setup("test_vpc_routers").await;
    let client = &cptestctx.external_client;

    let organization_name = "test-org";
    let project_name = "springfield-squidport";
    let vpc_name = "vpc1";
    let router_name = "router1";

    let get_routes_url = |router_name: &str| -> String {
        format!(
            "/organizations/{}/projects/{}/vpcs/{}/routers/{}/routes",
            organization_name, project_name, vpc_name, router_name
        )
    };

    let get_route_url = |router_name: &str, route_name: &str| -> String {
        format!(
            "/organizations/{}/projects/{}/vpcs/{}/routers/{}/routes/{}",
            organization_name, project_name, vpc_name, router_name, route_name
        )
    };

    create_organization(&client, organization_name).await;
    let _ = create_project(&client, organization_name, project_name).await;

    // Create a vpc
    create_vpc(&client, organization_name, project_name, vpc_name).await;

    // Get the system router's routes
    let system_router_routes = objects_list_page::<RouterRoute>(
        client,
        get_routes_url("system").as_str(),
    )
    .await
    .items;

    // The system should start with a single, pre-configured route
    assert_eq!(system_router_routes.len(), 1);

    // That route should be the default route
    let default_route = &system_router_routes[0];
    assert_eq!(default_route.kind, RouterRouteKind::Default);

    // It errors if you try to delete the default route
    let error = client
        .make_request_error(
            Method::DELETE,
            get_route_url("system", "default").as_str(),
            StatusCode::METHOD_NOT_ALLOWED,
        )
        .await;
    assert_eq!(error.message, "DELETE not allowed on system routes");

    // Create a custom router
    create_router(
        &client,
        organization_name,
        project_name,
        vpc_name,
        router_name,
    )
    .await;

    // Get routes list for custom router
    let routes = objects_list_page::<RouterRoute>(
        client,
        get_routes_url(router_name).as_str(),
    )
    .await
    .items;
    // There should be no custom routes to begin with
    assert_eq!(routes.len(), 0);

    let route_name = "custom-route";
    let route_url = get_route_url(router_name, route_name);

    // Create a new custom route
    objects_post::<RouterRouteCreateParams, RouterRoute>(
        client,
        get_routes_url(router_name).as_str(),
        RouterRouteCreateParams {
            identity: IdentityMetadataCreateParams {
                name: route_name.parse().unwrap(),
                description: "It's a route, what else can I say?".to_string(),
            },
            target: RouteTarget::Ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            destination: RouteDestination::Subnet("loopback".parse().unwrap()),
        },
    )
    .await;

    // Get the route and verify its state
    let route = object_get::<RouterRoute>(client, route_url.as_str()).await;
    assert_eq!(route.identity.name, route_name.parse::<Name>().unwrap());
    assert_eq!(route.kind, RouterRouteKind::Custom);
    assert_eq!(
        route.target,
        RouteTarget::Ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
    );
    assert_eq!(
        route.destination,
        RouteDestination::Subnet("loopback".parse().unwrap())
    );

    // Ensure a route can be updated
    client
        .make_request(
            Method::PUT,
            route_url.as_str(),
            Some(RouterRouteUpdateParams {
                identity: IdentityMetadataUpdateParams {
                    name: Some(route_name.parse().unwrap()),
                    description: None,
                },
                target: RouteTarget::Ip(IpAddr::V4(Ipv4Addr::new(
                    192, 168, 1, 1,
                ))),
                destination: RouteDestination::Subnet(
                    "loopback".parse().unwrap(),
                ),
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
    let route = object_get::<RouterRoute>(client, route_url.as_str()).await;

    assert_eq!(route.identity.name, route_name);
    assert_eq!(
        route.target,
        RouteTarget::Ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1,)))
    );

    object_delete(client, route_url.as_str()).await;

    // Requesting the deleted route 404s
    client
        .make_request_error(
            Method::GET,
            get_route_url(router_name, route_name).as_str(),
            StatusCode::NOT_FOUND,
        )
        .await;

    cptestctx.teardown().await;
}
