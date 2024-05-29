// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::Method;
use http::StatusCode;
use itertools::Itertools;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_route;
use nexus_test_utils::resource_helpers::create_route_with_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::SimpleIdentity;
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    RouteDestination, RouteTarget, RouterRoute, RouterRouteKind,
};
use std::net::IpAddr;
use std::net::Ipv4Addr;

use nexus_test_utils::resource_helpers::{
    create_project, create_router, create_vpc,
};

use crate::integration_tests::vpc_routers::PROJECT_NAME;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_router_routes_crud_operations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let project_name = "springfield-squidport";
    let vpc_name = "vpc1";
    let router_name = "router1";

    let get_routes_url = |router_name: &str| -> String {
        format!(
            "/v1/vpc-router-routes?project={}&vpc={}&router={}",
            project_name, vpc_name, router_name
        )
    };

    let get_route_url = |router_name: &str, route_name: &str| -> String {
        format!(
            "/v1/vpc-router-routes/{}?project={}&vpc={}&router={}",
            route_name, project_name, vpc_name, router_name
        )
    };

    let _ = create_project(&client, project_name).await;

    // Create a vpc
    create_vpc(&client, project_name, vpc_name).await;

    // Get the system router's routes
    let system_router_routes = objects_list_page_authz::<RouterRoute>(
        client,
        get_routes_url("system").as_str(),
    )
    .await
    .items;

    // The system should start with three preconfigured routes:
    // - a default v4 gateway route
    // - a default v6 gateway route
    // - a managed subnet route for the 'default' subnet
    assert_eq!(system_router_routes.len(), 3);

    let mut v4_route = None;
    let mut v6_route = None;
    let mut subnet_route = None;
    for route in system_router_routes {
        match (&route.kind, &route.destination, &route.target) {
            (RouterRouteKind::Default, RouteDestination::IpNet(IpNet::V4(_)), RouteTarget::InternetGateway(_)) => {v4_route = Some(route);},
            (RouterRouteKind::Default, RouteDestination::IpNet(IpNet::V6(_)), RouteTarget::InternetGateway(_)) => {v6_route = Some(route);},
            (RouterRouteKind::VpcSubnet, RouteDestination::Subnet(n0), RouteTarget::Subnet(n1)) if n0 == n1 && n0.as_str() == "default" => {subnet_route = Some(route);},
            _ => panic!("unexpected system route {route:?} -- wanted gateway and subnet"),
        }
    }

    let v4_route =
        v4_route.expect("no v4 gateway route found in system router");
    let v6_route =
        v6_route.expect("no v6 gateway route found in system router");
    let subnet_route =
        subnet_route.expect("no default subnet route found in system router");

    // Deleting any default system route is disallowed.
    for route in &[&v4_route, &v6_route, &subnet_route] {
        let error: dropshot::HttpErrorResponseBody =
            NexusRequest::expect_failure(
                client,
                StatusCode::BAD_REQUEST,
                Method::DELETE,
                get_route_url("system", route.name().as_str()).as_str(),
            )
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
        assert_eq!(error.message, "DELETE not allowed on system routes");
    }

    // Create a custom router
    create_router(&client, project_name, vpc_name, router_name).await;

    // Get routes list for custom router
    let routes = objects_list_page_authz::<RouterRoute>(
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
    let route_created: RouterRoute = NexusRequest::objects_post(
        client,
        get_routes_url(router_name).as_str(),
        &params::RouterRouteCreate {
            identity: IdentityMetadataCreateParams {
                name: route_name.parse().unwrap(),
                description: "It's a route, what else can I say?".to_string(),
            },
            target: RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1))),
            destination: RouteDestination::Subnet("loopback".parse().unwrap()),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(route_created.identity.name.to_string(), route_name);

    // Get the route and verify its state
    let route: RouterRoute =
        NexusRequest::object_get(client, route_url.as_str())
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    identity_eq(&route_created.identity, &route.identity);
    assert_eq!(route.kind, RouterRouteKind::Custom);
    assert_eq!(
        route.target,
        RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)))
    );
    assert_eq!(
        route.destination,
        RouteDestination::Subnet("loopback".parse().unwrap())
    );

    // Ensure a route can be updated
    NexusRequest::object_put(
        client,
        route_url.as_str(),
        Some(&params::RouterRouteUpdate {
            identity: IdentityMetadataUpdateParams {
                name: Some(route_name.parse().unwrap()),
                description: None,
            },
            target: RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(
                192, 168, 1, 1,
            ))),
            destination: RouteDestination::Subnet("loopback".parse().unwrap()),
        }),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    let route: RouterRoute =
        NexusRequest::object_get(client, route_url.as_str())
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(route.identity.name, route_name);
    assert_eq!(
        route.target,
        RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(192, 168, 1, 1,)))
    );

    NexusRequest::object_delete(client, route_url.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Requesting the deleted route 404s
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        get_route_url(router_name, route_name).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_router_routes_disallow_mixed_v4_v6(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _ = create_project(&client, PROJECT_NAME).await;

    let vpc_name = "default";
    let router_name = "routy";
    let _router =
        create_router(&client, PROJECT_NAME, vpc_name, router_name).await;

    // Some targets/strings refer to a mixed v4/v6 entity, e.g.,
    // subnet or instance. Others refer to one kind only (ipnet, ip).
    // Users should not be able to mix v4 and v6 in these latter routes
    // -- route resolution will ignore them, but a helpful error message
    // is more useful.
    let dest_set: [RouteDestination; 5] = [
        "ip:4.4.4.4".parse().unwrap(),
        "ipnet:4.4.4.0/24".parse().unwrap(),
        "ip:2001:4860:4860::8888".parse().unwrap(),
        "ipnet:2001:4860:4860::/64".parse().unwrap(),
        "subnet:named-subnet".parse().unwrap(),
    ];

    let target_set: [RouteTarget; 5] = [
        "ip:172.30.0.5".parse().unwrap(),
        "ip:fd37:faf4:cc25::5".parse().unwrap(),
        "instance:named-instance".parse().unwrap(),
        "inetgw:outbound".parse().unwrap(),
        "drop".parse().unwrap(),
    ];

    for (i, (dest, target)) in dest_set
        .into_iter()
        .cartesian_product(target_set.into_iter())
        .enumerate()
    {
        use RouteDestination as Rd;
        use RouteTarget as Rt;
        let allowed = match (&dest, &target) {
            (Rd::Ip(IpAddr::V4(_)), Rt::Ip(IpAddr::V4(_)))
            | (Rd::Ip(IpAddr::V6(_)), Rt::Ip(IpAddr::V6(_)))
            | (Rd::IpNet(IpNet::V4(_)), Rt::Ip(IpAddr::V4(_)))
            | (Rd::IpNet(IpNet::V6(_)), Rt::Ip(IpAddr::V6(_))) => true,
            (Rd::Ip(_), Rt::Ip(_)) | (Rd::IpNet(_), Rt::Ip(_)) => false,
            _ => true,
        };

        let route_name = format!("test-route-{i}");

        if allowed {
            create_route(
                client,
                PROJECT_NAME,
                vpc_name,
                router_name,
                &route_name,
                dest,
                target,
            )
            .await;
        } else {
            let err = create_route_with_error(
                client,
                PROJECT_NAME,
                vpc_name,
                router_name,
                &route_name,
                dest,
                target,
                StatusCode::BAD_REQUEST,
            )
            .await;
            assert_eq!(
                err.message,
                "cannot mix explicit IPv4 and IPv6 addresses between destination and target"
            );
        }
    }
}
