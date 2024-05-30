// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use dropshot::Method;
use http::StatusCode;
use itertools::Itertools;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_route;
use nexus_test_utils::resource_helpers::create_route_with_error;
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::object_put_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::RouterRouteUpdate;
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
use crate::integration_tests::vpc_routers::ROUTER_NAMES;
use crate::integration_tests::vpc_routers::VPC_NAME;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

fn get_routes_url(vpc_name: &str, router_name: &str) -> String {
    format!(
        "/v1/vpc-router-routes?project={}&vpc={}&router={}",
        PROJECT_NAME, vpc_name, router_name
    )
}

fn get_route_url(
    vpc_name: &str,
    router_name: &str,
    route_name: &str,
) -> String {
    format!(
        "/v1/vpc-router-routes/{}?project={}&vpc={}&router={}",
        route_name, PROJECT_NAME, vpc_name, router_name
    )
}

async fn get_system_routes(
    client: &ClientTestContext,
    vpc_name: &str,
) -> [RouterRoute; 3] {
    // Get the system router's routes
    let system_router_routes = objects_list_page_authz::<RouterRoute>(
        client,
        get_routes_url(vpc_name, "system").as_str(),
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

    [v4_route, v6_route, subnet_route]
}

#[nexus_test]
async fn test_router_routes_crud_operations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let vpc_name = "vpc1";
    let router_name = "router1";

    let _ = create_project(&client, PROJECT_NAME).await;

    // Create a vpc
    create_vpc(&client, PROJECT_NAME, vpc_name).await;

    // Get the system router's routes
    let [v4_route, v6_route, subnet_route] =
        get_system_routes(client, vpc_name).await;

    // Deleting any default system route is disallowed.
    for route in &[&v4_route, &v6_route, &subnet_route] {
        let error: dropshot::HttpErrorResponseBody =
            NexusRequest::expect_failure(
                client,
                StatusCode::BAD_REQUEST,
                Method::DELETE,
                get_route_url(vpc_name, "system", route.name().as_str())
                    .as_str(),
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
    create_router(&client, PROJECT_NAME, vpc_name, router_name).await;

    // Get routes list for custom router
    let routes = objects_list_page_authz::<RouterRoute>(
        client,
        get_routes_url(vpc_name, router_name).as_str(),
    )
    .await
    .items;
    // There should be no custom routes to begin with
    assert_eq!(routes.len(), 0);

    let route_name = "custom-route";
    let route_url = get_route_url(vpc_name, router_name, route_name);

    // Create a new custom route
    let route_created: RouterRoute = NexusRequest::objects_post(
        client,
        get_routes_url(vpc_name, router_name).as_str(),
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
        get_route_url(vpc_name, router_name, route_name).as_str(),
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
    let _ = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;

    let router_name = ROUTER_NAMES[0];
    let _router =
        create_router(&client, PROJECT_NAME, VPC_NAME, router_name).await;

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
                VPC_NAME,
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
                VPC_NAME,
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

#[nexus_test]
async fn test_router_routes_modify_system_routes(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _ = create_project(&client, PROJECT_NAME).await;
    let _ = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;

    // Attempting to add a new route to a system router should fail.
    let err = create_route_with_error(
        client,
        PROJECT_NAME,
        VPC_NAME,
        "system",
        "bad-route",
        "ipnet:240.0.0.0/8".parse().unwrap(),
        "inetgw:outbound".parse().unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "user-provided routes cannot be added to a system router"
    );

    // Get the system router's routes
    let [v4_route, v6_route, subnet_route] =
        get_system_routes(client, VPC_NAME).await;

    // Attempting to modify a VPC subnet route should fail.
    // Deletes are tested above.
    let err = object_put_error(
        client,
        &get_route_url(VPC_NAME, "system", subnet_route.name().as_str())
            .as_str(),
        &RouterRouteUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            target: "drop".parse().unwrap(),
            destination: "subnet:default".parse().unwrap(),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "routes of type VpcSubnet within the system router are not modifiable"
    );

    // Modifying the target of a Default (gateway) route should succeed.
    let v4_route: RouterRoute = object_put(
        client,
        &get_route_url(VPC_NAME, "system", v4_route.name().as_str()).as_str(),
        &RouterRouteUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            destination: v4_route.destination,
            target: "drop".parse().unwrap(),
        },
    )
    .await;
    assert_eq!(v4_route.target, RouteTarget::Drop);

    let v6_route: RouterRoute = object_put(
        client,
        &get_route_url(VPC_NAME, "system", v6_route.name().as_str()).as_str(),
        &RouterRouteUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            destination: v6_route.destination,
            target: "drop".parse().unwrap(),
        },
    )
    .await;
    assert_eq!(v6_route.target, RouteTarget::Drop);

    // Modifying the *destination* should not.
    let err = object_put_error(
        client,
        &get_route_url(VPC_NAME, "system", v4_route.name().as_str()).as_str(),
        &RouterRouteUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            destination: "ipnet:10.0.0.0/8".parse().unwrap(),
            target: "drop".parse().unwrap(),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "the destination and metadata of a Default route cannot be changed",
    );
}

#[nexus_test]
async fn test_router_routes_internet_gateway_target(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _ = create_project(&client, PROJECT_NAME).await;
    let _ = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;
    let router_name = ROUTER_NAMES[0];
    let _router =
        create_router(&client, PROJECT_NAME, VPC_NAME, router_name).await;

    // Internet gateways are not fully supported: only 'inetgw:outbound'
    // is a valid choice.
    let dest: RouteDestination = "ipnet:240.0.0.0/8".parse().unwrap();

    let err = create_route_with_error(
        client,
        PROJECT_NAME,
        VPC_NAME,
        &router_name,
        "bad-route",
        dest.clone(),
        "inetgw:not-a-real-gw".parse().unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "'outbound' is currently the only valid internet gateway"
    );

    // This can be used in a custom router, in addition
    // to its default system spot.
    let target: RouteTarget = "inetgw:outbound".parse().unwrap();
    let route = create_route(
        client,
        PROJECT_NAME,
        VPC_NAME,
        router_name,
        "good-route",
        dest.clone(),
        target.clone(),
    )
    .await;
    assert_eq!(route.destination, dest);
    assert_eq!(route.target, target);
}

#[nexus_test]
async fn test_router_routes_disallow_custom_targets(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _ = create_project(&client, PROJECT_NAME).await;
    let _ = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;
    let router_name = ROUTER_NAMES[0];
    let _router =
        create_router(&client, PROJECT_NAME, VPC_NAME, router_name).await;

    // Neither 'vpc:xxx' nor 'subnet:xxx' can be specified as route targets
    // in custom routers.
    let dest: RouteDestination = "ipnet:240.0.0.0/8".parse().unwrap();

    let err = create_route_with_error(
        client,
        PROJECT_NAME,
        VPC_NAME,
        &router_name,
        "bad-route",
        dest.clone(),
        "vpc:a-vpc-name-unknown".parse().unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "VPCs cannot be used as a destination or target in custom routers"
    );

    let err = create_route_with_error(
        client,
        PROJECT_NAME,
        VPC_NAME,
        &router_name,
        "bad-route",
        "vpc:a-vpc-name-unknown".parse().unwrap(),
        "drop".parse().unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "VPCs cannot be used as a destination or target in custom routers"
    );

    let err = create_route_with_error(
        client,
        PROJECT_NAME,
        VPC_NAME,
        &router_name,
        "bad-route",
        dest.clone(),
        "subnet:a-vpc-name-unknown".parse().unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "subnets cannot be used as a target in custom routers"
    );
}
