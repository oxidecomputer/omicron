// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::{
    http_testing::{AuthnMode, NexusRequest},
    resource_helpers::{
        attach_ip_address_to_igw, attach_ip_pool_to_igw, create_floating_ip,
        create_instance_with, create_internet_gateway, create_ip_pool,
        create_project, create_route, create_router, create_vpc,
        delete_internet_gateway, detach_ip_address_from_igw,
        detach_ip_pool_from_igw, link_ip_pool, objects_list_page_authz,
    },
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{
    params::{
        ExternalIpCreate, InstanceNetworkInterfaceAttachment,
        InstanceNetworkInterfaceCreate,
    },
    views::{InternetGateway, InternetGatewayIpAddress, InternetGatewayIpPool},
};
use nexus_types::identity::Resource;
use omicron_common::{
    address::{IpRange, Ipv4Range},
    api::external::{
        IdentityMetadataCreateParams, NameOrId, RouteDestination, RouteTarget,
    },
};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "delta-quadrant";
const VPC_NAME: &str = "dominion";
const IGW_NAME: &str = "wormhole";
const IP_POOL_NAME: &str = "ds9";
const IP_POOL_ATTACHMENT_NAME: &str = "runabout";
const IP_ADDRESS_ATTACHMENT_NAME: &str = "defiant";
const IP_ADDRESS_ATTACHMENT: &str = "198.51.100.47";
const IP_ADDRESS_ATTACHMENT_FROM_POOL: &str = "203.0.113.1";
const INSTANCE_NAME: &str = "odo";
const FLOATING_IP_NAME: &str = "floater";
const ROUTER_NAME: &str = "deepspace";
const ROUTE_NAME: &str = "subspace";

#[nexus_test]
async fn test_internet_gateway_basic_crud(ctx: &ControlPlaneTestContext) {
    let c = &ctx.external_client;
    test_setup(c).await;

    // should start with zero gateways
    let igws = list_internet_gateways(c, PROJECT_NAME, VPC_NAME).await;
    assert_eq!(igws.len(), 0, "should start with zero internet gateways");
    expect_igw_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;

    // create an internet gateway
    let gw = create_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    let igws = list_internet_gateways(c, PROJECT_NAME, VPC_NAME).await;
    assert_eq!(igws.len(), 1, "should now have one internet gateway");

    // should be able to get the gateway just created
    let same_igw = get_igw(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    assert_eq!(&gw.identity, &same_igw.identity);

    // a new igw should have zero ip pools
    let igw_pools =
        list_internet_gateway_ip_pools(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(igw_pools.len(), 0, "a new igw should have no pools");

    // a new igw should have zero ip addresses
    let igw_addrs =
        list_internet_gateway_ip_addresses(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(igw_addrs.len(), 0, "a new igw should have no addresses");

    // attach an ip pool
    attach_ip_pool_to_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_POOL_NAME,
        IP_POOL_ATTACHMENT_NAME,
    )
    .await;
    let igw_pools =
        list_internet_gateway_ip_pools(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(igw_pools.len(), 1, "should now have one attached ip pool");

    // ensure we cannot delete the IP gateway without cascading
    expect_igw_delete_fail(c, PROJECT_NAME, VPC_NAME, IGW_NAME, false).await;

    // ensure we cannot detach the igw ip pool without cascading
    expect_igw_ip_pool_detach_fail(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_POOL_ATTACHMENT_NAME,
        false,
    )
    .await;

    // attach an ip address
    attach_ip_address_to_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_ADDRESS_ATTACHMENT.parse().unwrap(),
        IP_ADDRESS_ATTACHMENT_NAME,
    )
    .await;
    let igw_pools =
        list_internet_gateway_ip_addresses(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(igw_pools.len(), 1, "should now have one attached address");

    // detach an ip pool, note we need to cascade here since a running instance
    // has a route that uses the ip pool association
    detach_ip_pool_from_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_POOL_ATTACHMENT_NAME,
        true,
    )
    .await;
    let igw_addrs =
        list_internet_gateway_ip_pools(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(igw_addrs.len(), 0, "should now have zero attached ip pool");

    // detach an ip address
    detach_ip_address_from_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_ADDRESS_ATTACHMENT_NAME,
        false,
    )
    .await;
    let igw_addrs =
        list_internet_gateway_ip_addresses(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(
        igw_addrs.len(),
        0,
        "should now have zero attached ip addresses"
    );

    // delete internet gateay
    delete_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME, false).await;
    let igws = list_internet_gateways(c, PROJECT_NAME, VPC_NAME).await;
    assert_eq!(igws.len(), 0, "should now have zero internet gateways");

    // looking for gateway should return 404
    expect_igw_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;

    // looking for gateway pools should return 404
    expect_igw_pools_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;

    // looking for gateway addresses should return 404
    expect_igw_addresses_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
}

#[nexus_test]
async fn test_internet_gateway_address_detach(ctx: &ControlPlaneTestContext) {
    let c = &ctx.external_client;
    test_setup(c).await;

    create_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    attach_ip_address_to_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_ADDRESS_ATTACHMENT_FROM_POOL.parse().unwrap(),
        IP_ADDRESS_ATTACHMENT_NAME,
    )
    .await;

    // ensure we cannot detach the igw address without cascading
    expect_igw_ip_address_detach_fail(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_ADDRESS_ATTACHMENT_NAME,
        false,
    )
    .await;

    // ensure that we can detach the igw address with cascading
    detach_ip_address_from_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_ADDRESS_ATTACHMENT_NAME,
        true,
    )
    .await;

    // should be no addresses attached to the igw
    let igw_addrs =
        list_internet_gateway_ip_addresses(c, PROJECT_NAME, VPC_NAME, IGW_NAME)
            .await;
    assert_eq!(
        igw_addrs.len(),
        0,
        "should now have zero attached ip addresses"
    );
}

#[nexus_test]
async fn test_internet_gateway_delete_cascade(ctx: &ControlPlaneTestContext) {
    let c = &ctx.external_client;
    test_setup(c).await;

    create_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    attach_ip_address_to_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_ADDRESS_ATTACHMENT_FROM_POOL.parse().unwrap(),
        IP_ADDRESS_ATTACHMENT_NAME,
    )
    .await;
    attach_ip_pool_to_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_POOL_NAME,
        IP_POOL_ATTACHMENT_NAME,
    )
    .await;

    delete_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME, true).await;

    // looking for gateway should return 404
    expect_igw_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    // looking for gateway pools should return 404
    expect_igw_pools_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    // looking for gateway addresses should return 404
    expect_igw_addresses_not_found(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
}

async fn test_setup(c: &ClientTestContext) {
    // create a project and vpc to test with
    let _proj = create_project(&c, PROJECT_NAME).await;
    let _vpc = create_vpc(&c, PROJECT_NAME, VPC_NAME).await;
    let _pool = create_ip_pool(
        c,
        IP_POOL_NAME,
        Some(IpRange::V4(Ipv4Range {
            first: "203.0.113.1".parse().unwrap(),
            last: "203.0.113.254".parse().unwrap(),
        })),
    )
    .await;
    link_ip_pool(&c, IP_POOL_NAME, &DEFAULT_SILO.id(), true).await;
    let _floater = create_floating_ip(
        c,
        FLOATING_IP_NAME,
        PROJECT_NAME,
        None,
        Some(IP_POOL_NAME),
    )
    .await;
    let nic_attach = InstanceNetworkInterfaceAttachment::Create(vec![
        InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                description: String::from("description"),
                name: "noname".parse().unwrap(),
            },
            ip: None,
            subnet_name: "default".parse().unwrap(),
            vpc_name: VPC_NAME.parse().unwrap(),
        },
    ]);
    let _inst = create_instance_with(
        c,
        PROJECT_NAME,
        INSTANCE_NAME,
        &nic_attach,
        Vec::new(),
        vec![ExternalIpCreate::Floating {
            floating_ip: NameOrId::Name(FLOATING_IP_NAME.parse().unwrap()),
        }],
        true,
    )
    .await;

    let _router = create_router(c, PROJECT_NAME, VPC_NAME, ROUTER_NAME).await;
    let route = create_route(
        c,
        PROJECT_NAME,
        VPC_NAME,
        ROUTER_NAME,
        ROUTE_NAME,
        RouteDestination::IpNet("0.0.0.0/0".parse().unwrap()),
        RouteTarget::InternetGateway(IGW_NAME.parse().unwrap()),
    )
    .await;
    println!("{}", route.target);
}

async fn list_internet_gateways(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
) -> Vec<InternetGateway> {
    let url = format!(
        "/v1/internet-gateways?project={}&vpc={}",
        project_name, vpc_name
    );
    let out = objects_list_page_authz::<InternetGateway>(client, &url).await;
    out.items
}

async fn list_internet_gateway_ip_pools(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
) -> Vec<InternetGatewayIpPool> {
    let url = format!(
        "/v1/internet-gateway-ip-pools?project={}&vpc={}&gateway={}",
        project_name, vpc_name, igw_name,
    );
    let out =
        objects_list_page_authz::<InternetGatewayIpPool>(client, &url).await;
    out.items
}

async fn list_internet_gateway_ip_addresses(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
) -> Vec<InternetGatewayIpAddress> {
    let url = format!(
        "/v1/internet-gateway-ip-addresses?project={}&vpc={}&gateway={}",
        project_name, vpc_name, igw_name,
    );
    let out =
        objects_list_page_authz::<InternetGatewayIpAddress>(client, &url).await;
    out.items
}

async fn expect_igw_not_found(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
) {
    // check 404 response
    let url = format!(
        "/v1/internet-gateways/{}?project={}&vpc={}",
        igw_name, project_name, vpc_name
    );
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: internet-gateway with name \"{IGW_NAME}\"")
    );
}

async fn get_igw(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
) -> InternetGateway {
    // check 404 response
    let url = format!(
        "/v1/internet-gateways/{}?project={}&vpc={}",
        igw_name, project_name, vpc_name
    );
    NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn expect_igw_delete_fail(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    cascade: bool,
) {
    let url = format!(
        "/v1/internet-gateways/{}?project={}&vpc={}&cascade={}",
        igw_name, project_name, vpc_name, cascade
    );
    let _error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
}

async fn expect_igw_ip_pool_detach_fail(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    ip_pool_attachment_name: &str,
    cascade: bool,
) {
    let url = format!(
        "/v1/internet-gateway-ip-pools/{}?project={}&vpc={}&gateway={}&cascade={}",
        ip_pool_attachment_name, project_name, vpc_name, igw_name, cascade
    );
    let _error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
}

async fn expect_igw_ip_address_detach_fail(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    ip_address_attachment_name: &str,
    cascade: bool,
) {
    let url = format!(
        "/v1/internet-gateway-ip-addresses/{}?project={}&vpc={}&gateway={}&cascade={}",
        ip_address_attachment_name, project_name, vpc_name, igw_name, cascade
    );
    let _error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
}

async fn expect_igw_pools_not_found(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
) {
    let url = format!(
        "/v1/internet-gateway-ip-pools?project={}&vpc={}&gateway={}",
        project_name, vpc_name, igw_name,
    );
    let _error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
}

async fn expect_igw_addresses_not_found(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
) {
    let url = format!(
        "/v1/internet-gateway-ip-addresses?project={}&vpc={}&gateway={}",
        project_name, vpc_name, igw_name,
    );
    let _error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
}
