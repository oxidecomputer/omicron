// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use nexus_test_utils::{
    http_testing::{AuthnMode, NexusRequest},
    resource_helpers::{
        attach_ip_address_to_igw, attach_ip_pool_to_igw,
        create_internet_gateway, create_ip_pool, create_project, create_vpc,
        delete_internet_gateway, detach_ip_address_from_igw,
        detach_ip_pool_from_igw, objects_list_page_authz,
    },
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::{
    InternetGateway, InternetGatewayIpAddress, InternetGatewayIpPool,
};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_internet_gateway_basic_crud(ctx: &ControlPlaneTestContext) {
    const PROJECT_NAME: &str = "delta-quadrant";
    const VPC_NAME: &str = "dominion";
    const IGW_NAME: &str = "wormhole";
    const IP_POOL_NAME: &str = "ds9";
    const IP_POOL_ATTACHMENT_NAME: &str = "runabout";
    const IP_ADDRESS_ATTACHMENT_NAME: &str = "defiant";
    const IP_ADDRESS_ATTACHMENT: &str = "198.51.100.47";

    let c = &ctx.external_client;

    // create a project and vpc to test with
    let _proj = create_project(&c, PROJECT_NAME).await;
    let _vpc = create_vpc(&c, PROJECT_NAME, VPC_NAME).await;
    let _pool = create_ip_pool(c, IP_POOL_NAME, None).await;

    // should start with zero gateways
    let igws = list_internet_gateways(c, PROJECT_NAME, VPC_NAME).await;
    assert_eq!(igws.len(), 0, "should start with zero internet gateways");

    // check 404 response
    let url = format!(
        "/v1/internet-gateways/{}?project={}&vpc={}",
        IGW_NAME, PROJECT_NAME, VPC_NAME
    );
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        c,
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

    // create an internet gateway
    let gw = create_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    let igws = list_internet_gateways(c, PROJECT_NAME, VPC_NAME).await;
    assert_eq!(igws.len(), 1, "should now have one internet gateway");

    // should be able to get the gateway just created
    let same_gw: InternetGateway = NexusRequest::object_get(c, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    assert_eq!(&gw.identity, &same_gw.identity);

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

    // detach an ip pool
    detach_ip_pool_from_igw(
        c,
        PROJECT_NAME,
        VPC_NAME,
        IGW_NAME,
        IP_POOL_ATTACHMENT_NAME,
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
    delete_internet_gateway(c, PROJECT_NAME, VPC_NAME, IGW_NAME).await;
    let igws = list_internet_gateways(c, PROJECT_NAME, VPC_NAME).await;
    assert_eq!(igws.len(), 0, "should now have zero internet gateways");
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
