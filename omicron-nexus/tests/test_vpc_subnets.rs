use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::Vpc;
use omicron_common::api::external::VpcCreateParams;
use omicron_common::api::external::VpcSubnet;
use omicron_common::api::external::VpcSubnetCreateParams;
use std::convert::TryFrom;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;

pub mod common;
use common::identity_eq;
use common::resource_helpers::create_project;
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_vpcs() {
    let cptestctx = test_setup("test_vpcs").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let vpcs_url = format!("/projects/{}/vpcs", project_name);
    let _ = create_project(&client, &project_name).await;

    /* Create a VPC. */
    let vpc_name = "vpc1";
    let new_vpc = VpcCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(vpc_name).unwrap(),
            description: String::from("sells rainsticks"),
        },
        dns_name: Name::try_from("abc").unwrap(),
    };
    let vpc: Vpc = objects_post(&client, &vpcs_url, new_vpc.clone()).await;

    let vpc_url = format!("{}/{}", vpcs_url, vpc_name);
    let subnets_url = format!("{}/subnets", vpc_url);

    // get subnets should be empty
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 0);

    let subnet_name = "subnet1";
    let subnet_url = format!("{}/{}", subnets_url, subnet_name);

    // fetching a particular subnet should 404
    let error = client
        .make_request_error(Method::GET, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc subnet with name \"subnet1\"");

    /*

    /* Create a VPC Subnet. */
    let new_subnet = VpcSubnetCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(subnet_name).unwrap(),
            description: String::from("it's below the net"),
        },
        ipv4_block: None,
        ipv6_block: None,
    };
    let subnet: VpcSubnet =
        objects_post(&client, &subnets_url, new_subnet.clone()).await;
    assert_eq!(subnet.identity.name, subnet_name);
    assert_eq!(subnet.identity.description, "it's below the net");
    assert_eq!(subnet.vpc_id, vpc.identity.id);
    assert_eq!(subnet.ipv4_block, None);
    assert_eq!(subnet.ipv6_block, None);

    // get subnet, should be the same
    let same_subnet = object_get::<VpcSubnet>(client, &subnet_url).await;
    subnets_eq(&subnet, &same_subnet);

    // subnets list should now have the one in it
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);
    subnets_eq(&subnets[0], &subnet);
    */

    // second subnet 404s
    // create second subnet
    // get list of subnets
    // delete first subnet
    // get list of subnets

    cptestctx.teardown().await;
}

fn subnets_eq(sn1: &VpcSubnet, sn2: &VpcSubnet) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}
