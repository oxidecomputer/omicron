use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::VpcSubnet;
use omicron_common::api::external::VpcSubnetCreateParams;
use omicron_common::api::external::VpcSubnetUpdateParams;
use std::convert::TryFrom;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;

pub mod common;
use common::identity_eq;
use common::resource_helpers::{create_project, create_vpc};
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_vpcs() {
    let cptestctx = test_setup("test_vpcs").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let vpcs_url = format!("/projects/{}/vpcs", project_name);
    let _ = create_project(&client, project_name).await;

    /* Create a VPC. */
    let vpc_name = "vpc1";
    let vpc = create_vpc(&client, project_name, vpc_name).await;

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

    // creating another subnet in the same VPC with the same name fails
    let error = client
        .make_request_error_body(
            Method::POST,
            &subnets_url,
            new_subnet.clone(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "already exists: vpc subnet \"subnet1\"");

    let subnet2_name = "subnet2";
    let subnet2_url = format!("{}/{}", subnets_url, subnet2_name);

    // second subnet 404s before it's created
    let error = client
        .make_request_error(Method::GET, &subnet2_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc subnet with name \"subnet2\"");

    // create second subnet
    let new_subnet = VpcSubnetCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(subnet2_name).unwrap(),
            description: String::from("it's also below the net"),
        },
        ipv4_block: None,
        ipv6_block: None,
    };
    let subnet2: VpcSubnet =
        objects_post(&client, &subnets_url, new_subnet.clone()).await;
    assert_eq!(subnet2.identity.name, subnet2_name);
    assert_eq!(subnet2.identity.description, "it's also below the net");
    assert_eq!(subnet2.vpc_id, vpc.identity.id);
    assert_eq!(subnet2.ipv4_block, None);
    assert_eq!(subnet2.ipv6_block, None);

    // subnets list should now have two in it
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 2);
    subnets_eq(&subnets[0], &subnet);
    subnets_eq(&subnets[1], &subnet2);

    // update first subnet
    let update_params = VpcSubnetUpdateParams {
        identity: IdentityMetadataUpdateParams {
            name: Some(Name::try_from("new-name").unwrap()),
            description: Some(String::from("another description")),
        },
        ipv4_block: None,
        ipv6_block: None,
    };
    client
        .make_request(
            Method::PUT,
            &subnet_url,
            Some(update_params),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // fetching by old name 404s
    let error = client
        .make_request_error(Method::GET, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc subnet with name \"subnet1\"");

    let subnet_url = format!("{}/{}", subnets_url, "new-name");

    // fetching by new name works
    let updated_subnet = object_get::<VpcSubnet>(client, &subnet_url).await;
    assert_eq!(&updated_subnet.identity.description, "another description");

    // fetching list should show updated one
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 2);
    subnets_eq(&subnets[0], &updated_subnet);

    // delete first subnet
    client
        .make_request_no_body(
            Method::DELETE,
            &subnet_url,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // subnets list should now have one again, the second one
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);
    subnets_eq(&subnets[0], &subnet2);

    // get subnet should 404
    let error = client
        .make_request_error(Method::GET, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc subnet with name \"new-name\"");

    // delete subnet should 404
    let error = client
        .make_request_error(Method::DELETE, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc subnet with name \"new-name\"");

    // Creating a subnet with the same name in a different VPC is allowed
    let vpc2_name = "vpc2";
    let vpc2 = create_vpc(&client, project_name, vpc2_name).await;

    let subnet_same_name: VpcSubnet = objects_post(
        &client,
        format!("{}/{}/subnets", vpcs_url, vpc2_name).as_str(),
        new_subnet.clone(),
    )
    .await;
    assert_eq!(subnet_same_name.identity.name, subnet2_name);
    assert_eq!(
        subnet_same_name.identity.description,
        "it's also below the net"
    );
    assert_eq!(subnet_same_name.vpc_id, vpc2.identity.id);
    assert_eq!(subnet_same_name.ipv4_block, None);
    assert_eq!(subnet_same_name.ipv6_block, None);

    cptestctx.teardown().await;
}

fn subnets_eq(sn1: &VpcSubnet, sn2: &VpcSubnet) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}
