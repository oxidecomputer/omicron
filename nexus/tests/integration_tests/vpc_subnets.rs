// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use ipnetwork::{Ipv4Network, Ipv6Network};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Ipv6Net;
use omicron_nexus::external_api::{params, views::VpcSubnet};

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;

use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::{
    create_organization, create_project, create_vpc,
};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

#[nexus_test]
async fn test_vpc_subnets(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let org_name = "test-org";
    create_organization(&client, &org_name).await;
    let project_name = "springfield-squidport";
    let vpcs_url =
        format!("/organizations/{}/projects/{}/vpcs", org_name, project_name);
    let _ = create_project(&client, org_name, project_name).await;

    /* Create a VPC. */
    let vpc_name = "vpc1";
    let vpc = create_vpc(&client, org_name, project_name, vpc_name).await;

    let vpc_url = format!("{}/{}", vpcs_url, vpc_name);
    let subnets_url = format!("{}/subnets", vpc_url);

    // get subnets should return the default subnet
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);

    // delete default subnet
    client
        .make_request_no_body(
            Method::DELETE,
            &format!("{}/{}", subnets_url, subnets[0].identity.name),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // get subnets should now be empty
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 0);

    let subnet_name = "subnet1";
    let subnet_url = format!("{}/{}", subnets_url, subnet_name);

    // fetching a particular subnet should 404
    let error = client
        .make_request_error(Method::GET, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc-subnet with name \"subnet1\"");

    /* Create a VPC Subnet. */
    let ipv4_block = Ipv4Net("10.1.9.32/16".parse::<Ipv4Network>().unwrap());
    let ipv6_block = Ipv6Net("2001:db8::0/96".parse::<Ipv6Network>().unwrap());
    let new_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet_name.parse().unwrap(),
            description: "it's below the net".to_string(),
        },
        ipv4_block,
        ipv6_block,
    };
    let subnet: VpcSubnet =
        objects_post(&client, &subnets_url, new_subnet.clone()).await;
    assert_eq!(subnet.identity.name, subnet_name);
    assert_eq!(subnet.identity.description, "it's below the net");
    assert_eq!(subnet.vpc_id, vpc.identity.id);
    assert_eq!(subnet.ipv4_block, ipv4_block);
    assert_eq!(subnet.ipv6_block, ipv6_block);

    // try to update ipv4_block with IPv6 value, should 400
    assert_put_400(
        client,
        &subnet_url,
        String::from("{ \"ipv4Block\": \"2001:db8::0/96\" }"),
        "unable to parse body: invalid address: 2001:db8::0",
    )
    .await;

    // try to update ipv6_block with IPv4 value, should 400
    assert_put_400(
        client,
        &subnet_url,
        String::from("{ \"ipv6Block\": \"10.1.9.32/16\" }"),
        "unable to parse body: invalid address: 10.1.9.32",
    )
    .await;

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
    assert_eq!(error.message, "already exists: vpc-subnet \"subnet1\"");

    let subnet2_name = "subnet2";
    let subnet2_url = format!("{}/{}", subnets_url, subnet2_name);

    // second subnet 404s before it's created
    let error = client
        .make_request_error(Method::GET, &subnet2_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc-subnet with name \"subnet2\"");

    // create second subnet
    let new_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet2_name.parse().unwrap(),
            description: "it's also below the net".to_string(),
        },
        ipv4_block,
        ipv6_block,
    };
    let subnet2: VpcSubnet =
        objects_post(&client, &subnets_url, new_subnet.clone()).await;
    assert_eq!(subnet2.identity.name, subnet2_name);
    assert_eq!(subnet2.identity.description, "it's also below the net");
    assert_eq!(subnet2.vpc_id, vpc.identity.id);
    assert_eq!(subnet2.ipv4_block, ipv4_block);
    assert_eq!(subnet2.ipv6_block, ipv6_block);

    // subnets list should now have two in it
    let subnets =
        objects_list_page::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 2);
    subnets_eq(&subnets[0], &subnet);
    subnets_eq(&subnets[1], &subnet2);

    // update first subnet
    let new_ipv4_block =
        Ipv4Net("10.1.9.33/16".parse::<Ipv4Network>().unwrap());
    let new_ipv6_block =
        Ipv6Net("2001:db9::0/96".parse::<Ipv6Network>().unwrap());
    let update_params = params::VpcSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("new-name".parse().unwrap()),
            description: Some("another description".to_string()),
        },
        ipv4_block: new_ipv4_block,
        ipv6_block: new_ipv6_block,
    };
    client
        .make_request(
            Method::PUT,
            &subnet_url,
            Some(update_params),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // fetching by old name 404s
    let error = client
        .make_request_error(Method::GET, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc-subnet with name \"subnet1\"");

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
    assert_eq!(error.message, "not found: vpc-subnet with name \"new-name\"");

    // delete subnet should 404
    let error = client
        .make_request_error(Method::DELETE, &subnet_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc-subnet with name \"new-name\"");

    // Creating a subnet with the same name in a different VPC is allowed
    let vpc2_name = "vpc2";
    let vpc2 = create_vpc(&client, org_name, project_name, vpc2_name).await;

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
    assert_eq!(subnet_same_name.ipv4_block, ipv4_block);
    assert_eq!(subnet_same_name.ipv6_block, ipv6_block);
}

fn subnets_eq(sn1: &VpcSubnet, sn2: &VpcSubnet) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}

async fn assert_put_400(
    client: &ClientTestContext,
    url: &str,
    body: String,
    message: &str,
) {
    let error = client
        .make_request_with_body(
            Method::PUT,
            &url,
            body.into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error.message.starts_with(message));
}
