// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::instances::instance_post;
use crate::integration_tests::instances::instance_simulate;
use crate::integration_tests::instances::InstanceOp;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, create_vpc,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views::VpcSubnet};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Ipv6Net;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_delete_vpc_subnet_with_interfaces_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;

    // Create a project that we'll use for testing.
    let project_name = "springfield-squidport";
    let instance_name = "inst";
    create_default_ip_pool(client).await;
    let _ = create_project(&client, project_name).await;

    let subnets_url =
        format!("/v1/vpc-subnets?project={}&vpc=default", project_name);
    let subnet_url =
        format!("/v1/vpc-subnets/default?project={}&vpc=default", project_name);

    // get subnets should return the default subnet
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);

    // Create an instance in the default VPC and VPC Subnet. Verify that we
    // cannot delete the subnet until the instance is gone.
    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    let instance = create_instance(client, &project_name, instance_name).await;
    instance_simulate(nexus, &instance.identity.id).await;
    let err: HttpErrorResponseBody = NexusRequest::expect_failure(
        &client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &subnet_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        err.message,
        "VPC Subnet cannot be deleted while \
        network interfaces in the subnet exist",
    );

    // Stop and then delete the instance
    instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(&nexus, &instance.identity.id).await;
    NexusRequest::object_delete(&client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Now deleting the subnet should succeed
    NexusRequest::object_delete(
        &client,
        &format!(
            "/v1/vpc-subnets/{}?project={project_name}&vpc=default",
            subnets[0].identity.name
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert!(subnets.is_empty());
}

#[nexus_test]
async fn test_vpc_subnets(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let project_name = "springfield-squidport";
    let _ = create_project(&client, project_name).await;

    // Create a VPC.
    let vpc_name = "vpc1";
    let vpc = create_vpc(&client, project_name, vpc_name).await;

    let subnets_url =
        format!("/v1/vpc-subnets?project={}&vpc={}", project_name, vpc_name);

    // get subnets should return the default subnet
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);

    // delete default subnet
    NexusRequest::object_delete(
        &client,
        &format!(
            "/v1/vpc-subnets/{}?project={}&vpc={}",
            subnets[0].identity.name, project_name, vpc_name
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // get subnets should now be empty
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 0);

    let subnet_name = "subnet1";
    let subnet_url = format!(
        "/v1/vpc-subnets/{}?project={}&vpc={}",
        subnet_name, project_name, vpc_name
    );

    // fetching a particular subnet should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &subnet_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-subnet with name \"subnet1\"");

    // Create a VPC Subnet.
    let ipv4_block = Ipv4Net("10.0.0.0/24".parse().unwrap());
    let other_ipv4_block = Ipv4Net("172.31.0.0/16".parse().unwrap());
    // Create the first two available IPv6 address ranges. */
    let prefix = vpc.ipv6_prefix.network();
    let ipv6_block = Ipv6Net(ipnetwork::Ipv6Network::new(prefix, 64).unwrap());
    let mut segments = prefix.segments();
    segments[3] = 1;
    let addr = std::net::Ipv6Addr::from(segments);
    let other_ipv6_block =
        Some(Ipv6Net(ipnetwork::Ipv6Network::new(addr, 64).unwrap()));
    let new_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet_name.parse().unwrap(),
            description: "it's below the net".to_string(),
        },
        ipv4_block,
        ipv6_block: Some(ipv6_block),
    };
    let subnet: VpcSubnet =
        NexusRequest::objects_post(client, &subnets_url, &new_subnet)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(subnet.identity.name, subnet_name);
    assert_eq!(subnet.identity.description, "it's below the net");
    assert_eq!(subnet.vpc_id, vpc.identity.id);
    assert_eq!(subnet.ipv4_block, ipv4_block);
    assert_eq!(subnet.ipv6_block, ipv6_block);
    assert!(subnet.ipv6_block.is_vpc_subnet(&vpc.ipv6_prefix));

    // get subnet, should be the same
    let same_subnet = NexusRequest::object_get(client, &subnet_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    subnets_eq(&subnet, &same_subnet);

    // get subnet by ID, should retrieve the same subnet
    let subnet_by_id_url = format!("/v1/vpc-subnets/{}", &subnet.identity.id);
    let same_subnet_again = NexusRequest::object_get(client, &subnet_by_id_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    subnets_eq(&subnet, &same_subnet_again);

    // subnets list should now have the one in it
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);
    subnets_eq(&subnets[0], &subnet);

    // creating another subnet in the same VPC with the same IP ranges fails
    let new_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: "new-name".parse().unwrap(),
            description: "it's below the net".to_string(),
        },
        ipv4_block,
        ipv6_block: Some(ipv6_block),
    };
    let expected_error = format!(
        "IP address range '{}' conflicts with an existing subnet",
        ipv4_block,
    );
    let error: dropshot::HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &subnets_url)
            .expect_status(Some(StatusCode::BAD_REQUEST))
            .body(Some(&new_subnet)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, expected_error);

    // creating another subnet in the same VPC with the same name, but different
    // IP address ranges also fails.
    let new_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet_name.parse().unwrap(),
            description: "it's below the net".to_string(),
        },
        ipv4_block: other_ipv4_block,
        ipv6_block: other_ipv6_block,
    };
    let error: dropshot::HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &subnets_url)
            .expect_status(Some(StatusCode::BAD_REQUEST))
            .body(Some(&new_subnet)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "already exists: vpc-subnet \"subnet1\"");

    let subnet2_name = "subnet2";
    let subnet2_url = format!(
        "/v1/vpc-subnets/{}?project={}&vpc={}",
        subnet2_name, project_name, vpc_name
    );

    // second subnet 404s before it's created
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &subnet2_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-subnet with name \"subnet2\"");

    // create second subnet, this time with an autogenerated IPv6 range.
    let ipv4_block = Ipv4Net("192.168.0.0/16".parse().unwrap());
    let new_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet2_name.parse().unwrap(),
            description: "it's also below the net".to_string(),
        },
        ipv4_block,
        ipv6_block: None,
    };
    let subnet2: VpcSubnet =
        NexusRequest::objects_post(client, &subnets_url, &new_subnet)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(subnet2.identity.name, subnet2_name);
    assert_eq!(subnet2.identity.description, "it's also below the net");
    assert_eq!(subnet2.vpc_id, vpc.identity.id);
    assert_eq!(subnet2.ipv4_block, ipv4_block);
    assert!(subnet2.ipv6_block.is_vpc_subnet(&vpc.ipv6_prefix));

    // subnets list should now have two in it
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 2);
    subnets_eq(&subnets[0], &subnet);
    subnets_eq(&subnets[1], &subnet2);

    // update first subnet
    let update_params = params::VpcSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("new-name".parse().unwrap()),
            description: Some("another description".to_string()),
        },
    };
    NexusRequest::object_put(client, &subnet_url, Some(&update_params))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // fetching by old name 404s
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &subnet_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-subnet with name \"subnet1\"");

    let subnet_url = format!(
        "/v1/vpc-subnets/new-name?project={}&vpc={}",
        project_name, vpc_name
    );

    // fetching by new name works
    let updated_subnet: VpcSubnet =
        NexusRequest::object_get(client, &subnet_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(&updated_subnet.identity.description, "another description");

    // fetching list should show updated one
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 2);
    subnets_eq(&subnets[0], &updated_subnet);

    // delete first subnet
    NexusRequest::object_delete(client, &subnet_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // subnets list should now have one again, the second one
    let subnets =
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items;
    assert_eq!(subnets.len(), 1);
    subnets_eq(&subnets[0], &subnet2);

    // get subnet should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &subnet_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-subnet with name \"new-name\"");

    // delete subnet should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &subnet_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-subnet with name \"new-name\"");

    // Creating a subnet with the same name in a different VPC is allowed
    let vpc2_name = "vpc2";
    let vpc2 = create_vpc(&client, project_name, vpc2_name).await;

    let subnet_same_name: VpcSubnet = NexusRequest::objects_post(
        client,
        &format!("/v1/vpc-subnets?project={project_name}&vpc={vpc2_name}"),
        &new_subnet,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(subnet_same_name.identity.name, subnet2_name);
    assert_eq!(
        subnet_same_name.identity.description,
        "it's also below the net"
    );
    assert_eq!(subnet_same_name.vpc_id, vpc2.identity.id);
    assert_eq!(
        subnet_same_name.ipv4_block,
        Ipv4Net("192.168.0.0/16".parse().unwrap())
    );
    assert!(subnet_same_name.ipv6_block.is_unique_local());
}

fn subnets_eq(sn1: &VpcSubnet, sn2: &VpcSubnet) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}
