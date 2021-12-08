// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Tests that subnet allocation will successfully allocate the entire space of a
 * subnet and error appropriately when the space is exhausted.
 */

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    Instance, InstanceCpuCount, Ipv4Net, NetworkInterface,
};
use omicron_nexus::external_api::params;
use std::net::IpAddr;

use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;

pub mod common;
use common::resource_helpers::{create_organization, create_project};
use common::test_setup;

async fn create_instance(
    client: &ClientTestContext,
    url_instances: &String,
    name: &str,
) {
    let new_instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name.parse().unwrap(),
            description: "".to_string(),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from_mebibytes_u32(256),
        hostname: name.to_string(),
    };
    objects_post::<_, Instance>(&client, url_instances, new_instance.clone())
        .await;
}

async fn create_instance_expect_failure(
    client: &ClientTestContext,
    url_instances: &String,
    name: &str,
) -> HttpErrorResponseBody {
    let new_instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name.parse().unwrap(),
            description: "".to_string(),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from_mebibytes_u32(256),
        hostname: name.to_string(),
    };
    client
        .make_request_error_body(
            Method::POST,
            &url_instances,
            new_instance,
            StatusCode::NOT_FOUND,
        )
        .await
}

#[tokio::test]
async fn test_subnet_allocation() {
    let cptestctx = test_setup("test_subnet_allocation").await;
    let client = &cptestctx.external_client;

    let organization_name = "test-org";
    let project_name = "springfield-squidport";

    // Create a project that we'll use for testing.
    create_organization(&client, organization_name).await;
    create_project(&client, organization_name, project_name).await;
    let url_instances = format!(
        "/organizations/{}/projects/{}/instances",
        organization_name, project_name
    );

    // Modify the default VPC to have a very small subnet so we don't need to
    // issue many requests
    let url_subnet = format!(
        "/organizations/{}/projects/{}/vpcs/default/subnets/default",
        organization_name, project_name
    );
    let subnet = "192.168.42.0/30".parse().unwrap();
    let subnet_update = params::VpcSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("default".parse().unwrap()),
            description: None,
        },
        ipv4_block: Some(Ipv4Net(subnet)),
        ipv6_block: None,
    };
    client
        .make_request(
            Method::PUT,
            &url_subnet,
            Some(subnet_update),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // The valid addresses for allocation in `subnet` are 192.168.42.1 and
    // 192.168.42.2. The rest are reserved as the network address and broadcast
    // address.
    create_instance(client, &url_instances, "i1").await;
    create_instance(client, &url_instances, "i2").await;

    // This should fail from address exhaustion
    let error =
        create_instance_expect_failure(client, &url_instances, "i3").await;
    assert_eq!(error.message, "no available IP addresses");

    // Verify the subnet lists the two addresses as in use
    let url_ips = format!("{}/ips", url_subnet);
    let network_interfaces =
        objects_list_page::<NetworkInterface>(client, &url_ips).await.items;
    assert_eq!(network_interfaces.len(), 2);
    assert_eq!(
        network_interfaces[0].ip,
        "192.168.42.1".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        network_interfaces[1].ip,
        "192.168.42.2".parse::<IpAddr>().unwrap()
    );

    cptestctx.teardown().await;
}
