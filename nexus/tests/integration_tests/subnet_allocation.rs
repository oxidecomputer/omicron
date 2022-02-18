// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Tests that subnet allocation will successfully allocate the entire space of a
 * subnet and error appropriately when the space is exhausted.
 */

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_instance;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    InstanceCpuCount, Ipv4Net, NetworkInterface,
};
use omicron_nexus::external_api::params;
use std::net::IpAddr;

use dropshot::test_util::objects_list_page;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;

use nexus_test_utils::resource_helpers::{create_organization, create_project};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

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
        network_interface: params::InstanceNetworkInterfaceAttachment::Default,
    };

    NexusRequest::new(
        RequestBuilder::new(&client, Method::POST, &url_instances)
            .body(Some(&new_instance))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

#[nexus_test]
async fn test_subnet_allocation(cptestctx: &ControlPlaneTestContext) {
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
    let subnet = "192.168.42.0/29".parse().unwrap();
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
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // The valid addresses for allocation in `subnet` are 192.168.42.5 and
    // 192.168.42.6. The rest are reserved as described in RFD21.
    create_instance(client, organization_name, project_name, "i1").await;
    create_instance(client, organization_name, project_name, "i2").await;

    // This should fail from address exhaustion
    let error =
        create_instance_expect_failure(client, &url_instances, "i3").await;
    assert_eq!(error.message, "No available IP addresses for interface");

    // Verify the subnet lists the two addresses as in use
    let url_ips = format!("{}/ips", url_subnet);
    let mut network_interfaces =
        objects_list_page::<NetworkInterface>(client, &url_ips).await.items;
    assert_eq!(network_interfaces.len(), 2);

    // Sort by IP address to simplify the checks
    network_interfaces.sort_by(|a, b| a.ip.cmp(&b.ip));
    assert_eq!(
        network_interfaces[0].ip,
        "192.168.42.5".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        network_interfaces[1].ip,
        "192.168.42.6".parse::<IpAddr>().unwrap()
    );
}
