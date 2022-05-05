// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that subnet allocation will successfully allocate the entire space of a
//! subnet and error appropriately when the space is exhausted.

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_instance_with_nics;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, InstanceCpuCount, Ipv4Net,
    NetworkInterface,
};
use omicron_nexus::external_api::params;

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;

use nexus_test_utils::resource_helpers::{create_organization, create_project};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

async fn create_instance_expect_failure(
    client: &ClientTestContext,
    url_instances: &String,
    name: &str,
    subnet_name: &str,
) -> HttpErrorResponseBody {
    let network_interfaces =
        params::InstanceNetworkInterfaceAttachment::Create(vec![
            params::NetworkInterfaceCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: String::from("description"),
                },
                vpc_name: "default".parse().unwrap(),
                subnet_name: subnet_name.parse().unwrap(),
                ip: None,
            },
        ]);
    let new_instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name.parse().unwrap(),
            description: "".to_string(),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from_mebibytes_u32(256),
        hostname: name.to_string(),
        user_data: vec![],
        network_interfaces,
        disks: vec![],
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

    // Create a new, small VPC Subnet, so we don't need to issue many requests
    // to test address exhaustion.
    let url_subnets = format!(
        "/organizations/{}/projects/{}/vpcs/default/subnets",
        organization_name, project_name
    );
    let subnet_name = "small";
    let subnet = "192.168.42.0/26".parse().unwrap();
    let subnet_create = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet_name.parse().unwrap(),
            description: String::from("a small subnet"),
        },
        // Use the minimum subnet size
        ipv4_block: Ipv4Net(subnet),
        ipv6_block: None,
    };
    NexusRequest::objects_post(client, &url_subnets, &Some(&subnet_create))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // The valid addresses for allocation in `subnet` are 192.168.42.5 and
    // 192.168.42.6. The rest are reserved as described in RFD21.
    let nic = params::InstanceNetworkInterfaceAttachment::Create(vec![
        params::NetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: "eth0".parse().unwrap(),
                description: String::from("some iface"),
            },
            vpc_name: "default".parse().unwrap(),
            subnet_name: "small".parse().unwrap(),
            ip: None,
        },
    ]);

    // Create a shit-ton of instances, up to the size of the IP subnet.
    //
    // The first 5 addresses in an IP subnet are reserved, as is the broadcast
    // address.
    let n_initial_reserved_addresses = 5;
    let n_final_reserved_addresses = 1;
    let n_reserved_addresses =
        n_initial_reserved_addresses + n_final_reserved_addresses;
    let subnet_size = subnet.size() - n_reserved_addresses;
    for i in 0..subnet_size {
        create_instance_with_nics(
            client,
            organization_name,
            project_name,
            &format!("i{}", i),
            &nic,
        )
        .await;
    }

    // This should fail from address exhaustion
    let error = create_instance_expect_failure(
        client,
        &url_instances,
        "new-inst",
        subnet_name,
    )
    .await;
    assert_eq!(error.message, "No available IP addresses for interface");

    // Verify the subnet lists the two addresses as in use
    let url_ips = format!("{}/{}/network-interfaces", url_subnets, subnet_name);
    let mut network_interfaces =
        objects_list_page_authz::<NetworkInterface>(client, &url_ips)
            .await
            .items;
    assert_eq!(network_interfaces.len(), subnet_size as usize);

    // Sort by IP address to simplify the checks
    network_interfaces.sort_by(|a, b| a.ip.cmp(&b.ip));
    for (iface, addr) in network_interfaces
        .iter()
        .zip(subnet.iter().skip(n_initial_reserved_addresses as usize))
    {
        assert_eq!(
            iface.ip,
            addr,
            "Nexus should provide auto-assigned IP addresses in order within an IP subnet"
        );
    }
}
