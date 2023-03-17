// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that subnet allocation will successfully allocate the entire space of a
//! subnet and error appropriately when the space is exhausted.

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use ipnetwork::Ipv4Network;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::populate_ip_pool;
use nexus_test_utils::resource_helpers::{create_organization, create_project};
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, InstanceCpuCount, Ipv4Net,
    NetworkInterface,
};
use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use omicron_nexus::external_api::params;
use std::net::Ipv4Addr;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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
                    // We're using the name of the instance purposefully, to
                    // avoid any naming conflicts on the interface.
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
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: name.to_string(),
        user_data: vec![],
        network_interfaces,
        external_ips: vec![],
        disks: vec![],
        start: true,
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
    populate_ip_pool(&client, "default", None).await;
    create_organization(&client, organization_name).await;
    create_project(&client, organization_name, project_name).await;
    let url_instances = format!(
        "/v1/instances?organization={}&project={}",
        organization_name, project_name
    );

    // Create a new, small VPC Subnet, so we don't need to issue many requests
    // to test address exhaustion.
    let subnet_size =
        cptestctx.server.apictx().nexus.tunables().max_vpc_ipv4_subnet_prefix;
    let vpc_selector = format!(
        "organization={}&project={}&vpc=default",
        organization_name, project_name
    );
    let subnets_url = format!("/v1/vpc-subnets?{}", vpc_selector);
    let subnet_name = "small";
    let network_address = Ipv4Addr::new(192, 168, 42, 0);
    let subnet = Ipv4Network::new(network_address, subnet_size)
        .expect("Invalid IPv4 network");
    let subnet_create = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: subnet_name.parse().unwrap(),
            description: String::from("a small subnet"),
        },
        // Use the minimum subnet size
        ipv4_block: Ipv4Net(subnet),
        ipv6_block: None,
    };
    NexusRequest::objects_post(client, &subnets_url, &Some(&subnet_create))
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

    // Create enough instances to fill the subnet. There are subnet.size() total
    // addresses, 6 of which are reserved.
    let n_final_reserved_addresses = 1;
    let n_reserved_addresses =
        NUM_INITIAL_RESERVED_IP_ADDRESSES + n_final_reserved_addresses;
    let subnet_size = subnet.size() as usize - n_reserved_addresses;
    for i in 0..subnet_size {
        create_instance_with(
            client,
            organization_name,
            project_name,
            &format!("i{}", i),
            &nic,
            // Disks=
            vec![],
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
    let url_ips = format!(
        "/v1/vpc-subnets/{}/network-interfaces?{}",
        subnet_name, vpc_selector
    );
    let mut network_interfaces =
        objects_list_page_authz::<NetworkInterface>(client, &url_ips)
            .await
            .items;
    assert_eq!(network_interfaces.len(), subnet_size);

    // Sort by IP address to simplify the checks
    network_interfaces.sort_by(|a, b| a.ip.cmp(&b.ip));
    for (iface, addr) in network_interfaces
        .iter()
        .zip(subnet.iter().skip(NUM_INITIAL_RESERVED_IP_ADDRESSES))
    {
        assert_eq!(
            iface.ip,
            addr,
            "Nexus should provide auto-assigned IP addresses in order within an IP subnet"
        );
    }
}
