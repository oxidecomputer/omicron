// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for Subnet Pools API stubs
//!
//! These tests verify that the stub endpoints return appropriate
//! "not implemented" errors. Once the full implementation is complete,
//! these tests should be replaced with proper CRUD tests.
//!
//! TODO(#9453): Replace stub tests with full implementation tests.

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_subnet_pool;
use nexus_test_utils::resource_helpers::create_subnet_pool_member;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::SubnetPoolMemberAdd;
use nexus_types::external_api::params::SubnetPoolMemberRemove;
use nexus_types::external_api::params::SubnetPoolUpdate;
use nexus_types::external_api::views::IpVersion;
use nexus_types::external_api::views::SubnetPool;
use nexus_types::external_api::views::SubnetPoolMember;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Name;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const SUBNET_POOLS_URL: &str = "/v1/system/subnet-pools";
const SUBNET_POOL_NAME: &str = "schist";

// Note: These tests verify that stub endpoints return 500 Internal Server Error.
// The detailed "endpoint is not implemented" message is intentionally not exposed
// to clients for security reasons (internal messages are logged server-side only).

#[nexus_test]
async fn basic_subnet_pool_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
    assert_eq!(pool.identity.name.as_str(), SUBNET_POOL_NAME);
    assert_eq!(pool.ip_version, IpVersion::V4);
    let time_created = pool.identity.time_created;
    let time_modified = pool.identity.time_modified;
    assert_eq!(time_created, time_modified);

    // Get the same object if we view it directly.
    let as_view = NexusRequest::object_get(
        client,
        format!("{}/{}", SUBNET_POOLS_URL, SUBNET_POOL_NAME).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request")
    .parsed_body::<SubnetPool>()
    .expect("a subnet pool");
    assert_eq!(as_view, pool);

    // Or if we list it.
    let listed = NexusRequest::object_get(client, SUBNET_POOLS_URL)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body::<ResultsPage<SubnetPool>>()
        .expect("failed to parse list of subnet pools");
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0], pool);

    // Update it, and ensure the updates stick.
    let new_name = "granite".parse::<Name>().unwrap();
    let new_description = String::from("an updated pool");
    let updates = SubnetPoolUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(new_name.clone()),
            description: Some(new_description.clone()),
        },
    };
    let new_pool = NexusRequest::object_put(
        client,
        format!("{}/{}", SUBNET_POOLS_URL, SUBNET_POOL_NAME).as_str(),
        Some(&updates),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request")
    .parsed_body::<SubnetPool>()
    .expect("updated subnet pool");
    assert_eq!(new_pool.identity.name, new_name);
    assert_eq!(new_pool.identity.description, new_description);
    assert_eq!(new_pool.identity.time_created, time_created);
    assert!(new_pool.identity.time_modified > time_modified);

    // Delete it, and we can't look it up anymore.
    NexusRequest::object_delete(
        client,
        format!("{}/{}", SUBNET_POOLS_URL, new_name).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        format!("{}/{}", SUBNET_POOLS_URL, new_name).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn can_list_subnet_pools(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    const N_POOLS: usize = 100;
    let mut pools = Vec::with_capacity(N_POOLS);
    for i in 0..N_POOLS {
        let name = format!("poolio-{i}");
        let pool =
            create_subnet_pool(client, name.as_str(), IpVersion::V4).await;
        pools.push(pool);
    }
    pools.sort_by(|a, b| a.identity.name.cmp(&b.identity.name));
    let listed = NexusRequest::object_get(client, SUBNET_POOLS_URL)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body::<ResultsPage<SubnetPool>>()
        .expect("failed to parse list of subnet pools");
    assert_eq!(pools, listed.items);
}

#[nexus_test]
async fn basic_subnet_pool_member_crd(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let url = format!("{}/{}/members", SUBNET_POOLS_URL, SUBNET_POOL_NAME);
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V6).await;
    let n_members = 100;
    let mut members = Vec::with_capacity(n_members);
    for i in 0..n_members {
        let subnet = format!("2001:db8:{i:x}::/48").parse().unwrap();
        let member =
            create_subnet_pool_member(client, SUBNET_POOL_NAME, subnet).await;
        assert_eq!(member.subnet, subnet);
        members.push(member);
    }

    let list = NexusRequest::object_get(client, url.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body::<ResultsPage<SubnetPoolMember>>()
        .expect("a list of subnet pool members")
        .items;
    assert_eq!(list, members);

    // Removing something not in the pool fails
    let to_remove =
        SubnetPoolMemberRemove { subnet: "2002:db8::/48".parse().unwrap() };
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        format!("{}/remove", url).as_str(),
        &to_remove,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Now delete one really in the pool.
    //
    // Use builder directly, because although we're issuing a POST, we expect to
    // get a `No Content` response back.
    let to_remove =
        SubnetPoolMemberRemove { subnet: "2001:db8::/48".parse().unwrap() };
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            format!("{}/remove", url).as_str(),
        )
        .body(Some(&to_remove))
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // List them and ensure it's really gone.
    let new_list = NexusRequest::object_get(client, url.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body::<ResultsPage<SubnetPoolMember>>()
        .expect("a list of subnet pool members")
        .items;
    assert_eq!(new_list.len(), list.len() - 1);
    assert!(!new_list.iter().any(|member| member.subnet == to_remove.subnet));

    // Creating an overlapping member fails.
    let to_add = SubnetPoolMemberAdd {
        subnet: "2001:db8:0:0:1::/64".parse().unwrap(),
        min_prefix_length: None,
        max_prefix_length: None,
    };
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        format!("{}/add", url).as_str(),
        &to_add,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Cannot delete the pool, because it still has members.
    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        format!("{}/{}", SUBNET_POOLS_URL, SUBNET_POOL_NAME).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn cannot_add_pool_member_of_different_ip_version(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/{}/members/add", SUBNET_POOLS_URL, SUBNET_POOL_NAME);
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
    let params = SubnetPoolMemberAdd {
        subnet: "2001:db8::/48".parse().unwrap(),
        min_prefix_length: None,
        max_prefix_length: None,
    };
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        url.as_str(),
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_silo_list_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/silos", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_utilization_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/utilization", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_silo_link_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/silos", SUBNET_POOLS_URL);

    let link_params = params::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Name(
            "test-silo".parse().unwrap(),
        ),
        is_default: false,
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &url,
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_silo_update_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/silos/test-silo", SUBNET_POOLS_URL);

    let update_params = params::SubnetPoolSiloUpdate { is_default: true };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::PUT,
        &url,
        &update_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_silo_unlink_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/silos/test-silo", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::DELETE,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}
