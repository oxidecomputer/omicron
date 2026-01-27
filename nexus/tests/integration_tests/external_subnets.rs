// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for External Subnets API endpoints.

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::resource_helpers::create_default_ip_pools;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_subnet_pool;
use nexus_test_utils::resource_helpers::create_subnet_pool_member;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::external_api::views::ExternalSubnet;
use omicron_common::address::IpVersion;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use oxnet::IpNet;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "test-project";
const SUBNET_POOL_NAME: &str = "geo";
const EXTERNAL_SUBNET_NAME: &str = "schist";

// Note: These tests verify that stub endpoints return 500 Internal Server Error.
// The detailed "endpoint is not implemented" message is intentionally not exposed
// to clients for security reasons (internal messages are logged server-side only).

fn external_subnets_url(project: &str) -> String {
    format!("/v1/external-subnets?project={}", project)
}

fn external_subnet_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}?project={}", name, project)
}

fn external_subnet_attach_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}/attach?project={}", name, project)
}

fn external_subnet_detach_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}/detach?project={}", name, project)
}

fn instance_external_subnets_url(instance: &str, project: &str) -> String {
    format!("/v1/instances/{}/external-subnets?project={}", instance, project)
}

#[nexus_test]
async fn external_subnet_basic_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a pool, member, and project first
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let ip_subnet: IpNet = "8.8.8.0/28".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Sanity check, can we CRUD a single subnet.
    let create_params = params::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: params::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: params::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert_eq!(external_subnet.subnet, ip_subnet);
    assert_eq!(external_subnet.identity.name.as_str(), EXTERNAL_SUBNET_NAME);
    let time_created = external_subnet.identity.time_created;
    assert_eq!(time_created, external_subnet.identity.time_modified);

    // Same thing if we read it.
    let via_read = NexusRequest::object_get(
        client,
        &external_subnet_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert_eq!(external_subnet, via_read);

    // Or list it.
    let via_list =
        NexusRequest::object_get(client, &external_subnets_url(PROJECT_NAME))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<ExternalSubnet>>()
            .await
            .items;
    assert_eq!(via_list.len(), 1);
    assert_eq!(via_list[0], via_read);

    // Update the metadata
    let new_name = "quartzite".parse::<Name>().unwrap();
    let updates = params::ExternalSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(new_name.clone()),
            description: None,
        },
    };
    let updated = NexusRequest::object_put(
        client,
        &external_subnet_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
        Some(&updates),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert_eq!(updated.identity.name, new_name);
    assert_eq!(
        updated.identity.time_created,
        external_subnet.identity.time_created
    );
    assert!(
        updated.identity.time_modified > external_subnet.identity.time_modified
    );

    // Now delete.
    NexusRequest::object_delete(
        client,
        &external_subnet_url(new_name.as_str(), PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // And it should really be gone.
    let via_list =
        NexusRequest::object_get(client, &external_subnets_url(PROJECT_NAME))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<ExternalSubnet>>()
            .await
            .items;
    assert!(via_list.is_empty());
}

#[nexus_test]
async fn external_subnet_pagination(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a pool, member, and project first
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Create several subnets.
    let n_subnets = 10;
    let mut subnets = Vec::with_capacity(n_subnets);
    for i in 0..n_subnets {
        let create_params = params::ExternalSubnetCreate {
            identity: IdentityMetadataCreateParams {
                name: format!("{EXTERNAL_SUBNET_NAME}-{i}").parse().unwrap(),
                description: String::from("A test external subnet"),
            },
            allocator: params::ExternalSubnetAllocator::Auto {
                prefix_len: 28,
                pool_selector: params::PoolSelector::Explicit {
                    pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
                },
            },
        };
        let external_subnet = NexusRequest::objects_post(
            client,
            &external_subnets_url(PROJECT_NAME),
            &create_params,
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ExternalSubnet>()
        .await;
        assert!(member_subnet.is_supernet_of(&external_subnet.subnet));
        subnets.push(external_subnet);
    }

    // We're using name url, so sort everything by that.
    subnets.sort_by(|a, b| a.identity.name.cmp(&b.identity.name));

    // List them in several pages, and ensure we get the same thing.
    let n_pages = 2;
    let page_size = n_subnets / n_pages;
    let mut as_pages: Vec<ExternalSubnet> = Vec::with_capacity(n_subnets);
    let mut page_token = None;
    for _ in 0..n_pages {
        let page_url = if let Some(token) = &page_token {
            format!(
                "{}&limit={}&page_token={}",
                external_subnets_url(PROJECT_NAME),
                page_size,
                token,
            )
        } else {
            format!(
                "{}&limit={}",
                external_subnets_url(PROJECT_NAME),
                page_size
            )
        };
        let ResultsPage { mut items, next_page } =
            objects_list_page_authz(client, &page_url).await;
        assert_eq!(items.len(), page_size);
        as_pages.append(&mut items);
        page_token = next_page;
    }

    // We should get the same thing, in the same order.
    assert_eq!(subnets, as_pages);

    // And there should be nothing left
    let page_url = format!(
        "{}&limit={}&page_token={}",
        external_subnets_url(PROJECT_NAME),
        page_size,
        page_token.unwrap(),
    );
    let ResultsPage { items, next_page } =
        objects_list_page_authz::<ExternalSubnet>(client, &page_url).await;
    assert!(items.is_empty());
    assert!(next_page.is_none());
}

#[nexus_test]
async fn test_external_subnet_attach_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    let attach_params = params::ExternalSubnetAttach {
        instance: "test-instance".parse().unwrap(),
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &external_subnet_attach_url("test-subnet", PROJECT_NAME),
        &attach_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_detach_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &external_subnet_detach_url("test-subnet", PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

const INSTANCE_NAME: &str = "test-instance";

#[nexus_test]
async fn test_instance_external_subnet_list_empty(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create default IP pools, project, and instance
    create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;
    let _ = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

    // List external subnets for the instance - should return empty list
    let subnets = objects_list_page_authz::<views::ExternalSubnet>(
        client,
        &instance_external_subnets_url(INSTANCE_NAME, PROJECT_NAME),
    )
    .await;
    assert!(subnets.items.is_empty());
}
