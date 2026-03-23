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
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::create_subnet_pool;
use nexus_test_utils::resource_helpers::create_subnet_pool_member;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::link_subnet_pool;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::policy::SiloRole;
use nexus_types::external_api::silo::SiloIdentityMode;
use nexus_types::external_api::subnet_pool;
use nexus_types::external_api::subnet_pool::SiloSubnetPool;
use nexus_types::external_api::subnet_pool::SubnetPool;
use nexus_types::external_api::subnet_pool::SubnetPoolMember;
use nexus_types::external_api::subnet_pool::SubnetPoolMemberAdd;
use nexus_types::external_api::subnet_pool::SubnetPoolMemberRemove;
use nexus_types::external_api::subnet_pool::SubnetPoolSiloLink;
use nexus_types::external_api::subnet_pool::SubnetPoolUpdate;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::address::IpVersion;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::SimpleIdentityOrName as _;
use std::collections::BTreeSet;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const SUBNET_POOLS_URL: &str = "/v1/system/subnet-pools";
const SUBNET_POOL_NAME: &str = "schist";
const SILO_URL: &str = "/v1/system/silos";
const SILO_NAME: &str = "marble";

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
async fn test_subnet_pool_silo_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let url = format!("{}/{}/silos", SUBNET_POOLS_URL, SUBNET_POOL_NAME);

    // Create a pool and a bunch of silos.
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
    let n_silos = 100;
    let mut silos = Vec::with_capacity(n_silos);
    for i in 0..n_silos {
        let silo = create_silo(
            client,
            &format!("test-silo-{i}"),
            true,
            SiloIdentityMode::LocalOnly,
        )
        .await;
        silos.push(silo)
    }

    // There should be none linked to the pool at first.
    let linked = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SubnetPoolSiloLink>>()
        .await
        .items;
    assert!(linked.is_empty());

    // Link the first few silos.
    let n_to_link = 10;
    let mut linked_silos = Vec::with_capacity(n_to_link);
    for silo in silos.iter().take(n_to_link) {
        let link_params = subnet_pool::SubnetPoolLinkSilo {
            silo: omicron_common::api::external::NameOrId::Id(silo.identity.id),
            is_default: false,
        };
        let link = NexusRequest::objects_post(client, &url, &link_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
            .await;
        linked_silos.push(link);
    }

    // Now we should list all and only those in the list.
    let linked = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SubnetPoolSiloLink>>()
        .await
        .items;
    assert_eq!(linked.len(), linked_silos.len());
    assert_eq!(
        linked.iter().map(|link| link.silo_id).collect::<BTreeSet<_>>(),
        linked_silos.iter().map(|link| link.silo_id).collect::<BTreeSet<_>>(),
    );

    // And fetching the list in two pages works too.
    let mut as_pages = Vec::with_capacity(n_to_link);
    let n_pages = 2;
    let page_size = n_to_link / n_pages;
    let mut page_token = None;
    for _ in 0..n_pages {
        let page_url = if let Some(token) = page_token {
            format!("{url}?limit={page_size}&page_token={token}")
        } else {
            format!("{url}?limit={page_size}")
        };
        let ResultsPage { mut items, next_page } =
            NexusRequest::object_get(client, &page_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute_and_parse_unwrap::<ResultsPage<SubnetPoolSiloLink>>()
                .await;
        assert_eq!(items.len(), page_size);
        as_pages.append(&mut items);
        page_token = next_page;
    }

    // After fetching all pages, we should have the same set in the same order
    // as fetching the full set in one page.
    assert_eq!(as_pages, linked);

    // And we should not fetch any more.
    let page_url =
        format!("{url}?limit={page_size}&page_token={}", page_token.unwrap());
    let should_be_empty = NexusRequest::object_get(client, &page_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SubnetPoolSiloLink>>()
        .await
        .items;
    assert!(should_be_empty.is_empty());

    // And if we unlink one, it no longer shows up.
    NexusRequest::object_delete(
        client,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL, SUBNET_POOL_NAME, linked[0].silo_id
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    let new_linked = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SubnetPoolSiloLink>>()
        .await
        .items;
    assert_eq!(new_linked.len(), linked.len() - 1);
    assert!(!new_linked.iter().any(|new| new.silo_id == linked[0].silo_id))
}

#[nexus_test]
async fn test_silo_subnet_pool_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let url = format!("{}/{}/subnet-pools", SILO_URL, SILO_NAME);

    // Create a silo and a bunch of pools.
    let silo =
        create_silo(client, SILO_NAME, false, SiloIdentityMode::LocalOnly)
            .await;
    let n_pools = 100;
    let mut pools = Vec::with_capacity(n_pools);
    for i in 0..n_pools {
        let pool = create_subnet_pool(
            client,
            &format!("test-pool-{i}"),
            IpVersion::V6,
        )
        .await;
        pools.push(pool)
    }

    // There should be none linked to the silo at first.
    let linked = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SiloSubnetPool>>()
        .await
        .items;
    assert!(linked.is_empty());

    // Link the first few pools.
    let n_to_link = 10;
    let mut linked_pools = Vec::with_capacity(n_to_link);
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(silo.identity.id),
        is_default: false,
    };
    for pool in pools.iter().take(n_to_link) {
        let pool_url = format!("{}/{}/silos", SUBNET_POOLS_URL, pool.name());
        let link = NexusRequest::objects_post(client, &pool_url, &link_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
            .await;
        linked_pools.push(link);
    }

    // Now we should list all and only those in the list.
    let linked = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SiloSubnetPool>>()
        .await
        .items;
    assert_eq!(linked.len(), linked_pools.len());
    assert_eq!(
        linked.iter().map(|pool| pool.identity.id).collect::<BTreeSet<_>>(),
        linked_pools
            .iter()
            .map(|link| link.subnet_pool_id)
            .collect::<BTreeSet<_>>(),
    );
    assert!(linked.iter().all(|pool| pool.ip_version == IpVersion::V6));
    assert!(linked.iter().all(|pool| !pool.is_default));

    // And fetching the list in two pages works too.
    let mut as_pages = Vec::with_capacity(n_to_link);
    let n_pages = 2;
    let page_size = n_to_link / n_pages;
    let mut page_token = None;
    for _ in 0..n_pages {
        let page_url = if let Some(token) = page_token {
            format!("{url}?limit={page_size}&page_token={token}")
        } else {
            format!("{url}?limit={page_size}")
        };
        let ResultsPage { mut items, next_page } =
            NexusRequest::object_get(client, &page_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute_and_parse_unwrap::<ResultsPage<SiloSubnetPool>>()
                .await;
        assert_eq!(items.len(), page_size);
        as_pages.append(&mut items);
        page_token = next_page;
    }

    // After fetching all pages, we should have the same set in the same order
    // as fetching the full set in one page.
    assert_eq!(as_pages, linked);

    // And we should not fetch any more.
    let page_url =
        format!("{url}?limit={page_size}&page_token={}", page_token.unwrap());
    let should_be_empty = NexusRequest::object_get(client, &page_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SiloSubnetPool>>()
        .await
        .items;
    assert!(should_be_empty.is_empty());

    // And if we unlink one, it no longer shows up.
    let to_unlink = pools[0].identity.id;
    NexusRequest::object_delete(
        client,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL,
            pools[0].name(),
            silo.id()
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    let new_linked = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<SiloSubnetPool>>()
        .await
        .items;
    assert_eq!(new_linked.len(), linked.len() - 1);
    assert!(!new_linked.iter().any(|pool| pool.identity.id == to_unlink))
}

#[nexus_test]
async fn test_current_silo_subnet_pool_list(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let silo =
        create_silo(client, SILO_NAME, false, SiloIdentityMode::LocalOnly)
            .await;
    let silo_url = format!("{}/{}", SILO_URL, SILO_NAME);

    let default_name = "default-subnet-pool";
    let other_name = "other-subnet-pool";
    let unlinked_name = "unlinked-subnet-pool";

    create_subnet_pool(client, default_name, IpVersion::V6).await;
    create_subnet_pool(client, other_name, IpVersion::V6).await;
    create_subnet_pool(client, unlinked_name, IpVersion::V6).await;

    // Link two of the pools to the silo.
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(silo.identity.id),
        is_default: true,
    };
    let _default_link = NexusRequest::objects_post(
        client,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, default_name),
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;

    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(silo.identity.id),
        is_default: false,
    };
    let _other_link = NexusRequest::objects_post(
        client,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, other_name),
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;

    // Create a silo user and make them a collaborator.
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let list = NexusRequest::object_get(client, "/v1/subnet-pools")
        .authn_as(AuthnMode::SiloUser(user.id))
        .execute_and_parse_unwrap::<ResultsPage<SiloSubnetPool>>()
        .await
        .items;

    assert_eq!(list.len(), 2);
    assert_eq!(list[0].identity.name.to_string(), default_name);
    assert!(list[0].is_default);
    assert_eq!(list[0].ip_version, IpVersion::V6);
    assert_eq!(list[1].identity.name.to_string(), other_name);
    assert!(!list[1].is_default);
    assert_eq!(list[1].ip_version, IpVersion::V6);
}

#[nexus_test]
async fn test_current_silo_subnet_pool_view(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let silo =
        create_silo(client, SILO_NAME, false, SiloIdentityMode::LocalOnly)
            .await;
    let silo_url = format!("{}/{}", SILO_URL, SILO_NAME);

    let default_name = "default-subnet-pool";
    let other_name = "other-subnet-pool";
    let unlinked_name = "unlinked-subnet-pool";

    create_subnet_pool(client, default_name, IpVersion::V6).await;
    create_subnet_pool(client, other_name, IpVersion::V4).await;
    create_subnet_pool(client, unlinked_name, IpVersion::V6).await;

    link_subnet_pool(client, default_name, &silo.identity.id, true).await;
    link_subnet_pool(client, other_name, &silo.identity.id, false).await;

    // Create a silo user and make them a collaborator.
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Fetch a linked default pool by name.
    let url = format!("/v1/subnet-pools/{}", default_name);
    let pool = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::SiloUser(user.id))
        .execute_and_parse_unwrap::<SiloSubnetPool>()
        .await;
    assert_eq!(pool.identity.name.as_str(), default_name);
    assert!(pool.is_default);
    assert_eq!(pool.ip_version, IpVersion::V6);

    // Fetch a linked non-default pool by name.
    let url = format!("/v1/subnet-pools/{}", other_name);
    let pool = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::SiloUser(user.id))
        .execute_and_parse_unwrap::<SiloSubnetPool>()
        .await;
    assert_eq!(pool.identity.name.as_str(), other_name);
    assert!(!pool.is_default);
    assert_eq!(pool.ip_version, IpVersion::V4);

    // Fetching an unlinked pool returns 404.
    let url = format!("/v1/subnet-pools/{}", unlinked_name);
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_subnet_pool_silo_link(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V6).await;
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(DEFAULT_SILO_ID),
        is_default: false,
    };

    // Check we can make a basic link.
    let link = NexusRequest::objects_post(
        client,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, SUBNET_POOL_NAME),
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;
    assert_eq!(link.subnet_pool_id, pool.identity.id);
    assert_eq!(link.silo_id, DEFAULT_SILO_ID);
    assert!(!link.is_default);

    // We can make it the default now.
    let params = subnet_pool::SubnetPoolSiloUpdate { is_default: true };
    let link = NexusRequest::object_put(
        client,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL, SUBNET_POOL_NAME, DEFAULT_SILO_ID,
        ),
        Some(&params),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;
    assert_eq!(link.subnet_pool_id, pool.identity.id);
    assert_eq!(link.silo_id, DEFAULT_SILO_ID);
    assert!(link.is_default);

    // We can link it to another silo.
    let new_silo =
        create_silo(client, "new-guy", false, SiloIdentityMode::LocalOnly)
            .await;
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(new_silo.identity.id),
        is_default: true,
    };
    let link = NexusRequest::objects_post(
        client,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, SUBNET_POOL_NAME),
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;
    assert_eq!(link.subnet_pool_id, pool.identity.id);
    assert_eq!(link.silo_id, new_silo.identity.id);
    assert!(link.is_default);

    // We should be able to link another pool to the same silo.
    let new_pool =
        create_subnet_pool(client, "new-pool-guy", IpVersion::V6).await;
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(new_silo.identity.id),
        is_default: false,
    };
    let link = NexusRequest::objects_post(
        client,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, new_pool.identity.id),
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;
    assert_eq!(link.subnet_pool_id, new_pool.identity.id);
    assert_eq!(link.silo_id, new_silo.identity.id);
    assert!(!link.is_default);

    // But we should not be able to make that the default, since we already have
    // one of this IP version.
    let params = subnet_pool::SubnetPoolSiloUpdate { is_default: true };
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::PUT,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL, new_pool.identity.id, new_silo.identity.id
        ),
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // But if we delete the link between the first pool and this silo, we should
    // now be able to make the second link we made the default.
    NexusRequest::object_delete(
        client,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL, pool.identity.id, new_silo.identity.id
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
    let link = NexusRequest::object_put(
        client,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL, new_pool.identity.id, new_silo.identity.id,
        ),
        Some(&params),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;
    assert_eq!(link.subnet_pool_id, new_pool.identity.id);
    assert_eq!(link.silo_id, new_silo.identity.id);
    assert!(link.is_default);
}

#[nexus_test]
async fn cannot_delete_nonexistent_silo_link(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V6).await;

    // It's not linked to the default silo, so what happens if we try to unlink
    // it?
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &format!(
            "{}/{}/silos/{}",
            SUBNET_POOLS_URL, SUBNET_POOL_NAME, DEFAULT_SILO_ID
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn cannot_link_multiple_times(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V6).await;

    // Now link it to the default silo.
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(DEFAULT_SILO_ID),
        is_default: false,
    };
    let _link = NexusRequest::objects_post(
        client,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, SUBNET_POOL_NAME),
        &link_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SubnetPoolSiloLink>()
    .await;

    // Doing that again should fail.
    let _err = NexusRequest::expect_failure_with_body(
        client,
        StatusCode::CONFLICT,
        Method::POST,
        &format!("{}/{}/silos", SUBNET_POOLS_URL, SUBNET_POOL_NAME),
        &link_params,
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
