// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::assert_subnet_pool_utilization;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::create_subnet_pool;
use nexus_test_utils::resource_helpers::create_subnet_pool_member;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::link_subnet_pool;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_delete_error;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_get_error;
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::object_put_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
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
    let pool_url = format!("{}/{}", SUBNET_POOLS_URL, SUBNET_POOL_NAME);
    let as_view: SubnetPool = object_get(client, &pool_url).await;
    assert_eq!(as_view, pool);

    // Or if we list it.
    let listed =
        objects_list_page_authz::<SubnetPool>(client, SUBNET_POOLS_URL).await;
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
    let new_pool: SubnetPool = object_put(client, &pool_url, &updates).await;
    assert_eq!(new_pool.identity.name, new_name);
    assert_eq!(new_pool.identity.description, new_description);
    assert_eq!(new_pool.identity.time_created, time_created);
    assert!(new_pool.identity.time_modified > time_modified);

    // Delete it, and we can't look it up anymore.
    let new_url = format!("{}/{}", SUBNET_POOLS_URL, new_name);
    object_delete(client, &new_url).await;
    object_get_error(client, &new_url, StatusCode::NOT_FOUND).await;
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
    let listed =
        objects_list_page_authz::<SubnetPool>(client, SUBNET_POOLS_URL).await;
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

    let list =
        objects_list_page_authz::<SubnetPoolMember>(client, &url).await.items;
    assert_eq!(list, members);

    // Removing something not in the pool fails
    let to_remove =
        SubnetPoolMemberRemove { subnet: "2002:db8::/48".parse().unwrap() };
    let remove_url = format!("{}/remove", url);
    object_create_error(
        client,
        &remove_url,
        &to_remove,
        StatusCode::BAD_REQUEST,
    )
    .await;

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
    let new_list =
        objects_list_page_authz::<SubnetPoolMember>(client, &url).await.items;
    assert_eq!(new_list.len(), list.len() - 1);
    assert!(!new_list.iter().any(|member| member.subnet == to_remove.subnet));

    // Creating an overlapping member fails.
    let to_add = SubnetPoolMemberAdd {
        subnet: "2001:db8:0:0:1::/64".parse().unwrap(),
        min_prefix_length: None,
        max_prefix_length: None,
    };
    let add_url = format!("{}/add", url);
    object_create_error(client, &add_url, &to_add, StatusCode::BAD_REQUEST)
        .await;

    // Cannot delete the pool, because it still has members.
    let pool_url = format!("{}/{}", SUBNET_POOLS_URL, SUBNET_POOL_NAME);
    object_delete_error(client, &pool_url, StatusCode::BAD_REQUEST).await;
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
    object_create_error(client, &url, &params, StatusCode::BAD_REQUEST).await;
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
    let linked =
        objects_list_page_authz::<SubnetPoolSiloLink>(client, &url).await.items;
    assert!(linked.is_empty());

    // Link the first few silos.
    let n_to_link = 10;
    for silo in silos.iter().take(n_to_link) {
        link_subnet_pool(client, SUBNET_POOL_NAME, &silo.identity.id, false)
            .await;
    }

    // Now we should list all and only those in the list.
    let linked =
        objects_list_page_authz::<SubnetPoolSiloLink>(client, &url).await.items;
    let linked_silo_ids: BTreeSet<_> =
        silos.iter().take(n_to_link).map(|s| s.identity.id).collect();
    assert_eq!(linked.len(), n_to_link);
    assert_eq!(
        linked.iter().map(|link| link.silo_id).collect::<BTreeSet<_>>(),
        linked_silo_ids,
    );
    assert!(linked.iter().all(|link| !link.is_default));

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
    let unlink_url = format!(
        "{}/{}/silos/{}",
        SUBNET_POOLS_URL, SUBNET_POOL_NAME, linked[0].silo_id
    );
    object_delete(client, &unlink_url).await;

    let new_linked =
        objects_list_page_authz::<SubnetPoolSiloLink>(client, &url).await.items;
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
    let linked =
        objects_list_page_authz::<SiloSubnetPool>(client, &url).await.items;
    assert!(linked.is_empty());

    // Link the first few pools.
    let n_to_link = 10;
    for pool in pools.iter().take(n_to_link) {
        link_subnet_pool(
            client,
            pool.name().as_str(),
            &silo.identity.id,
            false,
        )
        .await;
    }

    // Now we should list all and only those in the list.
    let linked =
        objects_list_page_authz::<SiloSubnetPool>(client, &url).await.items;
    let linked_pool_ids: BTreeSet<_> =
        pools.iter().take(n_to_link).map(|p| p.identity.id).collect();
    assert_eq!(linked.len(), n_to_link);
    assert_eq!(
        linked.iter().map(|pool| pool.identity.id).collect::<BTreeSet<_>>(),
        linked_pool_ids,
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
    let unlink_url =
        format!("{}/{}/silos/{}", SUBNET_POOLS_URL, pools[0].name(), silo.id());
    object_delete(client, &unlink_url).await;

    let new_linked =
        objects_list_page_authz::<SiloSubnetPool>(client, &url).await.items;
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
    let pool_id = pool.identity.id;

    // Link as non-default, then promote.
    link_subnet_pool(client, SUBNET_POOL_NAME, &DEFAULT_SILO_ID, false).await;
    assert_silos_for_pool(
        client,
        SUBNET_POOL_NAME,
        &[(DEFAULT_SILO_ID, false)],
    )
    .await;

    // Promote to default.
    let update = subnet_pool::SubnetPoolSiloUpdate { is_default: true };
    let link_url = format!(
        "{}/{}/silos/{}",
        SUBNET_POOLS_URL, SUBNET_POOL_NAME, DEFAULT_SILO_ID,
    );
    let link: SubnetPoolSiloLink = object_put(client, &link_url, &update).await;
    assert_eq!(link.subnet_pool_id, pool_id);
    assert_eq!(link.silo_id, DEFAULT_SILO_ID);
    assert!(link.is_default);
    assert_silos_for_pool(client, SUBNET_POOL_NAME, &[(DEFAULT_SILO_ID, true)])
        .await;

    // Link to another silo as default.
    let new_silo =
        create_silo(client, "new-guy", false, SiloIdentityMode::LocalOnly)
            .await;
    let new_silo_id = new_silo.identity.id;
    link_subnet_pool(client, SUBNET_POOL_NAME, &new_silo_id, true).await;
    assert_silos_for_pool(
        client,
        SUBNET_POOL_NAME,
        &[(DEFAULT_SILO_ID, true), (new_silo_id, true)],
    )
    .await;

    // Link a second pool to new_silo as non-default.
    let new_pool =
        create_subnet_pool(client, "new-pool-guy", IpVersion::V6).await;
    link_subnet_pool(client, "new-pool-guy", &new_silo_id, false).await;
    assert_silos_for_pool(client, "new-pool-guy", &[(new_silo_id, false)])
        .await;
    assert_pools_for_silo(
        client,
        "new-guy",
        &[(SUBNET_POOL_NAME, true), ("new-pool-guy", false)],
    )
    .await;

    // Cannot make the second pool the default, since there is already one
    // for this IP version.
    let params = subnet_pool::SubnetPoolSiloUpdate { is_default: true };
    let new_pool_link_url = format!(
        "{}/{}/silos/{}",
        SUBNET_POOLS_URL, new_pool.identity.id, new_silo_id,
    );
    object_put_error(
        client,
        &new_pool_link_url,
        &params,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // But if we unlink the first pool from this silo, we can promote the second.
    let first_pool_link_url = format!(
        "{}/{}/silos/{}",
        SUBNET_POOLS_URL, SUBNET_POOL_NAME, new_silo_id,
    );
    object_delete(client, &first_pool_link_url).await;
    let link: SubnetPoolSiloLink =
        object_put(client, &new_pool_link_url, &params).await;
    assert_eq!(link.subnet_pool_id, new_pool.identity.id);
    assert_eq!(link.silo_id, new_silo_id);
    assert!(link.is_default);
    assert_silos_for_pool(client, "new-pool-guy", &[(new_silo_id, true)]).await;
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
    let url = format!(
        "{}/{}/silos/{}",
        SUBNET_POOLS_URL, SUBNET_POOL_NAME, DEFAULT_SILO_ID
    );
    object_delete_error(client, &url, StatusCode::NOT_FOUND).await;
}

#[nexus_test]
async fn cannot_link_multiple_times(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V6).await;

    // Link it to the default silo.
    link_subnet_pool(client, SUBNET_POOL_NAME, &DEFAULT_SILO_ID, false).await;

    // Doing that again should fail.
    let link_params = subnet_pool::SubnetPoolLinkSilo {
        silo: omicron_common::api::external::NameOrId::Id(DEFAULT_SILO_ID),
        is_default: false,
    };
    let url = format!("{}/{}/silos", SUBNET_POOLS_URL, SUBNET_POOL_NAME);
    object_create_error(client, &url, &link_params, StatusCode::CONFLICT).await;
}

#[nexus_test]
async fn test_ipv4_subnet_pool_utilization(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;

    assert_subnet_pool_utilization(client, SUBNET_POOL_NAME, 0.0, 0.0).await;

    // Add a /24 member (256 addresses).
    let member_subnet: oxnet::IpNet = "10.0.0.0/24".parse().unwrap();
    create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet).await;

    assert_subnet_pool_utilization(client, SUBNET_POOL_NAME, 0.0, 256.0).await;
}

/// Assert that the silo links for a pool match the expected set of
/// (silo_id, is_default) pairs.
async fn assert_silos_for_pool(
    client: &dropshot::test_util::ClientTestContext,
    pool_name: &str,
    expected: &[(uuid::Uuid, bool)],
) {
    let url = format!("{}/{}/silos", SUBNET_POOLS_URL, pool_name);
    let links =
        objects_list_page_authz::<SubnetPoolSiloLink>(client, &url).await.items;
    assert_eq!(
        links.len(),
        expected.len(),
        "pool {pool_name}: expected {} links, got {}",
        expected.len(),
        links.len(),
    );
    for &(silo_id, is_default) in expected {
        let link =
            links.iter().find(|l| l.silo_id == silo_id).unwrap_or_else(|| {
                panic!("pool {pool_name}: no link for silo {silo_id}")
            });
        assert_eq!(
            link.is_default, is_default,
            "pool {pool_name}, silo {silo_id}: expected is_default={is_default}"
        );
    }
}

/// Assert that the subnet pools linked to a silo match the expected set of
/// (pool_name, is_default) pairs.
async fn assert_pools_for_silo(
    client: &dropshot::test_util::ClientTestContext,
    silo_name: &str,
    expected: &[(&str, bool)],
) {
    let url = format!("{}/{}/subnet-pools", SILO_URL, silo_name);
    let pools =
        objects_list_page_authz::<SiloSubnetPool>(client, &url).await.items;
    assert_eq!(
        pools.len(),
        expected.len(),
        "silo {silo_name}: expected {} pools, got {}",
        expected.len(),
        pools.len(),
    );
    for &(pool_name, is_default) in expected {
        let pool = pools
            .iter()
            .find(|p| p.identity.name.as_str() == pool_name)
            .unwrap_or_else(|| {
                panic!("silo {silo_name}: no link for pool {pool_name}")
            });
        assert_eq!(
            pool.is_default, is_default,
            "silo {silo_name}, pool {pool_name}: expected is_default={is_default}"
        );
    }
}

#[nexus_test]
async fn test_ipv6_subnet_pool_utilization(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V6).await;

    assert_subnet_pool_utilization(client, SUBNET_POOL_NAME, 0.0, 0.0).await;

    // Add a /48 member (2^80 addresses).
    let member_subnet: oxnet::IpNet = "2001:db8:1::/48".parse().unwrap();
    create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet).await;

    let capacity = (1u128 << 80) as f64;
    assert_subnet_pool_utilization(client, SUBNET_POOL_NAME, 0.0, capacity)
        .await;
}
