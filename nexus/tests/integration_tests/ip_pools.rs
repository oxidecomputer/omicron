// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on IP Pools

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use dropshot::ResultsPage;
use http::method::Method;
use http::StatusCode;
use nexus_db_queries::db::datastore::SERVICE_IP_POOL_NAME;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::link_ip_pool;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_delete_error;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_get_error;
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::object_put_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::IpPoolCreate;
use nexus_types::external_api::params::IpPoolLinkSilo;
use nexus_types::external_api::params::IpPoolSiloUpdate;
use nexus_types::external_api::params::IpPoolUpdate;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::Ipv4Range;
use nexus_types::external_api::shared::Ipv6Range;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::external_api::views::IpPool;
use nexus_types::external_api::views::IpPoolRange;
use nexus_types::external_api::views::IpPoolSiloLink;
use nexus_types::external_api::views::Silo;
use nexus_types::external_api::views::SiloIpPool;
use nexus_types::identity::Resource;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::SimpleIdentity;
use omicron_common::api::external::{IdentityMetadataCreateParams, Name};
use omicron_nexus::TestInterfaces;
use sled_agent_client::TestInterfaces as SledTestInterfaces;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Basic test verifying CRUD behavior on the IP Pool itself.
#[nexus_test]
async fn test_ip_pool_basic_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let ip_pools_url = "/v1/system/ip-pools";
    let pool_name = "p0";
    let description = "an ip pool";
    let ip_pool_url = format!("{}/{}", ip_pools_url, pool_name);
    let ip_pool_ranges_url = format!("{}/ranges", ip_pool_url);
    let ip_pool_add_range_url = format!("{}/add", ip_pool_ranges_url);

    let ip_pools = get_ip_pools(&client).await;
    assert_eq!(ip_pools.len(), 0, "Expected empty list of IP pools");

    // Verify 404 if the pool doesn't exist yet, both for creating or deleting
    let error =
        object_get_error(client, &ip_pool_url, StatusCode::NOT_FOUND).await;
    assert_eq!(
        error.message,
        format!("not found: ip-pool with name \"{}\"", pool_name),
    );

    let error =
        object_delete_error(client, &ip_pool_url, StatusCode::NOT_FOUND).await;
    assert_eq!(
        error.message,
        format!("not found: ip-pool with name \"{}\"", pool_name),
    );

    // Create the pool, verify we can get it back by either listing or fetching
    // directly
    let params = IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(pool_name).parse().unwrap(),
            description: String::from(description),
        },
    };
    let created_pool: IpPool =
        object_create(client, ip_pools_url, &params).await;
    assert_eq!(created_pool.identity.name, pool_name);
    assert_eq!(created_pool.identity.description, description);

    let list = get_ip_pools(client).await;
    assert_eq!(list.len(), 1, "Expected exactly 1 IP pool");
    assert_pools_eq(&created_pool, &list[0]);

    let fetched_pool: IpPool = object_get(client, &ip_pool_url).await;
    assert_pools_eq(&created_pool, &fetched_pool);

    // Verify we get a conflict error if we insert it again
    let error = object_create_error(
        client,
        ip_pools_url,
        &params,
        StatusCode::BAD_REQUEST,
    )
    .await;

    assert_eq!(
        error.message,
        format!("already exists: ip-pool \"{}\"", pool_name)
    );

    // Add a range, verify that we can't delete the Pool
    let range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 5),
        )
        .unwrap(),
    );
    let created_range: IpPoolRange =
        object_create(client, &ip_pool_add_range_url, &range).await;
    assert_eq!(range.first_address(), created_range.range.first_address());
    assert_eq!(range.last_address(), created_range.range.last_address());

    let error: HttpErrorResponseBody =
        object_delete_error(client, &ip_pool_url, StatusCode::BAD_REQUEST)
            .await;
    assert_eq!(
        error.message,
        "IP Pool cannot be deleted while it contains IP ranges",
    );

    // Rename the pool.
    //
    // Ensure we can fetch the pool under the new name, and not the old, and
    // that the modification time has changed.
    let new_pool_name = "p1";
    let new_ip_pool_url = format!("{}/{}", ip_pools_url, new_pool_name);
    let new_ip_pool_rem_range_url =
        format!("{}/ranges/remove", new_ip_pool_url);
    let updates = IpPoolUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(String::from(new_pool_name).parse().unwrap()),
            description: None,
        },
    };
    let modified_pool: IpPool =
        object_put(client, &ip_pool_url, &updates).await;
    assert_eq!(modified_pool.identity.name, new_pool_name);
    assert_eq!(modified_pool.identity.id, created_pool.identity.id);
    assert_eq!(
        modified_pool.identity.description,
        created_pool.identity.description
    );
    assert_eq!(
        modified_pool.identity.time_created,
        created_pool.identity.time_created
    );
    assert!(
        modified_pool.identity.time_modified
            > created_pool.identity.time_modified
    );

    let fetched_modified_pool: IpPool =
        object_get(client, &new_ip_pool_url).await;
    assert_pools_eq(&modified_pool, &fetched_modified_pool);

    let error: HttpErrorResponseBody =
        object_get_error(client, &ip_pool_url, StatusCode::NOT_FOUND).await;
    assert_eq!(
        error.message,
        format!("not found: ip-pool with name \"{}\"", pool_name),
    );

    // Delete the range, then verify we can delete the pool and everything looks
    // gravy.
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &new_ip_pool_rem_range_url)
            .body(Some(&range))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to delete IP range from a pool");
    NexusRequest::object_delete(client, &new_ip_pool_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected to be able to delete an empty IP Pool");
}

async fn get_ip_pools(client: &ClientTestContext) -> Vec<IpPool> {
    NexusRequest::iter_collection_authn::<IpPool>(
        client,
        "/v1/system/ip-pools",
        "",
        None,
    )
    .await
    .expect("Failed to list IP Pools")
    .all_items
}

// this test exists primarily because of a bug in the initial implementation
// where we included a duplicate of each pool in the list response for every
// associated silo
#[nexus_test]
async fn test_ip_pool_list_dedupe(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let ip_pools = get_ip_pools(&client).await;
    assert_eq!(ip_pools.len(), 0);

    let range1 = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 51),
            std::net::Ipv4Addr::new(10, 0, 0, 52),
        )
        .unwrap(),
    );
    let (pool1, ..) = create_ip_pool(client, "pool1", Some(range1)).await;
    let range2 = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 53),
            std::net::Ipv4Addr::new(10, 0, 0, 54),
        )
        .unwrap(),
    );
    let (pool2, ..) = create_ip_pool(client, "pool2", Some(range2)).await;

    let ip_pools = get_ip_pools(&client).await;
    assert_eq!(ip_pools.len(), 2);
    assert_eq!(ip_pools[0].identity.id, pool1.id());
    assert_eq!(ip_pools[1].identity.id, pool2.id());

    // create 3 silos and link
    let silo1 =
        create_silo(&client, "silo1", true, SiloIdentityMode::SamlJit).await;
    link_ip_pool(client, "pool1", &silo1.id(), false).await;
    // linking pool2 here only, just for variety
    link_ip_pool(client, "pool2", &silo1.id(), false).await;

    let silo2 =
        create_silo(&client, "silo2", true, SiloIdentityMode::SamlJit).await;
    link_ip_pool(client, "pool1", &silo2.id(), true).await;

    let silo3 =
        create_silo(&client, "silo3", true, SiloIdentityMode::SamlJit).await;
    link_ip_pool(client, "pool1", &silo3.id(), true).await;

    let ip_pools = get_ip_pools(&client).await;
    assert_eq!(ip_pools.len(), 2);
    assert_eq!(ip_pools[0].identity.id, pool1.id());
    assert_eq!(ip_pools[1].identity.id, pool2.id());

    let silo1_pools = pools_for_silo(client, "silo1").await;
    assert_eq!(silo1_pools.len(), 2);
    assert_eq!(silo1_pools[0].id(), pool1.id());
    assert_eq!(silo1_pools[1].id(), pool2.id());

    let silo2_pools = pools_for_silo(client, "silo2").await;
    assert_eq!(silo2_pools.len(), 1);
    assert_eq!(silo2_pools[0].identity.name, "pool1");

    let silo3_pools = pools_for_silo(client, "silo3").await;
    assert_eq!(silo3_pools.len(), 1);
    assert_eq!(silo3_pools[0].identity.name, "pool1");
}

/// The internal IP pool, defined by its association with the internal silo,
/// cannot be interacted with through the operator API. CRUD operations should
/// all 404 except fetch by name or ID.
#[nexus_test]
async fn test_ip_pool_service_no_cud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let internal_pool_name_url =
        format!("/v1/system/ip-pools/{}", SERVICE_IP_POOL_NAME);

    // we can fetch the service pool by name or ID
    let pool = NexusRequest::object_get(client, &internal_pool_name_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<IpPool>()
        .await;

    let internal_pool_id_url =
        format!("/v1/system/ip-pools/{}", pool.identity.id);
    let pool = NexusRequest::object_get(client, &internal_pool_id_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<IpPool>()
        .await;

    // but it does not come back in the list. there are none in the list
    let pools =
        objects_list_page_authz::<IpPool>(client, "/v1/system/ip-pools").await;
    assert_eq!(pools.items.len(), 0);

    // deletes fail

    let error = object_delete_error(
        client,
        &internal_pool_name_url,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"oxide-service-pool\""
    );

    let not_found_id =
        format!("not found: ip-pool with id \"{}\"", pool.identity.id);
    let error = object_delete_error(
        client,
        &internal_pool_id_url,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.message, not_found_id);

    // Update not allowed
    let put_body = params::IpPoolUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("test".parse().unwrap()),
            description: Some("test".to_string()),
        },
    };
    let error = object_put_error(
        client,
        &internal_pool_id_url,
        &put_body,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.message, not_found_id);

    // linking not allowed

    // let link_body = params::IpPoolLinkSilo {
    //     silo: NameOrId::Name(cptestctx.silo_name.clone()),
    //     is_default: false,
    // };
    // let link_url = format!("{}/silos", internal_pool_id_url);
    // let error = object_create_error(
    //     client,
    //     &link_url,
    //     &link_body,
    //     StatusCode::NOT_FOUND,
    // )
    // .await;
    // assert_eq!(error.message, not_found_id);

    // TODO: link, unlink, add/remove range by name or ID should all fail
}

#[nexus_test]
async fn test_ip_pool_silo_link(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let p0 = create_pool(client, "p0").await;
    let p1 = create_pool(client, "p1").await;

    // there should be no associations
    let assocs_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(assocs_p0.items.len(), 0);

    let silo_name = cptestctx.silo_name.as_str();
    let silo_pools = pools_for_silo(client, silo_name).await;
    assert_eq!(silo_pools.len(), 0);

    // expect 404 on association if the specified silo doesn't exist
    let nonexistent_silo_id = Uuid::new_v4();
    let params = params::IpPoolLinkSilo {
        silo: NameOrId::Id(nonexistent_silo_id),
        is_default: false,
    };

    let error = object_create_error(
        client,
        "/v1/system/ip-pools/p0/silos",
        &params,
        StatusCode::NOT_FOUND,
    )
    .await;
    let not_found =
        format!("not found: silo with id \"{nonexistent_silo_id}\"");
    assert_eq!(error.message, not_found);

    // pools for silo also 404s on nonexistent silo
    let url = format!("/v1/system/silos/{}/ip-pools", nonexistent_silo_id);
    let error = object_get_error(client, &url, StatusCode::NOT_FOUND).await;
    assert_eq!(error.message, not_found);

    // associate by name with silo that exists
    let silo = NameOrId::Name(cptestctx.silo_name.clone());
    let params =
        params::IpPoolLinkSilo { silo: silo.clone(), is_default: false };
    let _: IpPoolSiloLink =
        object_create(client, "/v1/system/ip-pools/p0/silos", &params).await;

    // second attempt to create the same link errors due to conflict
    let error = object_create_error(
        client,
        "/v1/system/ip-pools/p0/silos",
        &params,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.error_code.unwrap(), "ObjectAlreadyExists");

    // get silo ID so we can test association by ID as well
    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo_id = object_get::<Silo>(client, &silo_url).await.identity.id;

    let assocs_p0 = silos_for_pool(client, "p0").await;
    let silo_link = IpPoolSiloLink {
        ip_pool_id: p0.identity.id,
        silo_id,
        is_default: false,
    };
    assert_eq!(assocs_p0.items.len(), 1);
    assert_eq!(assocs_p0.items[0], silo_link);

    let silo_pools = pools_for_silo(client, silo_name).await;
    assert_eq!(silo_pools.len(), 1);
    assert_eq!(silo_pools[0].identity.id, p0.identity.id);
    assert_eq!(silo_pools[0].is_default, false);

    // associate same silo to other pool by ID instead of name
    let link_params = params::IpPoolLinkSilo {
        silo: NameOrId::Id(silo_id),
        is_default: true,
    };
    let url = "/v1/system/ip-pools/p1/silos";
    let _: IpPoolSiloLink = object_create(client, &url, &link_params).await;

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 1);
    assert_eq!(
        silos_p1.items[0],
        IpPoolSiloLink {
            ip_pool_id: p1.identity.id,
            is_default: true,
            silo_id
        }
    );

    let silo_pools = pools_for_silo(client, silo_name).await;
    assert_eq!(silo_pools.len(), 2);
    assert_eq!(silo_pools[0].id(), p0.id());
    assert_eq!(silo_pools[0].is_default, false);
    assert_eq!(silo_pools[1].id(), p1.id());
    assert_eq!(silo_pools[1].is_default, true);

    // creating a third pool and trying to link it as default: true should fail
    create_pool(client, "p2").await;
    let url = "/v1/system/ip-pools/p2/silos";
    let error = object_create_error(
        client,
        &url,
        &link_params,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.error_code.unwrap(), "ObjectAlreadyExists");

    // pool delete fails because it is linked to a silo
    let error = object_delete_error(
        client,
        "/v1/system/ip-pools/p1",
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        error.message,
        "IP Pool cannot be deleted while it is linked to a silo",
    );

    // unlink p1 from silo (doesn't matter that it's a default)
    let url = format!("/v1/system/ip-pools/p1/silos/{}", cptestctx.silo_name);
    object_delete(client, &url).await;

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 0);

    // after unlinking p1, only p0 is left
    let silo_pools = pools_for_silo(client, silo_name).await;
    assert_eq!(silo_pools.len(), 1);
    assert_eq!(silo_pools[0].identity.id, p0.identity.id);
    assert_eq!(silo_pools[0].is_default, false);

    // now we can delete the pool too
    object_delete(client, "/v1/system/ip-pools/p1").await;
}

#[nexus_test]
async fn test_ip_pool_update_default(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_pool(client, "p0").await;
    create_pool(client, "p1").await;

    // there should be no linked silos
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 0);

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 0);

    // put 404s if link doesn't exist yet
    let params = IpPoolSiloUpdate { is_default: true };
    let p0_silo_url =
        format!("/v1/system/ip-pools/p0/silos/{}", cptestctx.silo_name);
    let error =
        object_put_error(client, &p0_silo_url, &params, StatusCode::NOT_FOUND)
            .await;
    assert_eq!(
        error.message,
        "not found: ip-pool-resource with id \"(pool, silo)\""
    );

    // associate both pools with the test silo
    let silo = NameOrId::Name(cptestctx.silo_name.clone());
    let params =
        params::IpPoolLinkSilo { silo: silo.clone(), is_default: false };
    let _: IpPoolSiloLink =
        object_create(client, "/v1/system/ip-pools/p0/silos", &params).await;
    let _: IpPoolSiloLink =
        object_create(client, "/v1/system/ip-pools/p1/silos", &params).await;

    // now both are linked to the silo, neither is marked default
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 1);
    assert_eq!(silos_p0.items[0].is_default, false);

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 1);
    assert_eq!(silos_p1.items[0].is_default, false);

    // make p0 default
    let params = IpPoolSiloUpdate { is_default: true };
    let _: IpPoolSiloLink = object_put(client, &p0_silo_url, &params).await;

    // making the same one default again is not an error
    let _: IpPoolSiloLink = object_put(client, &p0_silo_url, &params).await;

    // now p0 is default
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 1);
    assert_eq!(silos_p0.items[0].is_default, true);

    // p1 still not default
    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 1);
    assert_eq!(silos_p1.items[0].is_default, false);

    // making p1 the default pool for the silo unsets it on p0

    // set p1 default
    let params = IpPoolSiloUpdate { is_default: true };
    let p1_silo_url =
        format!("/v1/system/ip-pools/p1/silos/{}", cptestctx.silo_name);
    let _: IpPoolSiloLink = object_put(client, &p1_silo_url, &params).await;

    // p1 is now default
    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 1);
    assert_eq!(silos_p1.items[0].is_default, true);

    // p0 is no longer default
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 1);
    assert_eq!(silos_p0.items[0].is_default, false);

    // we can also unset default
    let params = IpPoolSiloUpdate { is_default: false };
    let _: IpPoolSiloLink = object_put(client, &p1_silo_url, &params).await;

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 1);
    assert_eq!(silos_p1.items[0].is_default, false);
}

// IP pool list fetch logic includes a join to ip_pool_resource, which is
// unusual, so we want to make sure pagination logic still works
#[nexus_test]
async fn test_ip_pool_pagination(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let base_url = "/v1/system/ip-pools";
    let first_page = objects_list_page_authz::<IpPool>(client, &base_url).await;

    // we start out with no pools
    assert_eq!(first_page.items.len(), 0);

    let mut pool_names = vec![];

    // create more pools to work with, adding their names to the list so we
    // can use it to check order
    for i in 1..=8 {
        let name = format!("other-pool-{}", i);
        pool_names.push(name.clone());
        create_pool(client, &name).await;
    }

    let first_five_url = format!("{}?limit=5", base_url);
    let first_five =
        objects_list_page_authz::<IpPool>(client, &first_five_url).await;
    assert!(first_five.next_page.is_some());
    assert_eq!(get_names(first_five.items), &pool_names[0..5]);

    let next_page_url = format!(
        "{}?limit=5&page_token={}",
        base_url,
        first_five.next_page.unwrap()
    );
    let next_page =
        objects_list_page_authz::<IpPool>(client, &next_page_url).await;
    assert_eq!(get_names(next_page.items), &pool_names[5..8]);
}

/// helper to make tests less ugly
fn get_names(pools: Vec<IpPool>) -> Vec<String> {
    pools.iter().map(|p| p.identity.name.to_string()).collect()
}

async fn silos_for_pool(
    client: &ClientTestContext,
    pool: &str,
) -> ResultsPage<IpPoolSiloLink> {
    let url = format!("/v1/system/ip-pools/{}/silos", pool);
    objects_list_page_authz::<IpPoolSiloLink>(client, &url).await
}

async fn pools_for_silo(
    client: &ClientTestContext,
    silo: &str,
) -> Vec<SiloIpPool> {
    let url = format!("/v1/system/silos/{}/ip-pools", silo);
    objects_list_page_authz::<SiloIpPool>(client, &url).await.items
}

async fn create_pool(client: &ClientTestContext, name: &str) -> IpPool {
    let params = IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(name.to_string()).unwrap(),
            description: "".to_string(),
        },
    };
    NexusRequest::objects_post(client, "/v1/system/ip-pools", &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

// Data for testing overlapping IP ranges
struct TestRange {
    // A starting IP range that should be inserted correctly
    base_range: IpRange,
    // Ranges that should fail for various reasons of overlap
    bad_ranges: Vec<IpRange>,
}

// Integration test verifying the uniqueness of IP ranges when inserted /
// deleted across multiple pools
#[nexus_test]
async fn test_ip_pool_range_overlapping_ranges_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let ip_pools_url = "/v1/system/ip-pools";
    let pool_name = "p0";
    let description = "an ip pool";
    let ip_pool_url = format!("{}/{}", ip_pools_url, pool_name);
    let ip_pool_ranges_url = format!("{}/ranges", ip_pool_url);
    let ip_pool_add_range_url = format!("{}/add", ip_pool_ranges_url);

    // Create the pool, verify basic properties
    let params = IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(pool_name).parse().unwrap(),
            description: String::from(description),
        },
        // silo: None,
        // is_default: false,
    };
    let created_pool: IpPool =
        NexusRequest::objects_post(client, ip_pools_url, &params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(created_pool.identity.name, pool_name);
    assert_eq!(created_pool.identity.description, description);

    // Test data for IPv4 ranges that should fail due to overlap
    let ipv4_range = TestRange {
        base_range: IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 2),
                std::net::Ipv4Addr::new(10, 0, 0, 5),
            )
            .unwrap(),
        ),
        bad_ranges: vec![
            // The exact same range
            IpRange::V4(
                Ipv4Range::new(
                    std::net::Ipv4Addr::new(10, 0, 0, 2),
                    std::net::Ipv4Addr::new(10, 0, 0, 5),
                )
                .unwrap(),
            ),
            // Overlaps below
            IpRange::V4(
                Ipv4Range::new(
                    std::net::Ipv4Addr::new(10, 0, 0, 1),
                    std::net::Ipv4Addr::new(10, 0, 0, 2),
                )
                .unwrap(),
            ),
            // Overlaps above
            IpRange::V4(
                Ipv4Range::new(
                    std::net::Ipv4Addr::new(10, 0, 0, 5),
                    std::net::Ipv4Addr::new(10, 0, 0, 6),
                )
                .unwrap(),
            ),
            // Contains the base range
            IpRange::V4(
                Ipv4Range::new(
                    std::net::Ipv4Addr::new(10, 0, 0, 1),
                    std::net::Ipv4Addr::new(10, 0, 0, 6),
                )
                .unwrap(),
            ),
            // Contained by the base range
            IpRange::V4(
                Ipv4Range::new(
                    std::net::Ipv4Addr::new(10, 0, 0, 3),
                    std::net::Ipv4Addr::new(10, 0, 0, 4),
                )
                .unwrap(),
            ),
        ],
    };
    test_bad_ip_ranges(client, &ip_pool_add_range_url, &ipv4_range).await;

    // Test data for IPv6 ranges that should fail due to overlap
    let ipv6_range = TestRange {
        base_range: IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 20),
            )
            .unwrap(),
        ),
        bad_ranges: vec![
            // The exact same range
            IpRange::V6(
                Ipv6Range::new(
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 20),
                )
                .unwrap(),
            ),
            // Overlaps below
            IpRange::V6(
                Ipv6Range::new(
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 5),
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 15),
                )
                .unwrap(),
            ),
            // Overlaps above
            IpRange::V6(
                Ipv6Range::new(
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 15),
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 25),
                )
                .unwrap(),
            ),
            // Contains the base range
            IpRange::V6(
                Ipv6Range::new(
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0),
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 100),
                )
                .unwrap(),
            ),
            // Contained by the base range
            IpRange::V6(
                Ipv6Range::new(
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 12),
                    std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 13),
                )
                .unwrap(),
            ),
        ],
    };
    test_bad_ip_ranges(client, &ip_pool_add_range_url, &ipv6_range).await;
}

async fn test_bad_ip_ranges(
    client: &ClientTestContext,
    url: &str,
    ranges: &TestRange,
) {
    let created_range: IpPoolRange =
        NexusRequest::objects_post(client, url, &ranges.base_range)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(
        ranges.base_range.first_address(),
        created_range.range.first_address()
    );
    assert_eq!(
        ranges.base_range.last_address(),
        created_range.range.last_address()
    );

    // Everything else should fail
    for bad_range in ranges.bad_ranges.iter() {
        let error: HttpErrorResponseBody = NexusRequest::new(
            RequestBuilder::new(client, Method::POST, url)
                .body(Some(bad_range))
                .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
        let expected_message = format!(
            "The provided IP range {}-{} overlaps with an existing range",
            bad_range.first_address(),
            bad_range.last_address(),
        );
        assert_eq!(error.message, expected_message);
    }
}

#[nexus_test]
async fn test_ip_pool_range_pagination(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let ip_pools_url = "/v1/system/ip-pools";
    let pool_name = "p0";
    let description = "an ip pool";
    let ip_pool_url = format!("{}/{}", ip_pools_url, pool_name);
    let ip_pool_ranges_url = format!("{}/ranges", ip_pool_url);
    let ip_pool_add_range_url = format!("{}/add", ip_pool_ranges_url);

    // Create the pool, verify basic properties
    let params = IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(pool_name).parse().unwrap(),
            description: String::from(description),
        },
    };
    let created_pool: IpPool =
        NexusRequest::objects_post(client, ip_pools_url, &params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(created_pool.identity.name, pool_name);
    assert_eq!(created_pool.identity.description, description);

    // Add some ranges, out of order. These will be paginated by their first
    // address, which sorts all IPv4 before IPv6, then within protocol versions
    // by their first address.
    let ranges = [
        IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 11),
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 20),
            )
            .unwrap(),
        ),
        IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0),
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
            )
            .unwrap(),
        ),
        IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 1),
                std::net::Ipv4Addr::new(10, 0, 0, 2),
            )
            .unwrap(),
        ),
    ];

    let mut expected_ranges = Vec::with_capacity(ranges.len());
    for range in ranges.iter() {
        let created_range: IpPoolRange =
            NexusRequest::objects_post(client, &ip_pool_add_range_url, &range)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .unwrap()
                .parsed_body()
                .unwrap();
        assert_eq!(range.first_address(), created_range.range.first_address());
        assert_eq!(range.last_address(), created_range.range.last_address());
        expected_ranges.push(created_range);
    }
    expected_ranges
        .sort_by(|a, b| a.range.first_address().cmp(&b.range.first_address()));

    // List the first 2 results, then the last. These should appear sorted by
    // their first address.
    let first_page_url = format!("{}?limit=2", ip_pool_ranges_url);
    let first_page =
        objects_list_page_authz::<IpPoolRange>(client, &first_page_url).await;
    assert_eq!(first_page.items.len(), 2);

    let second_page_url = format!(
        "{}&page_token={}",
        first_page_url,
        first_page.next_page.unwrap()
    );
    let second_page =
        objects_list_page_authz::<IpPoolRange>(client, &second_page_url).await;
    assert_eq!(second_page.items.len(), 1);

    let actual_ranges = first_page.items.iter().chain(second_page.items.iter());
    for (expected_range, actual_range) in
        expected_ranges.iter().zip(actual_ranges)
    {
        assert_ranges_eq(expected_range, actual_range);
    }
}

#[nexus_test]
async fn test_ip_pool_list_in_silo(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let mypool_name = "mypool";

    const PROJECT_NAME: &str = "myproj";
    create_project(client, PROJECT_NAME).await;

    // create pool with range and link (as default pool) to default silo, which
    // is the privileged user's silo
    let mypool_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 51),
            std::net::Ipv4Addr::new(10, 0, 0, 52),
        )
        .unwrap(),
    );
    create_ip_pool(client, mypool_name, Some(mypool_range)).await;
    link_ip_pool(client, mypool_name, &DEFAULT_SILO.id(), true).await;

    // create another pool and don't link it to anything
    let otherpool_name = "other-pool";
    let otherpool_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 53),
            std::net::Ipv4Addr::new(10, 0, 0, 54),
        )
        .unwrap(),
    );
    create_ip_pool(client, otherpool_name, Some(otherpool_range)).await;

    let list = objects_list_page_authz::<SiloIpPool>(client, "/v1/ip-pools")
        .await
        .items;

    // only mypool shows up because it's linked to my silo
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].identity.name.to_string(), mypool_name);
    assert!(list[0].is_default);

    // fetch the pool directly too
    let url = format!("/v1/ip-pools/{}", mypool_name);
    let pool = object_get::<SiloIpPool>(client, &url).await;
    assert_eq!(pool.identity.name.as_str(), mypool_name);
    assert!(pool.is_default);

    // fetching the other pool directly 404s
    let url = format!("/v1/ip-pools/{}", otherpool_name);
    object_get_error(client, &url, StatusCode::NOT_FOUND).await;
}

#[nexus_test]
async fn test_ip_range_delete_with_allocated_external_ip_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let ip_pools_url = "/v1/system/ip-pools";
    let pool_name = "mypool";
    let ip_pool_url = format!("{}/{}", ip_pools_url, pool_name);
    let ip_pool_silos_url = format!("{}/{}/silos", ip_pools_url, pool_name);
    let ip_pool_ranges_url = format!("{}/ranges", ip_pool_url);
    let ip_pool_add_range_url = format!("{}/add", ip_pool_ranges_url);
    let ip_pool_rem_range_url = format!("{}/remove", ip_pool_ranges_url);

    // create pool
    let params = IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(pool_name).parse().unwrap(),
            description: String::from("right on cue"),
        },
    };
    NexusRequest::objects_post(client, ip_pools_url, &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<IpPool>()
        .await;

    // associate pool with default silo, which is the privileged user's silo
    let params = IpPoolLinkSilo {
        silo: NameOrId::Id(DEFAULT_SILO.id()),
        is_default: true,
    };
    NexusRequest::objects_post(client, &ip_pool_silos_url, &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<IpPoolSiloLink>()
        .await;

    // Add an IP range to the pool
    let range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 2),
        )
        .unwrap(),
    );
    let created_range: IpPoolRange =
        NexusRequest::objects_post(client, &ip_pool_add_range_url, &range)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(range.first_address(), created_range.range.first_address());
    assert_eq!(range.last_address(), created_range.range.last_address());

    // Create an org and project, and then an instance. The instance should have
    // an IP address from this range (since it's the only one that exists),
    // though we currently have no way to verify this as source NAT external IPs
    // are not part of the public API.
    const PROJECT_NAME: &str = "myproj";
    const INSTANCE_NAME: &str = "myinst";
    create_project(client, PROJECT_NAME).await;
    let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;

    // We should not be able to delete the range, since there's an external IP
    // address in use out of it.
    let err: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &ip_pool_rem_range_url)
            .body(Some(&range))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        err.message,
        "IP pool ranges cannot be deleted while \
        external IP addresses are allocated from them"
    );

    // Stop the instance, wait until it is in fact stopped.
    let instance_url =
        format!("/v1/instances/{}?project={}", INSTANCE_NAME, PROJECT_NAME,);
    let instance_stop_url = format!(
        "/v1/instances/{}/stop?project={}",
        INSTANCE_NAME, PROJECT_NAME,
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to stop instance");

    // Simulate the transition, wait until it is in fact stopped.
    let sa = nexus
        .instance_sled_by_id(&instance.identity.id)
        .await
        .unwrap()
        .expect("running instance should be on a sled");
    sa.instance_finish_transition(instance.identity.id).await;

    // Delete the instance
    NexusRequest::object_delete(client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to delete instance");

    // Now verify that we _can_ delete the IP range.
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &ip_pool_rem_range_url)
            .body(Some(&range))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect(
        "Should be able to delete IP range once no instances use its addresses",
    );
}

#[nexus_test]
async fn test_ip_pool_service(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let ip_pool_url = "/v1/system/ip-pools-service".to_string();
    let ip_pool_ranges_url = format!("{}/ranges", ip_pool_url);
    let ip_pool_add_range_url = format!("{}/add", ip_pool_ranges_url);
    let ip_pool_remove_range_url = format!("{}/remove", ip_pool_ranges_url);

    // View the pool, which should exist without explicit creation.
    let fetched_pool: IpPool = NexusRequest::object_get(client, &ip_pool_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    assert_eq!(
        fetched_pool.identity.name,
        nexus_db_queries::db::datastore::SERVICE_IP_POOL_NAME
    );
    assert_eq!(fetched_pool.identity.description, "IP Pool for Oxide Services");

    // Fetch any ranges already present.
    let existing_ranges =
        objects_list_page_authz::<IpPoolRange>(client, &ip_pool_ranges_url)
            .await
            .items;

    // Add some ranges. Pagination is tested more explicitly in the IP pool
    // implementation, but we just check that these endpoints work here.
    let ranges = [
        IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 1),
                std::net::Ipv4Addr::new(10, 0, 0, 2),
            )
            .unwrap(),
        ),
        IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0),
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
            )
            .unwrap(),
        ),
    ];

    let mut expected_ranges = existing_ranges.clone();
    for range in ranges.iter() {
        let created_range: IpPoolRange =
            NexusRequest::objects_post(client, &ip_pool_add_range_url, &range)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .unwrap()
                .parsed_body()
                .unwrap();
        assert_eq!(range.first_address(), created_range.range.first_address());
        assert_eq!(range.last_address(), created_range.range.last_address());
        expected_ranges.push(created_range);
    }
    expected_ranges
        .sort_by(|a, b| a.range.first_address().cmp(&b.range.first_address()));

    // List the ranges.
    let first_page =
        objects_list_page_authz::<IpPoolRange>(client, &ip_pool_ranges_url)
            .await;
    assert_eq!(first_page.items.len(), expected_ranges.len());

    let actual_ranges = first_page.items.iter();
    for (expected_range, actual_range) in
        expected_ranges.iter().zip(actual_ranges)
    {
        assert_ranges_eq(expected_range, actual_range);
    }

    // Remove both ranges, observe that the IP Pool is empty.
    for range in ranges.iter() {
        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                &ip_pool_remove_range_url,
            )
            .body(Some(&range))
            .expect_status(Some(StatusCode::NO_CONTENT)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to delete IP range from a pool");
    }

    let first_page =
        objects_list_page_authz::<IpPoolRange>(client, &ip_pool_ranges_url)
            .await;
    assert_eq!(first_page.items.len(), existing_ranges.len());
    for (expected_range, actual_range) in
        existing_ranges.iter().zip(first_page.items.iter())
    {
        assert_ranges_eq(expected_range, actual_range);
    }
}

fn assert_pools_eq(first: &IpPool, second: &IpPool) {
    assert_eq!(first.identity, second.identity);
}

fn assert_ranges_eq(first: &IpPoolRange, second: &IpPoolRange) {
    assert_eq!(first.id, second.id);
    assert_eq!(first.time_created, second.time_created);
    assert_eq!(first.range.first_address(), second.range.first_address());
    assert_eq!(first.range.last_address(), second.range.last_address());
}
