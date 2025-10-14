// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on IP Pools

use std::net::Ipv4Addr;

use crate::integration_tests::instances::create_project_and_pool;
use crate::integration_tests::instances::instance_wait_for_state;
use dropshot::HttpErrorResponseBody;
use dropshot::ResultsPage;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::assert_ip_pool_utilization;
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::grant_iam;
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
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::IpPoolCreate;
use nexus_types::external_api::params::IpPoolLinkSilo;
use nexus_types::external_api::params::IpPoolSiloUpdate;
use nexus_types::external_api::params::IpPoolUpdate;
use nexus_types::external_api::shared::IpPoolReservationType;
use nexus_types::external_api::shared::IpPoolType;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::Ipv4Range;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::external_api::views::IpPool;
use nexus_types::external_api::views::IpPoolRange;
use nexus_types::external_api::views::IpPoolSiloLink;
use nexus_types::external_api::views::IpVersion;
use nexus_types::external_api::views::Silo;
use nexus_types::external_api::views::SystemIpPool;
use nexus_types::identity::Resource;
use omicron_common::address::Ipv6Range;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::SimpleIdentityOrName;
use omicron_common::api::external::{IdentityMetadataCreateParams, Name};
use omicron_nexus::TestInterfaces;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
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
        ip_version: IpVersion::V4,
        pool_type: IpPoolType::Unicast,
        reservation_type: IpPoolReservationType::ExternalSilos,
    };
    let created_pool: SystemIpPool =
        object_create(client, ip_pools_url, &params).await;
    assert_eq!(created_pool.identity.name, pool_name);
    assert_eq!(created_pool.identity.description, description);
    assert_eq!(created_pool.ip_version, IpVersion::V4);
    assert_eq!(
        created_pool.reservation_type,
        IpPoolReservationType::ExternalSilos
    );

    let list = get_ip_pools(client).await;
    assert_eq!(list.len(), 1, "Expected exactly 1 IP pool");
    assert_pools_eq(&created_pool, &list[0]);

    let fetched_pool: SystemIpPool = object_get(client, &ip_pool_url).await;
    assert_pools_eq(&created_pool, &fetched_pool);

    // Verify we get a conflict error if we insert it again
    let error = object_create_error(
        client,
        ip_pools_url,
        &params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: pool_name.parse().unwrap(),
                description: String::new(),
            },
            ip_version: IpVersion::V4,
            pool_type: IpPoolType::Unicast,
            reservation_type: IpPoolReservationType::ExternalSilos,
        },
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
    let modified_pool: SystemIpPool =
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

    let fetched_modified_pool: SystemIpPool =
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

async fn get_ip_pools(client: &ClientTestContext) -> Vec<SystemIpPool> {
    NexusRequest::iter_collection_authn::<SystemIpPool>(
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

    // this is a great spot to check that deleting a pool cleans up the links!

    // first we have to delete the range, otherwise delete will fail
    let url = "/v1/system/ip-pools/pool1/ranges/remove";
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, url)
            .body(Some(&range1))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to delete IP range from a pool");

    object_delete(client, "/v1/system/ip-pools/pool1").await;

    let silo1_pools = pools_for_silo(client, "silo1").await;
    assert_eq!(silo1_pools.len(), 1);
    assert_eq!(silo1_pools[0].id(), pool2.id());

    let silo2_pools = pools_for_silo(client, "silo2").await;
    assert_eq!(silo2_pools.len(), 0);

    let silo3_pools = pools_for_silo(client, "silo3").await;
    assert_eq!(silo3_pools.len(), 0);
}

#[nexus_test]
async fn test_ip_pool_silo_link(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let p0 = create_ipv4_pool(client, "p0").await;
    let p1 = create_ipv4_pool(client, "p1").await;

    // there should be no associations
    let assocs_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(assocs_p0.items.len(), 0);

    // we need to use a discoverable silo because non-discoverable silos, while
    // linkable, are filtered out of the list of linked silos for a pool. the
    // test silo at cptestctx.silo_name is non-discoverable.
    let silo =
        create_silo(&client, "my-silo", true, SiloIdentityMode::SamlJit).await;

    let silo_pools = pools_for_silo(client, silo.name().as_str()).await;
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
    let params = params::IpPoolLinkSilo {
        silo: NameOrId::Name(silo.name().clone()),
        is_default: false,
    };
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
    let silo_url = format!("/v1/system/silos/{}", silo.name());
    let silo_id = object_get::<Silo>(client, &silo_url).await.identity.id;

    let assocs_p0 = silos_for_pool(client, "p0").await;
    let silo_link = IpPoolSiloLink {
        ip_pool_id: p0.identity.id,
        silo_id,
        is_default: false,
    };
    assert_eq!(assocs_p0.items.len(), 1);
    assert_eq!(assocs_p0.items[0], silo_link);

    let silo_pools = pools_for_silo(client, silo.name().as_str()).await;
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

    let silo_pools = pools_for_silo(client, silo.name().as_str()).await;
    assert_eq!(silo_pools.len(), 2);
    assert_eq!(silo_pools[0].id(), p0.id());
    assert_eq!(silo_pools[0].is_default, false);
    assert_eq!(silo_pools[1].id(), p1.id());
    assert_eq!(silo_pools[1].is_default, true);

    // creating a third pool and trying to link it as default: true should fail
    create_ipv4_pool(client, "p2").await;
    let url = "/v1/system/ip-pools/p2/silos";
    let error = object_create_error(
        client,
        &url,
        &link_params,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.error_code.unwrap(), "ObjectAlreadyExists");

    // unlink p1 from silo (doesn't matter that it's a default)
    let url = format!("/v1/system/ip-pools/p1/silos/{}", silo.name().as_str());
    object_delete(client, &url).await;

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 0);

    // after unlinking p1, only p0 is left
    let silo_pools = pools_for_silo(client, silo.name().as_str()).await;
    assert_eq!(silo_pools.len(), 1);
    assert_eq!(silo_pools[0].identity.id, p0.identity.id);
    assert_eq!(silo_pools[0].is_default, false);

    // now we can delete the pool too
    object_delete(client, "/v1/system/ip-pools/p1").await;
}

#[nexus_test]
async fn cannot_unlink_ip_pool_with_outstanding_instance_ips(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;

    // Create a project and add a default IP Pool.
    const POOL_NAME: &str = "default";
    let proj = create_project_and_pool(client).await;

    // Create an instance, which allocates an IP address.
    const INSTANCE_NAME: &str = "myinst";
    let instance =
        create_instance(client, proj.name().as_str(), INSTANCE_NAME).await;

    // We cannot delete the link between the silo and pool until the instance is
    // gone.
    let url = format!(
        "/v1/system/ip-pools/{}/silos/{}",
        POOL_NAME,
        DEFAULT_SILO.name().as_str()
    );
    object_delete_error(client, &url, StatusCode::BAD_REQUEST).await;

    // Stop the instance and wait until it's actually stopped.
    let instance_stop_url = format!(
        "/v1/instances/{}/stop?project={}",
        INSTANCE_NAME,
        proj.name().as_str(),
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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should be on a sled");
    info.sled_client.vmm_finish_transition(info.propolis_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Now that it's stopped, delete the instance, and then assert that we can
    // actually unlink the IP Pool from the Silo.
    object_delete(client, &format!("/v1/instances/{}", instance_id)).await;
    object_delete(client, &url).await;
}

#[nexus_test]
async fn cannot_unlink_ip_pool_with_outstanding_floating_ips(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project and add a default IP Pool.
    const POOL_NAME: &str = "default";
    let proj = create_project_and_pool(client).await;

    // Create a floating IP from the pool.
    let fip = create_floating_ip(
        client,
        "fip",
        proj.name().as_str(),
        None,
        Some(POOL_NAME),
    )
    .await;

    // We cannot delete the link between the silo and pool until the floating IP
    // has been deleted.
    let url = format!(
        "/v1/system/ip-pools/{}/silos/{}",
        POOL_NAME,
        DEFAULT_SILO.name().as_str()
    );
    object_delete_error(client, &url, StatusCode::BAD_REQUEST).await;

    // Now delete the FIP.
    object_delete(client, &format!("/v1/floating-ips/{}", fip.id())).await;

    // Now we can do the dew.
    object_delete(client, &url).await;
}

/// Non-discoverable silos can be linked to a pool, but they do not show up
/// in the list of silos for that pool, just as they do not show up in the
/// top-level list of silos
#[nexus_test]
async fn test_ip_pool_silo_list_only_discoverable(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_ipv4_pool(client, "p0").await;

    // there should be no linked silos
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 0);

    let silo_disc =
        create_silo(&client, "silo-disc", true, SiloIdentityMode::SamlJit)
            .await;
    link_ip_pool(client, "p0", &silo_disc.id(), false).await;

    let silo_non_disc =
        create_silo(&client, "silo-non-disc", false, SiloIdentityMode::SamlJit)
            .await;
    link_ip_pool(client, "p0", &silo_non_disc.id(), false).await;

    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 1);
    assert_eq!(silos_p0.items[0].silo_id, silo_disc.id());
}

#[nexus_test]
async fn test_ip_pool_update_default(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_ipv4_pool(client, "p0").await;
    create_ipv4_pool(client, "p1").await;

    // there should be no linked silos
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 0);

    let silos_p1 = silos_for_pool(client, "p1").await;
    assert_eq!(silos_p1.items.len(), 0);

    // we need to use a discoverable silo because non-discoverable silos, while
    // linkable, are filtered out of the list of linked silos for a pool. the
    // test silo at cptestctx.silo_name is non-discoverable.
    let silo =
        create_silo(&client, "my-silo", true, SiloIdentityMode::SamlJit).await;

    // put 404s if link doesn't exist yet
    let params = IpPoolSiloUpdate { is_default: true };
    let p0_silo_url = format!("/v1/system/ip-pools/p0/silos/{}", silo.name());
    let error =
        object_put_error(client, &p0_silo_url, &params, StatusCode::NOT_FOUND)
            .await;
    assert_eq!(
        error.message,
        "not found: ip-pool-resource with id \"(pool, silo)\""
    );

    // associate both pools with the test silo
    let params = params::IpPoolLinkSilo {
        silo: NameOrId::Name(silo.name().clone()),
        is_default: false,
    };
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
    let p1_silo_url = format!("/v1/system/ip-pools/p1/silos/{}", silo.name());
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
    let first_page =
        objects_list_page_authz::<SystemIpPool>(client, &base_url).await;

    // we start out with no pools
    assert_eq!(first_page.items.len(), 0);

    let mut pool_names = vec![];

    // create more pools to work with, adding their names to the list so we
    // can use it to check order
    for i in 1..=8 {
        let name = format!("other-pool-{}", i);
        pool_names.push(name.clone());
        create_ipv4_pool(client, &name).await;
    }

    let first_five_url = format!("{}?limit=5", base_url);
    let first_five =
        objects_list_page_authz::<SystemIpPool>(client, &first_five_url).await;
    assert!(first_five.next_page.is_some());
    assert_eq!(get_names(first_five.items), &pool_names[0..5]);

    let next_page_url = format!(
        "{}?limit=5&page_token={}",
        base_url,
        first_five.next_page.unwrap()
    );
    let next_page =
        objects_list_page_authz::<SystemIpPool>(client, &next_page_url).await;
    assert_eq!(get_names(next_page.items), &pool_names[5..8]);
}

#[nexus_test]
async fn test_ip_pool_silos_pagination(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // one pool, and there should be no linked silos
    create_ipv4_pool(client, "p0").await;
    let silos_p0 = silos_for_pool(client, "p0").await;
    assert_eq!(silos_p0.items.len(), 0);

    // create and link some silos. we need to use discoverable silos because
    // non-discoverable silos, while linkable, are filtered out of the list of
    // linked silos for a pool
    let mut silo_ids = vec![];
    for i in 1..=8 {
        let name = format!("silo-{}", i);
        let silo =
            create_silo(&client, &name, true, SiloIdentityMode::SamlJit).await;
        silo_ids.push(silo.id());
        link_ip_pool(client, "p0", &silo.id(), false).await;
    }

    // we paginate by ID, so these should be in order to match
    silo_ids.sort();

    let base_url = "/v1/system/ip-pools/p0/silos";
    let first_five_url = format!("{}?limit=5", base_url);
    let first_five =
        objects_list_page_authz::<IpPoolSiloLink>(client, &first_five_url)
            .await;
    assert!(first_five.next_page.is_some());
    assert_eq!(
        first_five.items.iter().map(|s| s.silo_id).collect::<Vec<_>>(),
        &silo_ids[0..5]
    );

    let next_page_url = format!(
        "{}?limit=5&page_token={}",
        base_url,
        first_five.next_page.unwrap()
    );
    let next_page =
        objects_list_page_authz::<IpPoolSiloLink>(client, &next_page_url).await;
    assert_eq!(
        next_page.items.iter().map(|s| s.silo_id).collect::<Vec<_>>(),
        &silo_ids[5..8]
    );
}

/// helper to make tests less ugly
fn get_names(pools: Vec<SystemIpPool>) -> Vec<String> {
    pools.iter().map(|p| p.identity.name.to_string()).collect()
}

async fn silos_for_pool(
    client: &ClientTestContext,
    pool: &str,
) -> ResultsPage<IpPoolSiloLink> {
    let url = format!("/v1/system/ip-pools/{}/silos", pool);
    objects_list_page_authz::<IpPoolSiloLink>(client, &url).await
}

async fn pools_for_silo(client: &ClientTestContext, silo: &str) -> Vec<IpPool> {
    let url = format!("/v1/system/silos/{}/ip-pools", silo);
    objects_list_page_authz::<IpPool>(client, &url).await.items
}

async fn create_ipv4_pool(
    client: &ClientTestContext,
    name: &str,
) -> SystemIpPool {
    create_pool(client, name, IpVersion::V4).await
}

async fn create_pool(
    client: &ClientTestContext,
    name: &str,
    ip_version: IpVersion,
) -> SystemIpPool {
    let params = IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(name.to_string()).unwrap(),
            description: "".to_string(),
        },
        ip_version,
        pool_type: IpPoolType::Unicast,
        reservation_type: IpPoolReservationType::ExternalSilos,
    };
    NexusRequest::objects_post(client, "/v1/system/ip-pools", &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

// This is mostly about testing the total field with huge numbers.
// testing allocated is done in a bunch of other places. look for
// assert_ip_pool_utilization calls
#[nexus_test]
async fn test_ipv4_ip_pool_utilization_total(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _pool = create_ipv4_pool(client, "p0").await;

    assert_ip_pool_utilization(client, "p0", 0, 0.0).await;

    let add_url = "/v1/system/ip-pools/p0/ranges/add";

    // add just 5 addresses to get the party started
    let range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 5),
        )
        .unwrap(),
    );
    object_create::<IpRange, IpPoolRange>(client, &add_url, &range).await;

    assert_ip_pool_utilization(client, "p0", 0, 5.0).await;
}

// We're going to test adding an IPv6 pool and collecting its utilization, even
// though that requires operating directly on the datastore. When we resolve
// https://github.com/oxidecomputer/omicron/issues/8881, we can switch this to
// use the API to create the IPv6 pool and ranges instead.
#[nexus_test]
async fn test_ipv6_ip_pool_utilization_total(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let log = cptestctx.logctx.log.new(o!());
    let opctx = OpContext::for_tests(log, datastore.clone());
    let identity = IdentityMetadataCreateParams {
        name: "p0".parse().unwrap(),
        description: String::new(),
    };
    let pool = datastore
        .ip_pool_create(
            &opctx,
            nexus_db_model::IpPool::new(
                &identity,
                nexus_db_model::IpVersion::V6,
                nexus_db_model::IpPoolReservationType::ExternalSilos,
            ),
        )
        .await
        .expect("should be able to create IPv6 pool");

    // Check the utilization is zero.
    assert_ip_pool_utilization(client, "p0", 0, 0.0).await;

    // Now let's add a gigantic range. This requires direct datastore
    // shenanigans because adding IPv6 ranges through the API is currently not
    // allowed. It's worth doing because we want this code to correctly handle
    // IPv6 ranges when they are allowed again.
    let by_id = NameOrId::Id(pool.id());
    let (authz_pool, db_pool) = nexus
        .ip_pool_lookup(&opctx, &by_id)
        .fetch_for(authz::Action::CreateChild)
        .await
        .expect("should be able to fetch pool we just created");
    let ipv6_range = Ipv6Range::new(
        std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0),
        std::net::Ipv6Addr::new(
            0xfd00, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
        ),
    )
    .unwrap();
    let big_range = IpRange::V6(ipv6_range);
    datastore
        .ip_pool_add_range(&opctx, &authz_pool, &db_pool, &big_range)
        .await
        .expect("could not add range");

    let capacity = ipv6_range.len() as f64;
    assert_ip_pool_utilization(client, "p0", 0, capacity).await;
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
        ip_version: IpVersion::V4,
        pool_type: IpPoolType::Unicast,
        reservation_type: IpPoolReservationType::ExternalSilos,
    };
    let created_pool: SystemIpPool =
        object_create(client, ip_pools_url, &params).await;
    assert_eq!(created_pool.identity.name, pool_name);
    assert_eq!(created_pool.identity.description, description);
    assert_eq!(created_pool.ip_version, IpVersion::V4);
    assert_eq!(
        created_pool.reservation_type,
        IpPoolReservationType::ExternalSilos
    );

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

    // IPv6 tests removed along with support for IPv6 ranges in
    // https://github.com/oxidecomputer/omicron/pull/5107
    // Put them back when IPv6 ranges are supported again.
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

// Support for IPv6 ranges removed in
// https://github.com/oxidecomputer/omicron/pull/5107
// Delete this test when we support IPv6 again.
#[nexus_test]
async fn test_ip_pool_range_rejects_v6(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_ip_pool(client, "p0", None).await;

    let range = IpRange::V6(
        Ipv6Range::new(
            std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
            std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 20),
        )
        .unwrap(),
    );

    let add_url = "/v1/system/ip-pools/p0/ranges/add";
    let error =
        object_create_error(client, add_url, &range, StatusCode::BAD_REQUEST)
            .await;

    assert_eq!(error.message, "IPv6 ranges are not allowed yet");
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
        ip_version: IpVersion::V4,
        pool_type: IpPoolType::Unicast,
        reservation_type: IpPoolReservationType::ExternalSilos,
    };
    let created_pool: SystemIpPool =
        object_create(client, ip_pools_url, &params).await;
    assert_eq!(created_pool.identity.name, pool_name);
    assert_eq!(created_pool.identity.description, description);
    assert_eq!(created_pool.ip_version, IpVersion::V4);
    assert_eq!(
        created_pool.reservation_type,
        IpPoolReservationType::ExternalSilos
    );

    // Add some ranges, out of order. These will be paginated by their first
    // address, which sorts all IPv4 before IPv6, then within protocol versions
    // by their first address.
    let ranges = [
        IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 3),
                std::net::Ipv4Addr::new(10, 0, 0, 4),
            )
            .unwrap(),
        ),
        IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 5),
                std::net::Ipv4Addr::new(10, 0, 0, 6),
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

    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: Silo = object_get(client, &silo_url).await;

    // manually create default pool and link to test silo, as opposed to default
    // silo, which is what the helper would do
    let _ = create_ip_pool(&client, "default", None).await;
    let default_name = "default";
    link_ip_pool(&client, default_name, &silo.identity.id, true).await;

    // create other pool and link to silo
    let other_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 1, 0, 1), Ipv4Addr::new(10, 1, 0, 5))
            .unwrap(),
    );
    let other_name = "other-pool";
    create_ip_pool(&client, other_name, Some(other_pool_range)).await;
    link_ip_pool(&client, other_name, &silo.identity.id, false).await;

    // create third pool and don't link to silo
    let unlinked_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 2, 0, 1), Ipv4Addr::new(10, 2, 0, 5))
            .unwrap(),
    );
    let unlinked_name = "unlinked-pool";
    create_ip_pool(&client, unlinked_name, Some(unlinked_pool_range)).await;

    // Create a silo user
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Make silo collaborator
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let list = NexusRequest::object_get(client, "/v1/ip-pools")
        .authn_as(AuthnMode::SiloUser(user.id))
        .execute_and_parse_unwrap::<ResultsPage<IpPool>>()
        .await
        .items;

    assert_eq!(list.len(), 2);
    assert_eq!(list[0].identity.name.to_string(), default_name);
    assert!(list[0].is_default);
    assert_eq!(list[1].identity.name.to_string(), other_name);
    assert!(!list[1].is_default);

    // fetch the pools directly too
    let url = format!("/v1/ip-pools/{}", default_name);
    let pool = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::SiloUser(user.id))
        .execute_and_parse_unwrap::<IpPool>()
        .await;
    assert_eq!(pool.identity.name.as_str(), default_name);
    assert!(pool.is_default);

    let url = format!("/v1/ip-pools/{}", other_name);
    let pool = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::SiloUser(user.id))
        .execute_and_parse_unwrap::<IpPool>()
        .await;
    assert_eq!(pool.identity.name.as_str(), other_name);
    assert!(!pool.is_default);

    // fetching the other pool directly 404s
    let url = format!("/v1/ip-pools/{}", unlinked_name);
    let _error = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap();
    dbg!(_error);
}

#[nexus_test]
async fn test_ip_range_delete_with_allocated_external_ip_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let ip_pools_url = "/v1/system/ip-pools";
    let pool_name = "mypool";
    let ip_pool_url = format!("{}/{}", ip_pools_url, pool_name);
    let ip_pool_silos_url = format!("{}/{}/silos", ip_pools_url, pool_name);
    let ip_pool_ranges_url = format!("{}/ranges", ip_pool_url);
    let ip_pool_add_range_url = format!("{}/add", ip_pool_ranges_url);
    let ip_pool_rem_range_url = format!("{}/remove", ip_pool_ranges_url);

    // create pool
    let _ = create_ipv4_pool(client, pool_name).await;

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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

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
    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should be on a sled");
    info.sled_client.vmm_finish_transition(info.propolis_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

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

fn assert_pools_eq(first: &SystemIpPool, second: &SystemIpPool) {
    assert_eq!(first.identity, second.identity);
    assert_eq!(first.ip_version, second.ip_version);
}

fn assert_ranges_eq(first: &IpPoolRange, second: &IpPoolRange) {
    assert_eq!(first.id, second.id);
    assert_eq!(first.time_created, second.time_created);
    assert_eq!(first.range.first_address(), second.range.first_address());
    assert_eq!(first.range.last_address(), second.range.last_address());
}

#[nexus_test]
async fn test_ip_pool_unicast_defaults(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Test that regular IP pool creation uses unicast defaults
    let pool = create_pool(client, "unicast-test", IpVersion::V4).await;
    assert_eq!(pool.pool_type, IpPoolType::Unicast);

    // Test that explicitly creating with default type still works
    let params = IpPoolCreate::new(
        IdentityMetadataCreateParams {
            name: "explicit-unicast".parse().unwrap(),
            description: "Explicit unicast pool".to_string(),
        },
        IpVersion::V4,
        IpPoolReservationType::ExternalSilos,
    );
    let pool: SystemIpPool =
        object_create(client, "/v1/system/ip-pools", &params).await;
    assert_eq!(pool.pool_type, IpPoolType::Unicast);
}
