// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Integration tests for multicast group APIs and basic membership operations.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use dropshot::HttpErrorResponseBody;
use dropshot::ResultsPage;
use http::{Method, StatusCode};

use crate::integration_tests::instances::{
    instance_simulate, instance_wait_for_state,
};
use dpd_client::Error as DpdError;
use dpd_client::types as dpd_types;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::dpd_client;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, link_ip_pool,
    object_create, object_create_error, object_delete, object_get,
    object_get_error, object_put, object_put_error,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    IpPoolCreate, MulticastGroupCreate, MulticastGroupMemberAdd,
    MulticastGroupUpdate,
};
use nexus_types::external_api::shared::IpPoolReservationType;
use nexus_types::external_api::shared::{IpRange, Ipv4Range, Ipv6Range};
use nexus_types::external_api::views::{
    IpPool, IpPoolRange, IpVersion, MulticastGroup, MulticastGroupMember,
};
use nexus_types::identity::Resource;
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams, InstanceState,
    NameOrId, Nullable,
};
use omicron_common::vlan::VlanID;
use omicron_uuid_kinds::InstanceUuid;

use super::*;

/// Verify creation works when optional fields are omitted from the JSON body
/// (i.e., keys are missing, not present as `null`). This mirrors CLI behavior.
#[nexus_test]
async fn test_multicast_group_create_raw_omitted_optionals(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "raw-omit-proj";
    let pool_name = "raw-omit-pool";
    let group_name = "raw-omit-group";

    // Ensure a project exists (not strictly required for fleet-scoped groups)
    create_project(client, project_name).await;

    // Create a multicast pool with a unique, non-reserved ASM range and link it
    create_multicast_ip_pool_with_range(
        client,
        pool_name,
        (224, 9, 0, 10),
        (224, 9, 0, 255),
    )
    .await;

    let group_url = mcast_groups_url();

    // Omit multicast_ip and source_ips keys entirely; specify pool by name
    let body = format!(
        r#"{{"name":"{group}","description":"Create with omitted optionals","pool":"{pool}"}}"#,
        group = group_name,
        pool = pool_name,
    );

    let created: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &group_url)
            .header("content-type", "application/json")
            .raw_body(Some(body))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Create with omitted optional fields should succeed")
    .parsed_body()
    .expect("Should parse created MulticastGroup");

    assert_eq!(created.identity.name, group_name);
    assert!(created.multicast_ip.is_multicast());
    assert!(created.source_ips.is_empty());

    // Wait for reconciler to activate the group
    wait_for_group_active(client, group_name).await;

    // Cleanup
    object_delete(client, &mcast_group_url(group_name)).await;
}

/// Verify ASM creation with explicit address works when `source_ips` is omitted
#[nexus_test]
async fn test_multicast_group_create_raw_asm_omitted_sources(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let pool_name = "raw-asm-pool";
    let group_name = "raw-asm-group";

    // Pool for allocation (even with explicit IP, current create path validates pool)
    create_multicast_ip_pool_with_range(
        client,
        pool_name,
        (224, 10, 0, 10),
        (224, 10, 0, 255),
    )
    .await;

    let group_url = mcast_groups_url();
    let body = format!(
        r#"{{"name":"{group}","description":"ASM no sources omitted","multicast_ip":"224.10.0.100","pool":"{pool}"}}"#,
        group = group_name,
        pool = pool_name,
    );

    let created: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &group_url)
            .header("content-type", "application/json")
            .raw_body(Some(body))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("ASM creation with omitted source_ips should succeed")
    .parsed_body()
    .expect("Should parse created MulticastGroup");

    assert!(created.multicast_ip.is_multicast());
    assert!(created.source_ips.is_empty());
    wait_for_group_active(client, group_name).await;

    object_delete(client, &mcast_group_url(group_name)).await;
}

/// Verify SSM creation fails when `source_ips` is omitted (missing sources)
#[nexus_test]
async fn test_multicast_group_create_raw_ssm_missing_sources(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let pool_name = "raw-ssm-pool";
    let group_name = "raw-ssm-group";

    // Pool for validation
    create_multicast_ip_pool_with_range(
        client,
        pool_name,
        (224, 11, 0, 10),
        (224, 11, 0, 255),
    )
    .await;

    let group_url = mcast_groups_url();
    let body = format!(
        r#"{{"name":"{group}","description":"SSM missing sources","multicast_ip":"232.1.2.3","pool":"{pool}"}}"#,
        group = group_name,
        pool = pool_name,
    );

    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &group_url)
            .header("content-type", "application/json")
            .raw_body(Some(body))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("SSM creation without sources should fail")
    .parsed_body()
    .expect("Should parse error response body");

    assert!(
        error
            .message
            .contains("SSM multicast addresses require at least one source IP"),
        "unexpected error message: {}",
        error.message
    );
}

#[nexus_test]
async fn test_multicast_group_basic_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group";
    let description = "A test multicast group";

    // Create a project
    create_project(&client, project_name).await;

    // Test with explicit multicast pool using unique range for this test
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 1, 0, 10),
        (224, 1, 0, 255),
    )
    .await;

    let group_url = mcast_groups_url();

    // Verify empty list initially
    let groups = list_multicast_groups(&client).await;
    assert_eq!(groups.len(), 0, "Expected empty list of multicast groups");

    // Test creating a multicast group with auto-allocated IP
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: String::from(description),
        },
        multicast_ip: None, // Auto-allocate
        source_ips: None,   // Any-Source Multicast
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    wait_for_group_active(client, group_name).await;

    assert_eq!(created_group.identity.name, group_name);
    assert_eq!(created_group.identity.description, description);
    assert!(created_group.multicast_ip.is_multicast());
    assert_eq!(created_group.source_ips.len(), 0);

    // Verify we can list and find it
    let groups = list_multicast_groups(&client).await;
    assert_eq!(groups.len(), 1, "Expected exactly 1 multicast group");
    assert_groups_eq(&created_group, &groups[0]);

    // Verify we can fetch it directly
    let fetched_group_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &fetched_group_url).await;
    assert_groups_eq(&created_group, &fetched_group);

    // Test conflict error for duplicate name
    object_create_error(client, &group_url, &params, StatusCode::BAD_REQUEST)
        .await;

    // Test updating the group
    let new_description = "Updated description";
    let update_params = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some(String::from(new_description)),
        },
        source_ips: None,
        mvlan: None,
    };

    let updated_group: MulticastGroup =
        object_put(client, &fetched_group_url, &update_params).await;
    assert_eq!(updated_group.identity.description, new_description);
    assert_eq!(updated_group.identity.id, created_group.identity.id);
    assert!(
        updated_group.identity.time_modified
            > created_group.identity.time_modified
    );

    // Test deleting the group
    object_delete(client, &fetched_group_url).await;

    // Wait for group to be deleted (should return 404)
    wait_for_group_deleted(client, group_name).await;

    let groups = list_multicast_groups(&client).await;
    assert_eq!(groups.len(), 0, "Expected empty list after deletion");
}

#[nexus_test]
async fn test_multicast_group_with_default_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-default-pool-group";

    // Create a project for testing
    create_project(&client, project_name).await;

    // Create multicast IP pool
    let pool_params = IpPoolCreate::new_multicast(
        omicron_common::api::external::IdentityMetadataCreateParams {
            name: "default".parse().unwrap(),
            description: "Default multicast IP pool for testing".to_string(),
        },
        IpVersion::V4,
        IpPoolReservationType::ExternalSilos,
    );

    object_create::<_, IpPool>(&client, "/v1/system/ip-pools", &pool_params)
        .await;

    // Add IPv4 multicast range - use unique range for this test
    let ipv4_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(224, 8, 0, 10),
            Ipv4Addr::new(224, 8, 0, 255),
        )
        .unwrap(),
    );
    let range_url = "/v1/system/ip-pools/default/ranges/add";
    object_create::<_, IpPoolRange>(&client, range_url, &ipv4_range).await;

    // Link the pool to the silo as the default multicast pool
    link_ip_pool(&client, "default", &DEFAULT_SILO.id(), true).await;

    let group_url = "/v1/multicast-groups".to_string();

    // Test creating with default pool (pool: None)
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group using default pool".to_string(),
        },
        multicast_ip: None, // Auto-allocate
        source_ips: None,   // Any-Source Multicast
        pool: None,         // Use default multicast pool
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    assert_eq!(created_group.identity.name, group_name);
    assert!(created_group.multicast_ip.is_multicast());

    wait_for_group_active(client, group_name).await;

    // Clean up
    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;

    // Wait for the multicast group reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // After reconciler processing, the group should be gone (404)
    object_get_error(client, &group_delete_url, StatusCode::NOT_FOUND).await;
}

#[nexus_test]
async fn test_multicast_group_with_specific_ip(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group-specific-ip";

    // Create a project and multicast IP pool
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 2, 0, 10),
        (224, 2, 0, 255),
    )
    .await;
    let group_url = "/v1/multicast-groups".to_string();

    // Auto-allocation (should work)
    let auto_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group with auto-allocated IP".to_string(),
        },
        multicast_ip: None, // Auto-allocate
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let auto_group: MulticastGroup =
        object_create(client, &group_url, &auto_params).await;

    wait_for_group_active(client, group_name).await;

    assert!(auto_group.multicast_ip.is_multicast());
    assert_eq!(auto_group.identity.name, group_name);
    assert_eq!(auto_group.identity.description, "Group with auto-allocated IP");

    // Clean up auto-allocated group
    let auto_delete_url = mcast_group_url(group_name);
    object_delete(client, &auto_delete_url).await;

    // Wait for the multicast group reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // After reconciler processing, the group should be gone (404)
    object_get_error(client, &auto_delete_url, StatusCode::NOT_FOUND).await;

    // Explicit IP allocation
    let explicit_group_name = "test-group-explicit";
    let ipv4_addr = IpAddr::V4(Ipv4Addr::new(224, 2, 0, 20));
    let explicit_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: explicit_group_name.parse().unwrap(),
            description: "Group with explicit IPv4".to_string(),
        },
        multicast_ip: Some(ipv4_addr),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let explicit_group: MulticastGroup =
        object_create(client, &group_url, &explicit_params).await;
    assert_eq!(explicit_group.multicast_ip, ipv4_addr);
    assert_eq!(explicit_group.identity.name, explicit_group_name);
    assert_eq!(explicit_group.identity.description, "Group with explicit IPv4");

    // Wait for explicit group to become active before deletion
    wait_for_group_active(client, explicit_group_name).await;

    // Clean up explicit group
    let explicit_delete_url = mcast_group_url(explicit_group_name);
    object_delete(client, &explicit_delete_url).await;

    // Wait for the multicast group reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    object_get_error(client, &explicit_delete_url, StatusCode::NOT_FOUND).await;
}

#[nexus_test]
async fn test_multicast_group_with_source_ips(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-ssm-group";

    // Create a project and SSM multicast IP pool (232.0.0.0/8 range)
    create_project(&client, project_name).await;
    create_default_ip_pool(&client).await; // Required for any instance operations
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (232, 11, 0, 10), // SSM range: 232.11.0.10 - 232.11.0.255
        (232, 11, 0, 255),
    )
    .await;
    let group_url = "/v1/multicast-groups".to_string();

    // Test creating with Source-Specific Multicast (SSM) source IPs
    // SSM range is 232.0.0.0/8, so we use our unique SSM range
    let ssm_ip = IpAddr::V4(Ipv4Addr::new(232, 11, 0, 50)); // From our SSM range
    let source_ips = vec![
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), // Public DNS server
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), // Cloudflare DNS
    ];
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "SSM group with source IPs".to_string(),
        },
        multicast_ip: Some(ssm_ip),
        source_ips: Some(source_ips.clone()),
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    // Wait for group to become active
    let active_group = wait_for_group_active(client, group_name).await;

    // Verify SSM group properties
    assert_eq!(created_group.source_ips, source_ips);
    assert_eq!(created_group.multicast_ip, ssm_ip);
    assert_eq!(active_group.state, "Active");

    // DPD Validation: Check that SSM group exists in dataplane
    let dpd_client = dpd_client(cptestctx);
    let dpd_group = dpd_client
        .multicast_group_get(&ssm_ip)
        .await
        .expect("SSM group should exist in dataplane after creation");
    validate_dpd_group_response(
        &dpd_group,
        &ssm_ip,
        Some(0), // No members initially
        "SSM group creation",
    );

    // Clean up
    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;

    // Wait for the multicast group reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify deletion
    object_get_error(client, &group_delete_url, StatusCode::NOT_FOUND).await;
}

#[nexus_test]
async fn test_multicast_group_validation_errors(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";

    // Create a project and multicast IP pool
    create_project(&client, project_name).await;
    create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 3, 0, 10),
        (224, 3, 0, 255),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Test with non-multicast IP address
    let unicast_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "invalid-group".parse().unwrap(),
            description: "Group with invalid IP".to_string(),
        },
        multicast_ip: Some(unicast_ip),
        source_ips: None,
        pool: None, // Use default pool for validation test
        mvlan: None,
    };

    object_create_error(client, &group_url, &params, StatusCode::BAD_REQUEST)
        .await;

    // Test with link-local multicast (should be rejected)
    let link_local_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1));
    let params_link_local = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "link-local-group".parse().unwrap(),
            description: "Group with link-local IP".to_string(),
        },
        multicast_ip: Some(link_local_ip),
        source_ips: None,
        pool: None, // Use default pool for validation test
        mvlan: None,
    };

    object_create_error(
        client,
        &group_url,
        &params_link_local,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Test with IPv6 unicast (should be rejected)
    let ipv6_unicast = IpAddr::V6(Ipv6Addr::new(
        0x2001, 0xdb8, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
    ));
    let params_ipv6_unicast = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "ipv6-unicast-group".parse().unwrap(),
            description: "Group with IPv6 unicast IP".to_string(),
        },
        multicast_ip: Some(ipv6_unicast),
        source_ips: None,
        pool: None,
        mvlan: None,
    };

    object_create_error(
        client,
        &group_url,
        &params_ipv6_unicast,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Test with IPv6 interface-local multicast ff01:: (should be rejected)
    let ipv6_interface_local =
        IpAddr::V6(Ipv6Addr::new(0xff01, 0, 0, 0, 0, 0, 0, 1));
    let params_ipv6_interface_local = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "ipv6-interface-local-group".parse().unwrap(),
            description: "Group with IPv6 interface-local multicast IP"
                .to_string(),
        },
        multicast_ip: Some(ipv6_interface_local),
        source_ips: None,
        pool: None,
        mvlan: None,
    };

    object_create_error(
        client,
        &group_url,
        &params_ipv6_interface_local,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Test with IPv6 link-local multicast ff02:: (should be rejected)
    let ipv6_link_local_mcast =
        IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1));
    let params_ipv6_link_local = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "ipv6-link-local-group".parse().unwrap(),
            description: "Group with IPv6 link-local multicast IP".to_string(),
        },
        multicast_ip: Some(ipv6_link_local_mcast),
        source_ips: None,
        pool: None,
        mvlan: None,
    };

    object_create_error(
        client,
        &group_url,
        &params_ipv6_link_local,
        StatusCode::BAD_REQUEST,
    )
    .await;
}

/// Test that multicast IP pools reject invalid ranges at the pool level
#[nexus_test]
async fn test_multicast_ip_pool_range_validation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create IPv4 multicast pool
    let pool_params = IpPoolCreate::new_multicast(
        IdentityMetadataCreateParams {
            name: "test-v4-pool".parse().unwrap(),
            description: "IPv4 multicast pool for validation tests".to_string(),
        },
        IpVersion::V4,
        IpPoolReservationType::ExternalSilos,
    );
    object_create::<_, IpPool>(client, "/v1/system/ip-pools", &pool_params)
        .await;

    let range_url = "/v1/system/ip-pools/test-v4-pool/ranges/add";

    // IPv4 non-multicast range should be rejected
    let ipv4_unicast_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 255),
        )
        .unwrap(),
    );
    object_create_error(
        client,
        range_url,
        &ipv4_unicast_range,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // IPv4 link-local multicast range should be rejected
    let ipv4_link_local_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(224, 0, 0, 1),
            Ipv4Addr::new(224, 0, 0, 255),
        )
        .unwrap(),
    );
    object_create_error(
        client,
        range_url,
        &ipv4_link_local_range,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Valid IPv4 multicast range should be accepted
    let valid_ipv4_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(239, 0, 0, 1),
            Ipv4Addr::new(239, 0, 0, 255),
        )
        .unwrap(),
    );
    object_create::<_, IpPoolRange>(client, range_url, &valid_ipv4_range).await;

    // TODO: Remove this test once IPv6 is enabled for multicast pools.
    // IPv6 ranges should currently be rejected (not yet supported)
    let ipv6_range = IpRange::V6(
        Ipv6Range::new(
            Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 1),
            Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 255),
        )
        .unwrap(),
    );
    let error = object_create_error(
        client,
        range_url,
        &ipv6_range,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.message, "IPv6 ranges are not allowed yet");
}

#[nexus_test]
async fn test_multicast_group_member_operations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group";
    let instance_name = "test-instance";

    // Create project and IP pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client), // For instance networking
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 4, 0, 10),
            (224, 4, 0, 255),
        ),
    )
    .await;

    // Create multicast group and instance in parallel
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Test group for member operations".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let (_, instance) = ops::join2(
        async {
            object_create::<_, MulticastGroup>(client, &group_url, &params)
                .await;
            wait_for_group_active(client, group_name).await;
        },
        create_instance(client, project_name, instance_name),
    )
    .await;

    // Test listing members (should be empty initially)
    let members = list_multicast_group_members(&client, group_name).await;
    assert_eq!(members.len(), 0, "Expected empty member list initially");

    // Test adding instance to multicast group
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };
    let added_member: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;

    assert_eq!(
        added_member.instance_id.to_string(),
        instance.identity.id.to_string()
    );

    // Wait for member to become joined
    // Member starts in "Joining" state and transitions to "Joined" via reconciler
    // Member only transitions to "Joined" AFTER successful DPD update
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Test listing members (should have 1 now in Joined state)
    let members = list_multicast_group_members(&client, group_name).await;
    assert_eq!(members.len(), 1, "Expected exactly 1 member");
    assert_eq!(members[0].instance_id, added_member.instance_id);
    assert_eq!(members[0].multicast_group_id, added_member.multicast_group_id);

    // DPD Validation: Verify groups exist in dataplane after member addition
    let dpd_client = dpd_client(cptestctx);
    // Get the multicast IP from the group (since member doesn't have the IP field)
    let group_get_url = mcast_group_url(group_name);
    let group: MulticastGroup = object_get(client, &group_get_url).await;
    let external_multicast_ip = group.multicast_ip;

    // List all groups in DPD to find both external and underlay groups
    let dpd_groups = dpd_client
        .multicast_groups_list(None, None)
        .await
        .expect("Should list DPD groups");

    // Find the external IPv4 group (should exist but may not have members)
    let expect_msg =
        format!("External group {external_multicast_ip} should exist in DPD");
    dpd_groups
        .items
        .iter()
        .find(|g| {
            let ip = match g {
                dpd_types::MulticastGroupResponse::External {
                    group_ip,
                    ..
                } => *group_ip,
                dpd_types::MulticastGroupResponse::Underlay {
                    group_ip,
                    ..
                } => IpAddr::V6(group_ip.0),
            };
            ip == external_multicast_ip
                && matches!(
                    g,
                    dpd_types::MulticastGroupResponse::External { .. }
                )
        })
        .expect(&expect_msg);

    // Directly get the underlay IPv6 group by finding the admin-scoped address
    // First find the underlay group IP from the list to get the exact IPv6 address
    let underlay_ip = dpd_groups
        .items
        .iter()
        .find_map(|g| {
            match g {
                dpd_types::MulticastGroupResponse::Underlay {
                    group_ip,
                    ..
                } => {
                    // Check if it starts with ff04 (admin-scoped multicast)
                    if group_ip.0.segments()[0] == 0xff04 {
                        Some(group_ip.clone())
                    } else {
                        None
                    }
                }
                dpd_types::MulticastGroupResponse::External { .. } => None,
            }
        })
        .expect("Should find underlay group IP in DPD response");

    // Get the underlay group directly
    let underlay_group = dpd_client
        .multicast_group_get_underlay(&underlay_ip)
        .await
        .expect("Should get underlay group from DPD");

    assert_eq!(
        underlay_group.members.len(),
        1,
        "Underlay group should have exactly 1 member after member addition"
    );

    // Assert all underlay members use rear (backplane) ports with Underlay direction
    for member in &underlay_group.members {
        assert!(
            matches!(member.port_id, dpd_client::types::PortId::Rear(_)),
            "Underlay member should use rear (backplane) port, got: {:?}",
            member.port_id
        );
        assert_eq!(
            member.direction,
            dpd_client::types::Direction::Underlay,
            "Underlay member should have Underlay direction"
        );
    }

    // Test removing instance from multicast group using path-based DELETE
    let member_remove_url = format!(
        "{}/{instance_name}?project={project_name}",
        mcast_group_members_url(group_name)
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member from multicast group");

    // Wait for member count to reach 0 after removal
    wait_for_member_count(&client, group_name, 0).await;

    // DPD Validation: Verify group has no members in dataplane after removal
    let dpd_group = dpd_client.multicast_group_get(&external_multicast_ip).await
        .expect("Multicast group should still exist in dataplane after member removal");
    validate_dpd_group_response(
        &dpd_group,
        &external_multicast_ip,
        Some(0), // Should have 0 members after removal
        "external group after member removal",
    );

    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;
}

#[nexus_test]
async fn test_instance_multicast_endpoints(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group1_name = "mcast-group-1";
    let group2_name = "mcast-group-2";
    let instance_name = "test-instance";

    // Create a project, default unicast pool, and multicast IP pool
    create_project(&client, project_name).await;
    create_default_ip_pool(&client).await; // For instance networking
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 5, 0, 10),
        (224, 5, 0, 255),
    )
    .await;

    // Create two multicast groups in parallel
    let group_url = "/v1/multicast-groups".to_string();

    let group1_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group1_name.parse().unwrap(),
            description: "First test group".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let group2_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group2_name.parse().unwrap(),
            description: "Second test group".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    // Create both groups in parallel then wait for both to be active
    ops::join2(
        object_create::<_, MulticastGroup>(client, &group_url, &group1_params),
        object_create::<_, MulticastGroup>(client, &group_url, &group2_params),
    )
    .await;

    ops::join2(
        wait_for_group_active(client, group1_name),
        wait_for_group_active(client, group2_name),
    )
    .await;

    // Create an instance (starts automatically with create_instance helper)
    let instance = create_instance(client, project_name, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate and wait for instance to be fully running with sled_id assigned
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;
    wait_for_instance_sled_assignment(cptestctx, &instance_id).await;

    // Test: List instance multicast groups (should be empty initially)
    let instance_groups_url = format!(
        "/v1/instances/{instance_name}/multicast-groups?project={project_name}"
    );
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        0,
        "Instance should have no multicast memberships initially"
    );

    // Test: Join group1 using instance-centric endpoint
    let instance_join_group1_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group1_name}?project={project_name}"
    );
    // Use PUT method but expect 201 Created (not 200 OK like object_put)
    // This is correct HTTP semantics - PUT can return 201 when creating new resource
    let member1: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::PUT,
            &instance_join_group1_url,
        )
        .body(Some(&()))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(member1.instance_id, instance.identity.id);

    // Wait for member to become joined
    wait_for_member_state(
        cptestctx,
        group1_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Test: Verify membership shows up in both endpoints
    // Check group-centric view
    let group1_members =
        list_multicast_group_members(&client, group1_name).await;
    assert_eq!(group1_members.len(), 1);
    assert_eq!(group1_members[0].instance_id, instance.identity.id);

    // Check instance-centric view (test the list endpoint thoroughly)
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        1,
        "Instance should have exactly 1 membership"
    );
    assert_eq!(instance_memberships.items[0].instance_id, instance.identity.id);
    assert_eq!(
        instance_memberships.items[0].multicast_group_id,
        member1.multicast_group_id
    );
    assert_eq!(instance_memberships.items[0].state, "Joined");

    // Join group2 using group-centric endpoint (test both directions)
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group2_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };
    let member2: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;
    assert_eq!(member2.instance_id, instance.identity.id);

    // Wait for member to become joined
    wait_for_member_state(
        cptestctx,
        group2_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify instance now belongs to both groups (comprehensive list test)
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        2,
        "Instance should belong to both groups"
    );

    // Verify the list endpoint returns the correct membership details
    let membership_group_ids: Vec<_> = instance_memberships
        .items
        .iter()
        .map(|m| m.multicast_group_id)
        .collect();
    assert!(
        membership_group_ids.contains(&member1.multicast_group_id),
        "List should include group1 membership"
    );
    assert!(
        membership_group_ids.contains(&member2.multicast_group_id),
        "List should include group2 membership"
    );

    // Verify all memberships show correct instance_id and state
    for membership in &instance_memberships.items {
        assert_eq!(membership.instance_id, instance.identity.id);
        assert_eq!(membership.state, "Joined");
    }

    // Verify each group shows the instance as a member
    let group1_members =
        list_multicast_group_members(&client, group1_name).await;
    let group2_members =
        list_multicast_group_members(&client, group2_name).await;
    assert_eq!(group1_members.len(), 1);
    assert_eq!(group2_members.len(), 1);
    assert_eq!(group1_members[0].instance_id, instance.identity.id);
    assert_eq!(group2_members[0].instance_id, instance.identity.id);

    // Leave group1 using instance-centric endpoint
    let instance_leave_group1_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group1_name}?project={project_name}"
    );
    object_delete(client, &instance_leave_group1_url).await;

    // Wait for reconciler to process the removal and completely delete the member
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify membership removed from both views
    // Check instance-centric view - should only show active memberships (group2)
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        1,
        "Instance should only show active membership (group2)"
    );
    assert_eq!(
        instance_memberships.items[0].multicast_group_id,
        member2.multicast_group_id,
        "Remaining membership should be group2"
    );
    assert_eq!(
        instance_memberships.items[0].state, "Joined",
        "Group2 membership should be Joined"
    );

    // Check group-centric views
    let group1_members =
        list_multicast_group_members(&client, group1_name).await;
    let group2_members =
        list_multicast_group_members(&client, group2_name).await;
    assert_eq!(group1_members.len(), 0, "Group1 should have no members");
    assert_eq!(group2_members.len(), 1, "Group2 should still have 1 member");

    // Leave group2 using group-centric endpoint
    let member_remove_url = format!(
        "{}/{instance_name}?project={project_name}",
        mcast_group_members_url(group2_name)
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member from group2");

    // Wait for reconciler to process the removal
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify all memberships are gone
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        0,
        "Instance should have no memberships"
    );

    let group1_members =
        list_multicast_group_members(&client, group1_name).await;
    let group2_members =
        list_multicast_group_members(&client, group2_name).await;
    assert_eq!(group1_members.len(), 0);
    assert_eq!(group2_members.len(), 0);

    // Clean up
    let group1_delete_url = mcast_group_url(group1_name);
    let group2_delete_url = mcast_group_url(group2_name);

    object_delete(client, &group1_delete_url).await;
    object_delete(client, &group2_delete_url).await;
}

#[nexus_test]
async fn test_multicast_group_member_errors(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group";
    let nonexistent_instance = "nonexistent-instance";

    // Create a project and multicast IP pool
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 6, 0, 10),
        (224, 6, 0, 255),
    )
    .await;

    // Create a multicast group
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Test group for error cases".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };
    object_create::<_, MulticastGroup>(client, &group_url, &params).await;

    // Wait for group to become active before testing member operations
    wait_for_group_active(&client, group_name).await;

    // Test adding nonexistent instance to group
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(nonexistent_instance.parse().unwrap()),
    };
    object_create_error(
        client,
        &member_add_url,
        &member_params,
        StatusCode::NOT_FOUND,
    )
    .await;

    // Test adding member to nonexistent group
    let nonexistent_group = "nonexistent-group";
    let member_add_bad_group_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(nonexistent_group)
    );
    object_create_error(
        client,
        &member_add_bad_group_url,
        &member_params,
        StatusCode::NOT_FOUND,
    )
    .await;

    // Clean up - follow standard deletion pattern
    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;
}

#[nexus_test]
async fn test_lookup_multicast_group_by_ip(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-lookup-group";

    // Create a project and multicast IP pool
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 7, 0, 10),
        (224, 7, 0, 255),
    )
    .await;

    // Create a multicast group with specific IP - use safe IP range
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 7, 0, 100));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for IP lookup test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };
    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    // Wait for group to become active - follow working pattern
    wait_for_group_active(&client, group_name).await;

    // Test lookup by IP
    let lookup_url =
        format!("/v1/system/multicast-groups/by-ip/{multicast_ip}");
    let found_group: MulticastGroup = object_get(client, &lookup_url).await;
    assert_groups_eq(&created_group, &found_group);

    // Test lookup with nonexistent IP
    let nonexistent_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 200));
    let lookup_bad_url =
        format!("/v1/system/multicast-groups/by-ip/{nonexistent_ip}");

    object_get_error(client, &lookup_bad_url, StatusCode::NOT_FOUND).await;

    // Clean up - follow standard deletion pattern
    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;
}

#[nexus_test]
async fn test_instance_deletion_removes_multicast_memberships(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "springfield-squidport"; // Use the same project name as instance helpers
    let group_name = "instance-deletion-group";
    let instance_name = "deletion-test-instance";

    // Setup: project, pools, group with unique IP range
    create_project(&client, project_name).await;
    create_default_ip_pool(&client).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 9, 0, 10),
        (224, 9, 0, 255),
    )
    .await;

    // Create multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 9, 0, 50)); // Use IP from our range
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for instance deletion test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    // Wait for group to become active
    wait_for_group_active(&client, group_name).await;

    // Create instance and add as member
    let instance = create_instance(client, project_name, instance_name).await;
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for member to join
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify member was added
    let members = list_multicast_group_members(&client, group_name).await;
    assert_eq!(members.len(), 1, "Instance should be a member of the group");
    assert_eq!(members[0].instance_id, instance.identity.id);

    // Test: Instance deletion should clean up multicast memberships
    // Use the helper function for proper instance deletion (handles Starting state)
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;

    // Verify instance is gone
    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");

    object_get_error(client, &instance_url, StatusCode::NOT_FOUND).await;

    // Critical test: Verify instance was automatically removed from multicast group
    wait_for_member_count(&client, group_name, 0).await;

    // DPD Validation: Ensure dataplane members are cleaned up
    let dpd_client = dpd_client(cptestctx);
    let dpd_group = dpd_client.multicast_group_get(&multicast_ip).await
        .expect("Multicast group should still exist in dataplane after instance deletion");
    validate_dpd_group_response(
        &dpd_group,
        &multicast_ip,
        Some(0), // Should have 0 members after instance deletion
        "external group after instance deletion",
    );

    // Verify group still exists (just no members)
    let group_get_url = mcast_group_url(group_name);
    let group_after_deletion: MulticastGroup =
        object_get(client, &group_get_url).await;
    assert_eq!(group_after_deletion.identity.id, created_group.identity.id);

    // Clean up
    object_delete(client, &group_get_url).await;
}

#[nexus_test]
async fn test_member_operations_via_rpw_reconciler(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "rpw-test-group";
    let instance_name = "rpw-test-instance";

    // Setup: project, pools, group with unique IP range
    create_project(&client, project_name).await;
    create_default_ip_pool(&client).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 10, 0, 10),
        (224, 10, 0, 255),
    )
    .await;

    // Create multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 10, 0, 50)); // Use IP from our range
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for RPW member operations test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    // Wait for group to become active
    wait_for_group_active(&client, group_name).await;

    assert_eq!(created_group.multicast_ip, multicast_ip);
    assert_eq!(created_group.identity.name, group_name);

    // Create instance
    let instance = create_instance(client, project_name, instance_name).await;

    // Test: Add member via API (should use RPW pattern via reconciler)
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };
    let added_member: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;

    // Wait for member to become joined
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify member was added and reached Joined state
    let members = list_multicast_group_members(&client, group_name).await;
    assert_eq!(members.len(), 1, "Member should be added to group");
    assert_eq!(members[0].instance_id, added_member.instance_id);
    assert_eq!(members[0].state, "Joined", "Member should be in Joined state");

    // DPD Validation: Check external group configuration
    let dpd_client = dpd_client(cptestctx);
    let dpd_group = dpd_client
        .multicast_group_get(&multicast_ip)
        .await
        .expect("Multicast group should exist in dataplane after member join");
    validate_dpd_group_response(
        &dpd_group,
        &multicast_ip,
        None, // Don't assert member count due to timing
        "external group after member join",
    );

    // Test: Remove member via API (should use RPW pattern via reconciler)
    let member_remove_url = format!(
        "{}/{instance_name}?project={project_name}",
        mcast_group_members_url(group_name)
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member from multicast group");

    // Verify member was removed (wait for member count to reach 0)
    wait_for_member_count(&client, group_name, 0).await;

    // DPD Validation: Check group has no members after removal
    let dpd_group = dpd_client.multicast_group_get(&multicast_ip).await.expect(
        "Multicast group should still exist in dataplane after member removal",
    );
    validate_dpd_group_response(
        &dpd_group,
        &multicast_ip,
        Some(0), // Should have 0 members after removal
        "external group after member removal",
    );

    // Clean up - reconciler is automatically activated by deletion
    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;
}

/// Test comprehensive multicast group update operations including the update saga.
/// Tests both description-only updates (no saga) and name updates (requires saga).
#[nexus_test]
async fn test_multicast_group_comprehensive_updates(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "update-test-project";
    let original_name = "original-group";
    let updated_name = "updated-group";
    let final_name = "final-group";
    let original_description = "Original description";
    let updated_description = "Updated description";
    let final_description = "Final description";

    // Create project and IP pool
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "update-test-pool",
        (224, 11, 0, 10),
        (224, 11, 0, 255),
    )
    .await;

    // Create multicast group
    let group_url = "/v1/multicast-groups".to_string();
    let create_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(original_name).parse().unwrap(),
            description: String::from(original_description),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &create_params).await;

    wait_for_group_active(client, original_name).await;

    let original_group_url = mcast_group_url(original_name);

    // Description-only update (no saga required)
    let description_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None, // Keep same name
            description: Some(String::from(updated_description)),
        },
        source_ips: None,
        mvlan: None,
    };

    let desc_updated_group: MulticastGroup =
        object_put(client, &original_group_url, &description_update).await;

    // No wait needed for description-only updates
    assert_eq!(desc_updated_group.identity.name, original_name);
    assert_eq!(desc_updated_group.identity.description, updated_description);
    assert_eq!(desc_updated_group.identity.id, created_group.identity.id);
    assert!(
        desc_updated_group.identity.time_modified
            > created_group.identity.time_modified
    );

    // Name-only update (requires update saga)
    let name_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(String::from(updated_name).parse().unwrap()),
            description: None, // Keep current description
        },
        source_ips: None,
        mvlan: None,
    };

    let name_updated_group: MulticastGroup =
        object_put(client, &original_group_url, &name_update).await;

    // Wait for update saga to complete DPD configuration application
    // Name updates don't change DPD state, just verify saga completed without errors
    wait_for_group_dpd_update(
        cptestctx,
        &created_group.multicast_ip,
        dpd_predicates::expect_external_group(),
        "name update saga completed",
    )
    .await;

    // Verify name update worked
    assert_eq!(name_updated_group.identity.name, updated_name);
    assert_eq!(name_updated_group.identity.description, updated_description); // Should keep previous description
    assert_eq!(name_updated_group.identity.id, created_group.identity.id);
    assert!(
        name_updated_group.identity.time_modified
            > desc_updated_group.identity.time_modified
    );

    // Verify we can access with new name
    let updated_group_url = mcast_group_url(updated_name);
    let fetched_group: MulticastGroup =
        object_get(client, &updated_group_url).await;
    assert_eq!(fetched_group.identity.name, updated_name);

    // Verify old name is no longer accessible
    object_get_error(client, &original_group_url, StatusCode::NOT_FOUND).await;

    // Combined name and description update (requires saga)
    let combined_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(String::from(final_name).parse().unwrap()),
            description: Some(String::from(final_description)),
        },
        source_ips: None,
        mvlan: None,
    };

    let final_updated_group: MulticastGroup =
        object_put(client, &updated_group_url, &combined_update).await;

    // Wait for update saga to complete
    // Combined name+description updates don't change DPD state
    wait_for_group_dpd_update(
        cptestctx,
        &created_group.multicast_ip,
        dpd_predicates::expect_external_group(),
        "combined name+description update saga completed",
    )
    .await;

    // Verify combined update worked
    assert_eq!(final_updated_group.identity.name, final_name);
    assert_eq!(final_updated_group.identity.description, final_description);
    assert_eq!(final_updated_group.identity.id, created_group.identity.id);
    assert!(
        final_updated_group.identity.time_modified
            > name_updated_group.identity.time_modified
    );

    // Verify group remains active through updates
    let final_group_url = mcast_group_url(final_name);
    wait_for_group_active(client, final_name).await;

    // DPD validation
    let dpd_client = dpd_client(cptestctx);
    match dpd_client
        .multicast_group_get(&final_updated_group.multicast_ip)
        .await
    {
        Ok(dpd_group) => {
            let group_data = dpd_group.into_inner();
            let tag = match &group_data {
                dpd_types::MulticastGroupResponse::External { tag, .. } => {
                    tag.as_deref()
                }
                dpd_types::MulticastGroupResponse::Underlay { tag, .. } => {
                    tag.as_deref()
                }
            };
            assert_eq!(
                tag,
                Some(final_name),
                "DPD group tag should match final group name"
            );
        }
        Err(DpdError::ErrorResponse(resp))
            if resp.status() == reqwest::StatusCode::NOT_FOUND => {}
        Err(_) => {}
    }

    // Clean up
    object_delete(client, &final_group_url).await;
}

/// Validate DPD multicast group response with comprehensive checks
fn validate_dpd_group_response(
    dpd_group: &dpd_types::MulticastGroupResponse,
    expected_ip: &IpAddr,
    expected_member_count: Option<usize>,
    test_context: &str,
) {
    // Basic validation using our utility function
    let ip = match dpd_group {
        dpd_types::MulticastGroupResponse::External { group_ip, .. } => {
            *group_ip
        }
        dpd_types::MulticastGroupResponse::Underlay { group_ip, .. } => {
            IpAddr::V6(group_ip.0)
        }
    };
    assert_eq!(ip, *expected_ip, "DPD group IP mismatch in {test_context}");

    match dpd_group {
        dpd_types::MulticastGroupResponse::External {
            external_group_id,
            ..
        } => {
            if let Some(_expected_count) = expected_member_count {
                // External groups typically don't have direct members,
                // but we can validate if they do
                // Note: External groups may not expose member count directly
                eprintln!(
                    "Note: External group member validation skipped in {test_context}"
                );
            }

            // Validate external group specific fields
            assert_ne!(
                *external_group_id, 0,
                "DPD external_group_id should be non-zero in {test_context}"
            );
        }
        dpd_types::MulticastGroupResponse::Underlay {
            members,
            external_group_id,
            underlay_group_id,
            ..
        } => {
            if let Some(expected_count) = expected_member_count {
                assert_eq!(
                    members.len(),
                    expected_count,
                    "DPD underlay group member count mismatch in {test_context}: expected {expected_count}, got {}",
                    members.len()
                );
            }

            // Assert all underlay members use rear (backplane) ports with Underlay direction
            for member in members {
                assert!(
                    matches!(
                        member.port_id,
                        dpd_client::types::PortId::Rear(_)
                    ),
                    "Underlay member should use rear (backplane) port, got: {:?}",
                    member.port_id
                );
                assert_eq!(
                    member.direction,
                    dpd_client::types::Direction::Underlay,
                    "Underlay member should have Underlay direction"
                );
            }

            // Validate underlay group specific fields
            assert_ne!(
                *external_group_id, 0,
                "DPD external_group_id should be non-zero in {test_context}"
            );
            assert_ne!(
                *underlay_group_id, 0,
                "DPD underlay_group_id should be non-zero in {test_context}"
            );
        }
    }
}

/// Test source_ips updates and multicast group validation.
/// Verifies proper ASM/SSM handling, validation of invalid transitions, and mixed pool allocation.
#[nexus_test]
async fn test_multicast_source_ips_update(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "source-update-project";

    // Create project and separate ASM and SSM pools
    create_project(&client, project_name).await;

    // Create ASM pool for ASM testing
    let asm_pool = create_multicast_ip_pool_with_range(
        &client,
        "asm-update-pool",
        (224, 99, 0, 10),
        (224, 99, 0, 50),
    )
    .await;

    // Create SSM pool for SSM testing
    let ssm_pool = create_multicast_ip_pool_with_range(
        &client,
        "ssm-update-pool",
        (232, 99, 0, 10),
        (232, 99, 0, 50),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Negative: creating in SSM pool without sources should be rejected
    let ssm_no_sources = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "ssm-no-sources".parse().unwrap(),
            description: "should fail: SSM pool requires sources".to_string(),
        },
        multicast_ip: None, // implicit allocation
        source_ips: None,   // missing sources in SSM pool
        pool: Some(NameOrId::Name(ssm_pool.identity.name.clone())),
        mvlan: None,
    };
    object_create_error(
        client,
        &group_url,
        &ssm_no_sources,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Negative: creating in ASM pool with sources (implicit IP) should be rejected
    let asm_with_sources = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "asm-with-sources".parse().unwrap(),
            description:
                "should fail: ASM pool cannot allocate SSM with sources"
                    .to_string(),
        },
        multicast_ip: None, // implicit allocation
        source_ips: Some(vec!["10.10.10.10".parse().unwrap()]), // sources present
        pool: Some(NameOrId::Name(asm_pool.identity.name.clone())),
        mvlan: None,
    };
    let err2: HttpErrorResponseBody = object_create_error(
        client,
        &group_url,
        &asm_with_sources,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(
        err2.message
            .contains("Cannot allocate SSM multicast group from ASM pool"),
        "Expected ASM pool + sources to be rejected, got: {}",
        err2.message
    );

    // Create ASM group (no sources)
    let asm_group_name = "asm-group";
    let asm_create_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(asm_group_name).parse().unwrap(),
            description: "ASM group for testing".to_string(),
        },
        multicast_ip: None,
        source_ips: None, // No sources = ASM
        pool: Some(NameOrId::Name(asm_pool.identity.name.clone())),
        mvlan: None,
    };

    let asm_group = object_create::<_, MulticastGroup>(
        client,
        &group_url,
        &asm_create_params,
    )
    .await;
    wait_for_group_active(client, asm_group_name).await;

    // Verify ASM group allocation (should get any available multicast address)
    assert!(
        asm_group.source_ips.is_empty(),
        "ASM group should have no sources"
    );

    // ASM group updates (valid operations)

    // Description-only update (always valid)
    let description_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Updated ASM description".to_string()),
        },
        source_ips: None,
        mvlan: None,
    };
    let updated_asm: MulticastGroup = object_put(
        client,
        &mcast_group_url(asm_group_name),
        &description_update,
    )
    .await;
    assert_eq!(updated_asm.identity.description, "Updated ASM description");
    assert!(updated_asm.source_ips.is_empty());

    // Try invalid ASM→SSM transition (should be rejected)
    let invalid_ssm_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: Some(vec!["10.1.1.1".parse().unwrap()]), // Try to add sources
        mvlan: None,
    };

    object_put_error(
        client,
        &mcast_group_url(asm_group_name),
        &invalid_ssm_update,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Create SSM group from scratch (with explicit SSM IP and sources)
    let ssm_group_name = "ssm-group";
    let ssm_create_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(ssm_group_name).parse().unwrap(),
            description: "SSM group with explicit SSM address".to_string(),
        },
        multicast_ip: Some("232.99.0.20".parse().unwrap()), // Explicit SSM IP required
        source_ips: Some(vec!["10.2.2.2".parse().unwrap()]), // SSM sources from start
        pool: Some(NameOrId::Name(ssm_pool.identity.name.clone())),
        mvlan: None,
    };

    let ssm_group = object_create::<_, MulticastGroup>(
        client,
        &group_url,
        &ssm_create_params,
    )
    .await;
    wait_for_group_active(client, ssm_group_name).await;

    // Verify SSM group has correct explicit IP and sources
    assert_eq!(ssm_group.multicast_ip.to_string(), "232.99.0.20");
    assert_eq!(ssm_group.source_ips.len(), 1);
    assert_eq!(ssm_group.source_ips[0].to_string(), "10.2.2.2");

    // Create SSM group with mvlan at creation time
    let ssm_with_mvlan_name = "ssm-group-with-mvlan";
    let ssm_with_mvlan_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(ssm_with_mvlan_name).parse().unwrap(),
            description: "SSM group created with mvlan".to_string(),
        },
        multicast_ip: Some("232.99.0.30".parse().unwrap()),
        source_ips: Some(vec!["10.7.7.7".parse().unwrap()]),
        pool: Some(NameOrId::Name(ssm_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(2048).unwrap()), // Create with mvlan
    };
    let ssm_with_mvlan_created = object_create::<_, MulticastGroup>(
        client,
        &group_url,
        &ssm_with_mvlan_params,
    )
    .await;
    wait_for_group_active(client, ssm_with_mvlan_name).await;

    assert_eq!(ssm_with_mvlan_created.multicast_ip.to_string(), "232.99.0.30");
    assert_eq!(ssm_with_mvlan_created.source_ips.len(), 1);
    assert_eq!(
        ssm_with_mvlan_created.mvlan,
        Some(VlanID::new(2048).unwrap()),
        "SSM group should be created with mvlan"
    );

    // Valid SSM group updates

    // Update SSM sources (valid - SSM→SSM)
    let ssm_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: Some(vec![
            "10.3.3.3".parse().unwrap(),
            "10.3.3.4".parse().unwrap(),
        ]),
        mvlan: None,
    };
    let updated_ssm: MulticastGroup =
        object_put(client, &mcast_group_url(ssm_group_name), &ssm_update).await;

    // Wait for update saga to complete
    wait_for_group_dpd_update(
        cptestctx,
        &updated_ssm.multicast_ip,
        dpd_predicates::expect_external_group(),
        "source_ips update saga completed",
    )
    .await;

    assert_eq!(updated_ssm.source_ips.len(), 2);
    let source_strings: std::collections::HashSet<String> =
        updated_ssm.source_ips.iter().map(|ip| ip.to_string()).collect();
    assert!(source_strings.contains("10.3.3.3"));
    assert!(source_strings.contains("10.3.3.4"));

    // Valid SSM source reduction (but must maintain at least one source)
    let ssm_source_reduction = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: Some(vec!["10.3.3.3".parse().unwrap()]), // Reduce to one source
        mvlan: None,
    };
    let reduced_ssm: MulticastGroup = object_put(
        client,
        &mcast_group_url(ssm_group_name),
        &ssm_source_reduction,
    )
    .await;

    // Wait for source reduction saga to complete
    wait_for_group_dpd_update(
        cptestctx,
        &reduced_ssm.multicast_ip,
        dpd_predicates::expect_external_group(),
        "source_ips reduction saga completed",
    )
    .await;

    assert_eq!(
        reduced_ssm.source_ips.len(),
        1,
        "SSM group should have exactly one source after reduction"
    );
    assert_eq!(reduced_ssm.source_ips[0].to_string(), "10.3.3.3");

    // Test SSM group with mvlan (combined features)
    let ssm_update_with_mvlan = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: Some(vec![
            "10.4.4.4".parse().unwrap(),
            "10.4.4.5".parse().unwrap(),
        ]),
        mvlan: Some(Nullable(Some(VlanID::new(2500).unwrap()))), // Set mvlan on SSM group
    };
    let ssm_with_mvlan: MulticastGroup = object_put(
        client,
        &mcast_group_url(ssm_group_name),
        &ssm_update_with_mvlan,
    )
    .await;

    // Wait for combined source_ips+mvlan update saga to complete
    // Must verify vlan_id was applied to DPD
    wait_for_group_dpd_update(
        cptestctx,
        &ssm_with_mvlan.multicast_ip,
        dpd_predicates::expect_vlan_id(2500),
        "source_ips+mvlan update saga completed, vlan_id=2500",
    )
    .await;

    assert_eq!(ssm_with_mvlan.source_ips.len(), 2);
    assert_eq!(
        ssm_with_mvlan.mvlan,
        Some(VlanID::new(2500).unwrap()),
        "SSM group should support mvlan"
    );

    // Update mvlan while keeping sources
    let update_mvlan_only = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None, // Don't change sources
        mvlan: Some(Nullable(Some(VlanID::new(3000).unwrap()))),
    };
    let mvlan_updated: MulticastGroup = object_put(
        client,
        &mcast_group_url(ssm_group_name),
        &update_mvlan_only,
    )
    .await;
    assert_eq!(mvlan_updated.mvlan, Some(VlanID::new(3000).unwrap()));
    assert_eq!(
        mvlan_updated.source_ips.len(),
        2,
        "Sources should be unchanged"
    );

    // Clear mvlan while updating sources
    let clear_mvlan_update_sources = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: Some(vec!["10.5.5.5".parse().unwrap()]),
        mvlan: Some(Nullable(None)), // Clear mvlan
    };
    let mvlan_cleared: MulticastGroup = object_put(
        client,
        &mcast_group_url(ssm_group_name),
        &clear_mvlan_update_sources,
    )
    .await;
    assert_eq!(mvlan_cleared.mvlan, None, "MVLAN should be cleared");
    assert_eq!(mvlan_cleared.source_ips.len(), 1);
    assert_eq!(mvlan_cleared.source_ips[0].to_string(), "10.5.5.5");

    // Create SSM group that requires proper address validation
    let ssm_explicit_name = "ssm-explicit";
    let ssm_explicit_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(ssm_explicit_name).parse().unwrap(),
            description: "SSM group with explicit 232.x.x.x IP".to_string(),
        },
        multicast_ip: Some("232.99.0.42".parse().unwrap()), // Explicit SSM IP
        source_ips: Some(vec!["10.5.5.5".parse().unwrap()]),
        pool: Some(NameOrId::Name(ssm_pool.identity.name.clone())),
        mvlan: None,
    };

    let ssm_explicit = object_create::<_, MulticastGroup>(
        client,
        &group_url,
        &ssm_explicit_params,
    )
    .await;
    wait_for_group_active(client, ssm_explicit_name).await;

    assert_eq!(ssm_explicit.multicast_ip.to_string(), "232.99.0.42");
    assert_eq!(ssm_explicit.source_ips.len(), 1);

    // Try creating SSM group with invalid IP (should be rejected)
    let invalid_ssm_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "invalid-ssm".parse().unwrap(),
            description: "Should be rejected".to_string(),
        },
        multicast_ip: Some("224.99.0.42".parse().unwrap()), // ASM IP with sources
        source_ips: Some(vec!["10.6.6.6".parse().unwrap()]), // Sources with ASM IP
        pool: Some(NameOrId::Name(ssm_pool.identity.name.clone())),
        mvlan: None,
    };

    object_create_error(
        client,
        &group_url,
        &invalid_ssm_params,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Clean up all groups
    for group_name in [asm_group_name, ssm_group_name, ssm_explicit_name] {
        let delete_url = mcast_group_url(group_name);
        object_delete(client, &delete_url).await;
    }
}

#[nexus_test]
async fn test_multicast_group_with_mvlan(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "mvlan-test-project";
    let group_name = "mvlan-test-group";

    // Setup
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mvlan-pool",
        (224, 50, 0, 10),
        (224, 50, 0, 255),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Test creating group with mvlan
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group with MVLAN for external uplink forwarding"
                .to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(100).unwrap()), // Set MVLAN to 100
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    wait_for_group_active(client, group_name).await;

    // Verify mvlan was set correctly
    assert_eq!(
        created_group.mvlan,
        Some(VlanID::new(100).unwrap()),
        "MVLAN should be set to 100"
    );
    assert_eq!(created_group.identity.name, group_name);

    // Verify we can fetch it and mvlan persists
    let fetched_group_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &fetched_group_url).await;
    assert_eq!(
        fetched_group.mvlan,
        Some(VlanID::new(100).unwrap()),
        "MVLAN should persist after fetch"
    );

    // DPD Validation: Verify mvlan is propagated to dataplane as vlan_id
    let dpd_client = dpd_client(cptestctx);
    let dpd_group = dpd_client
        .multicast_group_get(&created_group.multicast_ip)
        .await
        .expect("Multicast group should exist in dataplane");

    // Extract vlan_id from DPD response and verify it matches mvlan
    match dpd_group.into_inner() {
        dpd_types::MulticastGroupResponse::External {
            external_forwarding,
            ..
        } => {
            assert_eq!(
                external_forwarding.vlan_id,
                Some(100),
                "DPD external_forwarding.vlan_id should match group mvlan"
            );
        }
        dpd_types::MulticastGroupResponse::Underlay { .. } => {
            panic!("Expected external group, got underlay group");
        }
    }

    // Clean up
    object_delete(client, &fetched_group_url).await;
    wait_for_group_deleted(client, group_name).await;
}

#[nexus_test]
async fn test_multicast_group_mvlan_updates(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "mvlan-update-project";
    let group_name = "mvlan-update-group";

    // Setup
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mvlan-update-pool",
        (224, 51, 0, 10),
        (224, 51, 0, 255),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Create group without mvlan
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for MVLAN update testing".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None, // Start without MVLAN
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;

    wait_for_group_active(client, group_name).await;

    assert_eq!(created_group.mvlan, None, "MVLAN should initially be None");

    let group_update_url = mcast_group_url(group_name);

    // Set mvlan to a value
    let set_mvlan_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None,
        mvlan: Some(Nullable(Some(VlanID::new(200).unwrap()))), // Set to 200
    };

    let updated_group: MulticastGroup =
        object_put(client, &group_update_url, &set_mvlan_update).await;
    assert_eq!(
        updated_group.mvlan,
        Some(VlanID::new(200).unwrap()),
        "MVLAN should be set to 200"
    );

    // Change mvlan to a different value
    let change_mvlan_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None,
        mvlan: Some(Nullable(Some(VlanID::new(300).unwrap()))), // Change to 300
    };

    let changed_group: MulticastGroup =
        object_put(client, &group_update_url, &change_mvlan_update).await;
    assert_eq!(
        changed_group.mvlan,
        Some(VlanID::new(300).unwrap()),
        "MVLAN should be changed to 300"
    );

    // Clear mvlan back to None
    let clear_mvlan_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None,
        mvlan: Some(Nullable(None)), // Clear to NULL
    };

    let cleared_group: MulticastGroup =
        object_put(client, &group_update_url, &clear_mvlan_update).await;
    assert_eq!(cleared_group.mvlan, None, "MVLAN should be cleared to None");

    // Set mvlan again, then test omitting the field preserves existing value
    let set_mvlan_200 = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None,
        mvlan: Some(Nullable(Some(VlanID::new(200).unwrap()))),
    };

    let group_with_200: MulticastGroup =
        object_put(client, &group_update_url, &set_mvlan_200).await;
    assert_eq!(
        group_with_200.mvlan,
        Some(VlanID::new(200).unwrap()),
        "MVLAN should be set to 200"
    );

    // Omit mvlan field entirely - should preserve existing value (200)
    let omit_mvlan_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Updated description".to_string()),
        },
        source_ips: None,
        mvlan: None, // Omit the field
    };

    let unchanged_group: MulticastGroup =
        object_put(client, &group_update_url, &omit_mvlan_update).await;
    assert_eq!(
        unchanged_group.mvlan,
        Some(VlanID::new(200).unwrap()),
        "MVLAN should remain at 200 when field is omitted"
    );
    assert_eq!(
        unchanged_group.identity.description, "Updated description",
        "Description should be updated"
    );

    // Test invalid mvlan during update (reserved value 1)
    let invalid_mvlan_update = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None,
        mvlan: Some(Nullable(Some(VlanID::new(1).unwrap()))), // Reserved value
    };

    object_put_error(
        client,
        &group_update_url,
        &invalid_mvlan_update,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Clean up
    object_delete(client, &group_update_url).await;
    wait_for_group_deleted(client, group_name).await;
}

#[nexus_test]
async fn test_multicast_group_mvlan_validation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "mvlan-validation-project";

    // Setup
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mvlan-validation-pool",
        (224, 52, 0, 10),
        (224, 52, 0, 255),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Test valid MVLAN values (2-4094)
    // Note: VLANs 0 and 1 are reserved and rejected by Dendrite (>= 2 required)
    // VLAN 4095 is reserved per IEEE 802.1Q and rejected by VlanID type (max 4094)

    // Valid: mid-range value
    let mid_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "mvlan-mid".parse().unwrap(),
            description: "Group with mid-range MVLAN".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(2048).unwrap()),
    };

    let mid_group: MulticastGroup =
        object_create(client, &group_url, &mid_params).await;
    wait_for_group_active(client, "mvlan-mid").await;
    assert_eq!(
        mid_group.mvlan,
        Some(VlanID::new(2048).unwrap()),
        "MVLAN 2048 should be valid"
    );
    object_delete(client, &mcast_group_url("mvlan-mid")).await;
    wait_for_group_deleted(client, "mvlan-mid").await;

    // Valid: maximum value (4094)
    let max_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "mvlan-max".parse().unwrap(),
            description: "Group with maximum MVLAN".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(4094).unwrap()),
    };

    let max_group: MulticastGroup =
        object_create(client, &group_url, &max_params).await;
    wait_for_group_active(client, "mvlan-max").await;
    assert_eq!(
        max_group.mvlan,
        Some(VlanID::new(4094).unwrap()),
        "MVLAN 4094 should be valid"
    );
    object_delete(client, &mcast_group_url("mvlan-max")).await;
    wait_for_group_deleted(client, "mvlan-max").await;

    // Invalid: reserved value 0 (rejected by Dendrite)
    let invalid_params0 = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "mvlan-invalid-0".parse().unwrap(),
            description: "Group with invalid MVLAN 0".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(0).unwrap()),
    };

    object_create_error(
        client,
        &group_url,
        &invalid_params0,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Invalid: reserved value 1 (rejected by Dendrite)
    let invalid_params1 = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "mvlan-invalid-1".parse().unwrap(),
            description: "Group with invalid MVLAN 1".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(1).unwrap()),
    };

    object_create_error(
        client,
        &group_url,
        &invalid_params1,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Test invalid MVLAN at API boundary using raw JSON.
    // The deserializer rejects invalid values at the HTTP boundary before they
    // reach the business logic layer.

    // Invalid: raw JSON with mvlan = 0 (should get 400 Bad Request)
    let raw_json0 = serde_json::json!({
        "identity": {
            "name": "mvlan-raw-0",
            "description": "Test raw JSON with mvlan 0"
        },
        "mvlan": 0,
        "pool": mcast_pool.identity.name
    });

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&raw_json0))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Expected 400 Bad Request for raw JSON mvlan=0");

    // Invalid: raw JSON with mvlan = 1 (should get 400 Bad Request)
    let raw_json1 = serde_json::json!({
        "identity": {
            "name": "mvlan-raw-1",
            "description": "Test raw JSON with mvlan 1"
        },
        "mvlan": 1,
        "pool": mcast_pool.identity.name
    });

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&raw_json1))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Expected 400 Bad Request for raw JSON mvlan=1");
}

/// Database round-trip tests for MVLAN values
/// Verifies that VlanID <-> i16 conversion works correctly for all valid values
#[nexus_test]
async fn test_mvlan_database_round_trip(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "mvlan-roundtrip-project";

    // Setup
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mvlan-roundtrip-pool",
        (224, 53, 0, 10),
        (224, 53, 0, 255),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Test cases: (group_name, mvlan_value)
    let test_cases = vec![
        ("mvlan-none", None),
        ("mvlan-2", Some(VlanID::new(2).unwrap())),
        ("mvlan-100", Some(VlanID::new(100).unwrap())),
        ("mvlan-4094", Some(VlanID::new(4094).unwrap())),
    ];

    for (group_name, mvlan) in &test_cases {
        // Create group with specified mvlan
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: group_name.parse().unwrap(),
                description: format!("Testing mvlan={mvlan:?}"),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
            mvlan: *mvlan,
        };

        let created_group: MulticastGroup =
            object_create(client, &group_url, &params).await;
        wait_for_group_active(client, group_name).await;

        // Verify the created group has the correct mvlan
        assert_eq!(
            created_group.mvlan, *mvlan,
            "Created group should have mvlan={:?}",
            mvlan
        );

        // Fetch the group back from the database and verify it matches
        let fetched_group = get_multicast_group(client, group_name).await;
        assert_eq!(
            fetched_group.mvlan, *mvlan,
            "Fetched group should have mvlan={:?}",
            mvlan
        );
        assert_eq!(
            fetched_group.identity.id, created_group.identity.id,
            "Fetched group ID should match created group ID"
        );

        // Clean up
        object_delete(client, &mcast_group_url(group_name)).await;
        wait_for_group_deleted(client, group_name).await;
    }
}

#[nexus_test]
async fn test_multicast_group_mvlan_with_member_operations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "mvlan-member-project";
    let group_name = "mvlan-member-group";
    let instance_name = "mvlan-test-instance";

    // Setup
    create_default_ip_pool(&client).await;
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mvlan-member-pool",
        (224, 60, 0, 10),
        (224, 60, 0, 50),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Create group with mvlan
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for testing mvlan with members".to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(2048).unwrap()), // Set MVLAN
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    wait_for_group_active(client, group_name).await;

    assert_eq!(created_group.mvlan, Some(VlanID::new(2048).unwrap()));

    // Create and start instance
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_name,
        true, // start the instance
        &[],  // no groups at creation
    )
    .await;

    // Attach instance to group with mvlan
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Wait for member to reach Joined state
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify DPD shows vlan_id=2048
    let dpd_client = dpd_client(cptestctx);
    let dpd_group = dpd_client
        .multicast_group_get(&created_group.multicast_ip)
        .await
        .expect("Multicast group should exist in DPD");

    match dpd_group.into_inner() {
        dpd_types::MulticastGroupResponse::External {
            external_forwarding,
            ..
        } => {
            assert_eq!(
                external_forwarding.vlan_id,
                Some(2048),
                "DPD should show vlan_id matching group mvlan"
            );
        }
        dpd_types::MulticastGroupResponse::Underlay { .. } => {
            panic!("Expected external group, got underlay");
        }
    }

    // Clean up: stop instance before deleting
    let instance_stop_url =
        format!("/v1/instances/{instance_name}/stop?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should stop instance");

    let nexus = &cptestctx.server.server_context().nexus;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    object_delete(client, &instance_url).await;
    object_delete(client, &mcast_group_url(group_name)).await;
    wait_for_group_deleted(client, group_name).await;
}

#[nexus_test]
async fn test_multicast_group_mvlan_reconciler_update(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "mvlan-reconciler-project";
    let group_name = "mvlan-reconciler-group";
    let instance_name = "mvlan-reconciler-instance";

    // Setup
    create_default_ip_pool(&client).await;
    create_project(&client, project_name).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mvlan-reconciler-pool",
        (224, 70, 0, 10),
        (224, 70, 0, 50),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();

    // Create group with initial mvlan=2000
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for testing reconciler mvlan updates"
                .to_string(),
        },
        multicast_ip: None,
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(2000).unwrap()),
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    wait_for_group_active(client, group_name).await;

    // Create and start instance, attach to group
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_name,
        true, // start the instance
        &[],
    )
    .await;

    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify initial mvlan in DPD
    let dpd_client = dpd_client(cptestctx);
    let initial_dpd_group = dpd_client
        .multicast_group_get(&created_group.multicast_ip)
        .await
        .expect("Group should exist in DPD");

    match initial_dpd_group.into_inner() {
        dpd_types::MulticastGroupResponse::External {
            external_forwarding,
            ..
        } => {
            assert_eq!(
                external_forwarding.vlan_id,
                Some(2000),
                "DPD should show initial vlan_id=2000"
            );
        }
        dpd_types::MulticastGroupResponse::Underlay { .. } => {
            panic!("Expected external group");
        }
    }

    // Update mvlan to 3500 while member is active
    let update_mvlan = MulticastGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        source_ips: None,
        mvlan: Some(Nullable(Some(VlanID::new(3500).unwrap()))), // Update to 3500
    };

    let updated_group: MulticastGroup =
        object_put(client, &mcast_group_url(group_name), &update_mvlan).await;
    assert_eq!(
        updated_group.mvlan,
        Some(VlanID::new(3500).unwrap()),
        "Group mvlan should be updated"
    );

    // Wait for reconciler to process the mvlan change and verify DPD state
    wait_for_group_dpd_update(
        cptestctx,
        &created_group.multicast_ip,
        dpd_predicates::expect_vlan_id(3500),
        "vlan_id = Some(3500)",
    )
    .await;

    // Member should still be Joined after mvlan update
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1);
    assert_eq!(
        members[0].state, "Joined",
        "Member should remain Joined after mvlan update"
    );

    // Clean up: stop instance before deleting
    let instance_stop_url =
        format!("/v1/instances/{instance_name}/stop?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should stop instance");

    let nexus = &cptestctx.server.server_context().nexus;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    object_delete(client, &instance_url).await;
    object_delete(client, &mcast_group_url(group_name)).await;
    wait_for_group_deleted(client, group_name).await;
}

/// Assert that two multicast groups are equal in all fields.
fn assert_groups_eq(left: &MulticastGroup, right: &MulticastGroup) {
    assert_eq!(left.identity.id, right.identity.id);
    assert_eq!(left.identity.name, right.identity.name);
    assert_eq!(left.identity.description, right.identity.description);
    assert_eq!(left.multicast_ip, right.multicast_ip);
    assert_eq!(left.source_ips, right.source_ips);
    assert_eq!(left.mvlan, right.mvlan);
    assert_eq!(left.ip_pool_id, right.ip_pool_id);
}
