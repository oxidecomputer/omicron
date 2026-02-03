// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Tests for multicast API behavior and functionality.
//!
//! This module tests multicast group membership APIs including:
//!
//! - Stopped instance handling: Members in "Left" state, reconciler transitions
//! - Idempotency: Duplicate join operations succeed without creating duplicates
//! - UUID-based access: Fleet-scoped operations without project parameter
//! - Join-by-IP: Implicit group creation when joining by multicast IP
//!   - ASM (Any-Source): 224.0.0.0/4 except 232.0.0.0/8, sources optional
//!   - SSM (Source-Specific): 232.0.0.0/8, sources required per-member
//! - SSM validation: Every SSM member must specify sources (S,G subscription)
//!   - New groups: Validated before creation
//!   - Existing groups: Validated on join (by IP, name, or ID)
//!   - Empty sources array: Treated same as None (invalid for SSM)
//! - Source IP validation: ASM can have sources; SSM requires them
//! - Pool validation: IP must be in a linked multicast pool

use std::net::{IpAddr, Ipv4Addr};

use http::{Method, StatusCode};

use nexus_test_utils::http_testing::{
    AuthnMode, Collection, NexusRequest, RequestBuilder,
};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_instance, create_project, object_create,
    object_create_error,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceMulticastGroupJoin,
    InstanceNetworkInterfaceAttachment,
};
use nexus_types::external_api::views::MulticastGroupMember;
use omicron_common::address::is_ssm_address;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
};

use super::*;

/// Test various multicast API behaviors and scenarios.
#[nexus_test]
async fn test_multicast_api_behavior(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "api-edge-cases-project";
    let group_name = "api-edge-cases-group";

    // Setup in parallel
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool(client, "api-edge-pool"),
    )
    .await;

    // Case: Stopped instances (all APIs should handle stopped instances identically)

    // API Path: Instance created stopped, then added to group
    let instance1_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "edge-case-1".parse().unwrap(),
            description: "Stopped instance with multicast group".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "edge-case-1".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![], // No groups at creation
        disks: vec![],
        boot_disk: None,
        start: false, // Create stopped
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance1: Instance =
        object_create(client, &instance_url, &instance1_params).await;

    // Add instance1 to group using instance-centric API
    multicast_group_attach(cptestctx, project_name, "edge-case-1", group_name)
        .await;
    wait_for_group_active(client, group_name).await;

    // API Path: Second instance created stopped, then added to existing group
    let instance2_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "edge-case-2".parse().unwrap(),
            description: "Stopped instance, group added later".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "edge-case-2".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![], // No groups at creation
        disks: vec![],
        boot_disk: None,
        start: false, // Create stopped
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let instance2: Instance =
        object_create(client, &instance_url, &instance2_params).await;

    // Add to existing group using instance-centric API
    multicast_group_attach(cptestctx, project_name, "edge-case-2", group_name)
        .await;

    // Verify both stopped instances are in identical "Left" state
    //
    // State transition: "Joining" → "Left" (reconciler detects invalid instance)
    // Create saga creates member with state="Joining", sled_id=NULL
    // Reconciler runs, sees instance_valid=false (stopped/no VMM)
    // Reconciler immediately transitions "Joining"→"Left" (no DPD programming)
    //
    // This verifies stopped instances don't require sled assignment or DPD
    // underlay programming, as they go straight to "Left" state.
    for (i, instance) in [&instance1, &instance2].iter().enumerate() {
        wait_for_member_state(
            cptestctx,
            group_name,
            instance.identity.id,
            nexus_db_model::MulticastGroupMemberState::Left,
        )
        .await;

        assert_eq!(
            instance.runtime.run_state,
            InstanceState::Stopped,
            "Instance {} should be stopped",
            i + 1
        );
    }

    // Case: Idempotency (adding an existing member is safe)

    // Try to add instance1 again using instance join (should be idempotent)
    let duplicate_join_url = format!(
        "/v1/instances/edge-case-1/multicast-groups/{group_name}?project={project_name}"
    );
    let duplicate_join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    // This should succeed idempotently
    put_upsert::<_, MulticastGroupMember>(
        client,
        &duplicate_join_url,
        &duplicate_join_params,
    )
    .await;

    // Final verification: member count should still be 2 (no duplicates)
    let final_members = list_multicast_group_members(client, group_name).await;
    assert_eq!(
        final_members.len(),
        2,
        "Should have exactly 2 members (no duplicates from idempotency test)"
    );

    // Case: UUID-based API access (without project names)
    // Since multicast groups are fleet-scoped, UUID-based operations should work
    // without requiring project parameter

    let instance3_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "edge-case-3".parse().unwrap(),
            description: "Instance for UUID-based access".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "edge-case-3".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        start: false, // Create stopped to test UUID operations on non-running instances
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let (instance3, group) = ops::join2(
        object_create::<_, Instance>(client, &instance_url, &instance3_params),
        get_multicast_group(client, group_name),
    )
    .await;
    let instance_uuid = instance3.identity.id;
    let group_uuid = group.identity.id;

    // Join using UUIDs (no project parameter)
    let join_url_uuid =
        format!("/v1/instances/{instance_uuid}/multicast-groups/{group_uuid}");
    let member_uuid: MulticastGroupMember = put_upsert(
        client,
        &join_url_uuid,
        &InstanceMulticastGroupJoin::default(),
    )
    .await;

    assert_eq!(member_uuid.instance_id, instance_uuid);
    // Instance is stopped (start: false), so reconciler transitions "Joining"→"Left"
    wait_for_member_state(
        cptestctx,
        group_name,
        instance_uuid,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    // Verify membership via UUID-based instance group list (no project parameter)
    let instance_groups_url =
        format!("/v1/instances/{instance_uuid}/multicast-groups");
    let uuid_memberships: Vec<MulticastGroupMember> =
        NexusRequest::iter_collection_authn(
            client,
            &instance_groups_url,
            "",
            None,
        )
        .await
        .expect("UUID-based instance group list should succeed")
        .all_items;

    assert_eq!(
        uuid_memberships.len(),
        1,
        "UUID-based list should show 1 membership"
    );
    assert_eq!(uuid_memberships[0].instance_id, instance_uuid);

    // Verify UUID-based group member listing
    let group_members_url_uuid =
        mcast_group_members_url(&group_uuid.to_string());
    let uuid_based_members: Vec<MulticastGroupMember> =
        NexusRequest::iter_collection_authn(
            client,
            &group_members_url_uuid,
            "",
            None,
        )
        .await
        .expect("UUID-based group member list should succeed")
        .all_items;

    assert_eq!(
        uuid_based_members.len(),
        3,
        "Should show 3 members via UUID-based group list"
    );

    // Leave using UUIDs (no project parameter)
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &join_url_uuid)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("UUID-based leave should succeed");

    wait_for_member_count(client, group_name, 2).await;

    // Verify instance3 was actually removed
    let final_members_after_leave =
        list_multicast_group_members(client, group_name).await;
    assert!(
        !final_members_after_leave
            .iter()
            .any(|m| m.instance_id == instance_uuid),
        "instance3 should not be in the group after UUID-based leave"
    );

    // Negative test: invalid UUID should fail with 400 Bad Request
    let invalid_join_url =
        format!("/v1/instances/not-a-uuid/multicast-groups/{group_uuid}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &invalid_join_url)
            .body(Some(&InstanceMulticastGroupJoin::default()))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Invalid UUID should return 400 Bad Request");

    // Attempt create an instance with more than 32 group memberships.
    // We expect this to fail.
    let instance4_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "too-many-multis".parse().unwrap(),
            description: "This instance is just too popular!".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "too-many-multis".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: (0..33)
            .map(|i| MulticastGroupJoinSpec {
                group: MulticastGroupIdentifier::Ip(
                    Ipv4Addr::from_octets([224, 2, 10, 1 + i]).into(),
                ),
                source_ips: None,
                ip_version: None,
            })
            .collect(),
        disks: vec![],
        boot_disk: None,
        start: false, // Create stopped to test UUID operations on non-running instances
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let err = object_create_error(
        client,
        &instance_url,
        &instance4_params,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "An instance may not join more than 32 multicast groups",
    );

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["edge-case-1", "edge-case-2", "edge-case-3"],
    )
    .await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test join-by-IP for both ASM and SSM addresses in a single test context.
///
/// ASM (Any-Source Multicast, 224.x.x.x): sources optional
/// SSM (Source-Specific Multicast, 232.x.x.x): sources required
#[nexus_test]
async fn test_join_by_ip_asm(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-project";

    // Setup: project, ASM pool, SSM pool in parallel
    let (_, _, asm_pool, ssm_pool) = ops::join4(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "asm-pool",
            (224, 10, 0, 1),
            (224, 10, 0, 255),
        ),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-pool",
            (232, 20, 0, 1),
            (232, 20, 0, 255),
        ),
    )
    .await;

    // Create both instances in parallel
    let (inst_asm, inst_ssm) = ops::join2(
        create_instance(client, project_name, "inst-asm"),
        create_instance(client, project_name, "inst-ssm"),
    )
    .await;

    // Test ASM join-by-IP (no sources required)
    let asm_ip = "224.10.0.50";
    let asm_group_name = format!("mcast-{}", asm_ip.replace('.', "-"));
    {
        let join_url = format!(
            "/v1/instances/{}/multicast-groups/{asm_ip}?project={project_name}",
            inst_asm.identity.name
        );
        let member: MulticastGroupMember = put_upsert(
            client,
            &join_url,
            &InstanceMulticastGroupJoin::default(),
        )
        .await;

        assert_eq!(member.multicast_ip.to_string(), asm_ip);

        let group = wait_for_group_active(client, &asm_group_name).await;
        assert_eq!(group.multicast_ip.to_string(), asm_ip);
        assert_eq!(group.ip_pool_id, asm_pool.identity.id);
        assert!(group.source_ips.is_empty());
    }

    // Test SSM join-by-IP (sources required)
    let ssm_ip = "232.20.0.100";
    let ssm_group_name = format!("mcast-{}", ssm_ip.replace('.', "-"));
    let source_ip: IpAddr = "10.5.5.5".parse().unwrap();
    {
        let join_url = format!(
            "/v1/instances/{}/multicast-groups/{ssm_ip}?project={project_name}",
            inst_ssm.identity.name
        );
        let join_body = InstanceMulticastGroupJoin {
            source_ips: Some(vec![source_ip]),
            ip_version: None,
        };
        let member: MulticastGroupMember =
            put_upsert(client, &join_url, &join_body).await;

        assert_eq!(member.multicast_ip.to_string(), ssm_ip);

        let group = wait_for_group_active(client, &ssm_group_name).await;
        assert_eq!(group.ip_pool_id, ssm_pool.identity.id);
        assert_eq!(group.source_ips, vec![source_ip]);
    }

    // Cleanup both
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["inst-asm", "inst-ssm"],
    )
    .await;
    wait_for_group_deleted(cptestctx, &asm_group_name).await;
    wait_for_group_deleted(cptestctx, &ssm_group_name).await;
}

/// Test SSM with different source IPs per member.
///
/// Source IPs are per-member. The group's source_ips shows the union.
#[nexus_test]
async fn test_join_by_ip_ssm_with_sources(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-ssm-project";

    // Setup
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-pool",
            (232, 60, 0, 1),
            (232, 60, 0, 255),
        ),
    )
    .await;

    let (inst1, inst2) = ops::join2(
        create_instance(client, project_name, "ssm-inst-1"),
        create_instance(client, project_name, "ssm-inst-2"),
    )
    .await;

    let ssm_ip = "232.60.0.88";
    let group_name = format!("mcast-{}", ssm_ip.replace('.', "-"));
    let source1: IpAddr = "10.1.1.1".parse().unwrap();
    let source2: IpAddr = "10.2.2.2".parse().unwrap();

    // First instance creates SSM group with source1
    let join_url1 = format!(
        "/v1/instances/{}/multicast-groups/{ssm_ip}?project={project_name}",
        inst1.identity.name
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url1,
        &InstanceMulticastGroupJoin {
            source_ips: Some(vec![source1]),
            ip_version: None,
        },
    )
    .await;
    wait_for_group_active(client, &group_name).await;

    // Second instance joins with different source
    let join_url2 = format!(
        "/v1/instances/{}/multicast-groups/{ssm_ip}?project={project_name}",
        inst2.identity.name
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url2,
        &InstanceMulticastGroupJoin {
            source_ips: Some(vec![source2]),
            ip_version: None,
        },
    )
    .await;

    // Verify group source_ips is union
    let group: MulticastGroup =
        object_get(client, &format!("/v1/multicast-groups/{group_name}")).await;
    let mut actual = group.source_ips.clone();
    actual.sort();
    let mut expected = vec![source1, source2];
    expected.sort();
    assert_eq!(actual, expected);

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["ssm-inst-1", "ssm-inst-2"],
    )
    .await;
    wait_for_group_deleted(cptestctx, &group_name).await;
}

/// Test SSM source validation: all scenarios where SSM joins should fail.
///
/// SSM addresses (232.0.0.0/8) require source IPs for every member subscription.
/// This test validates three failure scenarios with a single setup:
///
/// 1. Join-by-IP with new SSM group without sources (implicit creation blocked)
/// 2. Join existing SSM group without sources (via ID, name, and IP lookup)
/// 3. Join-by-IP with empty sources array (treated same as no sources)
#[nexus_test]
async fn test_ssm_source_validation(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "ssm-source-validation-project";

    // Setup: project, pools, and instances for the rest of the test
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-validation-pool",
            (232, 30, 0, 1),
            (232, 30, 0, 255),
        ),
    )
    .await;

    // Create instances for all test cases
    create_instance(client, project_name, "ssm-inst-creator").await;
    create_instance(client, project_name, "ssm-inst-joiner").await;

    // Shared body for "no sources" cases
    let join_body_no_sources =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    // Case: Join-by-IP SSM without sources fails (implicit creation blocked)
    {
        let ssm_ip = "232.30.0.50";
        let join_url = format!(
            "/v1/instances/ssm-inst-joiner/multicast-groups/{ssm_ip}?project={project_name}"
        );

        let response = NexusRequest::new(
            RequestBuilder::new(client, Method::PUT, &join_url)
                .body(Some(&join_body_no_sources))
                .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("SSM without sources should fail");

        let error: dropshot::HttpErrorResponseBody =
            response.parsed_body().unwrap();
        assert_eq!(
            error.error_code,
            Some("InvalidRequest".to_string()),
            "Expected InvalidRequest for SSM without sources, got: {:?}",
            error.error_code
        );
    }

    // Case: Join existing SSM group without sources fails (all lookup methods)
    //
    // First, create an SSM group with sources using ssm-inst-creator
    let ssm_ip = "232.30.0.100";
    let source_ip: IpAddr = "10.30.0.1".parse().unwrap();
    let join_url_creator = format!(
        "/v1/instances/ssm-inst-creator/multicast-groups/{ssm_ip}?project={project_name}"
    );
    let join_body_with_sources = InstanceMulticastGroupJoin {
        source_ips: Some(vec![source_ip]),
        ip_version: None,
    };
    let member_creator: MulticastGroupMember =
        put_upsert(client, &join_url_creator, &join_body_with_sources).await;

    let group_id = member_creator.multicast_group_id;
    let group_name = format!("mcast-{}", ssm_ip.replace('.', "-"));

    // Join by ID without sources -> should fail
    {
        let join_url = format!(
            "/v1/instances/ssm-inst-joiner/multicast-groups/{group_id}?project={project_name}"
        );
        let error = NexusRequest::new(
            RequestBuilder::new(client, Method::PUT, &join_url)
                .body(Some(&join_body_no_sources))
                .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Join by ID without sources should fail")
        .parsed_body::<dropshot::HttpErrorResponseBody>()
        .unwrap();
        assert_eq!(
            error.error_code,
            Some("InvalidRequest".to_string()),
            "Expected InvalidRequest for join-by-ID, got: {:?}",
            error.error_code
        );
    }

    // Join by name without sources -> should fail
    {
        let join_url = format!(
            "/v1/instances/ssm-inst-joiner/multicast-groups/{group_name}?project={project_name}"
        );
        let error = NexusRequest::new(
            RequestBuilder::new(client, Method::PUT, &join_url)
                .body(Some(&join_body_no_sources))
                .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Join by name without sources should fail")
        .parsed_body::<dropshot::HttpErrorResponseBody>()
        .unwrap();
        assert_eq!(
            error.error_code,
            Some("InvalidRequest".to_string()),
            "Expected InvalidRequest for join-by-name, got: {:?}",
            error.error_code
        );
    }

    // Join by IP without sources -> should fail
    {
        let join_url = format!(
            "/v1/instances/ssm-inst-joiner/multicast-groups/{ssm_ip}?project={project_name}"
        );
        let error = NexusRequest::new(
            RequestBuilder::new(client, Method::PUT, &join_url)
                .body(Some(&join_body_no_sources))
                .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Join by IP without sources should fail")
        .parsed_body::<dropshot::HttpErrorResponseBody>()
        .unwrap();
        assert_eq!(
            error.error_code,
            Some("InvalidRequest".to_string()),
            "Expected InvalidRequest for join-by-IP, got: {:?}",
            error.error_code
        );
    }

    // Case: SSM with empty sources array fails (treated same as `None`)
    {
        let ssm_ip = "232.30.0.150";
        let join_url = format!(
            "/v1/instances/ssm-inst-joiner/multicast-groups/{ssm_ip}?project={project_name}"
        );
        let join_body_empty_sources = InstanceMulticastGroupJoin {
            source_ips: Some(vec![]),
            ip_version: None,
        };

        let response = NexusRequest::new(
            RequestBuilder::new(client, Method::PUT, &join_url)
                .body(Some(&join_body_empty_sources))
                .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("SSM with empty sources array should fail");

        let error: dropshot::HttpErrorResponseBody =
            response.parsed_body().unwrap();
        assert_eq!(
            error.error_code,
            Some("InvalidRequest".to_string()),
            "Expected InvalidRequest for SSM with empty sources, got: {:?}",
            error.error_code
        );
    }

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["ssm-inst-creator", "ssm-inst-joiner"],
    )
    .await;
    wait_for_group_deleted(cptestctx, &group_name).await;
}

/// Test join-by-IP with IP not in any pool should fail.
#[nexus_test]
async fn test_join_by_ip_not_in_pool_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-nopool-project";
    let instance_name = "join-by-ip-nopool-inst";

    // Setup: only create a pool with limited range
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "limited-pool",
            (224, 100, 0, 1),
            (224, 100, 0, 10), // Only 10 IPs
        ),
    )
    .await;

    create_instance(client, project_name, instance_name).await;

    // Try to join with IP outside any pool range
    let ip_not_in_pool = "224.200.0.50"; // Not in 224.100.0.1-10
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{ip_not_in_pool}?project={project_name}"
    );
    let join_body =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url)
            .body(Some(&join_body))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("IP not in pool should fail");

    let error_body: dropshot::HttpErrorResponseBody =
        error.parsed_body().unwrap();
    assert_eq!(
        error_body.error_code,
        Some("InvalidRequest".to_string()),
        "Expected InvalidRequest for IP not in pool, got: {:?}",
        error_body.error_code
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
}

/// Test joining existing group by IP: second instance joins the same IP
/// without specifying sources.
#[nexus_test]
async fn test_join_by_ip_existing_group(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-existing-project";

    // Setup
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "existing-pool",
            (224, 50, 0, 1),
            (224, 50, 0, 255),
        ),
    )
    .await;

    create_instance(client, project_name, "existing-inst-1").await;
    create_instance(client, project_name, "existing-inst-2").await;

    let explicit_ip = "224.50.0.77";
    let expected_group_name =
        format!("mcast-{}", explicit_ip.replace('.', "-"));

    // First instance implicitly creates the group by joining with IP
    let join_url_1 = format!(
        "/v1/instances/existing-inst-1/multicast-groups/{explicit_ip}?project={project_name}"
    );
    let member1: MulticastGroupMember =
        put_upsert(client, &join_url_1, &InstanceMulticastGroupJoin::default())
            .await;

    wait_for_group_active(client, &expected_group_name).await;

    // Second instance joins the same IP; should attach to existing group
    let join_url_2 = format!(
        "/v1/instances/existing-inst-2/multicast-groups/{explicit_ip}?project={project_name}"
    );
    let member2: MulticastGroupMember =
        put_upsert(client, &join_url_2, &InstanceMulticastGroupJoin::default())
            .await;

    // Both members should have the same group and IP
    assert_eq!(member1.multicast_group_id, member2.multicast_group_id);
    assert_eq!(member1.multicast_ip, member2.multicast_ip);
    assert_eq!(member1.multicast_ip.to_string(), explicit_ip);

    // Verify group has 2 members
    let members =
        list_multicast_group_members(client, &expected_group_name).await;
    assert_eq!(members.len(), 2, "Group should have 2 members");

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["existing-inst-1", "existing-inst-2"],
    )
    .await;
    wait_for_group_deleted(cptestctx, &expected_group_name).await;
}

/// Test that ASM groups can optionally have source IPs (IGMPv3/MLDv2 filtering).
///
/// Unlike SSM where sources are required, ASM addresses allow optional source
/// filtering. The group's `source_ips` field shows the union of all member sources.
#[nexus_test]
async fn test_join_by_ip_asm_with_sources_succeeds(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-asm-sources-project";

    // Setup: project and pools
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "asm-sources-pool",
            (224, 70, 0, 1),
            (224, 70, 0, 255),
        ),
    )
    .await;

    // Create two instances
    let (instance1, instance2) = ops::join2(
        create_instance(client, project_name, "asm-sources-inst-1"),
        create_instance(client, project_name, "asm-sources-inst-2"),
    )
    .await;

    let explicit_ip: IpAddr = "224.70.0.55".parse().unwrap();
    let expected_group_name =
        format!("mcast-{}", explicit_ip.to_string().replace('.', "-"));

    // First instance joins ASM group without sources
    let join_url1 = format!(
        "/v1/instances/{}/multicast-groups/{explicit_ip}?project={project_name}",
        instance1.identity.name
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url1,
        &InstanceMulticastGroupJoin::default(),
    )
    .await;

    wait_for_group_active(client, &expected_group_name).await;

    // Verify group has no source_ips initially
    let group: MulticastGroup = object_get(
        client,
        &format!("/v1/multicast-groups/{expected_group_name}"),
    )
    .await;
    assert!(
        group.source_ips.is_empty(),
        "ASM group with no-source member should have empty source_ips"
    );

    // Second instance joins the same ASM group with sources (valid for ASM)
    let join_url2 = format!(
        "/v1/instances/{}/multicast-groups/{explicit_ip}?project={project_name}",
        instance2.identity.name
    );
    let source1: IpAddr = "10.99.99.1".parse().unwrap();
    let source2: IpAddr = "10.99.99.2".parse().unwrap();
    let join_body_2 = InstanceMulticastGroupJoin {
        source_ips: Some(vec![source1, source2]),
        ip_version: None,
    };
    put_upsert::<_, MulticastGroupMember>(client, &join_url2, &join_body_2)
        .await;

    // Verify group source_ips is union of all member sources
    let group: MulticastGroup = object_get(
        client,
        &format!("/v1/multicast-groups/{expected_group_name}"),
    )
    .await;
    let mut actual_sources = group.source_ips.clone();
    actual_sources.sort();
    let mut expected_sources = vec![source1, source2];
    expected_sources.sort();
    assert_eq!(
        actual_sources, expected_sources,
        "ASM group source_ips should be union of member sources"
    );

    // Also verify list endpoint returns the same source_ips
    let groups: Collection<MulticastGroup> =
        NexusRequest::iter_collection_authn(
            client,
            "/v1/multicast-groups",
            "",
            None,
        )
        .await
        .expect("Should list multicast groups");

    let listed_group = groups
        .all_items
        .iter()
        .find(|g| g.identity.name == expected_group_name)
        .expect("ASM group should appear in list");
    let mut listed_sources = listed_group.source_ips.clone();
    listed_sources.sort();
    assert_eq!(
        listed_sources, expected_sources,
        "List endpoint should also show source_ips union for ASM group"
    );

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["asm-sources-inst-1", "asm-sources-inst-2"],
    )
    .await;
    wait_for_group_deleted(cptestctx, &expected_group_name).await;
}

/// Test that explicit IP determines pool selection, not source presence.
///
/// When both SSM and ASM pools are linked, joining by an ASM IP with sources
/// should use the ASM pool (determined by IP), not the SSM pool (which would
/// be auto-selected if sources triggered SSM selection in join-by-name).
///
/// This verifies that join-by-IP bypasses SSM/ASM auto-selection logic.
#[nexus_test]
async fn test_explicit_ip_bypasses_ssm_asm_selection(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "explicit-ip-bypass-project";
    let instance_name = "explicit-ip-bypass-inst";

    // Setup: create both SSM and ASM pools
    ops::join4(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "bypass-ssm-pool",
            (232, 80, 0, 1),
            (232, 80, 0, 255),
        ),
        create_multicast_ip_pool_with_range(
            client,
            "bypass-asm-pool",
            (224, 80, 0, 1),
            (224, 80, 0, 255),
        ),
    )
    .await;

    create_instance(client, project_name, instance_name).await;

    // Join by ASM IP with sources -> should use ASM pool
    let asm_ip: IpAddr = "224.80.0.50".parse().unwrap();
    let source_ip: IpAddr = "10.80.80.1".parse().unwrap();

    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{asm_ip}?project={project_name}"
    );
    let join_body = InstanceMulticastGroupJoin {
        source_ips: Some(vec![source_ip]),
        ip_version: None,
    };

    let member: MulticastGroupMember =
        put_upsert(client, &join_url, &join_body).await;

    // Verify member has the source IP
    assert_eq!(member.source_ips, vec![source_ip]);

    // Verify the group is in the ASM pool (224.x.x.x), not SSM pool (232.x.x.x)
    let expected_group_name =
        format!("mcast-{}", asm_ip.to_string().replace('.', "-"));
    let group = get_multicast_group(client, &expected_group_name).await;

    // The group IP should be the ASM IP we specified
    assert_eq!(
        group.multicast_ip, asm_ip,
        "Group should use the explicit ASM IP"
    );

    // Verify the group is not using SSM pool IPs
    assert!(
        !is_ssm_address(group.multicast_ip),
        "Group should be ASM (explicit IP), not SSM (even with sources)"
    );

    // Verify pool association by checking IP range
    let ip_octets = match group.multicast_ip {
        IpAddr::V4(v4) => v4.octets(),
        IpAddr::V6(_) => panic!("Expected IPv4"),
    };
    assert_eq!(ip_octets[0], 224, "First octet should be 224 (ASM pool)");
    assert_eq!(ip_octets[1], 80, "Second octet should be 80 (ASM pool)");

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, &expected_group_name).await;
}
