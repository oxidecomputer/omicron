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
//!   - ASM (Any-Source): 224.0.0.0/4 except 232.0.0.0/8, no source filtering
//!   - SSM (Source-Specific): 232.0.0.0/8, requires source IPs
//! - Source IP validation: Mismatch detection, ASM/SSM compatibility
//! - Pool validation: IP must be in a linked multicast pool

use std::net::IpAddr;

use http::{Method, StatusCode};

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, object_create,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceMulticastGroupJoin,
    InstanceNetworkInterfaceAttachment, MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::MulticastGroupMember;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    NameOrId,
};

use super::*;

/// Test various multicast API behaviors and scenarios.
#[nexus_test]
async fn test_multicast_api_behavior(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "api-edge-cases-project";
    let group_name = "api-edge-cases-group";

    // Setup in parallel
    let (_, _, _) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool(client, "api-edge-pool"),
    )
    .await;

    // Case: Stopped instances (all APIs should handle stopped instances
    // identically)

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
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
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

    // Add instance1 to group
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member1_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("edge-case-1".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member1_params,
    )
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
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
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

    // Add to existing group
    let member2_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("edge-case-2".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member2_params,
    )
    .await;

    // Verify both stopped instances are in identical "Left" state
    //
    // State transition: "Joining" → "Left" (reconciler detects invalid instance)
    // Create saga creates member with state="Joining", sled_id=NULL
    // Reconciler runs, sees instance_valid=false (stopped/no VMM)
    // Reconciler immediately transitions "Joining"→"Left" (no DPD programming)
    //
    // This verifies the reconciler correctly handles stopped instances without
    // requiring inventory/DPD readiness (unlike running instances).
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

    // Case: Idempotency test (adding already-existing member should be
    // safe for all APIs)

    // Try to add instance1 again using group member add (should be idempotent)
    let duplicate_member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("edge-case-1".parse().unwrap()),
        source_ips: None,
    };

    // This should succeed idempotently
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &member_add_url)
            .body(Some(&duplicate_member_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Idempotent member add should succeed");

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
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
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
    let member_uuid: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url_uuid)
            .body(Some(&InstanceMulticastGroupJoin::default()))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("UUID-based join should succeed")
    .parsed_body()
    .expect(
        "Failed to parse MulticastGroupMember from UUID-based join response",
    );

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

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["edge-case-1", "edge-case-2", "edge-case-3"],
    )
    .await;
    wait_for_group_deleted(client, group_name).await;
}

/// Test ASM (Any-Source Multicast) join-by-IP: instance joins by specifying
/// a multicast IP directly instead of a group name. The system finds the pool
/// containing the IP and implicitly creates the group with that explicit IP.
#[nexus_test]
async fn test_join_by_ip_asm(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-asm-project";
    let instance_name = "join-by-ip-inst-1";

    // Setup: project and pools
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "asm-pool",
            (224, 10, 0, 1),
            (224, 10, 0, 255),
        ),
    )
    .await;

    // Create instance
    create_instance(client, project_name, instance_name).await;

    // Join by IP - use an IP from the pool range as the "group name"
    let explicit_ip = "224.10.0.50";
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{explicit_ip}?project={project_name}"
    );
    let join_body = InstanceMulticastGroupJoin { source_ips: None };

    let response = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url)
            .body(Some(&join_body)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Request should execute");

    if response.status != StatusCode::CREATED {
        panic!(
            "Join-by-IP should succeed: expected {}, got {} - body: {}",
            StatusCode::CREATED,
            response.status,
            String::from_utf8_lossy(&response.body)
        );
    }

    let member: MulticastGroupMember =
        response.parsed_body().expect("Should parse member");

    // Verify the member has the expected multicast IP
    assert_eq!(
        member.multicast_ip.to_string(),
        explicit_ip,
        "Member should have the explicit IP specified in join"
    );

    // Verify the group was implicitly created with the explicit IP
    // Group name is auto-generated: "mcast-224-10-0-50"
    let expected_group_name =
        format!("mcast-{}", explicit_ip.replace('.', "-"));
    let group = wait_for_group_active(client, &expected_group_name).await;

    assert_eq!(
        group.multicast_ip.to_string(),
        explicit_ip,
        "Group should have the explicit multicast IP"
    );
    assert_eq!(
        group.ip_pool_id, mcast_pool.identity.id,
        "Group should be in the ASM pool"
    );
    assert!(group.source_ips.is_empty(), "ASM group should have no source IPs");

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, &expected_group_name).await;
}

/// Test SSM (Source-Specific Multicast) join-by-IP: instance joins an SSM IP
/// (232.x.x.x) with source IPs specified. The system implicitly creates the
/// group with explicit IP and sources.
#[nexus_test]
async fn test_join_by_ip_ssm_with_sources(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-ssm-project";
    let instance_name = "join-by-ip-ssm-inst";

    // Setup: project and pools
    let (_, _, ssm_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-pool",
            (232, 20, 0, 1),
            (232, 20, 0, 255),
        ),
    )
    .await;

    // Create instance
    create_instance(client, project_name, instance_name).await;

    // Join by SSM IP with source IPs
    let explicit_ssm_ip = "232.20.0.100";
    let source_ip: IpAddr = "10.5.5.5".parse().unwrap();
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{explicit_ssm_ip}?project={project_name}"
    );
    let join_body =
        InstanceMulticastGroupJoin { source_ips: Some(vec![source_ip]) };

    let member: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url)
            .body(Some(&join_body))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("SSM join-by-IP should succeed")
    .parsed_body()
    .expect("Should parse member");

    assert_eq!(
        member.multicast_ip.to_string(),
        explicit_ssm_ip,
        "Member should have the explicit SSM IP"
    );

    // Verify group was implicitly created with correct properties
    let expected_group_name =
        format!("mcast-{}", explicit_ssm_ip.replace('.', "-"));
    let group = wait_for_group_active(client, &expected_group_name).await;

    assert_eq!(
        group.multicast_ip.to_string(),
        explicit_ssm_ip,
        "Group should have the explicit SSM IP"
    );
    assert_eq!(
        group.ip_pool_id, ssm_pool.identity.id,
        "Group should be in the SSM pool"
    );
    assert_eq!(group.source_ips.len(), 1, "SSM group should have 1 source IP");
    assert_eq!(
        group.source_ips[0].to_string(),
        source_ip.to_string(),
        "SSM group should have the specified source IP"
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, &expected_group_name).await;
}

/// Test SSM join-by-IP without sources should fail.
/// SSM addresses (232.0.0.0/8) require source IPs for implicit creation.
#[nexus_test]
async fn test_join_by_ip_ssm_without_sources_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-ssm-fail-project";
    let instance_name = "join-by-ip-ssm-fail-inst";

    // Setup
    let (_, _, _ssm_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-fail-pool",
            (232, 30, 0, 1),
            (232, 30, 0, 255),
        ),
    )
    .await;

    create_instance(client, project_name, instance_name).await;

    // Try to join SSM IP without sources; should fail
    let ssm_ip = "232.30.0.50";
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{ssm_ip}?project={project_name}"
    );
    let join_body = InstanceMulticastGroupJoin {
        source_ips: None, // No sources!
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url)
            .body(Some(&join_body))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("SSM without sources should fail");

    let error_body: dropshot::HttpErrorResponseBody =
        error.parsed_body().unwrap();
    assert!(
        error_body.message.contains("SSM")
            || error_body.message.contains("source"),
        "Error should mention SSM or source IPs: {}",
        error_body.message
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
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
    let (_, _, _) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
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
    let join_body = InstanceMulticastGroupJoin { source_ips: None };

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
    assert!(
        error_body.message.contains("pool")
            || error_body.message.contains("range"),
        "Error should mention pool or range: {}",
        error_body.message
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
    let (_, _, _) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
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
    let member1: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url_1)
            .body(Some(&InstanceMulticastGroupJoin::default()))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("First join-by-IP should succeed")
    .parsed_body()
    .expect("Should parse member");

    wait_for_group_active(client, &expected_group_name).await;

    // Second instance joins the same IP; should attach to existing group
    let join_url_2 = format!(
        "/v1/instances/existing-inst-2/multicast-groups/{explicit_ip}?project={project_name}"
    );
    let member2: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url_2)
            .body(Some(&InstanceMulticastGroupJoin::default()))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Second join-by-IP should succeed")
    .parsed_body()
    .expect("Should parse member");

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
    wait_for_group_deleted(client, &expected_group_name).await;
}

/// Test source mismatch when joining existing group by IP.
/// If an SSM group exists and a new instance tries to join with different
/// sources, it should fail.
#[nexus_test]
async fn test_join_by_ip_source_mismatch_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-mismatch-project";

    // Setup with SSM pool
    let (_, _, _ssm_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "mismatch-ssm-pool",
            (232, 60, 0, 1),
            (232, 60, 0, 255),
        ),
    )
    .await;

    create_instance(client, project_name, "mismatch-inst-1").await;
    create_instance(client, project_name, "mismatch-inst-2").await;

    let explicit_ssm_ip = "232.60.0.88";
    let expected_group_name =
        format!("mcast-{}", explicit_ssm_ip.replace('.', "-"));
    let source1: IpAddr = "10.1.1.1".parse().unwrap();
    let source2: IpAddr = "10.2.2.2".parse().unwrap();

    // First instance implicitly creates SSM group with source1
    let join_url_1 = format!(
        "/v1/instances/mismatch-inst-1/multicast-groups/{explicit_ssm_ip}?project={project_name}"
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url_1)
            .body(Some(&InstanceMulticastGroupJoin {
                source_ips: Some(vec![source1]),
            }))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("First SSM join should succeed")
    .parsed_body::<MulticastGroupMember>()
    .expect("Should parse member");

    wait_for_group_active(client, &expected_group_name).await;

    // Second instance tries to join with different source; should fail
    let join_url_2 = format!(
        "/v1/instances/mismatch-inst-2/multicast-groups/{explicit_ssm_ip}?project={project_name}"
    );
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url_2)
            .body(Some(&InstanceMulticastGroupJoin {
                source_ips: Some(vec![source2]),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Mismatched sources should fail");

    let error_body: dropshot::HttpErrorResponseBody =
        error.parsed_body().unwrap();
    assert!(
        error_body.message.contains("source"),
        "Error should mention source IPs: {}",
        error_body.message
    );

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["mismatch-inst-1", "mismatch-inst-2"],
    )
    .await;
    wait_for_group_deleted(client, &expected_group_name).await;
}

/// Test that joining an existing ASM group with sources specified fails.
/// ASM groups have no source filtering, so specifying sources is invalid.
#[nexus_test]
async fn test_join_by_ip_asm_with_sources_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "join-by-ip-asm-sources-project";

    // Setup: project and pools
    let (_, _, _) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
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

    // First instance joins ASM IP without sources (valid for ASM)
    let explicit_ip: IpAddr = "224.70.0.55".parse().unwrap();
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/{explicit_ip}?project={project_name}",
        instance1.identity.name
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url)
            .body(Some(&InstanceMulticastGroupJoin::default()))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("First instance ASM join should succeed");

    let expected_group_name =
        format!("mcast-{}", explicit_ip.to_string().replace('.', "-"));
    wait_for_group_active(client, &expected_group_name).await;

    // Second instance tries to join the same ASM group WITH sources
    // ASM groups don't support source filtering
    let join_url2 = format!(
        "/v1/instances/{}/multicast-groups/{explicit_ip}?project={project_name}",
        instance2.identity.name
    );
    let bogus_source: IpAddr = "10.99.99.99".parse().unwrap();

    let error_response = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url2)
            .body(Some(&InstanceMulticastGroupJoin {
                source_ips: Some(vec![bogus_source]),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("ASM join with sources should fail");

    let error_body: dropshot::HttpErrorResponseBody =
        error_response.parsed_body().unwrap();
    assert!(
        error_body.message.to_lowercase().contains("source"),
        "Error should mention source IPs, got: {}",
        error_body.message
    );

    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["asm-sources-inst-1", "asm-sources-inst-2"],
    )
    .await;
    wait_for_group_deleted(client, &expected_group_name).await;
}
