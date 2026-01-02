// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Tests for multicast API behavior and functionality.
//!
//! This module tests various aspects of multicast group membership APIs, including:
//!
//! - Stopped instance handling
//! - Idempotency behavior
//! - API consistency

use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project, object_create,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceNetworkInterfaceAttachment, MulticastGroupCreate,
    MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::{MulticastGroup, MulticastGroupMember};
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
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool(client, "api-edge-pool"),
    )
    .await;

    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for API edge case testing".to_string(),
        },
        multicast_ip: None, // Test with auto-assigned IP
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Case: Stopped instances (all APIs should handle stopped instances
    // identically)

    // API Path: Instance created stopped with multicast group
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
        multicast_groups: vec![NameOrId::Name(group_name.parse().unwrap())],
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

    // API Path: Instance created stopped, then added to group
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

    // Add to group after creation
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("edge-case-2".parse().unwrap()),
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
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
            .body(Some(&()))
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
            .body(Some(&()))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Invalid UUID should return 400 Bad Request");

    // Cleanup - instance3 has already left the group above
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["edge-case-1", "edge-case-2", "edge-case-3"],
    )
    .await;
    cleanup_multicast_groups(client, &[group_name]).await;
}
