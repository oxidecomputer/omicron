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
        "/v1/multicast-groups/{}/members?project={}",
        group_name, project_name
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
    for (i, instance) in [&instance1, &instance2].iter().enumerate() {
        wait_for_member_state(
            client,
            group_name,
            instance.identity.id,
            "Left", // Stopped instances should be Left
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

    // This should not error (idempotent operation)
    let result = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &member_add_url)
            .body(Some(&duplicate_member_params))
            .expect_status(Some(StatusCode::CREATED)), // Should succeed idempotently
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await;

    match result {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => panic!("Unexpected error in idempotency test: {}", e),
    }

    // Final verification: member count should still be 2 (no duplicates)
    let final_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        final_members.len(),
        2,
        "Should have exactly 2 members (no duplicates from idempotency test)"
    );

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["edge-case-1", "edge-case-2"],
    )
    .await;
    cleanup_multicast_groups(client, &[group_name]).await;
}
