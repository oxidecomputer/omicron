// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/
//
// Copyright 2025 Oxide Computer Company

//! Tests multicast group + instance integration.
//!
//! Tests that verify multicast group functionality when integrated with
//! instance creation, modification, and deletion.

use std::net::{IpAddr, Ipv4Addr};

use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, object_create,
    object_delete, object_get, object_put,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceNetworkInterfaceAttachment, InstanceUpdate,
    MulticastGroupCreate, MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::{MulticastGroup, MulticastGroupMember};
use nexus_types::internal_api::params::InstanceMigrateRequest;

use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    InstanceState, NameOrId, Nullable,
};
use omicron_common::vlan::VlanID;
use omicron_nexus::TestInterfaces;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use sled_agent_client::TestInterfaces as _;

use super::*;
use crate::integration_tests::instances::{
    instance_simulate, instance_wait_for_state,
};

const PROJECT_NAME: &str = "test-project";

/// Consolidated multicast lifecycle test that combines multiple scenarios.
#[nexus_test]
async fn test_multicast_lifecycle(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Setup - create IP pool and project (shared across all operations)
    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool-comprehensive",
        (224, 30, 0, 1),   // Large range: 224.30.0.1
        (224, 30, 0, 255), // to 224.30.0.255 (255 IPs)
    )
    .await;

    // Create multiple multicast groups in parallel
    let group_specs = &[
        MulticastGroupForTest {
            name: "group-lifecycle-1",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 30, 0, 101)),
            description: Some("Group for lifecycle testing 1".to_string()),
        },
        MulticastGroupForTest {
            name: "group-lifecycle-2",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 30, 0, 102)),
            description: Some("Group for lifecycle testing 2".to_string()),
        },
        MulticastGroupForTest {
            name: "group-lifecycle-3",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 30, 0, 103)),
            description: Some("Group for lifecycle testing 3".to_string()),
        },
        MulticastGroupForTest {
            name: "group-lifecycle-4",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 30, 0, 104)),
            description: Some("Group for lifecycle testing 4".to_string()),
        },
    ];

    let groups =
        create_multicast_groups(client, &mcast_pool, group_specs).await;

    // Wait for all groups to become active in parallel
    let group_names: Vec<&str> = group_specs.iter().map(|g| g.name).collect();
    wait_for_groups_active(client, &group_names).await;

    // Create multiple instances in parallel - test various attachment scenarios
    let instances = vec![
        // Instance with group attached at creation
        instance_for_multicast_groups(
            cptestctx,
            PROJECT_NAME,
            "instance-create-attach",
            false,
            &["group-lifecycle-1"],
        )
        .await,
        // Instances for live attach/detach testing
        instance_for_multicast_groups(
            cptestctx,
            PROJECT_NAME,
            "instance-live-1",
            false,
            &[],
        )
        .await,
        instance_for_multicast_groups(
            cptestctx,
            PROJECT_NAME,
            "instance-live-2",
            false,
            &[],
        )
        .await,
        // Instance for multi-group testing
        instance_for_multicast_groups(
            cptestctx,
            PROJECT_NAME,
            "instance-multi-groups",
            false,
            &[],
        )
        .await,
    ];

    // Verify create-time attachment worked
    wait_for_member_state(
        client,
        "group-lifecycle-1",
        instances[0].identity.id,
        "Left", // Instance is stopped, so should be Left
    )
    .await;

    // Live attach/detach operations
    // Attach instance-live-1 to group-lifecycle-2
    multicast_group_attach(
        client,
        PROJECT_NAME,
        "instance-live-1",
        "group-lifecycle-2",
    )
    .await;

    // Attach instance-live-2 to group-lifecycle-2 (test multiple instances per group)
    multicast_group_attach(
        client,
        PROJECT_NAME,
        "instance-live-2",
        "group-lifecycle-2",
    )
    .await;

    // Verify both instances are attached to group-lifecycle-2
    for i in 0..2 {
        wait_for_member_state(
            client,
            "group-lifecycle-2",
            instances[i + 1].identity.id,
            "Left", // Stopped instances
        )
        .await;
    }

    // Multi-group attachment (instance to multiple groups)
    // Attach instance-multi-groups to multiple groups
    multicast_group_attach(
        client,
        PROJECT_NAME,
        "instance-multi-groups",
        "group-lifecycle-3",
    )
    .await;

    multicast_group_attach(
        client,
        PROJECT_NAME,
        "instance-multi-groups",
        "group-lifecycle-4",
    )
    .await;

    // Verify multi-group membership
    for group_name in ["group-lifecycle-3", "group-lifecycle-4"] {
        wait_for_member_state(
            client,
            group_name,
            instances[3].identity.id,
            "Left", // Stopped instance
        )
        .await;
    }

    // Detach operations and idempotency
    // Detach instance-live-1 from group-lifecycle-2
    multicast_group_detach(
        client,
        PROJECT_NAME,
        "instance-live-1",
        "group-lifecycle-2",
    )
    .await;

    // Test idempotency - detach again (should not error)
    multicast_group_detach(
        client,
        PROJECT_NAME,
        "instance-live-1",
        "group-lifecycle-2",
    )
    .await;

    // Verify instance-live-1 is no longer a member of group-lifecycle-2
    let members =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(
            client,
            &mcast_group_members_url("group-lifecycle-2"),
            &format!("project={PROJECT_NAME}"),
            None,
        )
        .await
        .expect("Failed to list multicast group members")
        .all_items;

    // Should only have instance-live-2 as member now
    assert_eq!(
        members.len(),
        1,
        "group-lifecycle-2 should have 1 member after detach"
    );
    assert_eq!(members[0].instance_id, instances[2].identity.id);

    // Verify groups are still active and functional
    for (i, group_name) in group_names.iter().enumerate() {
        let group_url = mcast_group_url(group_name);
        let current_group: MulticastGroup =
            object_get(client, &group_url).await;
        assert_eq!(
            current_group.state, "Active",
            "Group {} should remain Active throughout lifecycle",
            group_name
        );
        assert_eq!(current_group.identity.id, groups[i].identity.id);
    }

    // Cleanup - use our parallel cleanup functions
    cleanup_instances(
        cptestctx,
        client,
        PROJECT_NAME,
        &[
            "instance-create-attach",
            "instance-live-1",
            "instance-live-2",
            "instance-multi-groups",
        ],
    )
    .await;

    cleanup_multicast_groups(client, &group_names).await;
}

#[nexus_test]
async fn test_multicast_group_attach_conflicts(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool-conflicts",
        (224, 23, 0, 1),   // Unique range: 224.23.0.1
        (224, 23, 0, 255), // to 224.23.0.255
    )
    .await;

    // Create a multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 23, 0, 103));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "mcast-group-1".parse().unwrap(),
            description: "Group for conflict testing".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };
    object_create::<_, MulticastGroup>(client, &group_url, &params).await;

    // Wait for group to become Active before proceeding
    wait_for_group_active(client, "mcast-group-1").await;

    // Create first instance with the multicast group
    instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-1",
        false,
        &["mcast-group-1"],
    )
    .await;

    // Create second instance with the same multicast group
    // This should succeed (multicast groups can have multiple members, unlike floating IPs)
    instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-2",
        false,
        &["mcast-group-1"],
    )
    .await;

    // Wait for reconciler
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify both instances are members of the group
    let members =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(
            client,
            &mcast_group_members_url("mcast-group-1"),
            &format!("project={PROJECT_NAME}"),
            None,
        )
        .await
        .expect("Failed to list multicast group members")
        .all_items;

    assert_eq!(
        members.len(),
        2,
        "Multicast group should support multiple members (unlike floating IPs)"
    );

    // Clean up - use cleanup functions
    cleanup_instances(
        cptestctx,
        client,
        PROJECT_NAME,
        &["mcast-instance-1", "mcast-instance-2"],
    )
    .await;
    cleanup_multicast_groups(client, &["mcast-group-1"]).await;
}

#[nexus_test]
async fn test_multicast_group_attach_limits(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let mcast_pool = create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create multiple multicast groups in parallel to test per-instance limits
    let group_specs = &[
        MulticastGroupForTest {
            name: "limit-test-group-0",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 104)),
            description: Some("Group 0 for limit testing".to_string()),
        },
        MulticastGroupForTest {
            name: "limit-test-group-1",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 105)),
            description: Some("Group 1 for limit testing".to_string()),
        },
        MulticastGroupForTest {
            name: "limit-test-group-2",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 106)),
            description: Some("Group 2 for limit testing".to_string()),
        },
        MulticastGroupForTest {
            name: "limit-test-group-3",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 107)),
            description: Some("Group 3 for limit testing".to_string()),
        },
        MulticastGroupForTest {
            name: "limit-test-group-4",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 108)),
            description: Some("Group 4 for limit testing".to_string()),
        },
    ];

    create_multicast_groups(client, &mcast_pool, group_specs).await;
    let group_names: Vec<&str> = group_specs.iter().map(|g| g.name).collect();

    // Wait for all groups to become Active in parallel
    wait_for_groups_active(client, &group_names).await;

    // Try to create an instance with many multicast groups
    // (Check if there's a reasonable limit per instance)
    let multicast_group_names: Vec<&str> = group_names[0..3].to_vec();

    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-1",
        false,
        &multicast_group_names, // Test with 3 groups (reasonable limit)
    )
    .await;

    // Wait for members to reach "Left" state for each group (instance is stopped, so reconciler transitions "Joining"→"Left")
    for group_name in &multicast_group_names {
        wait_for_member_state(client, group_name, instance.identity.id, "Left")
            .await;
    }

    // Verify instance is member of multiple groups
    for group_name in &multicast_group_names {
        let members_url = mcast_group_members_url(group_name);
        let members = nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<MulticastGroupMember>(
             client,
             &members_url,
             &format!("project={PROJECT_NAME}"),
             None,
         )
         .await
         .expect("Failed to list multicast group members")
         .all_items;

        assert_eq!(
            members.len(),
            1,
            "Instance should be member of group {}",
            group_name
        );
        assert_eq!(members[0].instance_id, instance.identity.id);
    }

    // Clean up - use cleanup functions
    cleanup_instances(cptestctx, client, PROJECT_NAME, &["mcast-instance-1"])
        .await;
    cleanup_multicast_groups(client, &group_names).await;
}

#[nexus_test]
async fn test_multicast_group_instance_state_transitions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let mcast_pool = create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create a multicast group with explicit IP for easy DPD validation
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 200));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "state-test-group".parse().unwrap(),
            description: "Group for testing instance state transitions"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };
    object_create::<_, MulticastGroup>(client, &group_url, &params).await;

    // Wait for group to become Active before proceeding
    wait_for_group_active(client, "state-test-group").await;

    // Create stopped instance and add to multicast group
    let stopped_instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "state-test-instance",
        false, // Create stopped
        &["state-test-group"],
    )
    .await;

    // Verify instance is stopped and in multicast group
    assert_eq!(stopped_instance.runtime.run_state, InstanceState::Stopped);

    // Wait for member to reach "Left" state (reconciler transitions "Joining"→"Left" for stopped instance)
    wait_for_member_state(
        client,
        "state-test-group",
        stopped_instance.identity.id,
        "Left",
    )
    .await;

    // Start the instance and verify multicast behavior
    let instance_id =
        InstanceUuid::from_untyped_uuid(stopped_instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;

    // Start the instance using direct POST request (not PUT)
    let start_url = format!(
        "/v1/instances/state-test-instance/start?project={PROJECT_NAME}"
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Instance>()
    .unwrap();
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Running).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Stop the instance and verify multicast behavior persists
    let stop_url = format!(
        "/v1/instances/state-test-instance/stop?project={PROJECT_NAME}"
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Instance>()
    .unwrap();
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

    // Verify control plane still shows membership regardless of instance state
    let members_url = mcast_group_members_url("state-test-group");
    let final_members: Vec<MulticastGroupMember> =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn(
            client,
            &members_url,
            "",
            None,
        )
        .await
        .unwrap()
        .all_items;

    assert_eq!(
        final_members.len(),
        1,
        "Control plane should maintain multicast membership across instance state changes"
    );
    assert_eq!(final_members[0].instance_id, stopped_instance.identity.id);

    // Clean up
    object_delete(
        client,
        &format!(
            "/v1/instances/{}?project={}",
            "state-test-instance", PROJECT_NAME
        ),
    )
    .await;
    object_delete(client, &mcast_group_url("state-test-group")).await;
}

/// Test that multicast group membership persists through instance stop/start cycles
/// (parallel to external IP persistence behavior)
#[nexus_test]
async fn test_multicast_group_persistence_through_stop_start(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let mcast_pool = create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create a multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 200));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "persist-test-group".parse().unwrap(),
            description: "Group for stop/start persistence testing".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };
    object_create::<_, MulticastGroup>(client, &group_url, &params).await;

    // Wait for group to become Active
    wait_for_group_active(client, "persist-test-group").await;

    // Create instance with the multicast group and start it
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "persist-test-instance",
        true, // start the instance
        &["persist-test-group"],
    )
    .await;

    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate the instance transitioning to Running state
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;

    // Wait for member to be joined (reconciler will be triggered by instance start)
    wait_for_member_state(
        client,
        "persist-test-group",
        instance.identity.id,
        "Joined",
    )
    .await;

    // Verify instance is in the group
    let members_url = mcast_group_members_url("persist-test-group");
    let members_before_stop =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(client, &members_url, "", None)
        .await
        .expect("Failed to list group members before stop")
        .all_items;

    assert_eq!(
        members_before_stop.len(),
        1,
        "Group should have 1 member before stop"
    );
    assert_eq!(members_before_stop[0].instance_id, instance.identity.id);

    // Stop the instance
    let instance_stop_url = format!(
        "/v1/instances/{}/stop?project={}",
        "persist-test-instance", PROJECT_NAME
    );
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::POST,
            &instance_stop_url,
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to stop instance");

    // Simulate the transition and wait for stopped state
    let nexus = &cptestctx.server.server_context().nexus;
    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Running instance should be on a sled");
    info.sled_client.vmm_finish_transition(info.propolis_id).await;

    // Wait for instance to be stopped
    instance_wait_for_state(
        client,
        instance_id,
        omicron_common::api::external::InstanceState::Stopped,
    )
    .await;

    // Verify multicast group membership persists while stopped
    let members_while_stopped =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(client, &members_url, "", None)
        .await
        .expect("Failed to list group members while stopped")
        .all_items;

    assert_eq!(
        members_while_stopped.len(),
        1,
        "Group membership should persist while instance is stopped"
    );
    assert_eq!(members_while_stopped[0].instance_id, instance.identity.id);

    // Start the instance again
    let instance_start_url = format!(
        "/v1/instances/{}/start?project={}",
        "persist-test-instance", PROJECT_NAME
    );
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::POST,
            &instance_start_url,
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to start instance");

    // Simulate the instance transitioning back to "Running" state
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Wait for instance to be running again
    instance_wait_for_state(
        client,
        instance_id,
        omicron_common::api::external::InstanceState::Running,
    )
    .await;

    // Wait for reconciler to process the instance restart
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify multicast group membership still exists after restart
    let members_after_restart =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(client, &members_url, "", None)
        .await
        .expect("Failed to list group members after restart")
        .all_items;

    assert_eq!(
        members_after_restart.len(),
        1,
        "Group membership should persist after instance restart"
    );
    assert_eq!(members_after_restart[0].instance_id, instance.identity.id);

    // Wait for member to be joined again after restart
    wait_for_member_state(
        client,
        "persist-test-group",
        instance.identity.id,
        "Joined",
    )
    .await;

    // Clean up: Remove instance from multicast group before deletion
    let instance_update_url = format!(
        "/v1/instances/{}?project={}",
        "persist-test-instance", PROJECT_NAME
    );

    let update_params = InstanceUpdate {
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        boot_disk: Nullable(None),
        auto_restart_policy: Nullable(None),
        cpu_platform: Nullable(None),
        multicast_groups: Some(vec![]), // Remove from all multicast groups
    };

    object_put::<_, Instance>(client, &instance_update_url, &update_params)
        .await;

    // Stop the instance before deletion (some systems require this)
    let instance_stop_url = format!(
        "/v1/instances/{}/stop?project={}",
        "persist-test-instance", PROJECT_NAME
    );
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::POST,
            &instance_stop_url,
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to stop instance before deletion");

    // Simulate the stop transition
    let nexus = &cptestctx.server.server_context().nexus;
    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Running instance should be on a sled");
    info.sled_client.vmm_finish_transition(info.propolis_id).await;

    // Wait for instance to be stopped
    instance_wait_for_state(
        client,
        instance_id,
        omicron_common::api::external::InstanceState::Stopped,
    )
    .await;

    // Clean up
    object_delete(
        client,
        &format!(
            "/v1/instances/{}?project={}",
            "persist-test-instance", PROJECT_NAME
        ),
    )
    .await;

    object_delete(client, &mcast_group_url("persist-test-group")).await;
}

/// Verify concurrent multicast operations maintain correct member states.
///
/// The system handles multiple instances joining simultaneously, rapid attach/detach
/// cycles, and concurrent operations during reconciler processing. These scenarios
/// expose race conditions in member state transitions, reconciler processing, and
/// DPD synchronization that sequential tests can't catch.
#[nexus_test]
async fn test_multicast_concurrent_operations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "concurrent-pool",
        (224, 40, 0, 1),
        (224, 40, 0, 255),
    )
    .await;

    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 40, 0, 100));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "concurrent-test-group".parse().unwrap(),
            description: "Group for concurrent operations testing".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };
    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, "concurrent-test-group").await;

    // Create multiple instances for concurrent testing
    let instance_names = [
        "concurrent-instance-1",
        "concurrent-instance-2",
        "concurrent-instance-3",
        "concurrent-instance-4",
    ];

    // Create all instances in parallel (now that we fixed the cleanup double-delete bug)
    let create_futures = instance_names
        .iter()
        .map(|name| create_instance(client, PROJECT_NAME, name));
    let instances = ops::join_all(create_futures).await;

    // Attach all instances to the multicast group in parallel (this is the optimization)
    multicast_group_attach_bulk(
        client,
        PROJECT_NAME,
        &instance_names,
        "concurrent-test-group",
    )
    .await;

    // Verify all members reached correct state despite concurrent operations
    for instance in instances.iter() {
        wait_for_member_state(
            client,
            "concurrent-test-group",
            instance.identity.id,
            "Joined", // create_instance() starts instances, so they should be Joined
        )
        .await;
    }

    // Verify final member count matches expected (all 4 instances)
    let members =
        list_multicast_group_members(client, "concurrent-test-group").await;
    assert_eq!(
        members.len(),
        4,
        "All 4 instances should be members after concurrent addition"
    );

    // Concurrent rapid attach/detach cycles (stress test state transitions)

    // Detach first two instances concurrently
    let instance_names_to_detach =
        ["concurrent-instance-1", "concurrent-instance-2"];
    multicast_group_detach_bulk(
        client,
        PROJECT_NAME,
        &instance_names_to_detach,
        "concurrent-test-group",
    )
    .await;

    // Wait for member count to reach 2 after detachments
    wait_for_member_count(client, "concurrent-test-group", 2).await;

    // Re-attach one instance while detaching another (overlapping operations)
    let reattach_future = multicast_group_attach(
        client,
        PROJECT_NAME,
        "concurrent-instance-1",
        "concurrent-test-group",
    );
    let detach_future = multicast_group_detach(
        client,
        PROJECT_NAME,
        "concurrent-instance-3",
        "concurrent-test-group",
    );

    // Execute overlapping operations
    ops::join2(reattach_future, detach_future).await;

    // Wait for final state to be consistent (should still have 2 members)
    wait_for_member_count(client, "concurrent-test-group", 2).await;

    // Concurrent operations during reconciler processing

    // Start a member addition and immediately follow with another operation
    // This tests handling of operations that arrive while reconciler is processing
    let rapid_ops_future = async {
        multicast_group_attach(
            client,
            PROJECT_NAME,
            "concurrent-instance-3",
            "concurrent-test-group",
        )
        .await;
        // Don't wait for reconciler - immediately do another operation
        multicast_group_detach(
            client,
            PROJECT_NAME,
            "concurrent-instance-4",
            "concurrent-test-group",
        )
        .await;
    };

    rapid_ops_future.await;

    // Wait for system to reach consistent final state (should have 2 members)
    wait_for_member_count(client, "concurrent-test-group", 2).await;

    // Get the final members for state verification
    let post_rapid_members =
        list_multicast_group_members(client, "concurrent-test-group").await;

    // Wait for all remaining members to reach "Joined" state
    for member in &post_rapid_members {
        wait_for_member_state(
            client,
            "concurrent-test-group",
            member.instance_id,
            "Joined",
        )
        .await;
    }

    // Cleanup
    cleanup_instances(cptestctx, client, PROJECT_NAME, &instance_names).await;
    cleanup_multicast_groups(client, &["concurrent-test-group"]).await;
}

/// Verify that multicast members are properly cleaned up when an instance
/// is deleted without ever starting (orphaned member cleanup).
///
/// When an instance is created and added to a multicast group but never started,
/// the member enters "Joining" state with sled_id=NULL. If the instance is then
/// deleted before ever starting, the RPW reconciler must detect and clean up the
/// orphaned member to prevent it from remaining stuck in "Joining" state.
#[nexus_test]
async fn test_multicast_member_cleanup_instance_never_started(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "never-started-project";
    let group_name = "never-started-group";
    let instance_name = "never-started-instance";

    // Setup: project, pools, group
    create_project(client, project_name).await;
    create_default_ip_pool(client).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        client,
        "never-started-pool",
        (224, 50, 0, 1),
        (224, 50, 0, 255),
    )
    .await;

    // Create multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 50, 0, 100));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for never-started instance test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Create instance but don't start it - use start: false
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance that will never be started".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false, // Critical: don't start the instance
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;

    // Add instance as multicast member (will be in "Joining" state with no sled_id)
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

    // Wait specifically for member to reach "Left" state since instance was created stopped
    wait_for_member_state(client, group_name, instance.identity.id, "Left")
        .await;

    // Verify member count
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have one member");

    // Delete the instance directly without starting it
    // This simulates the case where an instance is created, added to multicast group,
    // but then deleted before ever starting (never gets a sled assignment)
    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    object_delete(client, &instance_url).await;

    // Wait for reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Critical test: Verify the orphaned member was cleaned up
    // The RPW reconciler should detect that the member's instance was deleted
    // and remove the member from the group
    let final_members = list_multicast_group_members(client, group_name).await;
    assert_eq!(
        final_members.len(),
        0,
        "Orphaned member should be cleaned up when instance is deleted without starting"
    );

    // Cleanup
    cleanup_multicast_groups(client, &[group_name]).await;
}

/// Verify multicast group membership persists through instance migration.
///
/// The RPW reconciler detects sled_id changes and updates DPD configuration on
/// both source and target switches to maintain uninterrupted multicast traffic.
/// Member state follows the expected lifecycle: Joined on source sled → sled_id
/// updated during migration → Joined again on target sled after reconciler
/// processes the change.
#[nexus_test(extra_sled_agents = 1)]
async fn test_multicast_group_membership_during_migration(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "migration-test-project";
    let group_name = "migration-test-group";
    let instance_name = "migration-test-instance";

    // Setup: project, pools, and multicast group
    create_project(client, project_name).await;
    create_default_ip_pool(client).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        client,
        "migration-pool",
        (224, 60, 0, 1),
        (224, 60, 0, 255),
    )
    .await;

    // Create multicast group with mvlan
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 60, 0, 100));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for migration testing with mvlan".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: Some(VlanID::new(3000).unwrap()), // Test mvlan persistence through migration
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Verify mvlan is set
    assert_eq!(
        created_group.mvlan,
        Some(VlanID::new(3000).unwrap()),
        "MVLAN should be set on group creation"
    );

    // Create and start instance with multicast group membership
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_name,
        true, // start the instance
        &[group_name],
    )
    .await;

    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate instance startup and wait for Running state
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Wait for instance to reach "Joined" state (member creation is processed by reconciler)
    wait_for_member_state(client, group_name, instance.identity.id, "Joined")
        .await;

    let pre_migration_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(pre_migration_members.len(), 1);
    assert_eq!(pre_migration_members[0].instance_id, instance.identity.id);
    assert_eq!(pre_migration_members[0].state, "Joined");

    // Verify mvlan is in DPD before migration
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    let pre_migration_dpd_group = dpd_client
        .multicast_group_get(&multicast_ip)
        .await
        .expect("Multicast group should exist in DPD before migration");

    match pre_migration_dpd_group.into_inner() {
        dpd_client::types::MulticastGroupResponse::External {
            external_forwarding,
            ..
        } => {
            assert_eq!(
                external_forwarding.vlan_id,
                Some(3000),
                "DPD should show vlan_id=3000 before migration"
            );
        }
        dpd_client::types::MulticastGroupResponse::Underlay { .. } => {
            panic!("Expected external group, got underlay");
        }
    }

    // Get source and target sleds for migration
    let source_sled_id = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Running instance should be on a sled")
        .sled_id;

    let target_sled_id = if source_sled_id == cptestctx.first_sled_id() {
        cptestctx.second_sled_id()
    } else {
        cptestctx.first_sled_id()
    };

    // Initiate migration
    let migrate_url = format!("/instances/{instance_id}/migrate");
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            lockstep_client,
            Method::POST,
            &migrate_url,
        )
        .body(Some(&InstanceMigrateRequest { dst_sled_id: target_sled_id }))
        .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to initiate instance migration");

    // Get propolis IDs for source and target - follow the pattern from existing tests
    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Instance should be on a sled");
    let src_propolis_id = info.propolis_id;
    let dst_propolis_id =
        info.dst_propolis_id.expect("Instance should have a migration target");

    // Helper function from instances.rs
    async fn vmm_simulate_on_sled(
        _cptestctx: &ControlPlaneTestContext,
        nexus: &std::sync::Arc<omicron_nexus::Nexus>,
        sled_id: omicron_uuid_kinds::SledUuid,
        propolis_id: omicron_uuid_kinds::PropolisUuid,
    ) {
        let sa = nexus.sled_client(&sled_id).await.unwrap();
        sa.vmm_finish_transition(propolis_id).await;
    }

    // Complete migration on source sled
    vmm_simulate_on_sled(cptestctx, nexus, source_sled_id, src_propolis_id)
        .await;

    // Complete migration on target sled
    vmm_simulate_on_sled(cptestctx, nexus, target_sled_id, dst_propolis_id)
        .await;

    // Wait for migration to complete
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Verify instance is now on the target sled
    let post_migration_sled = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Migrated instance should still be on a sled")
        .sled_id;

    assert_eq!(
        post_migration_sled, target_sled_id,
        "Instance should be on target sled after migration"
    );

    // Wait for multicast reconciler to process the sled_id change
    // The RPW reconciler should detect the sled_id change and re-apply DPD configuration
    wait_for_multicast_reconciler(lockstep_client).await;

    // Verify multicast membership persists after migration
    let post_migration_members =
        list_multicast_group_members(client, group_name).await;

    assert_eq!(
        post_migration_members.len(),
        1,
        "Multicast membership should persist through migration"
    );
    assert_eq!(post_migration_members[0].instance_id, instance.identity.id);

    // Wait for member to reach "Joined" state on target sled
    // The RPW reconciler should transition the member back to "Joined" after re-applying DPD configuration
    wait_for_member_state(client, group_name, instance.identity.id, "Joined")
        .await;

    let final_member_state = &post_migration_members[0];
    assert_eq!(
        final_member_state.state, "Joined",
        "Member should be in 'Joined' state after migration completes"
    );

    // Verify mvlan persisted in DPD after migration
    let post_migration_dpd_group = dpd_client
        .multicast_group_get(&multicast_ip)
        .await
        .expect("Multicast group should exist in DPD after migration");

    match post_migration_dpd_group.into_inner() {
        dpd_client::types::MulticastGroupResponse::External {
            external_forwarding,
            ..
        } => {
            assert_eq!(
                external_forwarding.vlan_id,
                Some(3000),
                "DPD should still show vlan_id=3000 after migration - mvlan must persist"
            );
        }
        dpd_client::types::MulticastGroupResponse::Underlay { .. } => {
            panic!("Expected external group, got underlay");
        }
    }

    // Cleanup: Stop and delete instance, then cleanup group
    let stop_url =
        format!("/v1/instances/{instance_name}/stop?project={project_name}");
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            Method::POST,
            &stop_url,
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to stop instance");

    // Simulate stop and wait for stopped state
    let final_info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Instance should still be active for stop");
    final_info.sled_client.vmm_finish_transition(final_info.propolis_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Delete instance and cleanup
    object_delete(
        client,
        &format!("/v1/instances/{instance_name}?project={project_name}"),
    )
    .await;

    cleanup_multicast_groups(client, &[group_name]).await;
}

/// Verify the RPW reconciler handles concurrent instance migrations within the same multicast group.
///
/// Multiple instances in the same multicast group can migrate simultaneously without
/// interfering with each other's membership states. The reconciler correctly processes
/// concurrent sled_id changes for all members, ensuring each reaches Joined state on
/// their respective target sleds.
#[nexus_test(extra_sled_agents = 2)]
async fn test_multicast_group_concurrent_member_migrations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "concurrent-migration-project";
    let group_name = "concurrent-migration-group";

    // Setup: project, pools, and multicast group
    create_project(client, project_name).await;
    create_default_ip_pool(client).await;
    let mcast_pool = create_multicast_ip_pool_with_range(
        client,
        "concurrent-migration-pool",
        (224, 62, 0, 1),
        (224, 62, 0, 255),
    )
    .await;

    // Create multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 62, 0, 100));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for concurrent migration testing".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Create multiple instances all in the same multicast group
    let instance_specs = [
        ("concurrent-instance-1", &[group_name][..]),
        ("concurrent-instance-2", &[group_name][..]),
    ];

    let instances = create_instances_with_multicast_groups(
        client,
        project_name,
        &instance_specs,
        true, // start instances
    )
    .await;

    let instance_ids: Vec<_> = instances
        .iter()
        .map(|i| InstanceUuid::from_untyped_uuid(i.identity.id))
        .collect();

    // Simulate all instances to Running state in parallel
    let simulate_futures = instance_ids.iter().map(|&instance_id| async move {
        instance_simulate(nexus, &instance_id).await;
        instance_wait_for_state(client, instance_id, InstanceState::Running)
            .await;
    });
    ops::join_all(simulate_futures).await;

    // Wait for all members to reach "Joined" state
    for instance in &instances {
        wait_for_member_state(
            client,
            group_name,
            instance.identity.id,
            "Joined",
        )
        .await;
    }

    // Verify we have 2 members initially
    let pre_migration_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(pre_migration_members.len(), 2);

    // Get current sleds for all instances
    let mut source_sleds = Vec::new();
    let mut target_sleds = Vec::new();

    let available_sleds =
        [cptestctx.first_sled_id(), cptestctx.second_sled_id()];

    for &instance_id in &instance_ids {
        let current_sled = nexus
            .active_instance_info(&instance_id, None)
            .await
            .unwrap()
            .expect("Running instance should be on a sled")
            .sled_id;
        source_sleds.push(current_sled);

        // Find a different sled for migration target
        let target_sled = available_sleds
            .iter()
            .find(|&&sled| sled != current_sled)
            .copied()
            .expect("Should have available target sled");
        target_sleds.push(target_sled);
    }

    // Initiate both migrations concurrently
    let migration_futures = instance_ids.iter().zip(target_sleds.iter()).map(
        |(&instance_id, &target_sled)| {
            let migrate_url = format!("/instances/{instance_id}/migrate");
            nexus_test_utils::http_testing::NexusRequest::new(
                nexus_test_utils::http_testing::RequestBuilder::new(
                    lockstep_client,
                    Method::POST,
                    &migrate_url,
                )
                .body(Some(&InstanceMigrateRequest {
                    dst_sled_id: target_sled,
                }))
                .expect_status(Some(StatusCode::OK)),
            )
            .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
            .execute()
        },
    );

    // Execute both migrations concurrently
    let migration_responses = ops::join_all(migration_futures).await;

    // Verify both migrations were initiated successfully
    for response in migration_responses {
        response.expect("Migration should initiate successfully");
    }

    // Complete both migrations by simulating on both source and target sleds
    for (i, &instance_id) in instance_ids.iter().enumerate() {
        // Get propolis IDs for this instance
        let info = nexus
            .active_instance_info(&instance_id, None)
            .await
            .unwrap()
            .expect("Instance should be on a sled");
        let src_propolis_id = info.propolis_id;
        let dst_propolis_id = info
            .dst_propolis_id
            .expect("Instance should have a migration target");

        // Helper function from instances.rs
        async fn vmm_simulate_on_sled(
            _cptestctx: &ControlPlaneTestContext,
            nexus: &std::sync::Arc<omicron_nexus::Nexus>,
            sled_id: omicron_uuid_kinds::SledUuid,
            propolis_id: omicron_uuid_kinds::PropolisUuid,
        ) {
            let sa = nexus.sled_client(&sled_id).await.unwrap();
            sa.vmm_finish_transition(propolis_id).await;
        }

        // Complete migration on source and target
        vmm_simulate_on_sled(
            cptestctx,
            nexus,
            source_sleds[i],
            src_propolis_id,
        )
        .await;
        vmm_simulate_on_sled(
            cptestctx,
            nexus,
            target_sleds[i],
            dst_propolis_id,
        )
        .await;

        instance_wait_for_state(client, instance_id, InstanceState::Running)
            .await;
    }

    // Verify all instances are on their target sleds
    for (i, &instance_id) in instance_ids.iter().enumerate() {
        let current_sled = nexus
            .active_instance_info(&instance_id, None)
            .await
            .unwrap()
            .expect("Migrated instance should be on target sled")
            .sled_id;

        assert_eq!(
            current_sled,
            target_sleds[i],
            "Instance {} should be on target sled after migration",
            i + 1
        );
    }

    // Wait for multicast reconciler to process all sled_id changes
    wait_for_multicast_reconciler(lockstep_client).await;

    // Verify all members are still in the group and reach "Joined" state
    let post_migration_members =
        list_multicast_group_members(client, group_name).await;

    assert_eq!(
        post_migration_members.len(),
        2,
        "Both instances should remain multicast group members after concurrent migration"
    );

    // Verify both members reach "Joined" state on their new sleds
    for instance in &instances {
        wait_for_member_state(
            client,
            group_name,
            instance.identity.id,
            "Joined",
        )
        .await;
    }

    // Cleanup
    let instance_names = ["concurrent-instance-1", "concurrent-instance-2"];
    cleanup_instances(cptestctx, client, project_name, &instance_names).await;
    cleanup_multicast_groups(client, &[group_name]).await;
}
