// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/
//
// Copyright 2025 Oxide Computer Company

//! Tests multicast group + instance integration.
//!
//! Instance lifecycle tests:
//!
//! - Full lifecycle: Create, attach, start, stop, delete flows
//! - Attach conflicts: Cannot attach same instance twice to same group
//! - Attach limits: Validates per-instance multicast group limits
//! - State transitions: Member states change with instance state
//! - Persistence: Memberships survive instance stop/start cycles
//! - Concurrent operations: Parallel attach/detach operations
//! - Never-started instances: Cleanup of members for instances never started
//! - Migration: Memberships update correctly when instance migrates

use http::{Method, StatusCode};

use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, object_create,
    object_delete, object_get,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceNetworkInterfaceAttachment, MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::{MulticastGroup, MulticastGroupMember};
use nexus_types::internal_api::params::InstanceMigrateRequest;

use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    InstanceState, NameOrId,
};
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

    // Create project and pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool-comprehensive",
            (224, 30, 0, 1),   // Large range: 224.30.0.1
            (224, 30, 0, 255), // to 224.30.0.255 (255 IPs)
        ),
    )
    .await;

    // Group names for implicit groups (implicitly created when first member joins)
    let group_names = [
        "group-lifecycle-1",
        "group-lifecycle-2",
        "group-lifecycle-3",
        "group-lifecycle-4",
    ];

    // Create instances first (groups will be implicitly created when members attach)
    let instances = vec![
        // Instance for group-lifecycle-1 (will implicitly create the group)
        instance_for_multicast_groups(
            cptestctx,
            PROJECT_NAME,
            "instance-create-attach",
            false,
            &[],
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

    // Implicitly create group-lifecycle-1 by adding a member
    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project={PROJECT_NAME}",
        group_names[0]
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("instance-create-attach".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group-lifecycle-1 to become active and verify membership
    wait_for_group_active(client, group_names[0]).await;
    wait_for_member_state(
        cptestctx,
        "group-lifecycle-1",
        instances[0].identity.id,
        // Instance is stopped, so should be "Left"
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    // Live attach/detach operations
    // Attach instance-live-1 to group-lifecycle-2 (implicitly creates the group)
    multicast_group_attach_with_pool(
        cptestctx,
        PROJECT_NAME,
        "instance-live-1",
        "group-lifecycle-2",
        Some(mcast_pool.identity.name.as_str()),
    )
    .await;

    // Wait for group-lifecycle-2 to become active
    wait_for_group_active(client, group_names[1]).await;

    // Attach instance-live-2 to group-lifecycle-2 (test multiple instances per group)
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "instance-live-2",
        "group-lifecycle-2",
    )
    .await;

    // Verify both instances are attached to group-lifecycle-2
    for i in 0..2 {
        wait_for_member_state(
            cptestctx,
            "group-lifecycle-2",
            instances[i + 1].identity.id,
            nexus_db_model::MulticastGroupMemberState::Left, // Stopped instances
        )
        .await;
    }

    // Multi-group attachment (instance to multiple groups)
    // Attach instance-multi-groups to group-lifecycle-3 (implicitly creates the group)
    multicast_group_attach_with_pool(
        cptestctx,
        PROJECT_NAME,
        "instance-multi-groups",
        "group-lifecycle-3",
        Some(mcast_pool.identity.name.as_str()),
    )
    .await;

    // Wait for group-lifecycle-3 to become active
    wait_for_group_active(client, group_names[2]).await;

    // Attach instance-multi-groups to group-lifecycle-4 (implicitly creates the group)
    multicast_group_attach_with_pool(
        cptestctx,
        PROJECT_NAME,
        "instance-multi-groups",
        "group-lifecycle-4",
        Some(mcast_pool.identity.name.as_str()),
    )
    .await;

    // Wait for group-lifecycle-4 to become active
    wait_for_group_active(client, group_names[3]).await;

    // Verify multi-group membership
    for group_name in ["group-lifecycle-3", "group-lifecycle-4"] {
        wait_for_member_state(
            cptestctx,
            group_name,
            instances[3].identity.id,
            nexus_db_model::MulticastGroupMemberState::Left, // Stopped instance
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

    // Test idempotency
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
        .expect("Should list multicast group members")
        .all_items;

    // Should only have instance-live-2 as member now
    assert_eq!(
        members.len(),
        1,
        "group-lifecycle-2 should have 1 member after detach"
    );
    assert_eq!(members[0].instance_id, instances[2].identity.id);

    // Verify groups are still active and functional
    for group_name in group_names.iter() {
        let group_url = mcast_group_url(group_name);
        let current_group: MulticastGroup =
            object_get(client, &group_url).await;
        assert_eq!(
            current_group.state, "Active",
            "Group {group_name} should remain Active throughout lifecycle"
        );
    }

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

    // Implicit model: groups are implicitly deleted when last member (instance) is removed
    ops::join4(
        wait_for_group_deleted(client, group_names[0]),
        wait_for_group_deleted(client, group_names[1]),
        wait_for_group_deleted(client, group_names[2]),
        wait_for_group_deleted(client, group_names[3]),
    )
    .await;
}

#[nexus_test]
async fn test_multicast_group_attach_conflicts(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool-conflicts",
            (224, 23, 0, 1),   // Unique range: 224.23.0.1
            (224, 23, 0, 255), // to 224.23.0.255
        ),
    )
    .await;

    // Create first instance (implicit model: first instance creates the group)
    instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-1",
        false,
        &[],
    )
    .await;

    // Add instance1 to group (group implicitly creates if it doesn't exist)
    let member_add_url = format!(
        "{}?project={PROJECT_NAME}",
        mcast_group_members_url("mcast-group-1")
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("mcast-instance-1".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become Active before proceeding
    wait_for_group_active(client, "mcast-group-1").await;

    // Create second instance and add to same multicast group
    // This should succeed (multicast groups can have multiple members, unlike floating IPs)
    instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-2",
        false,
        &[],
    )
    .await;
    let member2_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("mcast-instance-2".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member2_params,
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
        .expect("Should list multicast group members")
        .all_items;

    assert_eq!(
        members.len(),
        2,
        "Multicast group should support multiple members (unlike floating IPs)"
    );

    cleanup_instances(
        cptestctx,
        client,
        PROJECT_NAME,
        &["mcast-instance-1", "mcast-instance-2"],
    )
    .await;
    wait_for_group_deleted(client, "mcast-group-1").await;
}

#[nexus_test]
async fn test_multicast_group_attach_limits(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Group names for implicit groups (implicitly created when first member joins)
    let group_names = [
        "limit-test-group-0",
        "limit-test-group-1",
        "limit-test-group-2",
        "limit-test-group-3",
        "limit-test-group-4",
    ];

    // Create instance first (groups will be implicitly created when attached)
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-1",
        false,
        &[], // No groups at creation
    )
    .await;

    // Attach instance to 3 groups (implicitly creates each group)
    let multicast_group_names = &group_names[0..3];
    for group_name in multicast_group_names {
        multicast_group_attach_with_pool(
            cptestctx,
            PROJECT_NAME,
            "mcast-instance-1",
            group_name,
            Some(mcast_pool.identity.name.as_str()),
        )
        .await;
    }

    // Wait for all groups to become active in parallel
    wait_for_groups_active(client, multicast_group_names).await;

    // Wait for members to reach "Left" state for each group
    // (instance is stopped, so member starts in "Left" state with no sled_id)
    for group_name in multicast_group_names {
        wait_for_member_state(
            cptestctx,
            group_name,
            instance.identity.id,
            nexus_db_model::MulticastGroupMemberState::Left,
        )
        .await;
    }

    // Verify instance is member of multiple groups
    for group_name in multicast_group_names {
        let members_url = mcast_group_members_url(group_name);
        let members = nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<MulticastGroupMember>(
             client,
             &members_url,
             &format!("project={PROJECT_NAME}"),
             None,
         )
         .await
         .expect("Should list multicast group members")
         .all_items;

        assert_eq!(
            members.len(),
            1,
            "Instance should be member of group {group_name}"
        );
        assert_eq!(members[0].instance_id, instance.identity.id);
    }

    cleanup_instances(cptestctx, client, PROJECT_NAME, &["mcast-instance-1"])
        .await;
    // Groups are implicitly deleted when last member (instance) is removed
    // Only 3 groups were created (group_names[0..3])
    ops::join3(
        wait_for_group_deleted(client, group_names[0]),
        wait_for_group_deleted(client, group_names[1]),
        wait_for_group_deleted(client, group_names[2]),
    )
    .await;
}

#[nexus_test]
async fn test_multicast_group_instance_state_transitions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create stopped instance (no multicast groups at creation)
    let stopped_instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "state-test-instance",
        false,
        &[],
    )
    .await;

    // Add instance to group (group implicitly creates if it doesn't exist)
    let member_add_url = format!(
        "{}?project={PROJECT_NAME}",
        mcast_group_members_url("state-test-group")
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("state-test-instance".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become Active before proceeding
    wait_for_group_active(client, "state-test-group").await;

    // Verify instance is stopped and in multicast group
    assert_eq!(stopped_instance.runtime.run_state, InstanceState::Stopped);

    // Wait for member to reach "Left" state (stopped instance members start in "Left" state)
    wait_for_member_state(
        cptestctx,
        "state-test-group",
        stopped_instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
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

    object_delete(
        client,
        &format!("/v1/instances/state-test-instance?project={PROJECT_NAME}"),
    )
    .await;

    wait_for_group_deleted(client, "state-test-group").await;
}

/// Test that multicast group membership persists through instance stop/start cycles
/// (parallel to external IP persistence behavior)
#[nexus_test]
async fn test_multicast_group_persistence_through_stop_start(
    cptestctx: &ControlPlaneTestContext,
) {
    // Ensure inventory and DPD are ready before creating instances with multicast groups
    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance and start it (no multicast groups at creation)
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "persist-test-instance",
        true,
        &[],
    )
    .await;

    // Add instance to group (group implicitly creates if it doesn't exist)
    let member_add_url = format!(
        "{}?project={PROJECT_NAME}",
        mcast_group_members_url("persist-test-group")
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("persist-test-instance".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become Active
    wait_for_group_active(client, "persist-test-group").await;

    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate the instance transitioning to Running state
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Wait for member to be joined (reconciler will process the sled_id set by instance start)
    wait_for_member_state(
        cptestctx,
        "persist-test-group",
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify instance is in the group
    let members_url = mcast_group_members_url("persist-test-group");
    let members_before_stop =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(client, &members_url, "", None)
        .await
        .expect("Should list group members before stop")
        .all_items;

    assert_eq!(
        members_before_stop.len(),
        1,
        "Group should have 1 member before stop"
    );
    assert_eq!(members_before_stop[0].instance_id, instance.identity.id);

    // Stop the instance
    let instance_stop_url = format!(
        "/v1/instances/persist-test-instance/stop?project={PROJECT_NAME}"
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
    .expect("Should stop instance");

    // Simulate the stop transition
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;

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
        .expect("Should list group members while stopped")
        .all_items;

    assert_eq!(
        members_while_stopped.len(),
        1,
        "Group membership should persist while instance is stopped"
    );
    assert_eq!(members_while_stopped[0].instance_id, instance.identity.id);

    // Start the instance again
    let instance_start_url = format!(
        "/v1/instances/persist-test-instance/start?project={PROJECT_NAME}"
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
    .expect("Should start instance");

    // Simulate the instance transitioning back to "Running" state
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;

    // Wait for instance to be running again
    instance_wait_for_state(
        client,
        instance_id,
        omicron_common::api::external::InstanceState::Running,
    )
    .await;

    // Verify multicast group membership still exists after restart
    let members_after_restart =
        nexus_test_utils::http_testing::NexusRequest::iter_collection_authn::<
            MulticastGroupMember,
        >(client, &members_url, "", None)
        .await
        .expect("Should list group members after restart")
        .all_items;

    assert_eq!(
        members_after_restart.len(),
        1,
        "Group membership should persist after instance restart"
    );
    assert_eq!(members_after_restart[0].instance_id, instance.identity.id);

    // Wait for member to be joined again after restart
    wait_for_member_state(
        cptestctx,
        "persist-test-group",
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    cleanup_instances(
        cptestctx,
        client,
        PROJECT_NAME,
        &["persist-test-instance"],
    )
    .await;
    // Group is implicitly deleted when last member (instance) is removed
    wait_for_group_deleted(client, "persist-test-group").await;
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
    // Ensure inventory and DPD are ready before creating instances with multicast groups
    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool_with_range(
            &client,
            "concurrent-pool",
            (224, 40, 0, 1),
            (224, 40, 0, 255),
        ),
    )
    .await;

    // Create multiple instances for concurrent testing
    let instance_names = [
        "concurrent-instance-1",
        "concurrent-instance-2",
        "concurrent-instance-3",
        "concurrent-instance-4",
    ];

    // Create all instances in parallel
    let create_futures = instance_names
        .iter()
        .map(|name| create_instance(client, PROJECT_NAME, name));
    let instances = ops::join_all(create_futures).await;

    // First instance attach with pool (implicitly creates the group)
    multicast_group_attach_with_pool(
        cptestctx,
        PROJECT_NAME,
        instance_names[0],
        "concurrent-test-group",
        Some(mcast_pool.identity.name.as_str()),
    )
    .await;
    wait_for_group_active(client, "concurrent-test-group").await;

    // Attach remaining instances to the existing group in parallel
    multicast_group_attach_bulk(
        cptestctx,
        PROJECT_NAME,
        &instance_names[1..],
        "concurrent-test-group",
    )
    .await;

    // Verify all members reached correct state despite concurrent operations
    for instance in instances.iter() {
        wait_for_member_state(
            cptestctx,
            "concurrent-test-group",
            instance.identity.id,
            // create_instance() starts instances, so they should be Joined
            nexus_db_model::MulticastGroupMemberState::Joined,
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
        cptestctx,
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
            cptestctx,
            PROJECT_NAME,
            "concurrent-instance-3",
            "concurrent-test-group",
        )
        .await;
        // Don't wait for reconciler; immediately do another operation
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
            cptestctx,
            "concurrent-test-group",
            member.instance_id,
            nexus_db_model::MulticastGroupMemberState::Joined,
        )
        .await;
    }

    // Cleanup and delete instances (group is implicitly deleted when last member removed)
    cleanup_instances(cptestctx, client, PROJECT_NAME, &instance_names).await;
    wait_for_group_deleted(client, "concurrent-test-group").await;
}

/// Verify that multicast members are properly cleaned up when an instance
/// is deleted without ever starting (orphaned member cleanup).
///
/// When an instance is created and added to a multicast group but never started,
/// the member enters "Left" state with sled_id=NULL. If the instance is then
/// deleted before ever starting, the RPW reconciler must detect and clean up the
/// orphaned member.
#[nexus_test]
async fn test_multicast_member_cleanup_instance_never_started(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "never-started-project";
    let group_name = "never-started-group";
    let instance_name = "never-started-instance";

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "never-started-pool",
            (224, 50, 0, 1),
            (224, 50, 0, 255),
        ),
    )
    .await;

    // Create instance but don't start it
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
        start: false, // Don't start the instance
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;

    // Add instance as multicast member (implicitly creates group)
    // Member will be in "Left" state since instance is stopped with no sled_id
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;
    wait_for_group_active(client, group_name).await;

    // Wait for member to reach "Left" state (stopped instance with no sled_id)
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    // Verify member count
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have one member");

    // Save underlay group info BEFORE deleting the instance
    // (After deletion, the group will be deleted too since it was implicitly created)
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    // Fetch the external group from the view to get its multicast_ip
    let external_group_view = get_multicast_group(client, group_name).await;
    let multicast_ip = external_group_view.multicast_ip;

    // Fetch the external group from datastore to get its underlay_group_id
    let external_group = datastore
        .multicast_group_lookup_by_ip(&opctx, multicast_ip)
        .await
        .expect("Should lookup external multicast group by IP");

    let underlay_group_id = external_group
        .underlay_group_id
        .expect("External group should have underlay_group_id");

    // Fetch the underlay group to get its multicast IP
    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .expect("Should fetch underlay multicast group");

    let underlay_multicast_ip = underlay_group.multicast_ip.ip();

    // Delete the instance directly without starting it
    // This simulates the case where an instance is created, added to multicast group,
    // but then deleted before ever starting (never gets a sled assignment)
    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    object_delete(client, &instance_url).await;

    // Verify the orphaned member was cleaned up
    // The RPW reconciler should detect that the member's instance was deleted
    // and remove the member from the group. Since this was an implicitly created
    // group and the last member was removed, the group itself should be deleted.
    wait_for_group_deleted(client, group_name).await;

    // Verify that stale ports were removed from DPD
    // Since the instance never started (never had a `sled_id`), there should be
    // no rear/underlay ports in DPD for this group.
    // Note: We use the underlay IP we saved before deleting the instance.
    wait_for_group_deleted_from_dpd(cptestctx, underlay_multicast_ip).await;
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
    // Ensure inventory and DPD are ready before creating instances with multicast groups
    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "migration-test-project";
    let group_name = "migration-test-group";
    let instance_name = "migration-test-instance";

    // Create project and pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "migration-pool",
            (224, 60, 0, 1),
            (224, 60, 0, 255),
        ),
    )
    .await;

    // Create and start instance first (no multicast groups at creation)
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_name,
        true,
        &[],
    )
    .await;

    // Add instance to group (group implicitly creates if it doesn't exist)
    multicast_group_attach_with_pool(
        cptestctx,
        project_name,
        instance_name,
        group_name,
        Some(mcast_pool.identity.name.as_str()),
    )
    .await;
    wait_for_group_active(client, group_name).await;

    // Get the group's multicast IP for DPD verification later
    let created_group = get_multicast_group(client, group_name).await;
    let multicast_ip = created_group.multicast_ip;

    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate instance startup and wait for Running state
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Wait for instance to reach "Joined" state (member creation is processed by reconciler)
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    let pre_migration_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(pre_migration_members.len(), 1);
    assert_eq!(pre_migration_members[0].instance_id, instance.identity.id);
    assert_eq!(pre_migration_members[0].state, "Joined");

    // Verify group exists in DPD before migration
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    dpd_client
        .multicast_group_get(&multicast_ip)
        .await
        .expect("Multicast group should exist in DPD before migration");

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
    .expect("Should initiate instance migration");

    // Get propolis IDs for source and target
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

    // Complete migration on source sled and wait for instance to enter "Migrating"
    vmm_simulate_on_sled(cptestctx, nexus, source_sled_id, src_propolis_id)
        .await;

    // Instance should transition to "Migrating"; membership should remain "Joined"
    instance_wait_for_state(client, instance_id, InstanceState::Migrating)
        .await;
    let migrating_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        migrating_members.len(),
        1,
        "Membership should remain during migration"
    );
    assert_eq!(migrating_members[0].instance_id, instance.identity.id);
    assert_eq!(
        migrating_members[0].state, "Joined",
        "Member should stay Joined while migrating"
    );

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
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    let final_member_state = &post_migration_members[0];
    assert_eq!(
        final_member_state.state, "Joined",
        "Member should be in 'Joined' state after migration completes"
    );

    // Verify inventory-based port mapping updated correctly after migration
    // This confirms the RPW reconciler correctly mapped the new sled to its rear port
    verify_inventory_based_port_mapping(cptestctx, &instance_id)
        .await
        .expect("Port mapping should be updated after migration");

    // Verify group still exists in DPD after migration
    dpd_client
        .multicast_group_get(&multicast_ip)
        .await
        .expect("Multicast group should exist in DPD after migration");

    // Cleanup: Stop and delete instance
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
    .expect("Should stop instance");

    // Simulate stop and wait for stopped state
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Delete instance; group is implicitly deleted when last member removed
    object_delete(
        client,
        &format!("/v1/instances/{instance_name}?project={project_name}"),
    )
    .await;

    // Implicit model: group is implicitly deleted when last member (instance) is removed
    wait_for_group_deleted(client, group_name).await;
}

/// Verify the RPW reconciler handles concurrent instance migrations within the same multicast group.
///
/// Multiple instances in the same multicast group can migrate simultaneously without
/// interfering with each other's membership states. The reconciler correctly processes
/// concurrent sled_id changes for all members, ensuring each reaches Joined state on
/// their respective target sleds.
#[nexus_test(extra_sled_agents = 1)]
async fn test_multicast_group_concurrent_member_migrations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "concurrent-migration-project";
    let group_name = "concurrent-migration-group";

    // Create project and pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "concurrent-migration-pool",
            (224, 62, 0, 1),
            (224, 62, 0, 255),
        ),
    )
    .await;

    // Ensure inventory and DPD are ready before creating instances
    ensure_multicast_test_ready(cptestctx).await;

    // Create multiple instances
    let instance_names = ["concurrent-instance-1", "concurrent-instance-2"];
    let create_futures = instance_names
        .iter()
        .map(|name| create_instance(client, project_name, name));
    let instances = ops::join_all(create_futures).await;

    // First instance attach with pool (implicitly creates the group)
    multicast_group_attach_with_pool(
        cptestctx,
        project_name,
        instance_names[0],
        group_name,
        Some(mcast_pool.identity.name.as_str()),
    )
    .await;
    wait_for_group_active(client, group_name).await;

    // Second instance attach (group already exists)
    multicast_group_attach(
        cptestctx,
        project_name,
        instance_names[1],
        group_name,
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
            cptestctx,
            group_name,
            instance.identity.id,
            nexus_db_model::MulticastGroupMemberState::Joined,
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
            cptestctx,
            group_name,
            instance.identity.id,
            nexus_db_model::MulticastGroupMemberState::Joined,
        )
        .await;
    }

    // Cleanup and delete instances (group is automatically deleted when last member removed)
    cleanup_instances(cptestctx, client, project_name, &instance_names).await;
    wait_for_group_deleted(client, group_name).await;
}
