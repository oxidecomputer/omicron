// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Integration tests for multicast group failure and recovery scenarios.
//!
//! Tests resilience and error handling:
//!
//! - DPD communication failures: Recovery when switch is unavailable
//! - State consistency: Reconciler validates DB state matches DPD state
//! - DPD failures during state transitions: "Creating", "Active", "Deleting" states
//! - Implicit creation/deletion with DPD failures
//! - Concurrent operation races: Implicit creation, deletion with instance join
//! - Drift correction: Reconciler syncs DPD when state is lost
//! - Member lifecycle: "Joining"→"Left" on instance stop, "Left" waits for "Active"
//!
//! ## RPW vs Saga Responsibilities
//!
//! These tests focus on **RPW (Reliable Persistent Workflow)** behavior:
//! - Background reconciliation of database state to dataplane (DPD)
//! - Drift detection and correction when DPD state doesn't match DB
//! - Eventual consistency guarantees across failures
//!
//! **Sagas** (tested indirectly via API operations) handle:
//! - User API requests (instance create/start/stop/delete)
//! - Database state transitions (creating members, setting `sled_id`)
//! - Immediate validation (SSM source requirements, address family)

use http::{Method, StatusCode};

use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_instance, create_instance_with,
    create_project, object_get, objects_list_page_authz,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::instance::{
    ExternalIpCreate, InstanceDiskAttachment,
    InstanceNetworkInterfaceAttachment,
};
use nexus_types::external_api::multicast::{
    InstanceMulticastGroupJoin, MulticastGroup, MulticastGroupJoinSpec,
    MulticastGroupMember,
};
use omicron_common::api::external::{InstanceState, SwitchLocation};
use omicron_uuid_kinds::{InstanceUuid, MulticastGroupUuid};

use super::*;
use crate::integration_tests::instances as instance_helpers;

/// Test DPD failure during group "Creating" state.
///
/// When DPD is unavailable during group activation:
/// - Group stays in Creating state
/// - Member stays in Joining state
/// - After DPD recovery, group becomes Active and member becomes Joined
#[nexus_test]
async fn test_dpd_failure_during_creating_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "creating-dpd-fail-group";
    let instance_name = "creating-fail-instance";

    // Setup: project, pools
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance first
    create_instance(client, project_name, instance_name).await;

    // Stop DPD before implicit creation
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add instance to multicast group via instance-centric API
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Wait for reconciler to process
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Check group state - should remain in "Creating" since DPD is down
    let group_get_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    assert_eq!(
        fetched_group.state, "Creating",
        "Group should remain in Creating state when DPD is unavailable, found: {}",
        fetched_group.state
    );
    assert_eq!(fetched_group.identity.name.as_str(), group_name);

    // Verify member is in Joining state
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have exactly one member");
    assert_eq!(
        members[0].state, "Joining",
        "Member should be Joining when DPD unavailable"
    );

    // Recovery: restart DPD and verify group/member recover
    cptestctx.restart_dendrite(SwitchLocation::Switch0).await;
    activate_multicast_reconciler(&cptestctx.lockstep_client).await;
    wait_for_group_active(client, group_name).await;
    wait_for_member_state(
        cptestctx,
        group_name,
        members[0].instance_id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    let recovered_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        recovered_members[0].state, "Joined",
        "Member should recover to Joined after DPD restart"
    );

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test DPD failure during group "Active" state.
///
/// When DPD becomes unavailable after group is already Active:
/// - Group remains in Active state
/// - Member remains in Joined state
/// - Existing state is preserved despite DPD communication failure
#[nexus_test]
async fn test_dpd_failure_during_active_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "active-dpd-fail-group";
    let instance_name = "active-fail-instance";

    // Setup: project, pools
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    let instance = create_instance(client, project_name, instance_name).await;

    // Add instance to multicast group
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Wait for group to become Active and member to reach Joined
    wait_for_group_active(client, group_name).await;
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify group is Active
    let group_get_url = mcast_group_url(group_name);
    let active_group: MulticastGroup = object_get(client, &group_get_url).await;
    assert_eq!(active_group.state, "Active");

    // Now stop DPD while group is Active
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Wait for reconciler to process
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Group should remain Active despite DPD failure
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;
    assert_eq!(
        fetched_group.state, "Active",
        "Active group should remain Active despite DPD failure, found: {}",
        fetched_group.state
    );
    assert_eq!(fetched_group.identity.name.as_str(), group_name);

    // Member should remain Joined
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have exactly one member");
    assert_eq!(
        members[0].state, "Joined",
        "Member should remain Joined when DPD fails, got: {}",
        members[0].state
    );

    // Cleanup
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    cptestctx.restart_dendrite(SwitchLocation::Switch0).await;
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
}

/// Test DPD failure during group "Deleting" state.
///
/// When DPD is unavailable during implicit group deletion:
/// - Group stays in Deleting state (cannot complete cleanup)
/// - After DPD recovery, deletion completes
#[nexus_test]
async fn test_dpd_failure_during_deleting_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "deleting-dpd-fail-group";
    let instance_name = "deleting-fail-instance";

    // Setup: project, pools
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance and add to group
    create_instance(client, project_name, instance_name).await;
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Wait for group to reach Active state
    wait_for_group_active(client, group_name).await;

    // Stop DPD before triggering deletion
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Remove member to trigger implicit deletion
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;

    // Check group is in Deleting state
    let get_result = objects_list_page_authz::<MulticastGroup>(
        client,
        "/v1/multicast-groups",
    )
    .await;

    let remaining_groups: Vec<_> = get_result
        .items
        .into_iter()
        .filter(|g| g.identity.name == group_name)
        .collect();

    if !remaining_groups.is_empty() {
        let group = &remaining_groups[0];
        assert_eq!(
            group.state, "Deleting",
            "Group should be in Deleting state, found: {}",
            group.state
        );
    }

    // Wait for reconciler to attempt deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Group should remain in Deleting since DPD is unavailable
    let final_result = objects_list_page_authz::<MulticastGroup>(
        client,
        "/v1/multicast-groups",
    )
    .await;

    let final_groups: Vec<_> = final_result
        .items
        .into_iter()
        .filter(|g| g.identity.name == group_name)
        .collect();

    if !final_groups.is_empty() {
        let group = &final_groups[0];
        assert_eq!(
            group.state, "Deleting",
            "Group should remain in Deleting when DPD unavailable, found: {}",
            group.state
        );
        assert_eq!(group.identity.name.as_str(), group_name);
    }

    // Restart DPD and activate reconciler to complete deletion
    cptestctx.restart_dendrite(SwitchLocation::Switch0).await;
    activate_multicast_reconciler(&cptestctx.lockstep_client).await;
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

#[nexus_test]
async fn test_multicast_group_members_during_dpd_failure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "member-dpd-fail-group";
    let instance_name = "member-test-instance";

    // Setup: project, pools
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance first
    let instance = create_instance(client, project_name, instance_name).await;

    // Stop DPD to test member operations during failure
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add instance to multicast group via instance-centric API
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Get the implicitly created group for later verification
    let group_get_url = mcast_group_url(group_name);
    let created_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    // Verify member is accessible
    let members_url = format!("/v1/multicast-groups/{group_name}/members");
    let initial_members =
        nexus_test_utils::resource_helpers::objects_list_page_authz::<
            MulticastGroupMember,
        >(client, &members_url)
        .await
        .items;
    assert_eq!(initial_members.len(), 1, "Should have exactly one member");
    assert_eq!(initial_members[0].instance_id, instance.identity.id);

    // Wait for reconciler - group should remain in "Creating" state
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify members are still accessible despite DPD failure
    let members_during_failure =
        nexus_test_utils::resource_helpers::objects_list_page_authz::<
            MulticastGroupMember,
        >(client, &members_url)
        .await
        .items;
    assert_eq!(
        members_during_failure.len(),
        1,
        "Member should still be accessible during DPD failure"
    );
    assert_eq!(members_during_failure[0].instance_id, instance.identity.id);
    assert_eq!(
        members_during_failure[0].multicast_group_id,
        created_group.identity.id
    );

    // Verify member state during DPD failure
    // Instance is running, so member has sled_id, but DPD is unavailable so it
    // can't be programmed against.
    assert_eq!(
        members_during_failure[0].state, "Joining",
        "Member should be Joining when DPD unavailable (waiting to be programmed)"
    );

    // Verify group is still in "Creating" state
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    assert_eq!(
        fetched_group.state, "Creating",
        "Group should remain in Creating state during DPD failure, found: {}",
        fetched_group.state
    );

    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;

    // Wait for reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
}

/// Test that implicit deletion works correctly when DPD is unavailable.
///
/// When the last member leaves an implicit group, the system should:
/// 1. Mark the group for deletion (transition to "Deleting" state)
/// 2. The group should remain in "Deleting" state until DPD is available
///
/// This tests the implicit group lifecycle: groups are implicitly deleted
/// when the last instance leaves.
#[nexus_test]
async fn test_implicit_deletion_with_dpd_failure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "implicit-delete-dpd-fail-project";
    let group_name = "implicit-delete-dpd-fail-group";
    let instance_name = "implicit-delete-instance";

    // Create project and pools in parallel
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "implicit-delete-pool"),
    )
    .await;

    // Create instance first
    create_instance(client, project_name, instance_name).await;

    // Add instance to multicast group via instance-centric API
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Wait for group to become Active
    wait_for_group_active(client, group_name).await;

    // Get the group ID for later verification
    let group_url = mcast_group_url(group_name);
    let created_group: MulticastGroup = object_get(client, &group_url).await;

    // Now stop DPD before removing the last member
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Remove the last member (should trigger implicit deletion)
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;

    // Wait for reconciler to process
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // The group should be in "Deleting" state since DPD is unavailable
    // List groups to check if it still exists
    let groups_result = objects_list_page_authz::<MulticastGroup>(
        client,
        "/v1/multicast-groups",
    )
    .await;

    let remaining_groups: Vec<_> = groups_result
        .items
        .into_iter()
        .filter(|g| g.identity.id == created_group.identity.id)
        .collect();

    if !remaining_groups.is_empty() {
        let group = &remaining_groups[0];
        assert_eq!(
            group.state, "Deleting",
            "Group should be in Deleting state when last member leaves and DPD is unavailable, found: {}",
            group.state
        );
        assert_eq!(group.identity.id, created_group.identity.id);

        // Restart DPD and verify cleanup completes
        cptestctx.restart_dendrite(SwitchLocation::Switch0).await;
        activate_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Both group and orphaned members should be cleaned up
        wait_for_group_deleted(cptestctx, group_name).await;
    }
}

/// Test concurrent implicit creation race conditions.
///
/// When multiple instances try to join a non-existent group simultaneously,
/// only one should create the group and all should become members.
/// This tests that the implicit creation logic handles conflicts correctly.
#[nexus_test]
async fn test_concurrent_implicit_creation_race(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "concurrent-implicit-create-project";
    let group_name = "concurrent-implicit-create-group";

    // Create project and pools in parallel
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "concurrent-implicit-create-pool"),
    )
    .await;

    // Create multiple instances (without starting them to avoid saga timing issues)
    let instance_names =
        ["race-instance-1", "race-instance-2", "race-instance-3"];
    let create_instance_futures = instance_names.iter().map(|name| {
        create_instance_with(
            client,
            project_name,
            name,
            &InstanceNetworkInterfaceAttachment::DefaultIpv4,
            Vec::<InstanceDiskAttachment>::new(),
            Vec::<ExternalIpCreate>::new(),
            false, // start=false: Don't start instances to avoid timing issues
            Default::default(),
            None,
            Vec::<MulticastGroupJoinSpec>::new(),
        )
    });
    let instances = ops::join_all(create_instance_futures).await;

    // Ensure inventory and DPD are ready before adding members to groups
    ensure_inventory_ready(cptestctx).await;
    ensure_dpd_ready(cptestctx).await;

    // Try to add all instances to the non-existent group concurrently
    // This will race to implicitly create the group
    // Add all instances to the group concurrently
    let add_member_futures = instance_names.iter().map(|instance_name| {
        multicast_group_attach(
            cptestctx,
            project_name,
            instance_name,
            group_name,
        )
    });
    ops::join_all(add_member_futures).await;

    // Verify the group exists and has all members
    let group_url = mcast_group_url(group_name);
    let implicitly_created_group: MulticastGroup =
        object_get(client, &group_url).await;
    assert_eq!(implicitly_created_group.identity.name.as_str(), group_name);

    // Verify all instances are members
    let final_members = list_multicast_group_members(client, group_name).await;
    assert_eq!(
        final_members.len(),
        3,
        "Group should have all 3 members after concurrent implicit creation"
    );

    // Verify each instance is a member
    for instance in &instances {
        assert!(
            final_members.iter().any(|m| m.instance_id == instance.identity.id),
            "Instance {} should be a member of the group",
            instance.identity.name
        );
    }

    cleanup_instances(cptestctx, client, project_name, &instance_names).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test implicit deletion race with instance join.
///
/// When the last member is leaving (triggering implicit deletion) while another
/// instance is trying to join, the system should handle this gracefully.
/// Either the group survives with the new member, or implicit deletion completes
/// and the new member triggers a fresh implicit creation.
#[nexus_test]
async fn test_implicit_deletion_race_with_instance_join(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "delete-race-project";
    let group_name = "delete-race-group";

    // Create project and pools in parallel
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "delete-race-pool"),
    )
    .await;

    // Create instances
    let instance_names =
        ["leaving-instance", "joining-instance-1", "joining-instance-2"];
    let create_instance_futures = instance_names
        .iter()
        .map(|name| create_instance(client, project_name, name));
    let instances = ops::join_all(create_instance_futures).await;

    // Add first member via instance-centric API
    multicast_group_attach(
        cptestctx,
        project_name,
        "leaving-instance",
        group_name,
    )
    .await;

    // Wait for group to become "Active"
    wait_for_group_active(client, group_name).await;

    // Now execute detach and add concurrently
    // The leaving instance triggers implicit deletion, while joining instances try to add
    let detach_future = multicast_group_detach(
        client,
        project_name,
        "leaving-instance",
        group_name,
    );

    let join_futures = ["joining-instance-1", "joining-instance-2"]
        .iter()
        .map(|instance_name| {
            let join_url = format!(
                "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
            );
            let join_params = InstanceMulticastGroupJoin { source_ips: None, ip_version: None };
            async move {
                // This might fail if group is deleted, or succeed if it beats the delete
                let res = nexus_test_utils::http_testing::NexusRequest::new(
                    nexus_test_utils::http_testing::RequestBuilder::new(
                        client,
                        http::Method::PUT,
                        &join_url,
                    )
                    .body(Some(&join_params)),
                )
                .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
                .execute()
                .await;

                match res {
                    Ok(response)
                        if response.status == http::StatusCode::CREATED =>
                    {
                        Some(
                            response
                                .parsed_body::<nexus_types::external_api::multicast::MulticastGroupMember>()
                                .unwrap(),
                        )
                    }
                    _ => None, // Failed to join (group might be deleting)
                }
            }
        });

    // Execute concurrently
    let (_, join_results) =
        ops::join2(detach_future, ops::join_all(join_futures)).await;

    // Wait for reconciler to process everything
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Check the final state - group should exist if any join succeeded
    let successful_joins: Vec<_> = join_results.into_iter().flatten().collect();

    if !successful_joins.is_empty() {
        // At least one instance joined - group should exist
        let group_url = mcast_group_url(group_name);
        let final_group_result: Result<MulticastGroup, _> =
            nexus_test_utils::http_testing::NexusRequest::object_get(
                client, &group_url,
            )
            .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
            .execute()
            .await
            .map(|r| r.parsed_body().unwrap());

        match final_group_result {
            Ok(_) => {
                // Group exists - verify it has the joining instances
                let members =
                    list_multicast_group_members(client, group_name).await;
                assert!(
                    !members.is_empty(),
                    "Group should have members if it exists"
                );

                // The leaving instance should not be a member
                assert!(
                    !members
                        .iter()
                        .any(|m| m.instance_id == instances[0].identity.id),
                    "Leaving instance should not be a member"
                );
            }
            Err(_) => {
                // Group was deleted - that's also valid if timing worked out
                // The joining instances should have gotten errors
            }
        }
    }

    // Cleanup - delete instances; group is implicitly deleted when last member removed
    cleanup_instances(cptestctx, client, project_name, &instance_names).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test that joining a deleted instance to a multicast group returns NOT_FOUND.
///
/// This verifies proper error handling when attempting to add an instance that
/// was previously deleted to a multicast group.
#[nexus_test]
async fn test_multicast_join_deleted_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group";
    let instance_to_delete = "instance-to-delete";
    let remaining_instance = "remaining-instance";

    // Setup: project and pools
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create two instances
    ops::join2(
        create_instance(client, project_name, instance_to_delete),
        create_instance(client, project_name, remaining_instance),
    )
    .await;

    // Create group with the remaining instance (so the group stays alive)
    multicast_group_attach(
        cptestctx,
        project_name,
        remaining_instance,
        group_name,
    )
    .await;

    // Wait for group to become active
    wait_for_group_active(&client, group_name).await;

    // Delete the first instance using cleanup_instances (handles stop/delete flow)
    cleanup_instances(cptestctx, client, project_name, &[instance_to_delete])
        .await;

    // Now try to add the deleted instance to the group - should fail with NOT_FOUND
    // Uses instance-centric API: PUT /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{instance_to_delete}/multicast-groups/{group_name}?project={project_name}"
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &join_url)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Request should complete (with 404)");

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[remaining_instance])
        .await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test drift correction: DPD loses group state and reconciler re-syncs it.
///
/// This simulates DPD drift where the switch has lost the multicast group
/// information (e.g., after a switch restart). The reconciler should detect
/// the missing state and re-sync DPD to restore the group to "Active" state.
#[nexus_test]
async fn test_drift_correction_missing_group_in_dpd(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "drift-test-project";
    let group_name = "drift-test-group";
    let instance_name = "drift-test-instance";

    // Create project and pools in parallel
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "drift-pool"),
    )
    .await;

    // Create instance
    create_instance(client, project_name, instance_name).await;

    // Create group by adding member
    // Join group using instance-centric API
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    wait_for_group_active(client, group_name).await;

    let group_get_url = mcast_group_url(group_name);
    let active_group: MulticastGroup = object_get(client, &group_get_url).await;
    assert_eq!(active_group.state, "Active", "Group should be Active");

    // Get multicast IP for DPD queries
    let multicast_ip = active_group.multicast_ip;

    // Verify group exists in DPD before restart
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    assert!(
        dpd_client.multicast_group_get(&multicast_ip).await.is_ok(),
        "Group should exist in DPD before restart"
    );

    // Simulate drift: restart DPD (clears its state)
    // This leaves the group "Active" in DB but missing from DPD
    cptestctx.restart_dendrite(SwitchLocation::Switch0).await;

    // Verify group is missing from DPD after restart (drift exists)
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    assert!(
        dpd_client.multicast_group_get(&multicast_ip).await.is_err(),
        "Group should not exist in DPD after restart (this is the drift)"
    );

    // Activate reconciler - should detect missing group and re-program DPD
    activate_multicast_reconciler(&cptestctx.lockstep_client).await;
    wait_for_group_active(client, group_name).await;

    // Verify drift was corrected: group now exists in DPD again
    assert!(
        dpd_client.multicast_group_get(&multicast_ip).await.is_ok(),
        "Group should exist in DPD after drift correction"
    );

    // Verify group is still "Active" in DB
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;
    assert_eq!(
        fetched_group.state, "Active",
        "Group should remain Active after drift correction, found: {}",
        fetched_group.state
    );

    // Verify group properties maintained
    assert_eq!(fetched_group.identity.name.as_str(), group_name);
    assert_eq!(fetched_group.identity.id, active_group.identity.id);

    // Cleanup
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test member state transition: "Joining" → "Left" when instance becomes invalid.
///
/// When a member is in "Joining" state (waiting for DPD programming) and the
/// instance becomes invalid (stopped/failed), the RPW should transition the
/// member to "Left" state.
#[nexus_test]
async fn test_member_joining_to_left_on_instance_stop(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "joining-to-left-group";
    let instance_name = "joining-to-left-instance";

    // Setup: Create project and pools
    ops::join3(
        create_default_ip_pools(&client),
        create_project(client, project_name),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create and start instance
    let instance = create_instance(client, project_name, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Stop DPD to prevent member from transitioning to "Joined"
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add instance to group - member will be stuck in "Joining" since DPD is down
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;

    // Run reconciler - member should stay in "Joining" since DPD is unavailable
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify member is in "Joining" state (can't reach Joined without DPD)
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1);
    assert_eq!(
        members[0].state, "Joining",
        "Running instance member should be Joining when DPD unavailable, got: {}",
        members[0].state
    );

    // Stop the instance while member is in "Joining" state
    let stop_url =
        format!("/v1/instances/{instance_name}/stop?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    // For stopping, simulate once then wait (don't repeatedly simulate as VMM gets removed)
    instance_helpers::instance_simulate(
        &cptestctx.server.server_context().nexus,
        &instance_id,
    )
    .await;
    instance_helpers::instance_wait_for_state(
        client,
        instance_id,
        InstanceState::Stopped,
    )
    .await;

    // Run reconciler - should detect invalid instance and transition to "Left"
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify member transitioned to "Left" state
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    let members_after = list_multicast_group_members(client, group_name).await;
    assert_eq!(members_after.len(), 1);
    assert_eq!(
        members_after[0].state, "Left",
        "Member should transition to Left when instance stops while in Joining state"
    );
}

/// Test that "Left" members stay in "Left" while group is still "Creating".
///
/// When a member is in "Left" state and the instance starts running, the member
/// should stay in "Left" until the group becomes "Active". This prevents
/// premature member activation when the group hasn't been programmed in DPD.
#[nexus_test]
async fn test_left_member_waits_for_group_active(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "left-waits-group";
    let instance_name = "left-waits-instance";

    // Setup: Create project and pools
    ops::join3(
        create_default_ip_pools(&client),
        create_project(client, project_name),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create a stopped instance first (not running)
    let instance = create_instance_with(
        client,
        project_name,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv4,
        vec![],
        vec![],
        false,  // don't start
        None,   // auto_restart_policy
        None,   // cpu_platform
        vec![], // multicast_groups
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Stop DPD to keep group in "Creating" state
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add stopped instance to group - member will be in "Left" state
    // Uses instance-centric API: PUT /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };
    put_upsert::<_, MulticastGroupMember>(client, &join_url, &join_params)
        .await;

    // Verify group is stuck in "Creating" (DPD is down)
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    let group: MulticastGroup =
        object_get(client, &format!("/v1/multicast-groups/{group_name}")).await;
    assert_eq!(
        group.state, "Creating",
        "Group should be stuck in Creating without DPD"
    );

    // Verify member is in "Left" state (stopped instance)
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1);
    assert_eq!(
        members[0].state, "Left",
        "Stopped instance member should be in Left state"
    );

    // Start the instance while group is still Creating
    let start_url =
        format!("/v1/instances/{instance_name}/start?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    instance_wait_for_running_with_simulation(cptestctx, instance_id).await;

    // Run reconciler - member should stay in Left because group is not Active
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify member stays in "Left" (waiting for group to become Active)
    let members_after = list_multicast_group_members(client, group_name).await;
    assert_eq!(members_after.len(), 1);
    assert_eq!(
        members_after[0].state, "Left",
        "Member should stay in Left while group is Creating, got: {}",
        members_after[0].state
    );

    // Verify group is still Creating
    let group_after: MulticastGroup =
        object_get(client, &format!("/v1/multicast-groups/{group_name}")).await;
    assert_eq!(
        group_after.state, "Creating",
        "Group should still be Creating without DPD"
    );
}

/// Test underlay IP collision detection and salt-based retry mechanism.
///
/// This test verifies that when two external groups would map to the same
/// underlay IP, the reconciler detects the collision and retries with an
/// incremented salt value.
///
/// Test setup:
/// 1. Create group A with external IP 224.5.5.5 (via instance join)
/// 2. Pre-occupy B's target underlay IP (ff04::e001:0203) using A's tag
/// 3. Create group B with external IP 224.1.2.3 (maps to ff04::e001:0203 with salt=0)
/// 4. When reconciler processes B, it hits collision and retries with salt=1
#[nexus_test]
async fn test_multicast_group_underlay_collision_retry(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let log = &cptestctx.logctx.log;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    let project_name = "collision-test-project";
    let instance_a_name = "collision-instance-a";
    let instance_b_name = "collision-instance-b";
    // Use IP addresses as group identifiers for explicit IP allocation
    let group_a_ip = "224.5.5.5";
    let group_b_ip = "224.1.2.3"; // Known mapping: → ff04::e001:0203 with salt=0

    // Setup: project, pools
    ops::join3(
        create_project(&client, project_name),
        create_default_ip_pools(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 1, 0, 0),
            (224, 5, 255, 255),
        ),
    )
    .await;

    // Create two instances for membership
    ops::join2(
        create_instance(client, project_name, instance_a_name),
        create_instance(client, project_name, instance_b_name),
    )
    .await;

    // Stop DPD to control when groups transition to "Active"
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Create group A by joining instance to IP address (implicit group creation)
    multicast_group_attach(
        cptestctx,
        project_name,
        instance_a_name,
        group_a_ip,
    )
    .await;

    // Fetch group A (created by attach) from API using IP address as identifier
    let group_a = get_multicast_group(client, group_a_ip).await;
    let group_a_model = datastore
        .multicast_group_fetch(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(group_a.identity.id),
        )
        .await
        .expect("Should fetch group A");

    // Pre-occupy B's target underlay IP by creating an underlay group with A's tag
    // B's target: ff04::e001:0203 (known mapping for 224.1.2.3 with salt=0)
    let collision_ip: ipnetwork::IpNetwork = "ff04::e001:0203".parse().unwrap();
    let res = datastore
        .ensure_underlay_multicast_group(
            &opctx,
            group_a_model.clone(),
            collision_ip,
        )
        .await
        .expect("Should create collision underlay");

    // Verify the underlay was created (for group A, but with B's target IP)
    assert!(
        matches!(
            res,
            nexus_db_queries::db::datastore::multicast::EnsureUnderlayResult::Created(_)
        ),
        "Should have created underlay group with collision IP"
    );

    // Create group B by joining instance to IP address (implicit group creation)
    multicast_group_attach(
        cptestctx,
        project_name,
        instance_b_name,
        group_b_ip,
    )
    .await;

    // Fetch group B (created by attach) from API using IP address as identifier
    let group_b = get_multicast_group(client, group_b_ip).await;

    // Restart DPD and run reconciler, triggering collision detection
    cptestctx.restart_dendrite(SwitchLocation::Switch0).await;
    activate_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Fetch group B after reconciliation
    let group_b_after = datastore
        .multicast_group_fetch(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(group_b.identity.id),
        )
        .await
        .expect("Should fetch group B after reconciliation");

    // Verify B got a salt value > 0 (collision was detected and retried)
    assert!(
        group_b_after.underlay_salt.is_some(),
        "Group B should have a salt value after collision retry"
    );
    let salt_value = group_b_after.underlay_salt.unwrap();
    assert!(
        *salt_value > 0,
        "Salt should be > 0 after collision (got {})",
        *salt_value
    );

    // Verify B got an underlay group (different from the collision IP)
    assert!(
        group_b_after.underlay_group_id.is_some(),
        "Group B should have an underlay group after collision retry"
    );

    // Fetch A's underlay group and verify it has the collision IP
    let group_a_after = datastore
        .multicast_group_fetch(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(group_a.identity.id),
        )
        .await
        .expect("Should fetch group A after reconciliation");
    let underlay_a = datastore
        .underlay_multicast_group_fetch(
            &opctx,
            group_a_after.underlay_group_id.unwrap(),
        )
        .await
        .expect("Should fetch A's underlay group");

    assert_eq!(
        underlay_a.multicast_ip.ip().to_string(),
        "ff04::e001:203",
        "A's underlay IP should be the collision IP we set"
    );

    // Fetch B's underlay group and verify it has a different IP than the collision IP
    let underlay_b = datastore
        .underlay_multicast_group_fetch(
            &opctx,
            group_b_after.underlay_group_id.unwrap(),
        )
        .await
        .expect("Should fetch B's underlay group");

    assert_ne!(
        underlay_b.multicast_ip.ip().to_string(),
        "ff04::e001:203",
        "B's underlay IP should differ from collision IP due to salt"
    );

    // Verify A and B have different underlay IPs
    assert_ne!(
        underlay_a.multicast_ip, underlay_b.multicast_ip,
        "A and B should have different underlay IPs"
    );
}
