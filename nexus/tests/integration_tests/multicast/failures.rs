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
//! - Concurrent operation races: Implicit creation, deletion with member add
//! - Drift correction: Reconciler syncs DPD when state is lost
//! - Member lifecycle: "Joining"→"Left" on instance stop, "Left" waits for "Active"

use http::{Method, StatusCode};

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_instance_with,
    create_project, object_create, object_create_error, object_get,
    objects_list_page_authz,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    ExternalIpCreate, InstanceDiskAttachment,
    InstanceNetworkInterfaceAttachment, MulticastGroupIdentifier,
    MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::{MulticastGroup, MulticastGroupMember};
use omicron_common::api::external::{InstanceState, NameOrId, SwitchLocation};
use omicron_uuid_kinds::InstanceUuid;

use super::*;
use crate::integration_tests::instances::{
    instance_simulate, instance_wait_for_state,
};

#[nexus_test]
async fn test_multicast_group_dpd_communication_failure_recovery(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "dpd-failure-group";
    let instance_name = "dpd-failure-instance";

    // Setup: project, pools - parallelize creation
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance first
    create_instance(client, project_name, instance_name).await;

    // Stop DPD before implicit creation to test failure recovery
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add member to group
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}",
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

    // Verify group was implicitly created and is in "Creating" state since DPD is unavailable
    // The reconciler can't progress the group to Active without DPD communication
    let group_get_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    assert_eq!(
        fetched_group.state, "Creating",
        "Group should remain in Creating state when DPD is unavailable, found: {}",
        fetched_group.state
    );

    // Verify group properties are maintained despite DPD issues
    assert_eq!(fetched_group.identity.name.as_str(), group_name);

    // Case: Verify member state during DPD failure
    // Members should be in "Joining" or "Left" state when DPD is unavailable
    // (they can't transition to "Joined" without successful DPD programming)
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have exactly one member");
    assert!(
        members[0].state == "Joining" || members[0].state == "Left",
        "Member should be in Joining or Left state when DPD is unavailable, got: {}",
        members[0].state
    );
}

#[nexus_test]
async fn test_multicast_reconciler_state_consistency_validation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";

    // Setup: project and pools
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Group names for implicit groups (implicitly created when first member joins)
    let group_names =
        ["consistency-group-1", "consistency-group-2", "consistency-group-3"];

    // Create instances first (groups will be implicitly created when members attach)
    let instance_names: Vec<_> = group_names
        .iter()
        .map(|&group_name| format!("instance-{group_name}"))
        .collect();

    // Create all instances in parallel
    let create_futures = instance_names.iter().map(|instance_name| {
        create_instance(client, project_name, instance_name)
    });
    ops::join_all(create_futures).await;

    // Stop DPD before attaching members to test failure recovery
    // Groups will be implicitly created but stay in "Creating" state
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Attach instances to their respective groups (triggers implicit creation for each group)
    // Since DPD is down, groups will remain in "Creating" state
    for (instance_name, &group_name) in instance_names.iter().zip(&group_names)
    {
        let member_add_url = format!(
            "/v1/multicast-groups/{group_name}/members?project={project_name}",
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
    }

    // Wait for reconciler to attempt processing (will fail due to DPD being down)
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify each group is in a consistent state (DPD failure prevents reconciliation)
    for group_name in group_names.iter() {
        let group_get_url = mcast_group_url(group_name);
        let fetched_group: MulticastGroup =
            object_get(client, &group_get_url).await;

        // State should be "Creating" since DPD is down
        assert_eq!(
            fetched_group.state, "Creating",
            "Group {group_name} should remain in Creating state when DPD is unavailable, found: {}",
            fetched_group.state
        );

        // Case: Verify member state during DPD failure
        let members = list_multicast_group_members(client, group_name).await;
        assert_eq!(
            members.len(),
            1,
            "Group {group_name} should have exactly one member"
        );
        assert!(
            members[0].state == "Joining" || members[0].state == "Left",
            "Member in group {group_name} should be Joining or Left when DPD unavailable, got: {}",
            members[0].state
        );
    }

    let instance_name_refs: Vec<&str> =
        instance_names.iter().map(|s| s.as_str()).collect();
    cleanup_instances(cptestctx, client, project_name, &instance_name_refs)
        .await;

    // With DPD down, groups cannot complete state transitions - they may be stuck
    // in "Creating" (never reached "Active") or "Deleting" state.
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify groups are either deleted or stuck in "Creating"/"Deleting" state
    for group_name in group_names.iter() {
        verify_group_deleted_or_in_states(
            client,
            group_name,
            &["Creating", "Deleting"],
        )
        .await;
    }
}

#[nexus_test]
async fn test_dpd_failure_during_creating_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "creating-dpd-fail-group";
    let instance_name = "creating-fail-instance";

    // Setup: project, pools
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance first
    create_instance(client, project_name, instance_name).await;

    // Stop DPD before implicit creation
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add member to group
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Wait for reconciler to process - tests DPD communication handling during "Creating" state
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Check group state after reconciler processes with DPD unavailable
    let group_get_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    // Group should remain in "Creating" state since DPD is down
    assert_eq!(
        fetched_group.state, "Creating",
        "Group should remain in Creating state when DPD is unavailable during activation, found: {}",
        fetched_group.state
    );

    // Verify group properties are maintained
    assert_eq!(fetched_group.identity.name.as_str(), group_name);

    // Case: Verify member state during DPD failure
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have exactly one member");
    assert!(
        members[0].state == "Joining" || members[0].state == "Left",
        "Member should be Joining or Left when DPD unavailable during Creating state, got: {}",
        members[0].state
    );

    // Test cleanup - remove member, which triggers implicit deletion
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;

    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
}

#[nexus_test]
async fn test_dpd_failure_during_active_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "active-dpd-fail-group";
    let instance_name = "active-fail-instance";

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    let instance = create_instance(client, project_name, instance_name).await;

    // Add member to group
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Wait for group to become "Active" and member to reach "Joined" state
    wait_for_group_active(client, group_name).await;
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify group is now "Active"
    let group_get_url = mcast_group_url(group_name);
    let active_group: MulticastGroup = object_get(client, &group_get_url).await;
    assert_eq!(active_group.state, "Active");

    // Now stop DPD while group is "Active" and member is "Joined"
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Wait for reconciler to process - tests DPD communication handling during "Active" state
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Check group state after reconciler processes with DPD unavailable
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    // Group should remain "Active" - existing "Active" groups shouldn't change state due to DPD failures
    assert_eq!(
        fetched_group.state, "Active",
        "Active group should remain Active despite DPD communication failure, found: {}",
        fetched_group.state
    );

    // Verify group properties are maintained
    assert_eq!(fetched_group.identity.name.as_str(), group_name);

    // Case: Verify member state persists during DPD failure for Active groups
    // Members that were already "Joined" should remain "Joined" even when DPD is unavailable
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have exactly one member");
    assert_eq!(
        members[0].state, "Joined",
        "Member should remain Joined when DPD fails after group reached Active state, got: {}",
        members[0].state
    );

    // Test cleanup - remove member, which triggers implicit deletion
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;

    // Wait for reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
}

#[nexus_test]
async fn test_dpd_failure_during_deleting_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "deleting-dpd-fail-group";
    let instance_name = "deleting-fail-instance";

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance first
    create_instance(client, project_name, instance_name).await;

    // Add member to group
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Wait for group to reach "Active" state before testing deletion
    wait_for_group_active(client, group_name).await;

    // Stop DPD before triggering deletion (by removing member)
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Remove the member to trigger implicit deletion (group should go to "Deleting" state)
    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;

    // The group should now be in "Deleting" state and DPD is down
    // Let's check the state before reconciler runs
    // Group should be accessible via GET request

    // Try to get group - should be accessible in "Deleting" state
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
            "Group should be in Deleting state after deletion request, found: {}",
            group.state
        );
    }

    // Wait for reconciler to attempt deletion with DPD down
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Check final state - group should remain in "Deleting" state since DPD is unavailable
    // The reconciler cannot complete deletion without DPD communication
    let final_result =
        nexus_test_utils::resource_helpers::objects_list_page_authz::<
            MulticastGroup,
        >(client, "/v1/multicast-groups")
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
            "Group should remain in Deleting state when DPD is unavailable during deletion, found: {}",
            group.state
        );

        // Verify group properties are maintained during failed deletion
        assert_eq!(group.identity.name.as_str(), group_name);
    }
    // Note: If group is gone, that means deletion succeeded despite DPD being down,
    // which would indicate the reconciler has fallback cleanup logic
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
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instance first
    let instance = create_instance(client, project_name, instance_name).await;

    // Stop DPD to test member operations during failure
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add member to group
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Case: Verify member state during DPD failure
    assert!(
        members_during_failure[0].state == "Joining"
            || members_during_failure[0].state == "Left",
        "Member should be Joining or Left when DPD unavailable, got: {}",
        members_during_failure[0].state
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

/// Test that implicit creation works correctly when DPD is unavailable.
///
/// When a member is added to a non-existent group (by name), the system should:
/// 1. Implicitly create the group in "Creating" state
/// 2. Create the member in "Left" state (since instance is stopped)
/// 3. The group should remain in "Creating" state until DPD is available
///
/// This tests the implicit group lifecycle: groups are implicitly created
/// when the first instance joins.
#[nexus_test]
async fn test_implicit_creation_with_dpd_failure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "implicit-create-dpd-fail-project";
    let group_name = "implicit-created-dpd-fail-group";
    let instance_name = "implicit-create-instance";

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "implicit-create-pool"),
    )
    .await;

    // Create instance first
    let instance = create_instance(client, project_name, instance_name).await;

    // Stop DPD before implicit creation
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add the instance as a member to a non-existent group (triggers implicit creation)
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Verify the implicitly created group exists and is in "Creating" state
    let group_url = mcast_group_url(group_name);
    let implicitly_created_group: MulticastGroup =
        object_get(client, &group_url).await;

    assert_eq!(
        implicitly_created_group.state, "Creating",
        "Implicitly created group should start in Creating state"
    );
    assert_eq!(implicitly_created_group.identity.name.as_str(), group_name);

    // Wait for reconciler - group should remain in "Creating" since DPD is down
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify group is still in "Creating" state
    let fetched_group: MulticastGroup = object_get(client, &group_url).await;
    assert_eq!(
        fetched_group.state, "Creating",
        "Implicitly created group should remain in Creating state when DPD is unavailable"
    );

    // Verify member is still attached
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have one member");
    assert_eq!(members[0].instance_id, instance.identity.id);

    // Case: Verify member state during DPD failure for implicit creation
    assert!(
        members[0].state == "Joining" || members[0].state == "Left",
        "Member should be Joining or Left when DPD unavailable during implicit creation, got: {}",
        members[0].state
    );

    multicast_group_detach(client, project_name, instance_name, group_name)
        .await;
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
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "implicit-delete-pool"),
    )
    .await;

    // Create instance first
    create_instance(client, project_name, instance_name).await;

    // Add member to group
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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
    }
    // Note: If group is gone, implicit deletion succeeded despite DPD being down
    // (possibly via database-only cleanup)
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
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
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
            &InstanceNetworkInterfaceAttachment::Default,
            Vec::<InstanceDiskAttachment>::new(),
            Vec::<ExternalIpCreate>::new(),
            false, // start=false: Don't start instances to avoid timing issues
            Default::default(),
            None,
            Vec::<MulticastGroupIdentifier>::new(),
        )
    });
    let instances = ops::join_all(create_instance_futures).await;

    // Ensure inventory and DPD are ready before adding members to groups
    ensure_inventory_ready(cptestctx).await;
    ensure_dpd_ready(cptestctx).await;

    // Try to add all instances to the non-existent group concurrently
    // This will race to implicitly create the group
    let add_member_futures = instance_names.iter().map(|instance_name| {
        let member_add_url = format!(
            "/v1/multicast-groups/{group_name}/members?project={project_name}"
        );
        let member_params = MulticastGroupMemberAdd {
            instance: NameOrId::Name(instance_name.parse().unwrap()),
            source_ips: None,
        };
        async move {
            object_create::<_, MulticastGroupMember>(
                client,
                &member_add_url,
                &member_params,
            )
            .await
        }
    });

    // Execute all member additions concurrently
    let members = ops::join_all(add_member_futures).await;

    // All member additions should have succeeded
    assert_eq!(members.len(), 3, "All 3 member additions should succeed");

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

    // Wait for group to be implicitly deleted (may already be deleted if cleanup succeeded)
    wait_for_group_deleted(client, group_name).await;
}

/// Test implicit deletion race with member add.
///
/// When the last member is leaving (triggering implicit deletion) while another
/// instance is trying to join, the system should handle this gracefully.
/// Either the group survives with the new member, or implicit deletion completes
/// and the new member triggers a fresh implicit creation.
#[nexus_test]
async fn test_implicit_deletion_race_with_member_add(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "delete-race-project";
    let group_name = "delete-race-group";

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
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

    // Add first member
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}",
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("leaving-instance".parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become Active
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
            let member_add_url = format!(
                "/v1/multicast-groups/{group_name}/members?project={project_name}"
            );
            let member_params = MulticastGroupMemberAdd {
                instance: NameOrId::Name(instance_name.parse().unwrap()),
                source_ips: None,
            };
            async move {
                // This might fail if group is deleted, or succeed if it beats the delete
                let result = nexus_test_utils::http_testing::NexusRequest::new(
                    nexus_test_utils::http_testing::RequestBuilder::new(
                        client,
                        http::Method::POST,
                        &member_add_url,
                    )
                    .body(Some(&member_params)),
                )
                .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
                .execute()
                .await;

                match result {
                    Ok(response)
                        if response.status == http::StatusCode::CREATED =>
                    {
                        Some(
                            response
                                .parsed_body::<MulticastGroupMember>()
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

    // Implicit model: group is implicitly deleted when last member (instance) is removed
    // Wait for group to be deleted (may already be deleted if no joins succeeded)
    wait_for_group_deleted(client, group_name).await;
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
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create two instances
    create_instance(client, project_name, instance_to_delete).await;
    create_instance(client, project_name, remaining_instance).await;

    // Create group with the remaining instance (so the group stays alive)
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(remaining_instance.parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become active
    wait_for_group_active(&client, group_name).await;

    // Delete the first instance using cleanup_instances (handles stop/delete flow)
    cleanup_instances(cptestctx, client, project_name, &[instance_to_delete])
        .await;

    // Now try to add the deleted instance to the group - should fail with NOT_FOUND
    let member_params_deleted = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_to_delete.parse().unwrap()),
        source_ips: None,
    };
    object_create_error(
        client,
        &member_add_url,
        &member_params_deleted,
        http::StatusCode::NOT_FOUND,
    )
    .await;

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[remaining_instance])
        .await;
    wait_for_group_deleted(client, group_name).await;
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
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "drift-pool"),
    )
    .await;

    // Create instance
    create_instance(client, project_name, instance_name).await;

    // Create group by adding member
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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
        "Group should NOT exist in DPD after restart (this is the drift)"
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
    wait_for_group_deleted(client, group_name).await;
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
    let (_, _, _) = ops::join3(
        create_default_ip_pool(&client),
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
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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
    let nexus = &cptestctx.server.server_context().nexus;
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
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

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
    let (_, _, _) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, project_name),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create a stopped instance first (not running)
    let instance = create_instance_with(
        client,
        project_name,
        instance_name,
        &InstanceNetworkInterfaceAttachment::Default,
        vec![],
        vec![],
        false,  // don't start
        None,   // auto_restart_policy
        None,   // cpu_platform
        vec![], // multicast_groups
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;

    // Stop DPD to keep group in "Creating" state
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Add stopped instance to group - member will be in "Left" state
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Running).await;

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
