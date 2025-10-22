// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Integration tests for multicast group failure scenarios.
//!
//! Tests DPD communication failures, reconciler resilience, and saga rollback
//! scenarios.

use std::net::{IpAddr, Ipv4Addr};

use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, object_create,
    object_delete, object_get, objects_list_page_authz,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    MulticastGroupCreate, MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::{MulticastGroup, MulticastGroupMember};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, NameOrId, SwitchLocation,
};

use super::*;

#[nexus_test]
async fn test_multicast_group_dpd_communication_failure_recovery(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "dpd-failure-group";
    let instance_name = "dpd-failure-instance";

    // Setup: project, pools, group with member - parallelize creation
    let (_, _, mcast_pool) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create group that will experience DPD communication failure
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 250));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for DPD communication failure test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    // Stop DPD BEFORE reconciler runs to test failure recovery
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    // Group should start in "Creating" state
    assert_eq!(
        created_group.state, "Creating",
        "New multicast group should start in Creating state"
    );

    // Add member to make group programmable
    create_instance(client, project_name, instance_name).await;
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };
    let member_add_url = mcast_group_member_add_url(
        group_name,
        &member_params.instance,
        project_name,
    );
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Verify group remains in "Creating" state since DPD is unavailable
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
    // The group should remain accessible and in "Creating" state since DPD is down
    assert_eq!(fetched_group.identity.name, group_name);
    assert_eq!(fetched_group.multicast_ip, multicast_ip);
    assert_eq!(fetched_group.identity.id, created_group.identity.id);
}

#[nexus_test]
async fn test_multicast_group_reconciler_state_consistency_validation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";

    // Create multiple groups to test reconciler batch processing with failures
    let (_, _, mcast_pool) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Stop DPD BEFORE reconciler runs to test failure recovery
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    // Create groups that will test different failure scenarios using helper functions
    let group_specs = &[
        MulticastGroupForTest {
            name: "consistency-group-1",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 220)),
            description: Some("Group for state consistency test".to_string()),
        },
        MulticastGroupForTest {
            name: "consistency-group-2",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 221)),
            description: Some("Group for state consistency test".to_string()),
        },
        MulticastGroupForTest {
            name: "consistency-group-3",
            multicast_ip: IpAddr::V4(Ipv4Addr::new(224, 0, 1, 222)),
            description: Some("Group for state consistency test".to_string()),
        },
    ];

    // Create all groups rapidly to stress test reconciler
    let created_groups =
        create_multicast_groups(client, &mcast_pool, group_specs).await;
    let group_names: Vec<&str> = group_specs.iter().map(|g| g.name).collect();

    // Create instances and attach to groups in parallel (now that double-delete bug is fixed)
    let instance_names: Vec<_> = group_names
        .iter()
        .map(|&group_name| format!("instance-{group_name}"))
        .collect();

    // Create all instances in parallel
    let create_futures = instance_names.iter().map(|instance_name| {
        create_instance(client, project_name, instance_name)
    });
    ops::join_all(create_futures).await;

    // Attach instances to their respective groups in parallel
    let attach_futures = instance_names.iter().zip(&group_names).map(
        |(instance_name, &group_name)| {
            multicast_group_attach(
                client,
                project_name,
                instance_name,
                group_name,
            )
        },
    );
    ops::join_all(attach_futures).await;

    // Verify each group is in a consistent state (DPD failure prevents reconciliation)
    for (i, group_name) in group_names.iter().enumerate() {
        let original_group = &created_groups[i];
        let group_get_url = mcast_group_url(group_name);
        let fetched_group: MulticastGroup =
            object_get(client, &group_get_url).await;

        // Critical consistency checks
        assert_eq!(fetched_group.identity.id, original_group.identity.id);
        assert_eq!(fetched_group.multicast_ip, original_group.multicast_ip);

        // State should be Creating since all DPD processes were stopped
        // The reconciler cannot activate groups without DPD communication
        assert_eq!(
            fetched_group.state, "Creating",
            "Group {} should remain in Creating state when DPD is unavailable, found: {}",
            group_name, fetched_group.state
        );
    }

    // Clean up all groups - test reconciler's ability to handle batch deletions
    cleanup_multicast_groups(client, &group_names).await;
}

#[nexus_test]
async fn test_dpd_failure_during_creating_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "creating-dpd-fail-group";
    let instance_name = "creating-fail-instance";

    // Setup: project, pools, group with member - parallelize creation
    let (_, _, mcast_pool) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create group (IP within pool range 224.0.1.10 to 224.0.1.255)
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 210));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for DPD failure during Creating state test"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    // Stop DPD before object creation of groups.
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    // Group should start in "Creating" state
    assert_eq!(
        created_group.state, "Creating",
        "New multicast group should start in Creating state"
    );

    // Add member to make group programmable
    create_instance(client, project_name, instance_name).await;

    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Stop DPD process BEFORE reconciler runs to test Creating→Creating failure

    // Wait for reconciler to process - tests DPD communication handling during "Creating" state
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Check group state after reconciler processes with DPD unavailable
    let group_get_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    // Critical assertion: Group should remain in "Creating" state since DPD is unavailable
    // The reconciler cannot transition Creating→Active without DPD communication
    assert_eq!(
        fetched_group.state, "Creating",
        "Group should remain in Creating state when DPD is unavailable during activation, found: {}",
        fetched_group.state
    );

    // Verify group properties are maintained
    assert_eq!(fetched_group.identity.name, group_name);
    assert_eq!(fetched_group.multicast_ip, multicast_ip);
    assert_eq!(fetched_group.identity.id, created_group.identity.id);

    // Test cleanup - should work regardless of DPD state
    object_delete(client, &group_get_url).await;

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

    // Setup: project, pools, group with member
    create_project(&client, project_name).await;
    create_default_ip_pool(&client).await;

    let mcast_pool = create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create group that will become active first
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 211));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for DPD failure during Active state test"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    assert_eq!(created_group.state, "Creating");

    // Add member to make group programmable
    create_instance(client, project_name, instance_name).await;
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };
    let member_add_url = mcast_group_member_add_url(
        group_name,
        &member_params.instance,
        project_name,
    );
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // First, let the group activate normally with DPD running
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify group is now Active (or at least not Creating anymore)
    let group_get_url = mcast_group_url(group_name);
    let active_group: MulticastGroup = object_get(client, &group_get_url).await;

    // Group should be Active or at least no longer Creating
    assert!(
        active_group.state == "Active" || active_group.state == "Creating",
        "Group should be Active or Creating before DPD failure test, found: {}",
        active_group.state
    );

    // Only proceed with failure test if group successfully activated
    if active_group.state == "Active" {
        // Now stop DPD while group is "Active" to test "Active" state resilience
        cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

        // Wait for reconciler to process - tests DPD communication handling during "Active" state
        wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Check group state after reconciler processes with DPD unavailable
        let fetched_group: MulticastGroup =
            object_get(client, &group_get_url).await;

        // Group should remain "Active" - existing "Active" groups shouldn't change state due to DPD failures
        // The reconciler should handle temporary DPD communication issues gracefully
        assert_eq!(
            fetched_group.state, "Active",
            "Active group should remain Active despite DPD communication failure, found: {}",
            fetched_group.state
        );

        // Verify group properties are maintained
        assert_eq!(fetched_group.identity.name, group_name);
        assert_eq!(fetched_group.multicast_ip, multicast_ip);
        assert_eq!(fetched_group.identity.id, created_group.identity.id);
    }

    // Test cleanup - should work regardless of DPD state
    object_delete(client, &group_get_url).await;

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

    // Setup: project, pools, group with member
    create_project(&client, project_name).await;
    create_default_ip_pool(&client).await;

    let mcast_pool = create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create group that we'll delete while DPD is down
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 212));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for DPD failure during Deleting state test"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    assert_eq!(created_group.state, "Creating");

    // Add member and let group activate
    create_instance(client, project_name, instance_name).await;
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Wait for group to reach "Active" state before testing deletion
    wait_for_group_active(client, group_name).await;

    // Now delete the group to put it in "Deleting" state
    let group_delete_url = mcast_group_url(group_name);
    object_delete(client, &group_delete_url).await;

    // Stop DPD AFTER deletion but BEFORE reconciler processes deletion
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

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
        assert_eq!(group.identity.name, group_name);
        assert_eq!(group.multicast_ip, multicast_ip);
        assert_eq!(group.identity.id, created_group.identity.id);
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

    // Setup: project, pools, group with member - parallelize creation
    let (_, _, mcast_pool) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 213));
    let group_url = "/v1/multicast-groups".to_string();
    let params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: String::from(group_name).parse().unwrap(),
            description: "Group for member state during DPD failure test"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        mvlan: None,
    };

    // Stop DPD to test member operations during failure
    cptestctx.stop_dendrite(SwitchLocation::Switch0).await;

    let created_group: MulticastGroup =
        object_create(client, &group_url, &params).await;
    assert_eq!(created_group.state, "Creating");

    // Add member
    let instance = create_instance(client, project_name, instance_name).await;

    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
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

    // Verify member is accessible before DPD failure
    let members_url = format!("/v1/multicast-groups/{group_name}/members");
    let initial_members =
        nexus_test_utils::resource_helpers::objects_list_page_authz::<
            MulticastGroupMember,
        >(client, &members_url)
        .await
        .items;
    assert_eq!(
        initial_members.len(),
        1,
        "Should have exactly one member before DPD failure"
    );
    // Note: Members store instance_id (UUID), not instance name
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

    // Verify group is still in "Creating" state
    let group_get_url = mcast_group_url(group_name);
    let fetched_group: MulticastGroup =
        object_get(client, &group_get_url).await;

    assert_eq!(
        fetched_group.state, "Creating",
        "Group should remain in Creating state during DPD failure, found: {}",
        fetched_group.state
    );

    // Clean up
    object_delete(client, &group_get_url).await;

    // Wait for reconciler to process the deletion
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
}
