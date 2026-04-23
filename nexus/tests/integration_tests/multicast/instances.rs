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
//! - Idempotency: Duplicate attach operations succeed without creating duplicates
//! - Attach limits: Validates per-instance multicast group limits
//! - State transitions: Member states change with instance state
//! - Persistence: Memberships survive instance stop/start cycles
//! - Concurrent operations: Parallel attach/detach operations
//! - Never-started instances: Cleanup of members for instances never started
//! - Migration: Memberships update correctly when instance migrates
//! - SSM validation via instance operations:
//!   - Instance create with SSM: Must specify sources in multicast_groups
//!   - Instance reconfigure adding SSM: Must specify sources for new SSM groups
//!   - SSM sources are per-member (S,G subscription model)

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use http::{Method, StatusCode};

use nexus_db_model::MulticastGroupMemberState;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_instance, create_project, object_create,
    object_delete, object_get,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::instance::{
    InstanceCreate, InstanceNetworkInterfaceAttachment, InstanceUpdate,
};
use nexus_types::external_api::multicast::{
    InstanceMulticastGroupJoin, MulticastGroup, MulticastGroupJoinSpec,
    MulticastGroupMember,
};
use nexus_types::internal_api::params::InstanceMigrateRequest;

use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    InstanceState, Nullable,
};
use omicron_nexus::TestInterfaces;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};

use super::*;
use crate::integration_tests::instances::{
    instance_simulate, instance_wait_for_state, vmm_simulate_on_sled,
};

const PROJECT_NAME: &str = "test-project";

/// Consolidated multicast lifecycle test that combines multiple scenarios.
#[nexus_test]
async fn test_multicast_lifecycle(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    ops::join3(
        create_default_ip_pools(&client),
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
    let instances = [
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
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "instance-create-attach",
        group_names[0],
    )
    .await;

    // Wait for group-lifecycle-1 to become active and verify membership
    wait_for_group_active(client, group_names[0]).await;
    wait_for_member_state(
        cptestctx,
        "group-lifecycle-1",
        instances[0].identity.id,
        // Instance is stopped, so should be "Left"
        MulticastGroupMemberState::Left,
    )
    .await;

    // Live attach/detach operations
    // Attach instance-live-1 to group-lifecycle-2 (implicitly creates the group)
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "instance-live-1",
        "group-lifecycle-2",
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

    // Verify both instances are attached to group-lifecycle-2 (Left state
    // because the instances are stopped).
    let expected_left: Vec<_> = (0..2)
        .map(|i| {
            (instances[i + 1].identity.id, MulticastGroupMemberState::Left)
        })
        .collect();
    wait_for_members_state(cptestctx, "group-lifecycle-2", &expected_left)
        .await;

    // Multi-group attachment (instance to multiple groups)
    // Attach instance-multi-groups to group-lifecycle-3 (implicitly creates the group)
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "instance-multi-groups",
        "group-lifecycle-3",
    )
    .await;

    // Wait for group-lifecycle-3 to become active
    wait_for_group_active(client, group_names[2]).await;

    // Attach instance-multi-groups to group-lifecycle-4 (implicitly creates the group)
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "instance-multi-groups",
        "group-lifecycle-4",
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
            MulticastGroupMemberState::Left, // Stopped instance
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
        wait_for_group_deleted(cptestctx, group_names[0]),
        wait_for_group_deleted(cptestctx, group_names[1]),
        wait_for_group_deleted(cptestctx, group_names[2]),
        wait_for_group_deleted(cptestctx, group_names[3]),
    )
    .await;
}

#[nexus_test]
async fn test_multicast_group_attach_conflicts(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    ops::join3(
        create_default_ip_pools(&client),
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
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-1",
        "mcast-group-1",
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
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-2",
        "mcast-group-1",
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
    wait_for_group_deleted(cptestctx, "mcast-group-1").await;
}

#[nexus_test]
async fn test_multicast_group_attach_multiple(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    ops::join3(
        create_default_ip_pools(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    let group_names =
        ["limit-test-group-0", "limit-test-group-1", "limit-test-group-2"];

    // Create instance first (groups will be implicitly created when attached)
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "mcast-instance-1",
        false,
        &[], // No groups at creation
    )
    .await;

    // Attach instance to multiple groups (implicitly creates each group)
    let multicast_group_names = &group_names;
    for group_name in multicast_group_names {
        multicast_group_attach(
            cptestctx,
            PROJECT_NAME,
            "mcast-instance-1",
            group_name,
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
            MulticastGroupMemberState::Left,
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
        wait_for_group_deleted(cptestctx, group_names[0]),
        wait_for_group_deleted(cptestctx, group_names[1]),
        wait_for_group_deleted(cptestctx, group_names[2]),
    )
    .await;
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
    ops::join3(
        create_default_ip_pools(&client),
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

    // First instance attach (implicitly creates the group)
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        instance_names[0],
        "concurrent-test-group",
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

    // Verify all members reached correct state despite concurrent operations.
    // create_instance() starts instances, so they should all be Joined.
    let expected: Vec<_> = instances
        .iter()
        .map(|i| (i.identity.id, MulticastGroupMemberState::Joined))
        .collect();
    wait_for_members_state(cptestctx, "concurrent-test-group", &expected).await;

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

    // Back-to-back operations without waiting for reconciler between them.
    // Tests that the reconciler handles state changes that arrive while it
    // is still processing a previous batch.
    multicast_group_attach(
        cptestctx,
        PROJECT_NAME,
        "concurrent-instance-3",
        "concurrent-test-group",
    )
    .await;
    multicast_group_detach(
        client,
        PROJECT_NAME,
        "concurrent-instance-4",
        "concurrent-test-group",
    )
    .await;

    // Wait for system to reach consistent final state (should have 2 members)
    wait_for_member_count(client, "concurrent-test-group", 2).await;

    // Get the final members for state verification
    let post_rapid_members =
        list_multicast_group_members(client, "concurrent-test-group").await;

    // Wait for all remaining members to reach "Joined" state.
    let expected: Vec<_> = post_rapid_members
        .iter()
        .map(|m| (m.instance_id, MulticastGroupMemberState::Joined))
        .collect();
    wait_for_members_state(cptestctx, "concurrent-test-group", &expected).await;

    // Cleanup and delete instances (group is implicitly deleted when last member removed)
    cleanup_instances(cptestctx, client, PROJECT_NAME, &instance_names).await;
    wait_for_group_deleted(cptestctx, "concurrent-test-group").await;
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
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
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
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    wait_for_group_active(client, group_name).await;

    // Wait for member to reach "Left" state (stopped instance with no sled_id)
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        MulticastGroupMemberState::Left,
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
    wait_for_group_deleted(cptestctx, group_name).await;

    // Verify that stale ports were removed from DPD
    // Since the instance never started (never had a `sled_id`), there should be
    // no rear/underlay ports in DPD for this group.
    // Note: We use the underlay IP we saved before deleting the instance.
    wait_for_group_deleted_from_dpd(cptestctx, underlay_multicast_ip).await;
}

/// Test multicast group membership during instance migration.
///
/// This test verifies two migration scenarios:
/// 1. Single instance migration: membership persists, DPD is updated, port mapping works
/// 2. Concurrent migrations: multiple instances migrate simultaneously without interference
///
/// The RPW reconciler detects `sled_id` changes and updates DPD configuration on
/// both source and target switches to maintain uninterrupted multicast traffic.
#[nexus_test(extra_sled_agents = 1)]
async fn test_multicast_migration_scenarios(
    cptestctx: &ControlPlaneTestContext,
) {
    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "migration-project";

    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "migration-pool",
            (224, 60, 0, 1),
            (224, 60, 0, 255),
        ),
    )
    .await;

    let available_sleds =
        [cptestctx.first_sled_id(), cptestctx.second_sled_id()];

    // Case: Single instance migration with DPD verification

    let group1_name = "single-migration-group";
    let instance1 = instance_for_multicast_groups(
        cptestctx,
        project_name,
        "single-migration-inst",
        true,
        &[],
    )
    .await;
    let instance1_id = InstanceUuid::from_untyped_uuid(instance1.identity.id);

    multicast_group_attach(
        cptestctx,
        project_name,
        "single-migration-inst",
        group1_name,
    )
    .await;
    wait_for_group_active(client, group1_name).await;

    let group1 = get_multicast_group(client, group1_name).await;
    let multicast_ip = group1.multicast_ip;

    instance_simulate(nexus, &instance1_id).await;
    instance_wait_for_state(client, instance1_id, InstanceState::Running).await;
    wait_for_member_state(
        cptestctx,
        group1_name,
        instance1.identity.id,
        MulticastGroupMemberState::Joined,
    )
    .await;

    for (slot, dpd) in nexus_test_utils::dpd_clients_by_switch(cptestctx) {
        dpd.multicast_group_get(&multicast_ip).await.unwrap_or_else(|e| {
            panic!("{slot:?}: group should exist in DPD before migration: {e}")
        });
    }

    // Migrate instance
    let source_sled = nexus
        .active_instance_info(&instance1_id, None)
        .await
        .unwrap()
        .expect("Running instance should be on a sled")
        .sled_id;
    let target_sled =
        *available_sleds.iter().find(|&&s| s != source_sled).unwrap();

    let migrate_url = format!("/instances/{instance1_id}/migrate");
    NexusRequest::new(
        RequestBuilder::new(lockstep_client, Method::POST, &migrate_url)
            .body(Some(&InstanceMigrateRequest { dst_sled_id: target_sled }))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should initiate migration");

    let info =
        nexus.active_instance_info(&instance1_id, None).await.unwrap().unwrap();
    let src_propolis = info.propolis_id;
    let dst_propolis = info.dst_propolis_id.unwrap();

    vmm_simulate_on_sled(cptestctx, nexus, source_sled, src_propolis).await;
    instance_wait_for_state(client, instance1_id, InstanceState::Migrating)
        .await;

    // Verify membership persists during migration
    let migrating_members =
        list_multicast_group_members(client, group1_name).await;
    assert_eq!(migrating_members.len(), 1);
    assert_eq!(migrating_members[0].state, "Joined");

    vmm_simulate_on_sled(cptestctx, nexus, target_sled, dst_propolis).await;
    instance_wait_for_state(client, instance1_id, InstanceState::Running).await;

    // Verify post-migration state
    let post_sled = nexus
        .active_instance_info(&instance1_id, None)
        .await
        .unwrap()
        .unwrap()
        .sled_id;
    assert_eq!(post_sled, target_sled, "Instance should be on target sled");

    wait_for_member_state(
        cptestctx,
        group1_name,
        instance1.identity.id,
        MulticastGroupMemberState::Joined,
    )
    .await;

    verify_inventory_based_port_mapping(cptestctx, &instance1_id)
        .await
        .expect("Port mapping should be updated");
    for (slot, dpd) in nexus_test_utils::dpd_clients_by_switch(cptestctx) {
        dpd.multicast_group_get(&multicast_ip).await.unwrap_or_else(|e| {
            panic!("{slot:?}: group should exist in DPD after migration: {e}")
        });
    }

    // Verify sled-agent state after migration: the target sled should
    // have the VMM subscription and M2P mapping. The source sled should
    // not have any subscription for the old propolis.
    {
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let external_group = datastore
            .multicast_group_lookup_by_ip(&opctx, multicast_ip)
            .await
            .expect("Should look up multicast group by IP");

        let underlay_group_id = external_group
            .underlay_group_id
            .expect("Active group should have underlay_group_id");

        let underlay_group = datastore
            .underlay_multicast_group_fetch(&opctx, underlay_group_id)
            .await
            .expect("Should fetch underlay group");

        let underlay_ipv6 = match underlay_group.multicast_ip.ip() {
            IpAddr::V6(v6) => v6,
            other => {
                panic!("Expected IPv6 underlay address, got {other}")
            }
        };

        // Target sled should have the VMM subscription after the
        // reconciler pushes it via verify_members. Poll because the
        // reconciler may still be propagating state to the sled-agent.
        let target_agent = cptestctx
            .sled_agents
            .iter()
            .find(|sa| sa.sled_agent_id() == target_sled)
            .unwrap()
            .sled_agent();

        activate_then_wait_for_condition(
            &cptestctx.lockstep_client,
            || async {
                let groups =
                    target_agent.instance_multicast_groups.lock().unwrap();
                let has_sub = groups.get(&instance1_id).map_or(false, |g| {
                    g.iter().any(|m| m.group_ip == multicast_ip)
                });
                if has_sub { Ok(()) } else { Err(CondCheckError::NotYet::<()>) }
            },
            &POLL_INTERVAL,
            &POLL_TIMEOUT,
        )
        .await
        .expect(
            "Target sled should have instance subscription after migration",
        );

        // Target sled should have M2P mapping.
        activate_then_wait_for_condition(
            &cptestctx.lockstep_client,
            || async {
                let m2p = target_agent.m2p_mappings.lock().unwrap();
                if m2p.contains(&(multicast_ip, underlay_ipv6)) {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet::<()>)
                }
            },
            &POLL_INTERVAL,
            &POLL_TIMEOUT,
        )
        .await
        .expect("Target sled should have M2P mapping after migration");

        // TODO: assert the source sled no longer holds a multicast
        // subscription for the old propolis_id. On real hardware,
        // VMM teardown (release_opte_ports -> PortTicket::release_inner)
        // clears it. The sim does not model per-propolis cleanup on
        // unregister for any of the networking maps (external_ips,
        // attached_subnets, multicast_groups).
    }

    // Case: Concurrent migrations

    let group2_name = "concurrent-migration-group";
    let instance_names = ["concurrent-inst-1", "concurrent-inst-2"];
    let create_futures =
        instance_names.iter().map(|n| create_instance(client, project_name, n));
    let instances = ops::join_all(create_futures).await;

    multicast_group_attach(
        cptestctx,
        project_name,
        instance_names[0],
        group2_name,
    )
    .await;

    wait_for_group_active(client, group2_name).await;

    multicast_group_attach(
        cptestctx,
        project_name,
        instance_names[1],
        group2_name,
    )
    .await;

    let instance_ids: Vec<_> = instances
        .iter()
        .map(|i| InstanceUuid::from_untyped_uuid(i.identity.id))
        .collect();

    // Start all instances via simulation in parallel.
    ops::join_all(instance_ids.iter().map(|&instance_id| async move {
        instance_simulate(nexus, &instance_id).await;
        instance_wait_for_state(client, instance_id, InstanceState::Running)
            .await;
    }))
    .await;
    let expected_joined: Vec<_> = instances
        .iter()
        .map(|inst| (inst.identity.id, MulticastGroupMemberState::Joined))
        .collect();
    wait_for_members_state(cptestctx, group2_name, &expected_joined).await;

    // Get source/target sleds for each instance
    let mut source_sleds = Vec::new();
    let mut target_sleds = Vec::new();
    for &instance_id in &instance_ids {
        let current_sled = nexus
            .active_instance_info(&instance_id, None)
            .await
            .unwrap()
            .expect("Running instance should be on a sled")
            .sled_id;
        source_sleds.push(current_sled);
        target_sleds.push(
            *available_sleds.iter().find(|&&s| s != current_sled).unwrap(),
        );
    }

    // Initiate concurrent migrations
    let migration_futures =
        instance_ids.iter().zip(target_sleds.iter()).map(|(&id, &target)| {
            let url = format!("/instances/{id}/migrate");
            NexusRequest::new(
                RequestBuilder::new(lockstep_client, Method::POST, &url)
                    .body(Some(&InstanceMigrateRequest { dst_sled_id: target }))
                    .expect_status(Some(StatusCode::OK)),
            )
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
        });
    let responses = ops::join_all(migration_futures).await;
    for r in responses {
        r.expect("Migration should initiate");
    }

    // Complete all migrations in parallel.
    ops::join_all(instance_ids.iter().enumerate().map(|(i, &instance_id)| {
        let source_sled = source_sleds[i];
        let target_sled = target_sleds[i];
        async move {
            let info = nexus
                .active_instance_info(&instance_id, None)
                .await
                .unwrap()
                .unwrap();
            vmm_simulate_on_sled(
                cptestctx,
                nexus,
                source_sled,
                info.propolis_id,
            )
            .await;
            vmm_simulate_on_sled(
                cptestctx,
                nexus,
                target_sled,
                info.dst_propolis_id.unwrap(),
            )
            .await;
            instance_wait_for_state(
                client,
                instance_id,
                InstanceState::Running,
            )
            .await;
        }
    }))
    .await;

    // Verify all on target sleds
    for (i, &instance_id) in instance_ids.iter().enumerate() {
        let sled = nexus
            .active_instance_info(&instance_id, None)
            .await
            .unwrap()
            .unwrap()
            .sled_id;
        assert_eq!(
            sled,
            target_sleds[i],
            "Instance {} should be on target sled",
            i + 1
        );
    }

    let post_members = list_multicast_group_members(client, group2_name).await;
    assert_eq!(
        post_members.len(),
        2,
        "Both members should persist after concurrent migration"
    );

    let post_migration_joined: Vec<_> = instances
        .iter()
        .map(|inst| (inst.identity.id, MulticastGroupMemberState::Joined))
        .collect();
    wait_for_members_state(cptestctx, group2_name, &post_migration_joined)
        .await;

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["single-migration-inst", instance_names[0], instance_names[1]],
    )
    .await;
    ops::join2(
        wait_for_group_deleted(cptestctx, group1_name),
        wait_for_group_deleted(cptestctx, group2_name),
    )
    .await;
}

/// Test that source_ips are preserved across instance stop/start.
///
/// This verifies that when an instance is stopped and started:
/// a) Member goes to "Left" state on stop
/// b) Member is reactivated on start
/// c) The `source_ips` configured via explicit API are not wiped
#[nexus_test]
async fn test_source_ips_preserved_on_instance_restart(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "source-preserve-project";
    let instance_name = "source-preserve-inst";

    // Setup: project and SSM pool
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "source-preserve-ssm-pool",
            (232, 50, 0, 1),
            (232, 50, 0, 255),
        ),
    )
    .await;

    // Create and start instance
    let instance = create_instance(client, project_name, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Start the instance
    let instance_start_url =
        format!("/v1/instances/{instance_name}/start?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should start instance");

    // Simulate and wait for running
    let nexus = &cptestctx.server.server_context().nexus;
    instance_wait_for_running_with_simulation(cptestctx, instance_id).await;

    // Join SSM multicast group with source_ips
    let ssm_ip = "232.50.0.100";
    let source_ip: IpAddr = "10.99.99.1".parse().unwrap();
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{ssm_ip}?project={project_name}"
    );
    let join_body = InstanceMulticastGroupJoin {
        source_ips: Some(vec![source_ip]),
        ip_version: None,
    };

    let member_before: MulticastGroupMember =
        put_upsert(client, &join_url, &join_body).await;

    // Verify source_ips are set
    assert_eq!(
        member_before.source_ips.len(),
        1,
        "Member should have 1 source IP after join"
    );
    assert_eq!(
        member_before.source_ips[0], source_ip,
        "Member should have the specified source IP"
    );

    // Stop the instance
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

    // Simulate and wait for stopped
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Start the instance again
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should restart instance");

    // Simulate and wait for running
    instance_wait_for_running_with_simulation(cptestctx, instance_id).await;

    // Verify source_ips are preserved after restart
    // Get the member via the group members list
    let expected_group_name = format!("mcast-{}", ssm_ip.replace('.', "-"));
    let members_url =
        format!("/v1/multicast-groups/{expected_group_name}/members");
    let members_after: Vec<MulticastGroupMember> =
        NexusRequest::iter_collection_authn(client, &members_url, "", None)
            .await
            .expect("Should list members after restart")
            .all_items;

    assert_eq!(members_after.len(), 1, "Should have 1 member after restart");
    let member_after = &members_after[0];

    assert_eq!(
        member_after.source_ips.len(),
        1,
        "Member should still have 1 source IP after restart"
    );
    assert_eq!(
        member_after.source_ips[0], source_ip,
        "Member source_ips should be PRESERVED after instance restart"
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, &expected_group_name).await;
}

/// Test that source_ips are preserved when instance is reconfigured with multicast_groups.
///
/// This verifies that when an instance already has a membership with source_ips
/// and the instance is reconfigured with multicast_groups that includes that group:
/// 1. The existing membership (with source_ips) is not replaced
/// 2. New groups are added with empty source_ips
/// 3. Groups not in the new list are removed
#[nexus_test]
async fn test_source_ips_preserved_on_instance_reconfigure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "reconfig-preserve-project";
    let instance_name = "reconfig-preserve-inst";

    // Setup: create project and pools
    ops::join3(
        create_default_ip_pools(&client),
        create_project(client, project_name),
        // SSM pool for source-filtered groups
        create_multicast_ip_pool_with_range(
            client,
            "reconfig-ssm-pool",
            (232, 60, 0, 1),
            (232, 60, 0, 255),
        ),
    )
    .await;

    // Also create an ASM pool for the second group
    create_multicast_ip_pool_with_range(
        client,
        "reconfig-asm-pool",
        (224, 60, 0, 1),
        (224, 60, 0, 255),
    )
    .await;

    // Create instance with specified resources (need to match InstanceUpdate)
    let instance: Instance = object_create(
        client,
        &format!("/v1/instances?project={project_name}"),
        &InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description:
                    "test instance for reconfigure source_ips preservation"
                        .into(),
            },
            ncpus: InstanceCpuCount(2),
            memory: ByteCount::from_gibibytes_u32(4),
            hostname: instance_name.parse().unwrap(),
            user_data: Vec::new(),
            ssh_public_keys: None,
            network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
            external_ips: Vec::new(),
            disks: Vec::new(),
            boot_disk: None,
            start: false,
            auto_restart_policy: None,
            anti_affinity_groups: Vec::new(),
            cpu_platform: None,
            multicast_groups: Vec::new(),
        },
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Start instance so we can join groups
    let instance_start_url =
        format!("/v1/instances/{instance_name}/start?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_start_url)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should start instance");

    // Simulate and wait for running
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Join SSM multicast group with source_ips via explicit API
    let ssm_ip = "232.60.0.100";
    let source_ip: IpAddr = "10.60.60.1".parse().unwrap();
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{ssm_ip}?project={project_name}"
    );
    let join_body = InstanceMulticastGroupJoin {
        source_ips: Some(vec![source_ip]),
        ip_version: None,
    };

    let member_before: MulticastGroupMember =
        put_upsert(client, &join_url, &join_body).await;

    // Verify source_ips are set
    let ssm_group_name = format!("mcast-{}", ssm_ip.replace('.', "-"));
    assert_eq!(
        member_before.source_ips.len(),
        1,
        "Member should have 1 source IP after join"
    );
    assert_eq!(
        member_before.source_ips[0], source_ip,
        "Member should have the specified source IP"
    );

    // Now reconfigure instance with multicast_groups that includes the
    // SSM group and adds a new ASM group
    let asm_ip = "224.60.0.50";
    let asm_group_name = format!("mcast-{}", asm_ip.replace('.', "-"));

    let update_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    let update_body = serde_json::json!({
        "ncpus": 2,
        "memory": 4294967296_u64,  // 4 GiB in bytes
        "boot_disk": null,
        "auto_restart_policy": null,
        "cpu_platform": null,
        "multicast_groups": [
            // Existing group: source_ips=null to preserve existing sources
            { "group": ssm_ip, "source_ips": null },
            // New group: source_ips=null (will have empty sources since no prior)
            { "group": asm_ip, "source_ips": null },
        ]
    });

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &update_url)
            .body(Some(&update_body))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should reconfigure instance with multicast_groups");

    // Wait for ASM group to be created
    wait_for_group_active(client, &asm_group_name).await;

    // Verify SSM group source_ips are PRESERVED
    let ssm_members_url =
        format!("/v1/multicast-groups/{ssm_group_name}/members");
    let ssm_members: Vec<MulticastGroupMember> =
        NexusRequest::iter_collection_authn(
            client,
            &ssm_members_url,
            "",
            Some(10),
        )
        .await
        .expect("Should list SSM group members")
        .all_items;

    assert_eq!(ssm_members.len(), 1, "SSM group should have 1 member");
    let ssm_member = &ssm_members[0];
    assert_eq!(
        ssm_member.source_ips.len(),
        1,
        "SSM member should still have 1 source IP after reconfigure"
    );
    assert_eq!(
        ssm_member.source_ips[0], source_ip,
        "SSM member source_ips should be PRESERVED after instance reconfigure"
    );

    // Verify ASM group was created with empty source_ips
    let asm_members_url =
        format!("/v1/multicast-groups/{asm_group_name}/members");
    let asm_members: Vec<MulticastGroupMember> =
        NexusRequest::iter_collection_authn(
            client,
            &asm_members_url,
            "",
            Some(10),
        )
        .await
        .expect("Should list ASM group members")
        .all_items;

    assert_eq!(asm_members.len(), 1, "ASM group should have 1 member");
    let asm_member = &asm_members[0];
    assert!(
        asm_member.source_ips.is_empty(),
        "ASM member added via reconfigure should have empty source_ips"
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, &ssm_group_name).await;
    wait_for_group_deleted(cptestctx, &asm_group_name).await;
}

/// Test creating an instance with SSM multicast groups via MulticastGroupJoinSpec.
///
/// This tests the new implicit flow that allows specifying source_ips
/// when creating an instance with multicast_groups.
#[nexus_test]
async fn test_instance_create_with_ssm_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_types::external_api::multicast::MulticastGroupJoinSpec;

    let client = &cptestctx.external_client;
    let project_name = "ssm-create-project";
    let instance_name = "ssm-create-instance";

    // Setup: create pools and project
    ops::join3(
        create_default_ip_pools(&client),
        create_project(client, project_name),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-create-pool",
            (232, 70, 0, 1),
            (232, 70, 0, 100),
        ),
    )
    .await;

    // Create instance with SSM multicast group + source_ips via implicit flow
    let ssm_ip: IpAddr = "232.70.0.10".parse().unwrap();
    let source_ip: IpAddr = "10.70.70.1".parse().unwrap();

    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance created with SSM multicast groups".into(),
        },
        ncpus: InstanceCpuCount(2),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: instance_name.parse().unwrap(),
        user_data: Vec::new(),
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: Vec::new(),
        disks: Vec::new(),
        boot_disk: None,
        start: true, // Start the instance
        auto_restart_policy: None,
        anti_affinity_groups: Vec::new(),
        cpu_platform: None,
        // Key part: SSM group with source_ips via MulticastGroupJoinSpec
        multicast_groups: vec![MulticastGroupJoinSpec {
            group: ssm_ip.to_string().parse().unwrap(),
            source_ips: Some(vec![source_ip]),
            ip_version: None,
        }],
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate instance to running state
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Verify the SSM group was created
    let ssm_group_name =
        format!("mcast-{}", ssm_ip.to_string().replace('.', "-"));
    wait_for_group_active(client, &ssm_group_name).await;

    // Verify the member was created with source_ips
    let members = list_multicast_group_members(client, &ssm_group_name).await;
    assert_eq!(members.len(), 1, "Should have one member");

    let member = &members[0];
    assert_eq!(member.instance_id, instance.identity.id);
    assert_eq!(member.source_ips.len(), 1, "Member should have 1 source IP");
    assert_eq!(
        member.source_ips[0], source_ip,
        "Member should have the specified source IP"
    );

    // Verify the group-level source_ips shows the union (just the one source)
    let group = get_multicast_group(client, &ssm_group_name).await;
    assert_eq!(
        group.source_ips.len(),
        1,
        "Group should show 1 source IP (union of members)"
    );
    assert_eq!(group.source_ips[0], source_ip);

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, &ssm_group_name).await;
}

/// Test that SSM multicast groups without sources fail validation on both
/// instance create and reconfigure paths.
///
/// SSM addresses (232/8 for IPv4) require source IPs to be specified. This
/// test verifies the validation happens during both:
/// a). Instance creation (POST /v1/instances) with SSM group without sources
/// b). Instance reconfigure (PUT /v1/instances) adding new SSM group without sources
#[nexus_test]
async fn test_ssm_without_sources_fails_create_and_reconfigure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "ssm-nosrc-project";
    let instance_name = "ssm-nosrc-instance";

    // Setup: create pools and project
    ops::join3(
        create_default_ip_pools(&client),
        create_project(client, project_name),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-nosrc-pool",
            (232, 80, 0, 1),
            (232, 80, 0, 100),
        ),
    )
    .await;

    let ssm_ip: IpAddr = "232.80.0.10".parse().unwrap();

    // Case: Instance creation with SSM group without sources should fail
    let instance_params_with_ssm = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance should fail with SSM without sources".into(),
        },
        ncpus: InstanceCpuCount(2),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: instance_name.parse().unwrap(),
        user_data: Vec::new(),
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: Vec::new(),
        disks: Vec::new(),
        boot_disk: None,
        start: true,
        auto_restart_policy: None,
        anti_affinity_groups: Vec::new(),
        cpu_platform: None,
        multicast_groups: vec![MulticastGroupJoinSpec {
            group: ssm_ip.to_string().parse().unwrap(),
            source_ips: None, // Missing sources for SSM!
            ip_version: None,
        }],
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let create_error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_url)
            .body(Some(&instance_params_with_ssm))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Creating instance with SSM without sources should fail");

    let create_error_body: serde_json::Value =
        serde_json::from_slice(&create_error.body).unwrap();
    let create_error_message =
        create_error_body["message"].as_str().unwrap_or("");
    assert!(
        create_error_message.contains("SSM")
            || create_error_message.contains("source"),
        "Create error should mention SSM or source IPs: {create_error_message}"
    );

    // Case: Instance reconfiguration while adding SSM group without sources
    // should fail
    //
    // We first create instance without multicast groups
    let instance_params_no_mcast = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance for SSM reconfigure test".into(),
        },
        ncpus: InstanceCpuCount(2),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: instance_name.parse().unwrap(),
        user_data: Vec::new(),
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: Vec::new(),
        disks: Vec::new(),
        boot_disk: None,
        start: true,
        auto_restart_policy: None,
        anti_affinity_groups: Vec::new(),
        cpu_platform: None,
        multicast_groups: vec![], // No multicast groups init
    };

    let instance: Instance =
        object_create(client, &instance_url, &instance_params_no_mcast).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Try to reconfigure to add SSM group without sources
    let update_params = InstanceUpdate {
        ncpus: InstanceCpuCount(2),
        memory: ByteCount::from_gibibytes_u32(4),
        boot_disk: Nullable(None),
        auto_restart_policy: Nullable(None),
        cpu_platform: Nullable(None),
        multicast_groups: Some(vec![MulticastGroupJoinSpec {
            group: ssm_ip.to_string().parse().unwrap(),
            source_ips: None, // Missing sources for new SSM group!
            ip_version: None,
        }]),
    };

    let update_url =
        format!("/v1/instances/{instance_name}?project={project_name}");
    let reconfig_error = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &update_url)
            .body(Some(&update_params))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Reconfigure adding SSM group without sources should fail");

    let reconfig_error_body: serde_json::Value =
        serde_json::from_slice(&reconfig_error.body).unwrap();
    let reconfig_error_message =
        reconfig_error_body["message"].as_str().unwrap_or("");
    assert!(
        reconfig_error_message.contains("SSM")
            || reconfig_error_message.contains("source"),
        "Reconfigure error should mention SSM or source IPs: {reconfig_error_message}"
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
}

/// Test that instance deletion only removes that instance's membership,
/// preserving other instances' memberships in the same group.
///
/// This tests the invariant that `multicast_group_member_delete_by_group_and_instance`
/// filters by both `group_id` and `instance_id`, not just `group_id`. This is
/// important for saga undo correctness: if instance B's create saga fails after
/// joining a group, the undo must not affect instance A's existing membership
/// in the same group.
///
/// This also verifies the shared-sled underlay invariant. Underlay membership
/// is port-scoped, not member-scoped, as members on the same sled share a
/// single rear-port entry in the underlay group. To exercise this, the test
/// forces a multi-sled layout via migration:
///
/// - instances A and B sit on the same sled (sharing one rear port).
/// - instance C sits on the other sled (its own rear port).
///
/// Deleting instance B must leave the rear-port set in the underlay group
/// unchanged, since A still needs the shared port and C still needs its own.
#[nexus_test(extra_sled_agents = 1)]
async fn test_instance_delete_preserves_other_memberships(
    cptestctx: &ControlPlaneTestContext,
) {
    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "delete-preserve-project";
    let group_name = "delete-preserve-group";

    ops::join3(
        create_default_ip_pools(client),
        create_project(client, project_name),
        create_multicast_ip_pool_with_range(
            client,
            "delete-preserve-pool",
            (224, 90, 0, 1),
            (224, 90, 0, 255),
        ),
    )
    .await;

    let available_sleds =
        [cptestctx.first_sled_id(), cptestctx.second_sled_id()];

    // Bring up A, B, C as "Running" with the group attached.
    let instances = ["instance-a", "instance-b", "instance-c"].iter().map(
        |name| async move {
            let inst = instance_for_multicast_groups(
                cptestctx,
                project_name,
                name,
                true,
                &[group_name],
            )
            .await;
            let id = InstanceUuid::from_untyped_uuid(inst.identity.id);
            instance_simulate(nexus, &id).await;
            instance_wait_for_state(client, id, InstanceState::Running).await;
            (inst, id)
        },
    );
    let started: Vec<(Instance, InstanceUuid)> = ops::join_all(instances).await;
    let (instance_a, instance_a_uuid) = &started[0];
    let (_instance_b, instance_b_uuid) = &started[1];
    let (instance_c, instance_c_uuid) = &started[2];

    wait_for_group_active(client, group_name).await;
    let initial_joined: Vec<_> = started
        .iter()
        .map(|(inst, _)| (inst.identity.id, MulticastGroupMemberState::Joined))
        .collect();
    wait_for_members_state(cptestctx, group_name, &initial_joined).await;

    // Pick a "shared" sled (where A and B will live) and a "solo" sled
    // (where C will live), based on A's current placement.
    let shared_sled = nexus
        .active_instance_info(instance_a_uuid, None)
        .await
        .unwrap()
        .expect("instance A should be on a sled")
        .sled_id;
    let solo_sled = *available_sleds
        .iter()
        .find(|&&s| s != shared_sled)
        .expect("two distinct sleds expected");

    migrate_instance_to(cptestctx, *instance_b_uuid, shared_sled).await;
    migrate_instance_to(cptestctx, *instance_c_uuid, solo_sled).await;

    // After migration, the reconciler must observe each member's new
    // sled_id before the rear-port snapshot. We explicitly
    // poll until the DB row matches the post-migration placement for
    // every member.
    let expected_placement = [
        (instance_a.identity.id, shared_sled),
        (instance_b_uuid.into_untyped_uuid(), shared_sled),
        (instance_c.identity.id, solo_sled),
    ];
    wait_for_member_sled_ids(cptestctx, group_name, &expected_placement).await;

    let members_before = list_multicast_group_members(client, group_name).await;
    assert_eq!(
        members_before.len(),
        3,
        "all three instances should be members"
    );

    let group_view = get_multicast_group(client, group_name).await;
    let multicast_ip = group_view.multicast_ip;
    let underlay_admin_ip =
        fetch_underlay_admin_ip(cptestctx, multicast_ip).await;

    // Each switch independently programs the full set of rear ports for the
    // group's underlay members. Read every switch's underlay group so a
    // missing-on-one-switch fanout regression is caught.
    let collect_rear_ports_by_switch = || {
        let admin_ip = underlay_admin_ip.clone();
        let dpd_clients = nexus_test_utils::dpd_clients_by_switch(cptestctx);
        async move {
            let mut by_switch: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
            for (slot, dpd) in dpd_clients {
                let resp = dpd
                    .multicast_group_get_underlay(&admin_ip)
                    .await
                    .expect("underlay group should exist in DPD")
                    .into_inner();
                // Key on (port_id, link_id) so breakout-link members are
                // distinguished and any link/direction drift would be
                // visible as a set difference.
                let ports: BTreeSet<_> = resp
                    .members
                    .into_iter()
                    .filter(|m| {
                        matches!(m.port_id, dpd_client::types::PortId::Rear(_))
                            && m.direction
                                == dpd_client::types::Direction::Underlay
                    })
                    .map(|m| (m.port_id, m.link_id))
                    .collect();
                by_switch.insert(slot, ports);
            }
            by_switch
        }
    };

    // The DB sled_id update precedes DPD programming, and switches are
    // updated independently per pass. Poll until every switch has both
    // rear-port entries (shared sled + solo sled) before snapshotting.
    let rear_ports_before = wait_for_condition(
        || async {
            let by_switch = collect_rear_ports_by_switch().await;
            if by_switch.values().all(|ports| ports.len() == 2) {
                Ok(by_switch)
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "underlay group did not converge to one rear-port entry per \
             occupied sled (shared by A+B, plus C's own) on every switch: \
             {e:?}"
        )
    });

    // Delete instance B. A still occupies the shared sled, so the
    // shared rear port must remain; C's separate rear port must also
    // remain.
    cleanup_instances(cptestctx, client, project_name, &["instance-b"]).await;

    let members_after_b =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(members_after_b.len(), 2, "A and C must survive B's deletion");
    let remaining_instance_ids: BTreeSet<_> =
        members_after_b.iter().map(|m| m.instance_id).collect();
    assert!(
        remaining_instance_ids.contains(&instance_a.identity.id),
        "A's membership must remain"
    );
    assert!(
        remaining_instance_ids.contains(&instance_c.identity.id),
        "C's membership must remain"
    );

    let group = get_multicast_group(client, group_name).await;
    assert_eq!(group.state, "Active");

    // Per-switch DPD updates lag the member-list change. Poll until
    // every switch returns to its pre-delete state: A still on the
    // shared sled, C on its own.
    let rear_ports_after = wait_for_condition(
        || async {
            let by_switch = collect_rear_ports_by_switch().await;
            if by_switch == rear_ports_before {
                Ok(by_switch)
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "per-switch rear-port set diverged from pre-deletion snapshot \
             (expected {rear_ports_before:?}): {e:?}"
        )
    });
    assert_eq!(rear_ports_after, rear_ports_before);

    assert_mrib_route_exists(cptestctx, multicast_ip).await;

    // Cleanup: deleting A and C drops both rear ports and tears down
    // the group.
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["instance-a", "instance-c"],
    )
    .await;
    wait_for_group_deleted(cptestctx, group_name).await;
    wait_for_group_deleted_from_dpd(cptestctx, multicast_ip).await;
    assert_mrib_route_absent(cptestctx, multicast_ip).await;
}

/// Test IPv6 multicast group lifecycle: create, start, stop, delete.
///
/// This mirrors the IPv4 lifecycle tests but uses IPv6 multicast addresses
/// from a global-scope (ff0e::/16) pool to verify IPv6 support end-to-end.
#[nexus_test]
async fn test_multicast_ipv6_lifecycle(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "ipv6-lifecycle-project";
    let group_name = "ipv6-lifecycle-group";

    // Setup: create project and IPv6 multicast pool
    ops::join3(
        create_default_ip_pools(client),
        create_project(client, project_name),
        create_multicast_ip_pool_v6(client, "ipv6-lifecycle-pool"),
    )
    .await;
    ensure_multicast_test_ready(cptestctx).await;

    // Create an instance (not started yet)
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        "ipv6-instance",
        false,
        &[],
    )
    .await;

    // Join the IPv6 multicast group (implicitly creates the group)
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/{group_name}?project={project_name}",
        instance.identity.id
    );
    let member: MulticastGroupMember = put_upsert(
        client,
        &join_url,
        &nexus_types::external_api::multicast::InstanceMulticastGroupJoin {
            source_ips: None,
            ip_version: None, // Only one pool, no ambiguity
        },
    )
    .await;

    assert_eq!(member.instance_id, instance.identity.id);

    // Activate reconciler and wait for group to become Active
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    let group = wait_for_group_active(client, group_name).await;

    // Verify the group got an IPv6 address from the pool
    match group.multicast_ip {
        std::net::IpAddr::V4(_) => {
            panic!(
                "Expected IPv6 multicast address, got IPv4: {}",
                group.multicast_ip
            );
        }
        std::net::IpAddr::V6(v6) => {
            assert!(
                v6.segments()[0] == 0xff0e,
                "Expected global-scope IPv6 multicast (ff0e::), got {}",
                group.multicast_ip
            );
        }
    }

    // Start the instance - member should transition to "Joined"
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    let start_url =
        format!("/v1/instances/ipv6-instance/start?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &start_url)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Start should succeed");
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        MulticastGroupMemberState::Joined,
    )
    .await;

    // Stop the instance - member should transition to "Left"
    let stop_url =
        format!("/v1/instances/ipv6-instance/stop?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should stop instance");

    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        MulticastGroupMemberState::Left,
    )
    .await;

    // Delete the instance - this should delete the group since it's the only member
    cleanup_instances(cptestctx, client, project_name, &["ipv6-instance"])
        .await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Test that a group with all members in "Left" state remains "Active".
///
/// When all instances in a multicast group are stopped (members go to "Left"),
/// the group should remain "Active". We only delete when members are removed
/// (instance delete), not when they're stopped.
#[nexus_test]
async fn test_group_with_all_members_left(cptestctx: &ControlPlaneTestContext) {
    // Ensure inventory and DPD are ready before creating instances with multicast groups
    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;
    let project_name = "all-left-project";
    let group_name = "all-left-group";

    // Setup
    ops::join3(
        create_default_ip_pools(client),
        create_project(client, project_name),
        create_multicast_ip_pool(client, "all-left-pool"),
    )
    .await;

    // Create instance and start it (no multicast groups at creation)
    let instance1 = instance_for_multicast_groups(
        cptestctx,
        project_name,
        "left-instance-1",
        true,
        &[],
    )
    .await;

    // Add instance to group (group implicitly creates if it doesn't exist)
    multicast_group_attach(
        cptestctx,
        project_name,
        "left-instance-1",
        group_name,
    )
    .await;

    // Wait for group to become Active
    wait_for_group_active(client, group_name).await;

    let id1 = InstanceUuid::from_untyped_uuid(instance1.identity.id);

    // Simulate the instance transitioning to Running state
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &id1).await;
    instance_wait_for_state(client, id1, InstanceState::Running).await;

    // Wait for member to be joined
    wait_for_member_state(
        cptestctx,
        group_name,
        instance1.identity.id,
        MulticastGroupMemberState::Joined,
    )
    .await;

    // Now add a second instance to the same group
    let instance2 = instance_for_multicast_groups(
        cptestctx,
        project_name,
        "left-instance-2",
        true,
        &[],
    )
    .await;

    multicast_group_attach(
        cptestctx,
        project_name,
        "left-instance-2",
        group_name,
    )
    .await;

    let id2 = InstanceUuid::from_untyped_uuid(instance2.identity.id);
    instance_simulate(nexus, &id2).await;
    instance_wait_for_state(client, id2, InstanceState::Running).await;

    // Wait for member2 to be joined (member1 already verified above)
    wait_for_member_state(
        cptestctx,
        group_name,
        instance2.identity.id,
        MulticastGroupMemberState::Joined,
    )
    .await;

    // Stop both instances -> members should go to "Left"
    for (name, id) in [("left-instance-1", id1), ("left-instance-2", id2)] {
        let stop_url =
            format!("/v1/instances/{name}/stop?project={project_name}");
        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, &stop_url)
                .body(None as Option<&serde_json::Value>)
                .expect_status(Some(StatusCode::ACCEPTED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Should stop instance");

        instance_simulate(nexus, &id).await;
        instance_wait_for_state(client, id, InstanceState::Stopped).await;
    }

    // Verify both members are "Left"
    wait_for_members_state(
        cptestctx,
        group_name,
        &[
            (instance1.identity.id, MulticastGroupMemberState::Left),
            (instance2.identity.id, MulticastGroupMemberState::Left),
        ],
    )
    .await;

    // Group should still be "Active" (not deleted)
    let group = get_multicast_group(client, group_name).await;
    assert_eq!(
        group.state, "Active",
        "Group should remain Active when all members are Left"
    );

    // Start one instance again - member should go back to "Joined"
    let start_url =
        format!("/v1/instances/left-instance-1/start?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should start instance");

    instance_simulate(nexus, &id1).await;
    instance_wait_for_state(client, id1, InstanceState::Running).await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance1.identity.id,
        MulticastGroupMemberState::Joined,
    )
    .await;

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &["left-instance-1", "left-instance-2"],
    )
    .await;
    wait_for_group_deleted(cptestctx, group_name).await;
}
