// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for multicast reconciler cache invalidation.
//!
//! Tests inventory and backplane caches used by the multicast reconciler:
//!
//! - Sled move detection: When a sled moves to a different switch port, the
//!   reconciler detects this via inventory and updates DPD port mappings
//! - Cache TTL refresh: Verifies caches are refreshed when TTL expires
//! - Backplane cache expiry: Tests that stale backplane mappings are cleaned up

use gateway_client::types::{PowerState, RotState, SpState};
use nexus_db_queries::context::OpContext;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::params::MulticastGroupMemberAdd;
use nexus_types::external_api::views::MulticastGroupMember;
use nexus_types::inventory::SpType;
use omicron_common::api::external::NameOrId;
use omicron_nexus::Server;
use omicron_nexus::TestInterfaces;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};

use super::*;

/// Test that multicast operations can handle physical sled movement.
///
/// This test simulates a sled being physically moved to a different rack slot:
/// - Create a multicast group and instance, wait for member to join
/// - Verify the member is programmed on the correct rear port (based on original `sp_slot`)
/// - Insert a new inventory collection with a different `sp_slot` for the same sled
/// - Trigger cache invalidation and reconciler activation
/// - Verify DPD now uses the new rear port matching the new `sp_slot`
#[nexus_test(server = Server)]
async fn test_sled_move_updates_multicast_port_mapping(
    cptestctx: &ControlPlaneTestContext,
) {
    const PROJECT_NAME: &str = "test-project";
    const GROUP_NAME: &str = "sled-move-test-group";
    const INSTANCE_NAME: &str = "sled-move-test-instance";

    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let log = &cptestctx.logctx.log;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "sled-move-pool"),
    )
    .await;

    // Create instance (no multicast groups at creation - implicit model)
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        INSTANCE_NAME,
        true,
        &[],
    )
    .await;

    // Add instance to multicast group
    let member_add_url = format!(
        "{}?project={PROJECT_NAME}",
        mcast_group_members_url(GROUP_NAME)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;
    wait_for_group_active(client, GROUP_NAME).await;

    let instance_uuid = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Wait for member to join
    wait_for_member_state(
        cptestctx,
        GROUP_NAME,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify initial port mapping (based on current inventory `sp_slot`)
    verify_inventory_based_port_mapping(cptestctx, &instance_uuid)
        .await
        .expect("Should verify initial port mapping");

    // Assert that the member is in Joined state
    let members_before = list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(members_before.len(), 1, "should have exactly one member");
    assert_eq!(
        members_before[0].state, "Joined",
        "member should be in Joined state before sled move"
    );

    // Get the sled this instance is running on
    let sled_id = nexus
        .active_instance_info(&instance_uuid, None)
        .await
        .expect("Active instance info should be available")
        .expect("Instance should be on a sled")
        .sled_id;

    // Get sled baseboard information
    let sleds = datastore
        .sled_list_all_batched(&opctx, SledFilter::InService)
        .await
        .expect("Should list in-service sleds");
    let sled = sleds
        .into_iter()
        .find(|s| s.id() == sled_id)
        .expect("Should find sled in database");

    // Get current inventory to see the original sp_slot
    let original_inventory = datastore
        .inventory_get_latest_collection(&opctx)
        .await
        .expect("Should fetch latest inventory collection")
        .expect("Inventory collection should exist");

    let original_sp = original_inventory
        .sps
        .iter()
        .find(|(bb, _)| bb.serial_number == sled.serial_number())
        .map(|(_, sp)| sp)
        .expect("Should find SP for sled in original inventory");

    let original_slot = original_sp.sp_slot;
    let sled_serial = sled.serial_number().to_string();
    let sled_part_number = sled.part_number().to_string();

    // Verify DPD has the original port before the move
    let dpd = nexus_test_utils::dpd_client(cptestctx);
    let original_port_id = dpd_client::types::PortId::Rear(
        dpd_client::types::Rear::try_from(format!("rear{original_slot}"))
            .expect("Should be valid rear port string"),
    );

    // Determine a valid target slot by querying DPD's backplane map.
    // Prefer a different slot if available; otherwise fall back to the same.
    let backplane = dpd
        .backplane_map()
        .await
        .expect("Should fetch backplane map")
        .into_inner();
    let mut valid_slots: Vec<u16> = backplane
        .keys()
        .filter_map(|k| {
            k.strip_prefix("rear").and_then(|s| s.parse::<u16>().ok())
        })
        .collect();
    valid_slots.sort_unstable();
    valid_slots.dedup();
    let new_slot = valid_slots
        .iter()
        .copied()
        .find(|s| *s != original_slot)
        .unwrap_or(original_slot);

    // Build a new inventory collection with the sled in a different slot
    let mut builder = nexus_inventory::CollectionBuilder::new("sled-move-test");
    builder.found_sp_state(
        "test-sp",
        SpType::Sled,
        new_slot,
        SpState {
            serial_number: sled_serial,
            model: sled_part_number,
            power_state: PowerState::A0,
            revision: 0,
            base_mac_address: [0; 6],
            hubris_archive_id: "test-hubris".to_string(),
            rot: RotState::CommunicationFailed {
                message: "test-rot-state".to_string(),
            },
        },
    );

    let new_collection = builder.build();

    // Insert the new inventory collection
    datastore
        .inventory_insert_collection(&opctx, &new_collection)
        .await
        .expect("Should insert new inventory collection");

    // Invalidate multicast caches to force refresh from new inventory
    nexus.invalidate_multicast_caches();

    // Wait for reconciler to process the cache invalidation and refresh mappings
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify that DPD now uses the new rear port (matching new `sp_slot`)
    // This helper reads the latest inventory and asserts DPD has a member
    // on rear{`sp_slot`}, so it will verify the new mapping is right
    verify_inventory_based_port_mapping(cptestctx, &instance_uuid)
        .await
        .expect("Port mapping should be updated after cache invalidation");

    // Assert that the member is still in "Joined" state after the move
    let members_after = list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(members_after.len(), 1, "should still have exactly one member");
    assert_eq!(
        members_after[0].state, "Joined",
        "member should still be in Joined state after sled move"
    );
    assert_eq!(
        members_after[0].instance_id, instance.identity.id,
        "member should still reference the same instance"
    );

    // Verify stale port cleanup: fetch DPD state and ensure old port was removed
    let members = datastore
        .multicast_group_members_list_by_instance(&opctx, instance_uuid)
        .await
        .expect("Should list multicast members for instance");
    let member = members
        .first()
        .expect("Instance should have at least one multicast membership");

    let external_group = datastore
        .multicast_group_fetch(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(member.external_group_id),
        )
        .await
        .expect("Should fetch external multicast group");
    let underlay_group_id = external_group
        .underlay_group_id
        .expect("External group should have underlay_group_id");

    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .expect("Should fetch underlay multicast group");

    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    let underlay_group_response = dpd_client
        .multicast_group_get(&underlay_group.multicast_ip.ip())
        .await
        .expect("DPD multicast_group_get should succeed")
        .into_inner();

    let dpd_members = match underlay_group_response {
        dpd_client::types::MulticastGroupResponse::Underlay {
            members, ..
        } => members,
        dpd_client::types::MulticastGroupResponse::External { .. } => {
            panic!("Expected Underlay group, got External");
        }
    };

    // Verify that the old port membership has been removed (stale port cleanup)
    let has_old_port_member = dpd_members.iter().any(|m| {
        matches!(m.direction, dpd_client::types::Direction::Underlay)
            && m.port_id == original_port_id
    });

    assert!(
        !has_old_port_member,
        "Old underlay member with rear{original_slot} should have been removed after sled move"
    );
}

/// Test that cache TTL expiry automatically refreshes sled-to-port mappings:
///
/// - Start test server with sled_cache_ttl = 1 second
/// - Create multicast group and instance, wait for member to join
/// - Insert new inventory with different `sp_slot` (simulating sled move)
/// - Wait for TTL to expire (sleep 1.5 seconds)
/// - Activate reconciler (which should refresh cache due to TTL)
/// - Verify DPD uses the new rear port
#[tokio::test]
async fn test_cache_ttl_driven_refresh() {
    const PROJECT_NAME: &str = "ttl-test-project";
    const GROUP_NAME: &str = "ttl-test-group";
    const INSTANCE_NAME: &str = "ttl-test-instance";

    // Start test server with custom config
    let cptestctx = nexus_test_utils::ControlPlaneBuilder::new(
        "test_cache_ttl_driven_refresh",
    )
    .customize_nexus_config(&|config| {
        // Set short cache TTLs for testing (2 seconds for sled cache)
        config.pkg.background_tasks.multicast_reconciler.sled_cache_ttl_secs =
            chrono::TimeDelta::seconds(2).to_std().unwrap();
        config
            .pkg
            .background_tasks
            .multicast_reconciler
            .backplane_cache_ttl_secs =
            chrono::TimeDelta::seconds(1).to_std().unwrap();

        // Ensure multicast is enabled
        config.pkg.multicast.enabled = true;
    })
    .start::<omicron_nexus::Server>()
    .await;

    ensure_multicast_test_ready(&cptestctx).await;

    // Local handles for DB and opctx
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "ttl-test-pool"),
    )
    .await;

    // Create instance (no multicast groups at creation - implicit model)
    let instance = instance_for_multicast_groups(
        &cptestctx,
        PROJECT_NAME,
        INSTANCE_NAME,
        true,
        &[],
    )
    .await;

    // Add instance to multicast group
    let member_add_url = format!(
        "{}?project={PROJECT_NAME}",
        mcast_group_members_url(GROUP_NAME)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;
    wait_for_group_active(client, GROUP_NAME).await;

    let instance_uuid = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Wait for member to join
    wait_for_member_state(
        &cptestctx,
        GROUP_NAME,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify initial port mapping (this populates the cache)
    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
        .await
        .expect("Should verify initial port mapping");

    // Get the sled this instance is running on
    let sled_id = nexus
        .active_instance_info(&instance_uuid, None)
        .await
        .expect("Active instance info should be available")
        .expect("Instance should be on a sled")
        .sled_id;

    // Get sled baseboard information
    let sleds = datastore
        .sled_list_all_batched(&opctx, SledFilter::InService)
        .await
        .expect("Should list in-service sleds");
    let sled = sleds
        .into_iter()
        .find(|s| s.id() == sled_id)
        .expect("Should find sled in database");

    // Get current inventory to see the original sp_slot
    let original_inventory = datastore
        .inventory_get_latest_collection(&opctx)
        .await
        .expect("Should fetch latest inventory collection")
        .expect("Inventory collection should exist");

    let original_sp = original_inventory
        .sps
        .iter()
        .find(|(bb, _)| bb.serial_number == sled.serial_number())
        .map(|(_, sp)| sp)
        .expect("Should find SP for sled in original inventory");

    let original_slot = original_sp.sp_slot;
    let sled_serial = sled.serial_number().to_string();
    let sled_part_number = sled.part_number().to_string();

    // Determine a valid target slot by querying DPD's backplane map.
    // Prefer a different slot if available; otherwise fall back to the same.
    let dpd = nexus_test_utils::dpd_client(&cptestctx);
    let backplane = dpd
        .backplane_map()
        .await
        .expect("Should fetch backplane map")
        .into_inner();
    let mut valid_slots: Vec<u16> = backplane
        .keys()
        .filter_map(|k| {
            k.strip_prefix("rear").and_then(|s| s.parse::<u16>().ok())
        })
        .collect();
    valid_slots.sort_unstable();
    valid_slots.dedup();
    let new_slot = valid_slots
        .iter()
        .copied()
        .find(|s| *s != original_slot)
        .unwrap_or(original_slot);

    // Build a new inventory collection with the sled in a different slot
    let mut builder =
        nexus_inventory::CollectionBuilder::new("ttl-refresh-test");
    builder.found_sp_state(
        "test-sp",
        SpType::Sled,
        new_slot,
        SpState {
            serial_number: sled_serial,
            model: sled_part_number,
            power_state: PowerState::A0,
            revision: 0,
            base_mac_address: [0; 6],
            hubris_archive_id: "test-hubris".to_string(),
            rot: RotState::CommunicationFailed {
                message: "test-rot-state".to_string(),
            },
        },
    );

    let new_collection = builder.build();

    // Insert the new inventory collection
    datastore
        .inventory_insert_collection(&opctx, &new_collection)
        .await
        .expect("Should insert new inventory collection");

    // Wait for cache TTL to expire (sled_cache_ttl = 1 second)
    // Sleep for 1.5 seconds to ensure TTL has expired
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            // Try to verify the inventory-based port mapping
            // This will succeed once DPD has been updated with the new rear port
            match verify_inventory_based_port_mapping(
                &cptestctx,
                &instance_uuid,
            )
            .await
            {
                Ok(()) => Ok(()),
                Err(_) => {
                    // Not yet updated, reconciler needs another cycle
                    Err(CondCheckError::<String>::NotYet)
                }
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .expect("DPD should update with new rear port after TTL expiry");

    cptestctx.teardown().await;
}

/// Test that backplane cache TTL expiry triggers automatic refresh from DPD.
///
/// This test verifies that the backplane map cache expires independently from
/// the sled mapping cache and continues to work correctly after TTL expiry:
///
/// - Start test server with backplane_cache_ttl = 1 second (shorter than sled cache)
/// - Create multicast group and instance, wait for member to join (populates both caches)
/// - Verify initial port mapping works
/// - Wait for backplane TTL to expire (sleep 2 seconds)
/// - Trigger reconciler (which refreshes expired backplane cache from DPD)
/// - Verify port mapping still works (confirms cache refresh succeeded)
#[tokio::test]
async fn test_backplane_cache_ttl_expiry() {
    const PROJECT_NAME: &str = "backplane-ttl-project";
    const GROUP_NAME: &str = "backplane-ttl-group";
    const INSTANCE_NAME: &str = "backplane-ttl-instance";

    let cptestctx = nexus_test_utils::ControlPlaneBuilder::new(
        "test_backplane_cache_ttl_expiry",
    )
    .customize_nexus_config(&|config| {
        // Set backplane cache TTL to 1 second (shorter than sled cache to test
        // independently)
        config
            .pkg
            .background_tasks
            .multicast_reconciler
            .backplane_cache_ttl_secs =
            chrono::TimeDelta::seconds(1).to_std().unwrap();

        // Keep sled cache TTL longer to ensure we're testing backplane cache
        // expiry
        config.pkg.background_tasks.multicast_reconciler.sled_cache_ttl_secs =
            chrono::TimeDelta::seconds(10).to_std().unwrap();

        // Ensure multicast is enabled
        config.pkg.multicast.enabled = true;
    })
    .start::<omicron_nexus::Server>()
    .await;

    ensure_multicast_test_ready(&cptestctx).await;

    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "backplane-ttl-pool"),
    )
    .await;

    // Create instance (no multicast groups at creation - implicit model)
    let instance = instance_for_multicast_groups(
        &cptestctx,
        PROJECT_NAME,
        INSTANCE_NAME,
        true,
        &[],
    )
    .await;

    // Add instance to multicast group
    let member_add_url = format!(
        "{}?project={PROJECT_NAME}",
        mcast_group_members_url(GROUP_NAME)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;
    wait_for_group_active(client, GROUP_NAME).await;

    let instance_uuid = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Wait for member to join (this populates both caches)
    wait_for_member_state(
        &cptestctx,
        GROUP_NAME,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify initial port mapping (confirms both caches are populated)
    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
        .await
        .expect("Should verify initial port mapping");

    // Wait for backplane cache TTL to expire (500ms) but not sled cache (5 seconds)
    // Sleep for 1 second to ensure backplane TTL has expired
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Force cache access by triggering reconciler
    // This will cause the reconciler to check backplane cache, find it expired,
    // and refresh from DPD. The sled cache should still be valid.
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify member is still on the right port after backplane cache refresh
    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
        .await
        .expect("Port mapping should work after backplane cache TTL expiry");

    // Verify member is still in "Joined" state
    let members = list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(members.len(), 1, "should still have exactly one member");
    assert_eq!(
        members[0].state, "Joined",
        "member should remain in Joined state after backplane cache refresh"
    );
    assert_eq!(
        members[0].instance_id, instance.identity.id,
        "member should still reference the same instance"
    );

    cptestctx.teardown().await;
}
