// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for multicast reconciler cache invalidation.
//!
//! Tests inventory and backplane caches used by the multicast reconciler:
//!
//! - Sled move detection: When a sled moves to a different switch port, the
//!   reconciler detects this via inventory and updates DPD port mappings

// TODO (mcast): Remove this module once the multicast RPW no longer maps sleds
// to rear ports by way of the inventory, even as a fallback.

use http::{Method, StatusCode};

use gateway_types::component::{PowerState, SpState};
use gateway_types::rot::RotState;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_project,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::sled;
use nexus_types::inventory::SpType;
use omicron_nexus::Server;
use omicron_nexus::TestInterfaces;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};
use sp_sim::SimulatedSp;

use super::*;
use crate::integration_tests::instances::instance_wait_for_state;

async fn insert_initial_inventory(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());
    let gateway =
        cptestctx.gateway.values().next().expect("Should have a gateway");
    let mut builder =
        nexus_inventory::CollectionBuilder::new("multicast-cache-test");

    for (slot, sp) in gateway.simrack.gimlets.iter().enumerate() {
        builder.found_sp_state(
            "test-sp",
            nexus_test_utils::RACK_UUID,
            SpType::Sled,
            slot.try_into().unwrap(),
            sp.state().await,
        );
    }

    datastore
        .inventory_insert_collection(&opctx, &builder.build())
        .await
        .expect("Should insert initial inventory collection");
    nexus_test_utils::background::run_inventory_loader(
        &cptestctx.lockstep_client,
    )
    .await;
}

/// Test that multicast operations can handle physical sled movement.
///
/// This test simulates a sled being physically moved to a different rack slot:
/// - Create a multicast group and instance, wait for member to join
/// - Verify the member is programmed on the correct rear port (based on original `sp_slot`)
/// - Run reconciler multiple times without inventory change to verify no spurious invalidation
/// - Insert a new inventory collection with a different `sp_slot` for the same sled
/// - Reconciler detects sled location change and invalidates caches automatically
/// - Verify DPD now uses the new rear port matching the new `sp_slot`
#[tokio::test]
async fn test_sled_move_updates_multicast_port_mapping() {
    const PROJECT_NAME: &str = "test-project";
    const GROUP_NAME: &str = "sled-move-test-group";
    const INSTANCE_NAME: &str = "sled-move-test-instance";

    let cptestctx = nexus_test_utils::ControlPlaneBuilder::new(
        "test_sled_move_updates_multicast_port_mapping",
    )
    .customize_nexus_config(&|config| {
        config.pkg.background_tasks.inventory.disable_collect = true;
    })
    .start::<Server>()
    .await;

    insert_initial_inventory(&cptestctx).await;
    ensure_multicast_test_ready(&cptestctx).await;

    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let log = &cptestctx.logctx.log;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    // Create project and pools in parallel
    ops::join3(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "sled-move-pool"),
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

    // Add instance to multicast group via instance-centric API
    multicast_group_attach(&cptestctx, PROJECT_NAME, INSTANCE_NAME, GROUP_NAME)
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

    // Verify initial port mapping (based on current inventory `sp_slot`)
    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
        .await
        .expect("Should verify initial port mapping");

    // Run reconciler again without new inventory to establish the
    // baseline collection ID in the reconciler. Running it twice ensures the
    // first run sets `last_seen_collection_id`, and the second run confirms
    // no unnecessary cache invalidation occurs when collection is unchanged.
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify port mapping is unchanged (no spurious cache invalidation)
    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
        .await
        .expect("Port mapping should be unchanged when inventory unchanged");

    // Assert that the member is in "Joined" state
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
    let dpd = nexus_test_utils::dpd_client(&cptestctx);
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
        nexus_test_utils::RACK_UUID,
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

    // Activate the inventory loader to update the watch channel with the new
    // collection, then activate the reconciler which will detect the sled
    // location change and invalidate caches.
    nexus_test_utils::background::run_inventory_loader(
        &cptestctx.lockstep_client,
    )
    .await;
    nexus.invalidate_multicast_caches();
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify that DPD now uses the new rear port (matching new `sp_slot`)
    // This helper reads the latest inventory and asserts DPD has a member
    // on rear{`sp_slot`}, so it will verify the new mapping is right
    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
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
        .multicast_group_members_list_by_instance(
            &opctx,
            instance_uuid,
            &DataPageParams::max_page(),
        )
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

    let dpd_client = nexus_test_utils::dpd_client(&cptestctx);
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

    cptestctx.teardown().await;
}

/// Test demonstrating that loading a new inventory collection invalidates
/// the multicast caches without an explicit invalidation call.
///
/// The inventory loader wakes the reconciler through the inventory-load
/// watcher, which then catches the sled's new `sp_slot` and invalidates
/// the sled and backplane caches.
///
/// Note: only the sled cache is checked here, as backplane map is the same
/// before and after the transition.
#[tokio::test]
async fn test_inventory_load_invalidates_multicast_caches() {
    const PROJECT_NAME: &str = "inventory-load-project";
    const GROUP_NAME: &str = "inventory-load-group";
    const INSTANCE_NAME: &str = "inventory-load-instance";

    // Start test server with custom config
    let cptestctx = nexus_test_utils::ControlPlaneBuilder::new(
        "test_inventory_load_invalidates_multicast_caches",
    )
    .customize_nexus_config(&|config| {
        config.pkg.background_tasks.inventory.disable_collect = true;

        // TTLs set well past `MULTICAST_OPERATION_TIMEOUT` to rule out cache
        // expiry as a refresh path. The inventory-load watcher has to refresh
        // the caches before the wait times out.
        config.pkg.background_tasks.multicast_reconciler.sled_cache_ttl_secs =
            std::time::Duration::from_secs(3600);
        config
            .pkg
            .background_tasks
            .multicast_reconciler
            .backplane_cache_ttl_secs = std::time::Duration::from_secs(3600);

        // Ensure multicast is enabled
        config.pkg.multicast.enabled = true;
    })
    .start::<omicron_nexus::Server>()
    .await;

    insert_initial_inventory(&cptestctx).await;
    ensure_multicast_test_ready(&cptestctx).await;

    // Local handles for DB and opctx
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    ops::join3(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "inventory-load-pool"),
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

    // Create a multicast group and wait for the member to join
    multicast_group_attach(&cptestctx, PROJECT_NAME, INSTANCE_NAME, GROUP_NAME)
        .await;
    wait_for_group_active(client, GROUP_NAME).await;

    let instance_uuid = InstanceUuid::from_untyped_uuid(instance.identity.id);

    wait_for_member_state(
        &cptestctx,
        GROUP_NAME,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify initial port mapping (this populates both caches)
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
    let new_slot =
        valid_slots.iter().copied().find(|s| *s != original_slot).expect(
            "Backplane map should have a rear port besides the original",
        );

    // Build a new inventory collection with the sled in a different slot
    let mut builder =
        nexus_inventory::CollectionBuilder::new("inventory-load-test");
    builder.found_sp_state(
        "test-sp",
        nexus_test_utils::RACK_UUID,
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

    // Insert a new inventory collection with a different `sp_slot` for the sled
    datastore
        .inventory_insert_collection(&opctx, &new_collection)
        .await
        .expect("Should insert new inventory collection");

    // Run the inventory loader without calling `invalidate_multicast_caches`
    nexus_test_utils::background::run_inventory_loader(
        &cptestctx.lockstep_client,
    )
    .await;

    // Verify DPD moves the member to the rear port for the new `sp_slot`
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
                    Err(CondCheckError::<String>::NotYet { status: None })
                }
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .expect("DPD should update with new rear port after the inventory load");

    // Verify member is still in "Joined" state after all cache operations
    let members = list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(members.len(), 1, "should still have exactly one member");
    assert_eq!(
        members[0].state, "Joined",
        "member should remain in Joined state after cache operations"
    );
    assert_eq!(
        members[0].instance_id, instance.identity.id,
        "member should still reference the same instance"
    );

    cptestctx.teardown().await;
}

/// Verify expunged sleds are excluded from multicast cache after refresh.
#[nexus_test(extra_sled_agents = 1)]
async fn test_sled_expunge_removes_from_multicast_cache(
    cptestctx: &ControlPlaneTestContext,
) {
    const PROJECT_NAME: &str = "expunge-test-project";
    const GROUP_NAME: &str = "expunge-test-group";
    const INSTANCE_NAME: &str = "expunge-test-instance";

    ensure_multicast_test_ready(cptestctx).await;

    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    // Make the second sled non-provisionable so instances go to the first sled
    let (authz_sled, ..) = LookupPath::new(&opctx, datastore)
        .sled_id(cptestctx.second_sled_id())
        .lookup_for(nexus_db_queries::authz::Action::Modify)
        .await
        .expect("lookup authz_sled");
    datastore
        .sled_set_provision_policy(
            &opctx,
            &authz_sled,
            nexus_types::external_api::sled::SledProvisionPolicy::NonProvisionable,
        )
        .await
        .expect("set sled provision policy");

    ops::join3(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "expunge-test-pool"),
    )
    .await;

    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        INSTANCE_NAME,
        true,
        &[],
    )
    .await;

    multicast_group_attach(&cptestctx, PROJECT_NAME, INSTANCE_NAME, GROUP_NAME)
        .await;
    wait_for_group_active(client, GROUP_NAME).await;

    let instance_uuid = InstanceUuid::from_untyped_uuid(instance.identity.id);

    wait_for_member_state(
        cptestctx,
        GROUP_NAME,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    verify_inventory_based_port_mapping(&cptestctx, &instance_uuid)
        .await
        .expect("Should verify initial port mapping");

    let first_sled_id = cptestctx.first_sled_id();
    cptestctx
        .lockstep_client
        .make_request(
            Method::POST,
            "/sleds/expunge",
            Some(sled::SledSelector { sled: first_sled_id }),
            StatusCode::OK,
        )
        .await
        .expect("Failed to expunge sled");

    // Wait for instance to fail (instance-watcher marks instances on expunged sleds as "Failed")
    instance_wait_for_state(client, instance_uuid, InstanceState::Failed).await;

    // Manually invalidate caches.
    //
    // Inventory-based invalidation is tested in
    // `test_sled_move_updates_multicast_port_mapping`. This test verifies cache
    // refresh uses SledFilter::InService, which excludes expunged sleds.
    nexus.invalidate_multicast_caches();
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    wait_for_member_state(
        cptestctx,
        GROUP_NAME,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    let in_service_sleds = datastore
        .sled_list_all_batched(&opctx, SledFilter::InService)
        .await
        .expect("Failed to list in-service sleds");

    assert!(
        !in_service_sleds.iter().any(|s| s.id() == first_sled_id),
        "Expunged sled should not appear in InService sled list"
    );

    let all_sleds = datastore
        .sled_list_all_batched(&opctx, SledFilter::All)
        .await
        .expect("Failed to list all sleds");

    assert!(
        all_sleds.iter().any(|s| s.id() == first_sled_id),
        "Expunged sled should still appear in All filter"
    );
}
