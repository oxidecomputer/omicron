// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test that inventory matching works correctly between sled agents and SPs.

use nexus_db_queries::context::OpContext;
use nexus_test_utils_macros::nexus_test;
use nexus_types::identity::Asset;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// Test that simulated sleds and SPs have matching baseboard identifiers
/// so inventory can properly map sleds to switch ports.
#[nexus_test]
async fn test_sled_sp_inventory_matching(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Get the latest inventory collection
    let inventory = datastore
        .inventory_get_latest_collection(&opctx)
        .await
        .expect("failed to get inventory collection")
        .expect("no inventory collection available");

    // Get all sleds
    let sleds = datastore
        .sled_list_all_batched(
            &opctx,
            nexus_types::deployment::SledFilter::InService,
        )
        .await
        .expect("failed to list sleds");

    // Verify we have at least one sled
    assert!(!sleds.is_empty(), "expected at least one sled");

    // Track whether we found matching SP data for any sled
    let mut found_matching_sp = false;

    // Check each sled for matching SP data
    for sled in sleds {
        let sled_serial = sled.serial_number();
        let sled_part = sled.part_number();

        // Look for matching SP in inventory
        let sp_match = inventory.sps.iter().find(|(bb, _sp)| {
            bb.serial_number == sled_serial && bb.part_number == sled_part
        });

        if let Some((_bb, sp)) = sp_match {
            found_matching_sp = true;

            // Verify the SP has a valid sp_slot for switch port mapping
            assert!(
                sp.sp_slot < 32,
                "SP slot {} is unexpectedly large",
                sp.sp_slot
            );
        } else {
            eprintln!(
                "No exact SP match found for sled {} (serial={sled_serial}, part={sled_part})",
                sled.id()
            );

            // Check if there's a serial-only match (indicating part number mismatch)
            let serial_only_match = inventory
                .sps
                .iter()
                .find(|(bb, _sp)| bb.serial_number == sled_serial);

            if let Some((bb, _sp)) = serial_only_match {
                eprintln!(
                    "Found SP with same serial but different part: SP has part={}",
                    bb.part_number
                );
            }
        }
    }

    assert!(found_matching_sp, "No sleds had matching SP data in inventory");
}

/// Verify that the baseboard model is correctly set to "i86pc" for simulated
/// hardware.
#[nexus_test]
async fn test_simulated_baseboard_model(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Get all sleds
    let sleds = datastore
        .sled_list_all_batched(
            &opctx,
            nexus_types::deployment::SledFilter::InService,
        )
        .await
        .expect("failed to list sleds");

    for sled in sleds {
        // Simulated sleds should use "i86pc" as the model to match SP simulator
        assert_eq!(
            sled.part_number(),
            "i86pc",
            "Sled {} has incorrect model '{}', expected 'i86pc'",
            sled.id(),
            sled.part_number()
        );
    }
}
