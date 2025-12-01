// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use expectorate::assert_contents;
use nexus_reconfigurator_simulation::BlueprintId;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use omicron_common::api::external::Generation;
use omicron_test_utils::dev::test_setup_log;
use reconfigurator_cli::test_utils::ReconfiguratorCliTestState;
use std::mem;

#[track_caller]
fn assert_blueprint_diff_is_empty(
    bp1: &Blueprint,
    bp2: &Blueprint,
) {
    let summary = bp2.diff_since_blueprint(&bp1);
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 0);
}

/// Runs through a basic sequence of blueprints for adding a sled
#[test]
fn test_basic_add_sled() {
    static TEST_NAME: &str = "planner_basic_add_sled";
    let logctx = test_setup_log(TEST_NAME);
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);

    // Use our example system.
    sim.load_example(|builder| Ok(builder)).unwrap();
    let blueprint1 = sim.blueprint(BlueprintId::Latest).unwrap().clone();
    println!("{}", blueprint1.display());
    sim.assert_latest_blueprint_is_blippy_clean();

    // Now run the planner.  It should do nothing because our initial
    // system didn't have any issues that the planner currently knows how to
    // fix.
    let blueprint2 = sim.run_planner().unwrap();
    let summary = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expected no changes):\n{}", summary.display());
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 0);
    assert_eq!(summary.diff.sleds.unchanged().count(), 3);
    assert_eq!(summary.total_zones_added(), 0);
    assert_eq!(summary.total_zones_removed(), 0);
    assert_eq!(summary.total_zones_modified(), 0);
    assert_eq!(summary.total_disks_added(), 0);
    assert_eq!(summary.total_disks_removed(), 0);
    assert_eq!(summary.total_disks_modified(), 0);
    assert_eq!(summary.total_datasets_added(), 0);
    assert_eq!(summary.total_datasets_removed(), 0);
    assert_eq!(summary.total_datasets_modified(), 0);
    mem::drop(summary);
    sim.assert_latest_blueprint_is_blippy_clean();

    // Now add a new sled.
    let new_sled_id =
        sim.add_sled("add new sled", |sled| sled).expect("added sled");

    // Check that the first step is to add an NTP zone
    let blueprint3 = sim.run_planner().unwrap();
    let summary = blueprint3.diff_since_blueprint(&blueprint2);

    println!(
        "2 -> 3 (expect new NTP zone on new sled):\n{}",
        summary.display()
    );
    assert_contents(
        "tests/output/planner_basic_add_sled_2_3.txt",
        &summary.display().to_string(),
    );

    assert_eq!(summary.diff.sleds.added.len(), 1);
    assert_eq!(summary.total_disks_added(), 10);

    // 10 disks added means 30 datasets (each disk adds a debug + zone root
    //    + local storage), plus one transient zone root for the NTP zone
    assert_eq!(summary.total_datasets_added(), 31);

    let (&sled_id, sled_added) =
        summary.diff.sleds.added.first_key_value().unwrap();
    // We have defined elsewhere that the first generation contains no
    // zones.  So the first one with zones must be newer.  See
    // OmicronZonesConfig::INITIAL_GENERATION.
    assert!(sled_added.sled_agent_generation > Generation::new());
    assert_eq!(*sled_id, new_sled_id);
    assert_eq!(sled_added.zones.len(), 1);
    assert!(matches!(
        sled_added.zones.first().unwrap().kind(),
        ZoneKind::InternalNtp
    ));
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 0);
    mem::drop(summary);
    sim.assert_latest_blueprint_is_blippy_clean();

    // Check that with no change in inventory, the planner makes no changes.
    // It needs to wait for inventory to reflect the new NTP zone before
    // proceeding.
    let blueprint4 = sim.run_planner().unwrap();
    assert_blueprint_diff_is_empty(&blueprint3, &blueprint4);

    // Now update the inventory to have the requested NTP zone.
    sim.deploy_configs_to_active_sleds("add NTP zone", &blueprint3)
        .expect("deployed configs");
    let _inventory = sim
        .generate_inventory("inventory with new NTP zone")
        .expect("generated inventory");
    let blueprint5 = sim.run_planner().unwrap();

    // Check that the next step is to add Crucible zones
    let summary = blueprint5.diff_since_blueprint(&blueprint3);
    println!("3 -> 5 (expect Crucible zones):\n{}", summary.display());
    assert_contents(
        "tests/output/planner_basic_add_sled_3_5.txt",
        &summary.display().to_string(),
    );
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 1);
    let sled_id = summary.diff.sleds.modified_keys().next().unwrap();
    assert_eq!(*sled_id, new_sled_id);
    // No removed or modified zones on this sled
    let sled_cfg_diff = &summary.diff.sleds.get_modified(sled_id).unwrap();
    let zones_diff = sled_cfg_diff.diff_pair().zones;
    assert!(zones_diff.removed.is_empty());
    assert_eq!(zones_diff.modified().count(), 0);
    // 10 crucible zones addeed
    assert_eq!(
        sled_cfg_diff.after.sled_agent_generation,
        sled_cfg_diff.before.sled_agent_generation.next()
    );

    assert_eq!(zones_diff.added.len(), 10);
    for zone in &zones_diff.added {
        if zone.kind() != ZoneKind::Crucible {
            panic!("unexpectedly added a non-Crucible zone: {zone:?}");
        }
    }
    mem::drop(zones_diff);
    mem::drop(summary);
    sim.assert_latest_blueprint_is_blippy_clean();

    // Check that there are no more steps once the Crucible zones are
    // deployed.
    sim.deploy_configs_to_active_sleds(
        "deploy new Crucible zones",
        &blueprint5,
    )
    .expect("deployed configs");
    let _inventory = sim
        .generate_inventory("inventory with new NTP zone")
        .expect("generated inventory");
    let blueprint6 = sim.run_planner().unwrap();
    assert_blueprint_diff_is_empty(&blueprint5, &blueprint6);

    logctx.cleanup_successful();
}
