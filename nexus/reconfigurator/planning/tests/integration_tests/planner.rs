// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use expectorate::assert_contents;
use nexus_reconfigurator_blippy::Blippy;
use nexus_reconfigurator_blippy::BlippyReportSortKey;
use nexus_reconfigurator_planning::system::SledBuilder;
use nexus_reconfigurator_simulation::BlueprintId;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::external::Generation;
use omicron_test_utils::dev::test_setup_log;
use reconfigurator_cli::test_utils::ReconfiguratorCliTestState;
use std::mem;

/// Checks various conditions that should be true for all blueprints
#[track_caller]
fn verify_sim_latest_blueprint(sim: &ReconfiguratorCliTestState) {
    let blueprint = sim.blueprint(BlueprintId::Latest).unwrap();
    let planning_input = sim.planning_input(BlueprintId::Latest).unwrap();

    let blippy_report = Blippy::new(blueprint, &planning_input)
        .into_report(BlippyReportSortKey::Kind);
    if !blippy_report.notes().is_empty() {
        eprintln!("{}", blueprint.display());
        eprintln!("---");
        eprintln!("{}", blippy_report.display());
        panic!("expected blippy report for blueprint to have no notes");
    }
}

#[track_caller]
fn assert_sim_planning_makes_no_changes(sim: &mut ReconfiguratorCliTestState) {
    sim.run(&["blueprint-plan latest latest"]).unwrap();
    verify_sim_latest_blueprint(sim);

    let summary = sim.blueprint_diff_parent(BlueprintId::Latest).unwrap();
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
    let blueprint1 = sim.blueprint(BlueprintId::Latest).unwrap();
    println!("{}", blueprint1.display());
    verify_sim_latest_blueprint(&sim);

    // Now run the planner.  It should do nothing because our initial
    // system didn't have any issues that the planner currently knows how to
    // fix.
    let (_blueprint2, summary) = sim.run_planner().unwrap();
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
    verify_sim_latest_blueprint(&sim);

    // Now add a new sled.
    let mut new_sled_id = None;
    sim.change_state(|state| {
        let sled_id = state.rng_mut().next_sled_id();
        let new_sled = SledBuilder::new().id(sled_id);
        state.system_mut().description_mut().sled(new_sled)?;
        new_sled_id = Some(sled_id);
        Ok(format!("added sled {sled_id}"))
    })
    .expect("added sled");

    // Check that the first step is to add an NTP zone
    let (_blueprint3, summary) = sim.run_planner().unwrap();

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

    let (&&new_sled_id, sled_added) =
        summary.diff.sleds.added.first_key_value().unwrap();
    // We have defined elsewhere that the first generation contains no
    // zones.  So the first one with zones must be newer.  See
    // OmicronZonesConfig::INITIAL_GENERATION.
    assert!(sled_added.sled_agent_generation > Generation::new());
    assert_eq!(sled_added.zones.len(), 1);
    assert!(matches!(
        sled_added.zones.first().unwrap().kind(),
        ZoneKind::InternalNtp
    ));
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 0);
    mem::drop(summary);
    let blueprint3_id = sim.blueprint(BlueprintId::Latest).unwrap().id;
    verify_sim_latest_blueprint(&sim);

    // Check that with no change in inventory, the planner makes no changes.
    // It needs to wait for inventory to reflect the new NTP zone before
    // proceeding.
    assert_sim_planning_makes_no_changes(&mut sim);

    // Now update the inventory to have the requested NTP zone.
    sim.run(&[
        &format!("sled-set {new_sled_id} omicron-config latest"),
        "inventory-generate",
        "blueprint-plan latest latest",
    ])
    .unwrap();

    // Check that the next step is to add Crucible zones
    let summary = sim
        .blueprint_diff(BlueprintId::Id(blueprint3_id), BlueprintId::Latest)
        .unwrap();

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
    verify_sim_latest_blueprint(&sim);

    // Check that there are no more steps once the Crucible zones are
    // deployed.
    sim.run(&[
        &format!("sled-set {new_sled_id} omicron-config latest"),
        "inventory-generate",
    ])
    .unwrap();
    assert_sim_planning_makes_no_changes(&mut sim);

    logctx.cleanup_successful();
}
