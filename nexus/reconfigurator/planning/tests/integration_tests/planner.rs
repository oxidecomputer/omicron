// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use assert_matches::assert_matches;
use chrono::DateTime;
use chrono::Utc;
use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
use clickhouse_admin_types::KeeperId;
use expectorate::assert_contents;
use iddqd::IdOrdMap;
use nexus_reconfigurator_blippy::Blippy;
use nexus_reconfigurator_blippy::BlippyReportSortKey;
use nexus_reconfigurator_planning::blueprint_editor::ExternalNetworkingAllocator;
use nexus_reconfigurator_planning::example::ExampleSystem;
use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
use nexus_reconfigurator_planning::example::SimRngState;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_simulation::BlueprintId;
use nexus_reconfigurator_simulation::CollectionId;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintDiffSummary;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ClickhouseMode;
use nexus_types::deployment::ClickhousePolicy;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDisk;
use nexus_types::deployment::TargetReleaseDescription;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::blueprint_zone_type::InternalDns;
use nexus_types::external_api::views::PhysicalDiskPolicy;
use nexus_types::external_api::views::PhysicalDiskState;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::CockroachStatus;
use nexus_types::inventory::Collection;
use nexus_types::inventory::InternalDnsGenerationStatus;
use nexus_types::inventory::TimeSync;
use omicron_common::address::Ipv4Range;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::TufArtifactMeta;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::api::external::TufRepoMeta;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DiskIdentity;
use omicron_common::policy::BOUNDARY_NTP_REDUNDANCY;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_common::policy::CRUCIBLE_PANTRY_REDUNDANCY;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_common::update::ArtifactId;
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use oxnet::Ipv6Net;
use reconfigurator_cli::test_utils::ReconfiguratorCliTestState;
use semver::Version;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;
use typed_rng::TypedUuidRng;
use uuid::Uuid;

#[track_caller]
fn assert_blueprint_diff_is_empty(bp1: &Blueprint, bp2: &Blueprint) {
    // A blueprint always has an empty diff with itself; if someone passes the
    // same blueprint twice it's probably a typo in the caller.
    assert_ne!(
        bp1.id, bp2.id,
        "assert_blueprint_diff_is_empty() called with the same blueprint twice"
    );

    let summary = bp2.diff_since_blueprint(&bp1);
    if !summary.diff.sleds.added.is_empty()
        || !summary.diff.sleds.removed.is_empty()
        || summary.diff.sleds.modified().count() != 0
    {
        panic!(
            "expected empty blueprint diff, but got nonempty diff:\n{}",
            summary.display()
        );
    }
}

/// Checks various conditions that should be true for all blueprints
fn verify_blueprint(blueprint: &Blueprint, planning_input: &PlanningInput) {
    let blippy_report = Blippy::new(blueprint, planning_input)
        .into_report(BlippyReportSortKey::Kind);
    if !blippy_report.notes().is_empty() {
        eprintln!("{}", blueprint.display());
        eprintln!("---");
        eprintln!("{}", blippy_report.display());
        panic!("expected blippy report for blueprint to have no notes");
    }
}

// Generate a ClickhousePolicy ignoring fields we don't care about for
// planner tests
fn clickhouse_policy(mode: ClickhouseMode) -> ClickhousePolicy {
    ClickhousePolicy { version: 0, mode, time_created: Utc::now() }
}

fn get_nexus_ids_at_generation(
    blueprint: &Blueprint,
    generation: Generation,
) -> BTreeSet<OmicronZoneUuid> {
    blueprint
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_, z)| match &z.zone_type {
            BlueprintZoneType::Nexus(nexus_zone)
                if nexus_zone.nexus_generation == generation =>
            {
                Some(z.id)
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>()
}

/// Many tests want to ensure that if the planner runs again at a particular
/// point (usually the end), it doesn't make any additional changes. There are
/// two different intentions here:
///
/// * The planner is rerun after the current blueprint is executed, any new sled
///   configs are deployed, and an inventory collection has been taken that
///   reflects those deployments. This is `DeployLatestConfigs`.
/// * The planner is rerun with the current blueprint as parent but no other
///   changes (in particular, keeping the same inventory as was used to
///   generatee the parent). This is `InputUnchanged`.
///
/// Most tests should use `DeployLatestConfigs`. Some tests that deal with
/// expungements use `InputUnchanged` instead, because the planner does make
/// (effectively spurious) changes after deploying configs: specifically, it
/// will mark expunged zones as "ready for cleanup" once inventory reports that
/// sleds have received the config that shuts down those expunged zones. Those
/// tests could add extra steps to walk through this stage, but that's largely
/// noise and so don't all bother.
enum AssertPlanningMakesNoChangesMode {
    DeployLatestConfigs,
    InputUnchanged,
}

#[track_caller]
fn sim_assert_planning_makes_no_changes(
    sim: &mut ReconfiguratorCliTestState,
    mode: AssertPlanningMakesNoChangesMode,
) -> Arc<Blueprint> {
    // Get the latest blueprint.
    let blueprint = sim
        .blueprint(BlueprintId::Latest)
        .expect("always have a latest blueprint")
        .clone();

    match mode {
        AssertPlanningMakesNoChangesMode::InputUnchanged => {
            // Nothing to do!
        }
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs => {
            sim_update_collection_from_blueprint(sim, &blueprint);
        }
    }

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    assert_blueprint_diff_is_empty(&blueprint, &new_blueprint);

    new_blueprint
}

/// Runs through a basic sequence of blueprints for adding a sled
#[test]
fn test_basic_add_sled() {
    static TEST_NAME: &str = "planner_basic_add_sled";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();
    println!("{}", blueprint1.display());

    // Now run the planner.  It should do nothing because our initial
    // system didn't have any issues that the planner currently knows how to
    // fix.
    let blueprint2 = sim.run_planner().expect("planning succeeded");
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

    // Now add a new sled.
    let new_sled_id = sim.sled_add("add new sled").expect("added sled");

    // Check that the first step is to add an NTP zone
    let blueprint3 = sim.run_planner().expect("planning succeeded");
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

    // Check that with no change in inventory, the planner makes no changes.
    // It needs to wait for inventory to reflect the new NTP zone before
    // proceeding.
    let blueprint4 = sim.run_planner().expect("planning succeeded");
    assert_blueprint_diff_is_empty(&blueprint3, &blueprint4);

    // Now update the inventory to have the requested NTP zone.
    sim_update_collection_from_blueprint(&mut sim, &blueprint3);

    // Check that the next step is to add Crucible zones
    let blueprint5 = sim.run_planner().expect("planning succeeded");
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

    // Check that there are no more steps once the Crucible zones are deployed.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Check that the planner will add more Nexus zones to a single sled, if
/// needed
#[test]
fn test_add_multiple_nexus_to_one_sled() {
    static TEST_NAME: &str = "planner_add_multiple_nexus_to_one_sled";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system with one sled and one Nexus instance as a
    // starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| Ok(builder.nsleds(1).nexus_count(1)))
        .expect("loaded example system");

    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // This blueprint should only have 1 Nexus instance on the one sled we
    // kept.
    assert_eq!(blueprint1.sleds.len(), 1);
    let sled_id = blueprint1.sleds().next().unwrap();
    assert_eq!(
        blueprint1
            .sleds
            .get(&sled_id)
            .unwrap()
            .zones
            .iter()
            .filter(|z| z.zone_type.is_nexus())
            .count(),
        1
    );

    // Change the policy to 5 Nexus zones and only a single internal DNS zone.
    // (The default policy for internal DNS is higher than one, and we don't
    // want that to pollute our diffs below.)
    let target_nexus_zone_count = 5;
    sim.change_description("update target zone counts", |desc| {
        desc.set_target_nexus_zone_count(target_nexus_zone_count)
            .set_target_internal_dns_zone_count(1);
        Ok(())
    })
    .expect("changed policy");

    // Now run the planner.  It should add additional Nexus zones to the
    // one sled we have.
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    let summary = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (added additional Nexus zones):\n{}", summary.display());
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 1);
    let (changed_sled_id, changed_sled) =
        summary.diff.sleds.modified().next().unwrap();

    assert_eq!(*changed_sled_id, sled_id);
    assert_eq!(changed_sled.diff_pair().datasets.added.len(), 4);

    let zones_added = &summary
        .diff
        .sleds
        .get_modified(&sled_id)
        .unwrap()
        .diff_pair()
        .zones
        .added;
    assert_eq!(zones_added.len(), target_nexus_zone_count - 1);
    for zone in zones_added {
        if zone.kind() != ZoneKind::Nexus {
            panic!("unexpectedly added a non-Nexus zone: {zone:?}");
        }
    }

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Check that the planner will spread additional Nexus zones out across
/// sleds as it adds them
#[test]
fn test_spread_additional_nexus_zones_across_sleds() {
    static TEST_NAME: &str =
        "planner_spread_additional_nexus_zones_across_sleds";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // This blueprint should only have 3 Nexus zones: one on each sled.
    assert_eq!(blueprint1.sleds.len(), 3);
    for sled_config in blueprint1.sleds.values() {
        assert_eq!(
            sled_config.zones.iter().filter(|z| z.zone_type.is_nexus()).count(),
            1
        );
    }

    // Now run the planner with a high number of target Nexus zones.
    sim.change_description("update target zone counts", |desc| {
        desc.set_target_nexus_zone_count(14);
        Ok(())
    })
    .expect("changed policy");
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    let summary = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (added additional Nexus zones):\n{}", summary.display());
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 3);

    // All 3 sleds should get additional Nexus zones. We expect a total of
    // 11 new Nexus zones, which should be spread evenly across the three
    // sleds (two should get 4 and one should get 3).
    let mut total_new_nexus_zones = 0;
    for (sled_id, modified_sled) in summary.diff.sleds.modified() {
        let zones_diff = &modified_sled.diff_pair().zones;
        assert!(zones_diff.removed.is_empty());
        assert_eq!(zones_diff.modified().count(), 0);
        let zones_added = &zones_diff.added;
        match zones_added.len() {
            n @ (3 | 4) => {
                total_new_nexus_zones += n;
            }
            n => {
                panic!("unexpected number of zones added to {sled_id}: {n}")
            }
        }
        for zone in zones_added {
            if zone.kind() != ZoneKind::Nexus {
                panic!("unexpectedly added a non-Nexus zone: {zone:?}");
            }
        }
    }
    assert_eq!(total_new_nexus_zones, 11);

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Check that the planner will spread additional internal DNS zones out across
/// sleds as it adds them
#[test]
fn test_spread_internal_dns_zones_across_sleds() {
    static TEST_NAME: &str = "planner_spread_internal_dns_zones_across_sleds";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // This blueprint should have exactly 3 internal DNS zones: one on each
    // sled.
    assert_eq!(blueprint1.sleds.len(), 3);
    for sled_config in blueprint1.sleds.values() {
        assert_eq!(
            sled_config
                .zones
                .iter()
                .filter(|z| z.zone_type.is_internal_dns())
                .count(),
            1
        );
    }

    // Try to run the planner with a high number of internal DNS zones;
    // it will fail because the target is > INTERNAL_DNS_REDUNDANCY.
    {
        let mut sim = sim.clone();
        sim.change_description("change policy", |desc| {
            desc.set_target_internal_dns_zone_count(14);
            Ok(())
        })
        .expect("changed policy");

        match sim.run_planner() {
            Ok(_) => panic!("unexpected success"),
            Err(err) => {
                let err = InlineErrorChain::new(&*err).to_string();
                assert!(
                    err.contains(
                        "no reserved subnets available for internal DNS"
                    ),
                    "unexpected error: {err}"
                );
            }
        }
    }

    // Expunge two of the internal DNS zones; the planner should put new
    // zones back in their places.
    let zones_to_expunge = blueprint1
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(sled_id, zone)| {
            zone.zone_type.is_internal_dns().then_some((sled_id, zone.id))
        })
        .take(2);
    let mut nexpunged = 0;
    let blueprint2 = sim
        .blueprint_edit_latest("expunge 2 internal DNS zones", |builder| {
            for (sled_id, zone_id) in zones_to_expunge {
                builder
                    .sled_expunge_zone(sled_id, zone_id)
                    .expect("expunged zone");
                builder
                    .sled_mark_expunged_zone_ready_for_cleanup(sled_id, zone_id)
                    .expect("marked zone ready for cleanup");
                nexpunged += 1;
            }
            Ok(())
        })
        .expect("expunged zones");
    assert_eq!(nexpunged, 2);

    // Deploy this blueprint and generate a new inventory from it.
    sim_update_collection_from_blueprint(&mut sim, &blueprint2);

    // The planner should put new zones back in their places.
    let blueprint3 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint3.diff_since_blueprint(&blueprint2);
    println!(
        "2 -> 3 (added additional internal DNS zones):\n{}",
        summary.display()
    );
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 2);

    // 2 sleds should each get 1 additional internal DNS zone.
    let mut total_new_zones = 0;
    for (sled_id, modified_sled) in summary.diff.sleds.modified() {
        let zones_diff = &modified_sled.diff_pair().zones;
        assert!(zones_diff.removed.is_empty());
        assert_eq!(zones_diff.modified().count(), 0);
        let zones_added = &zones_diff.added;
        match zones_added.len() {
            0 => {}
            n @ 1 => {
                total_new_zones += n;
            }
            n => {
                panic!("unexpected number of zones added to {sled_id}: {n}")
            }
        }
        for zone in zones_added {
            assert_eq!(
                zone.kind(),
                ZoneKind::InternalDns,
                "unexpectedly added a non-internal-DNS zone: {zone:?}"
            );
        }
    }
    assert_eq!(total_new_zones, 2);

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Check that the planner will reuse external IPs that were previously
/// assigned to expunged zones
#[test]
fn test_reuse_external_ips_from_expunged_zones() {
    static TEST_NAME: &str = "planner_reuse_external_ips_from_expunged_zones";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // Expunge the first sled we see, which will result in a Nexus external
    // IP no longer being associated with a running zone, and a new Nexus
    // zone being added to one of the two remaining sleds.
    let sled_id = blueprint1.sleds().next().expect("at least one sled");
    sim.sled_expunge("expunge first sled", sled_id).expect("expunged sled");

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunged sled):\n{}", diff.display());

    // The expunged sled should have an expunged Nexus zone.
    let zone = blueprint2.sleds[&sled_id]
        .zones
        .iter()
        .find(|zone| matches!(zone.zone_type, BlueprintZoneType::Nexus(_)))
        .expect("no nexus zone found");
    assert_eq!(
        zone.disposition,
        BlueprintZoneDisposition::Expunged {
            as_of_generation: blueprint2.sleds[&sled_id].sled_agent_generation,
            ready_for_cleanup: true,
        }
    );

    // Set the target Nexus zone count to one that will completely exhaust
    // the service IP pool. This will force reuse of the IP that was
    // allocated to the expunged Nexus zone.
    sim.change_description("adjust target Nexus count", |desc| {
        let num_available_external_ips = desc
            .external_ip_policy()
            .clone()
            .into_non_external_dns_ips()
            .count();
        desc.set_target_nexus_zone_count(num_available_external_ips);
        Ok(())
    })
    .expect("updated target Nexus count");

    let blueprint3 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint3.diff_since_blueprint(&blueprint2);
    println!("2 -> 3 (maximum Nexus):\n{}", diff.display());

    // Planning succeeded, but let's prove that we reused the IP address!
    let expunged_ip = zone.zone_type.external_networking().unwrap().0.ip();
    let new_zone = blueprint3
        .sleds
        .values()
        .flat_map(|c| c.zones.iter())
        .find(|zone| {
            zone.disposition == BlueprintZoneDisposition::InService
                && zone
                    .zone_type
                    .external_networking()
                    .map_or(false, |(ip, _)| expunged_ip == ip.ip())
        })
        .expect("couldn't find that the external IP was reused");
    println!(
        "zone {} reused external IP {} from expunged zone {}",
        new_zone.id, expunged_ip, zone.id
    );

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Check that the planner will reuse external DNS IPs that were
/// previously assigned to expunged zones
#[test]
fn test_reuse_external_dns_ips_from_expunged_zones() {
    static TEST_NAME: &str =
        "planner_reuse_external_dns_ips_from_expunged_zones";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // Change the policy: add some external DNS IPs.
    let external_dns_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"].map(|addr| {
        addr.parse::<Ipv4Addr>().expect("can't parse external DNS IP address")
    });
    let external_ip_policy = sim
        .change_description("add external DNS IPs to policy", |desc| {
            // We should not be able to add any external DNS zones yet,
            // because we haven't give it any addresses (which currently
            // come only from RSS). This is not an error, though.
            assert!(desc.external_ip_policy().external_dns_ips().is_empty());
            let mut external_networking_alloc =
                ExternalNetworkingAllocator::from_blueprint(
                    &blueprint1,
                    desc.external_ip_policy(),
                )
                .expect("constructed allocator");
            external_networking_alloc
                .for_new_external_dns()
                .expect_err("should not have available IPs for external DNS");

            let mut ip_policy =
                desc.external_ip_policy().clone().into_builder();

            // Add a "service IP pool" covering our external DNS IP range.
            ip_policy
                .push_service_pool_ipv4_range(
                    Ipv4Range::new(external_dns_ips[0], external_dns_ips[2])
                        .unwrap(),
                )
                .unwrap();
            // Set these IPs as "for external DNS".
            for ip in external_dns_ips {
                ip_policy.add_external_dns_ip(ip.into()).unwrap();
            }

            let external_ip_policy = ip_policy.build();
            desc.set_external_ip_policy(external_ip_policy.clone());

            Ok(external_ip_policy)
        })
        .expect("added external DNS IPs to policy");

    // Manually place 3 external DNS zones: two on sled_1 and one on sled_2.
    let (sled_1, sled_2) = {
        let mut sleds = blueprint1.sleds();
        (
            sleds.next().expect("at least one sled"),
            sleds.next().expect("at least two sleds"),
        )
    };
    let blueprint1a = sim
        .blueprint_edit_latest("add external DNS zones", |blueprint_builder| {
            let mut external_networking_alloc =
                ExternalNetworkingAllocator::from_current_zones(
                    &blueprint_builder,
                    &external_ip_policy,
                )
                .expect("constructed allocator");
            blueprint_builder
                .sled_add_zone_external_dns(
                    sled_1,
                    BlueprintZoneImageSource::InstallDataset,
                    external_networking_alloc
                        .for_new_external_dns()
                        .expect("got IP for external DNS"),
                )
                .expect("added external DNS zone");
            blueprint_builder
                .sled_add_zone_external_dns(
                    sled_1,
                    BlueprintZoneImageSource::InstallDataset,
                    external_networking_alloc
                        .for_new_external_dns()
                        .expect("got IP for external DNS"),
                )
                .expect("added external DNS zone");
            blueprint_builder
                .sled_add_zone_external_dns(
                    sled_2,
                    BlueprintZoneImageSource::InstallDataset,
                    external_networking_alloc
                        .for_new_external_dns()
                        .expect("got IP for external DNS"),
                )
                .expect("added external DNS zone");

            Ok(())
        })
        .expect("built new blueprint");

    assert_eq!(
        blueprint1a
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, zone)| zone.zone_type.is_external_dns())
            .count(),
        3,
        "can't find external DNS zones in new blueprint"
    );

    // Plan with external DNS.
    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (added external DNS zones):\n{}", diff.display());

    // This blueprint should have three external DNS zones.
    assert_eq!(
        blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, zone)| zone.zone_type.is_external_dns())
            .count(),
        3,
        "can't find external DNS zones in planned blueprint"
    );

    // Expunge the first sled and re-plan. That gets us two expunged
    // external DNS zones; two external DNS zones should then be added to
    // the remaining sleds.
    sim.sled_expunge("expunge sled with 2 external DNS zones", sled_1)
        .expect("expunged sled");
    let blueprint3 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint3.diff_since_blueprint(&blueprint2);
    println!("2 -> 3 (expunged sled):\n{}", diff.display());
    assert_eq!(
        blueprint3.sleds[&sled_1]
            .zones
            .iter()
            .filter(|zone| {
                zone.disposition
                    == BlueprintZoneDisposition::Expunged {
                        as_of_generation: blueprint3.sleds[&sled_1]
                            .sled_agent_generation,
                        ready_for_cleanup: true,
                    }
                    && zone.zone_type.is_external_dns()
            })
            .count(),
        2
    );

    // The IP addresses of the new external DNS zones should be the
    // same as the original set that we set up.
    let mut ips = blueprint3
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_id, zone)| {
            zone.zone_type
                .is_external_dns()
                .then(|| zone.zone_type.external_networking().unwrap().0.ip())
        })
        .collect::<Vec<IpAddr>>();
    ips.sort();
    assert_eq!(
        ips, external_dns_ips,
        "wrong addresses for new external DNS zones"
    );

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_crucible_allocation_skips_nonprovisionable_disks() {
    static TEST_NAME: &str =
        "planner_crucible_allocation_skips_nonprovisionable_disks";
    let logctx = test_setup_log(TEST_NAME);

    // Create an example system with a single sled, a single Nexus, and a single
    // internal DNS to avoid churn unrelated to this test.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| {
        builder.nsleds(1).nexus_count(1).internal_dns_count(1)
    })
    .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();
    let sled_id = blueprint1.sleds().next().expect("1 sled");

    // Make generated disk ids deterministic
    let mut disk_rng = TypedUuidRng::from_seed(TEST_NAME, "NewPhysicalDisks");
    let mut new_sled_disk = |policy| SledDisk {
        disk_identity: DiskIdentity {
            vendor: "test-vendor".to_string(),
            serial: "test-serial".to_string(),
            model: "test-model".to_string(),
        },
        disk_id: PhysicalDiskUuid::from(disk_rng.next()),
        policy,
        state: PhysicalDiskState::Active,
    };

    // Inject some new disks into the input.
    //
    // These counts are arbitrary, as long as they're non-zero
    // for the sake of the test.

    const NEW_IN_SERVICE_DISKS: usize = 2;
    const NEW_EXPUNGED_DISKS: usize = 1;

    let mut zpool_rng = TypedUuidRng::from_seed(TEST_NAME, "NewZpools");
    sim.change_description("add new disks", |desc| {
        let resources =
            desc.get_sled_mut(sled_id).expect("sled exists").resources_mut();
        for _ in 0..NEW_IN_SERVICE_DISKS {
            resources.zpools.insert(
                ZpoolUuid::from(zpool_rng.next()),
                new_sled_disk(PhysicalDiskPolicy::InService),
            );
        }
        for _ in 0..NEW_EXPUNGED_DISKS {
            resources.zpools.insert(
                ZpoolUuid::from(zpool_rng.next()),
                new_sled_disk(PhysicalDiskPolicy::Expunged),
            );
        }
        Ok(())
    })
    .expect("added new disks");

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (some new disks, one expunged):\n{}", summary.display());
    assert_eq!(summary.diff.sleds.modified().count(), 1);

    // We should be adding a Crucible zone for each new in-service disk.
    assert_eq!(summary.total_zones_added(), NEW_IN_SERVICE_DISKS);
    assert_eq!(summary.total_zones_removed(), 0);
    assert_eq!(summary.total_disks_added(), NEW_IN_SERVICE_DISKS);
    assert_eq!(summary.total_disks_removed(), 0);

    // Five new datasets created per disk:
    // - Zone Root
    // - Debug
    // - Local Storage
    // - 1 for the Crucible Agent
    // - Transient Crucible Zone Root
    assert_eq!(summary.total_datasets_added(), NEW_IN_SERVICE_DISKS * 5);
    assert_eq!(summary.total_datasets_removed(), 0);
    assert_eq!(summary.total_datasets_modified(), 0);

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_dataset_settings_modified_in_place() {
    static TEST_NAME: &str = "planner_dataset_settings_modified_in_place";
    let logctx = test_setup_log(TEST_NAME);

    // Create an example system with a single sled and a single Nexus.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| Ok(builder.nsleds(1).nexus_count(1)))
        .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();
    let sled_id = blueprint1.sleds().next().expect("1 sled");

    // Manually update the blueprint to report an abnormal "Debug dataset"
    let blueprint1a = sim
        .blueprint_edit_latest_low_level(
            "change dataset properties",
            |blueprint| {
                let sled_config = blueprint.sleds.get_mut(&sled_id).unwrap();
                let mut dataset_config = sled_config
                    .datasets
                    .iter_mut()
                    .find(|config| {
                        matches!(
                            config.kind,
                            omicron_common::disk::DatasetKind::Debug
                        )
                    })
                    .expect("No debug dataset found");

                // These values are out-of-sync with what the blueprint will
                // typically enforce.
                dataset_config.quota = None;
                dataset_config.reservation =
                    Some(ByteCount::from_gibibytes_u32(1));

                Ok(())
            },
        )
        .expect("changed properties");

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint2.diff_since_blueprint(&blueprint1a);
    println!("1 -> 2 (modify a dataset):\n{}", summary.display());
    assert_contents(
        "tests/output/planner_dataset_settings_modified_in_place_1_2.txt",
        &summary.display().to_string(),
    );

    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 1);

    assert_eq!(summary.total_zones_added(), 0);
    assert_eq!(summary.total_zones_removed(), 0);
    assert_eq!(summary.total_zones_modified(), 0);
    assert_eq!(summary.total_disks_added(), 0);
    assert_eq!(summary.total_disks_removed(), 0);
    assert_eq!(summary.total_disks_modified(), 0);
    assert_eq!(summary.total_datasets_added(), 0);
    assert_eq!(summary.total_datasets_removed(), 0);
    assert_eq!(summary.total_datasets_modified(), 1);

    logctx.cleanup_successful();
}

#[test]
fn test_disk_add_expunge_decommission() {
    static TEST_NAME: &str = "planner_disk_add_expunge_decommission";
    let logctx = test_setup_log(TEST_NAME);

    // Create an example system with two sleds. We're going to expunge one
    // of these sleds.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| {
        builder.nsleds(2).internal_dns_count(1)
    })
    .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // The initial blueprint configuration has generation 2
    let (sled_id, sled_config) = blueprint1.sleds.first_key_value().unwrap();
    assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(2));

    // All disks should have an `InService` disposition and `Active` state
    for disk in &sled_config.disks {
        assert_eq!(
            disk.disposition,
            BlueprintPhysicalDiskDisposition::InService
        );
    }

    // Let's expunge a disk. Its disposition should change to `Expunged`
    // but its state should remain active.
    let expunged_disk_id = sim
        .change_description("expunge one disk", |desc| {
            let expunged_disk = desc
                .get_sled_mut(*sled_id)
                .unwrap()
                .resources_mut()
                .zpools
                .iter_mut()
                // Skip over the first disk - this is the one which hosts
                // many of our zones, like Nexus, and is more complicated
                // to expunge.
                .nth(1)
                .unwrap()
                .1;
            expunged_disk.policy = PhysicalDiskPolicy::Expunged;
            Ok(expunged_disk.disk_id)
        })
        .expect("expunged disk");

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunge a disk):\n{}", diff.display());

    let sled_config = &blueprint2.sleds.first_key_value().unwrap().1;

    // The generation goes from 2 -> 3
    assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(3));
    // One disk should have it's disposition set to
    // `Expunged{ready_for_cleanup: false, ..}`.
    for disk in &sled_config.disks {
        if disk.id == expunged_disk_id {
            assert!(matches!(
                disk.disposition,
                BlueprintPhysicalDiskDisposition::Expunged {
                    ready_for_cleanup: false,
                    ..
                }
            ));
        } else {
            assert_eq!(
                disk.disposition,
                BlueprintPhysicalDiskDisposition::InService
            );
        }
        println!("{disk:?}");
    }

    // We haven't updated the inventory, so no changes should be made
    let blueprint2a = sim.run_planner().expect("planning succeeded");
    assert_blueprint_diff_is_empty(&blueprint2, &blueprint2a);

    // Let's update the inventory to reflect that the sled-agent
    // has learned about the expungement.
    sim_update_collection_from_blueprint(&mut sim, &blueprint2);

    let blueprint3 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint3.diff_since_blueprint(&blueprint2);
    println!("2 -> 3 (decommission a disk):\n{}", diff.display());

    let sled_config = &blueprint3.sleds.first_key_value().unwrap().1;

    // The config generation does not change, as decommissioning doesn't
    // bump the generation.
    //
    // The reason for this is because the generation is there primarily to
    // inform the sled-agent that it has work to do, but decommissioning
    // doesn't trigger any sled-agent changes.
    assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(3));
    // One disk should have its disposition set to
    // `Expunged{ready_for_cleanup: true, ..}`.
    for disk in &sled_config.disks {
        if disk.id == expunged_disk_id {
            assert_matches!(
                disk.disposition,
                BlueprintPhysicalDiskDisposition::Expunged {
                    ready_for_cleanup: true,
                    ..
                }
            );
        } else {
            assert_eq!(
                disk.disposition,
                BlueprintPhysicalDiskDisposition::InService
            );
        }
        println!("{disk:?}");
    }

    // Now let's expunge a sled via the planning input. All disks should get
    // expunged and decommissioned in the same planning round. We also have
    // to manually expunge all the disks via policy, which would happen in a
    // database transaction when an operator expunges a sled.
    //
    // We don't rely on the sled-agents learning about expungement to
    // decommission because by definition expunging a sled means it's
    // already gone.
    sim.sled_expunge("expunge full sled", *sled_id).expect("expunged sled");
    let blueprint4 = sim.run_planner().expect("planning succeeded");

    let diff = blueprint4.diff_since_blueprint(&blueprint3);
    println!(
        "3 -> 4 (expunge and decommission all disks):\n{}",
        diff.display()
    );

    let sled_config = &blueprint4.sleds.first_key_value().unwrap().1;

    // The config generation goes from 3 -> 4
    assert_eq!(sled_config.sled_agent_generation, Generation::from_u32(4));
    // We should still have 10 disks
    assert_eq!(sled_config.disks.len(), 10);
    // All disks should have their disposition set to
    // `Expunged{ready_for_cleanup: true, ..}`.
    for disk in &sled_config.disks {
        assert!(matches!(
            disk.disposition,
            BlueprintPhysicalDiskDisposition::Expunged {
                ready_for_cleanup: true,
                ..
            }
        ));
        println!("{disk:?}");
    }

    logctx.cleanup_successful();
}

#[test]
fn test_disk_expungement_removes_zones_durable_zpool() {
    static TEST_NAME: &str =
        "planner_disk_expungement_removes_zones_durable_zpool";
    let logctx = test_setup_log(TEST_NAME);

    // Create an example system with a single sled
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| {
        builder.nsleds(1).nexus_count(1).internal_dns_count(1)
    })
    .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();
    let sled_id = blueprint1.sleds().next().expect("1 sled");

    // The example system should be assigning crucible zones to each
    // in-service disk. When we expunge one of these disks, the planner
    // should remove the associated zone.
    //
    // Find a disk which is only used by a single zone, if one exists.
    //
    // If we don't do this, we might select a physical disk supporting
    // multiple zones of distinct types.
    let mut zpool_by_zone_usage = HashMap::new();
    for sled in blueprint1.sleds.values() {
        for zone in &sled.zones {
            let pool = &zone.filesystem_pool;
            zpool_by_zone_usage
                .entry(pool.id())
                .and_modify(|count| *count += 1)
                .or_insert_with(|| 1);
        }
    }
    sim.change_description("expunge disk with 1 zone", |desc| {
        let (_, disk) = desc
            .get_sled_mut(sled_id)
            .unwrap()
            .resources_mut()
            .zpools
            .iter_mut()
            .find(|(zpool_id, _disk)| {
                *zpool_by_zone_usage.get(*zpool_id).unwrap() == 1
            })
            .expect("Couldn't find zpool only used by a single zone");
        disk.policy = PhysicalDiskPolicy::Expunged;
        Ok(())
    })
    .expect("expunged disk");

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunge a disk):\n{}", summary.display());
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 1);

    // We should be removing a single zone, associated with the Crucible
    // using that device.
    assert_eq!(summary.total_zones_added(), 0);
    assert_eq!(summary.total_zones_removed(), 0);
    assert_eq!(summary.total_zones_modified(), 1);
    assert_eq!(summary.total_disks_added(), 0);
    assert_eq!(summary.total_disks_removed(), 0);
    assert_eq!(summary.total_datasets_added(), 0);
    // NOTE: Expunging a disk doesn't immediately delete datasets; see the
    // "decommissioned_disk_cleaner" background task for more context.
    assert_eq!(summary.total_datasets_removed(), 0);

    // The disposition has changed from `InService` to `Expunged` for the 5
    // datasets (debug, zone root, local storage, crucible zone root, and
    // crucible agent) on this sled.
    assert_eq!(summary.total_datasets_modified(), 5);
    // We don't know the expected name, other than the fact it's a crucible zone
    let test_transient_zone_kind = DatasetKind::TransientZone {
        name: "some-crucible-zone-name".to_string(),
    };
    let mut expected_kinds = BTreeSet::from_iter([
        DatasetKind::Crucible,
        DatasetKind::Debug,
        DatasetKind::TransientZoneRoot,
        DatasetKind::LocalStorage,
        test_transient_zone_kind.clone(),
    ]);
    let mut modified_sled_configs = Vec::new();
    for modified_sled in summary.diff.sleds.modified_values_diff() {
        for modified in modified_sled.datasets.modified_diff() {
            assert_eq!(
                *modified.disposition.before,
                BlueprintDatasetDisposition::InService
            );
            assert_eq!(
                *modified.disposition.after,
                BlueprintDatasetDisposition::Expunged
            );
            if let DatasetKind::TransientZone { name } = &modified.kind.before {
                assert!(name.starts_with("oxz_crucible"));
                assert!(expected_kinds.remove(&test_transient_zone_kind));
            } else {
                assert!(expected_kinds.remove(&modified.kind.before));
            }
        }
        modified_sled_configs.push(modified_sled);
    }
    assert!(expected_kinds.is_empty());

    assert_eq!(modified_sled_configs.len(), 1);
    let modified_sled_config = modified_sled_configs.pop().unwrap();
    assert!(modified_sled_config.zones.added.is_empty());
    assert!(modified_sled_config.zones.removed.is_empty());
    let mut modified_zones =
        modified_sled_config.zones.modified_diff().collect::<Vec<_>>();
    assert_eq!(modified_zones.len(), 1);
    let modified_zone = modified_zones.pop().unwrap();
    assert!(
        modified_zone.zone_type.before.is_crucible(),
        "Expected the modified zone to be a Crucible zone, \
             but it was: {:?}",
        modified_zone.zone_type.before.kind()
    );
    assert_eq!(
        *modified_zone.disposition.after,
        BlueprintZoneDisposition::Expunged {
            as_of_generation: modified_sled_config
                .sled_agent_generation
                .before
                .next(),
            ready_for_cleanup: false,
        },
        "Should have expunged this zone"
    );

    // Let's update the inventory to reflect that the sled-agent
    // has learned about the expungement. Re-planning should flip the
    // `ready_for_cleanup` bit to true for our modified zone.
    sim_update_collection_from_blueprint(&mut sim, &blueprint2);
    let blueprint3 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint3.diff_since_blueprint(&blueprint2);
    assert_eq!(summary.total_zones_added(), 0);
    assert_eq!(summary.total_zones_removed(), 0);
    assert_eq!(summary.total_zones_modified(), 1);
    let bp3_modified_sled =
        summary.diff.sleds.modified_values_diff().next().unwrap();
    assert!(bp3_modified_sled.zones.added.is_empty());
    assert!(bp3_modified_sled.zones.removed.is_empty());
    let mut bp3_modified_zones =
        bp3_modified_sled.zones.modified_diff().collect::<Vec<_>>();
    assert_eq!(bp3_modified_zones.len(), 1);
    let bp3_modified_zone = bp3_modified_zones.pop().unwrap();
    assert!(
        bp3_modified_zone.zone_type.before.is_crucible(),
        "Expected the modified zone to be a Crucible zone, \
             but it was: {:?}",
        bp3_modified_zone.zone_type.before.kind()
    );
    assert_eq!(
        *bp3_modified_zone.disposition.after,
        BlueprintZoneDisposition::Expunged {
            as_of_generation: modified_sled_config
                .sled_agent_generation
                .before
                .next(),
            ready_for_cleanup: true,
        },
        "Should have marked this zone ready for cleanup"
    );

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_disk_expungement_removes_zones_transient_filesystem() {
    static TEST_NAME: &str =
        "planner_disk_expungement_removes_zones_transient_filesystem";
    let logctx = test_setup_log(TEST_NAME);

    // Create an example system with a single sled
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| Ok(builder.nsleds(1).nexus_count(2)))
        .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();
    let sled_id = blueprint1.sleds().next().expect("1 sled");

    // Find whatever pool NTP was using
    let pool_to_expunge = blueprint1
        .sleds
        .iter()
        .find_map(|(_, sled_config)| {
            for zone_config in &sled_config.zones {
                if zone_config.zone_type.is_ntp() {
                    return Some(zone_config.filesystem_pool);
                }
            }
            None
        })
        .expect("No NTP zone pool?");

    // This is mostly for test stability across "example system" changes:
    // Find all the zones using this same zpool.
    let mut zones_on_pool = BTreeSet::new();
    let mut zone_kinds_on_pool = BTreeMap::<_, usize>::new();
    for (_, zone_config) in
        blueprint1.all_omicron_zones(BlueprintZoneDisposition::is_in_service)
    {
        if pool_to_expunge == zone_config.filesystem_pool {
            zones_on_pool.insert(zone_config.id);
            *zone_kinds_on_pool
                .entry(zone_config.zone_type.kind())
                .or_default() += 1;
        }
    }
    assert!(
        !zones_on_pool.is_empty(),
        "We should be expunging at least one zone using this zpool"
    );

    // For that pool, find the physical disk behind it, and mark it
    // expunged.
    sim.change_description("expunge disk hosting NTP", |desc| {
        desc.get_sled_mut(sled_id)
            .unwrap()
            .resources_mut()
            .zpools
            .get_mut(&pool_to_expunge.id())
            .unwrap()
            .policy = PhysicalDiskPolicy::Expunged;
        Ok(())
    })
    .expect("expunged disk");

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunge a disk):\n{}", summary.display());
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 1);

    // No zones should have been removed from the blueprint entirely.
    assert_eq!(summary.total_zones_removed(), 0);

    // We should have expunged all the zones on this pool.
    let mut zones_expunged = BTreeSet::new();
    for sled in summary.diff.sleds.modified_values_diff() {
        let expected_generation =
            sled.sled_agent_generation.diff_pair().before.next();
        for z in sled.zones.modified() {
            assert_eq!(
                z.after().disposition,
                BlueprintZoneDisposition::Expunged {
                    as_of_generation: expected_generation,
                    ready_for_cleanup: false,
                },
                "Should have expunged this zone"
            );
            zones_expunged.insert(z.after().id);
        }
    }
    assert_eq!(zones_on_pool, zones_expunged);

    // We also should have added back a new zone for each kind that was
    // removed, except the Crucible zone (which is specific to the disk) and
    // the internal DNS zone (which can't be replaced until the original
    // zone is ready for cleanup per inventory). Remove these from our
    // original counts, then check against the added zones count.
    assert_eq!(zone_kinds_on_pool.remove(&ZoneKind::Crucible), Some(1));
    assert_eq!(zone_kinds_on_pool.remove(&ZoneKind::InternalDns), Some(1));
    let mut zone_kinds_added = BTreeMap::new();
    for sled in summary.diff.sleds.modified_values_diff() {
        for z in &sled.zones.added {
            *zone_kinds_added.entry(z.zone_type.kind()).or_default() += 1_usize;
        }
    }
    assert_eq!(zone_kinds_on_pool, zone_kinds_added);

    // Test a no-op planning iteration. We use `InputUnchanged` here because if
    // we deploy the configs, the planner _will_ make changes (marking the
    // expunged zones as ready for cleanup).
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    logctx.cleanup_successful();
}

/// Check that the planner will skip non-provisionable sleds when allocating
/// extra Nexus zones
#[test]
fn test_nexus_allocation_skips_nonprovisionable_sleds() {
    static TEST_NAME: &str =
        "planner_nexus_allocation_skips_nonprovisionable_sleds";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    //
    // Request two extra sleds here so we test non-provisionable, expunged,
    // and decommissioned sleds. (When we add more kinds of
    // non-provisionable states in the future, we'll have to add more
    // sleds.)
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| Ok(builder.nsleds(5).nexus_count(5)))
        .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // This blueprint should only have 5 Nexus zones: one on each sled.
    assert_eq!(blueprint1.sleds.len(), 5);
    for sled_config in blueprint1.sleds.values() {
        assert_eq!(
            sled_config.zones.iter().filter(|z| z.zone_type.is_nexus()).count(),
            1
        );
    }

    // Arbitrarily choose some of the sleds and mark them non-provisionable
    // in various ways.
    let nonprovisionable_sled_id;
    let expunged_sled_id;
    let decommissioned_sled_id;
    {
        let mut sleds_iter = blueprint1.sleds();
        nonprovisionable_sled_id = sleds_iter.next().unwrap();
        expunged_sled_id = sleds_iter.next().unwrap();
        decommissioned_sled_id = sleds_iter.next().unwrap();
    }

    // We need to mark the decommissioned sled as such in the blueprint; it's
    // not possible (outside of tests!) to have the database record a sled in
    // the `Decommissioned` state without a blueprint having caused that change.
    let blueprint1a = sim
        .blueprint_edit_latest("decommission sled", |builder| {
            builder.expunge_sled(decommissioned_sled_id).unwrap();
            builder.set_sled_decommissioned(decommissioned_sled_id).unwrap();
            Ok(())
        })
        .expect("decommissioned sled");

    sim.change_description("make sleds non-provisionable", |desc| {
        // Change the sled policy for the nonprovisionable sled.
        desc.sled_set_policy(
            nonprovisionable_sled_id,
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
        )
        .expect("set policy");

        // Expunge the expunged and decommissioned sleds.
        desc.sled_expunge(expunged_sled_id)
            .expect("expunged sled")
            .sled_expunge(decommissioned_sled_id)
            .expect("expunged sled");

        // Mark the updated state on the decommissioned sled.
        desc.sled_set_state(decommissioned_sled_id, SledState::Decommissioned)
            .expect("set state");

        // Change to a high number of target Nexus zones. The
        // number (9) is chosen such that:
        //
        // * we start with 5 sleds with 1 Nexus each
        // * we take two sleds out of service (one expunged, one
        //   decommissioned), so we're down to 3 in-service Nexuses: we need to
        //   add 6 to get to the new policy target of 9
        // * of the remaining 3 sleds, only 2 are eligible for provisioning
        // * each of those 2 sleds should get exactly 3 new Nexuses
        desc.set_target_nexus_zone_count(9);

        // Disable addition of zone types we're not checking for below.
        desc.set_target_internal_dns_zone_count(0);
        desc.set_target_crucible_pantry_zone_count(0);

        Ok(())
    })
    .expect("changed state");

    println!("1 -> 2: marked non-provisionable {nonprovisionable_sled_id}");
    println!("1 -> 2: expunged {expunged_sled_id}");
    println!("1 -> 2: decommissioned {decommissioned_sled_id}");

    let mut blueprint2 = sim.run_planner().expect("planning succeeded");
    // Define a time_created for consistent output across runs.
    {
        let blueprint2 = Arc::make_mut(&mut blueprint2);
        blueprint2.time_created = DateTime::<Utc>::UNIX_EPOCH;
    }

    assert_contents(
        "tests/output/planner_nonprovisionable_bp2.txt",
        &blueprint2.display().to_string(),
    );

    let summary = blueprint2.diff_since_blueprint(&blueprint1a);
    println!(
        "1 -> 2 (added additional Nexus zones, take 2 sleds out of service):"
    );
    println!("{}", summary.display());
    assert_contents(
        "tests/output/planner_nonprovisionable_1_2.txt",
        &summary.display().to_string(),
    );

    // The expunged and decommissioned sleds should have had all zones be
    // marked as expunged. (Not removed! Just marked as expunged.)
    //
    // Note that at this point we're neither removing zones from the
    // blueprint nor marking sleds as decommissioned -- we still need to do
    // cleanup, and we aren't performing garbage collection on zones or
    // sleds at the moment.

    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 3);
    assert_eq!(summary.diff.sleds.unchanged().count(), 2);

    assert_all_zones_expunged(&summary, expunged_sled_id, "expunged sled");

    // Only 2 of the 3 remaining sleds (not the non-provisionable sled)
    // should get additional Nexus zones. We expect a total of 6 new Nexus
    // zones, which should be split evenly between the two sleds, while the
    // non-provisionable sled should be unchanged.
    let remaining_modified_sleds = summary
        .diff
        .sleds
        .modified()
        .filter_map(|(&sled_id, sled)| {
            (sled_id != expunged_sled_id).then_some((sled_id, sled))
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(remaining_modified_sleds.len(), 2);
    let mut total_new_nexus_zones = 0;
    for (sled_id, modified_sled) in remaining_modified_sleds {
        assert!(sled_id != nonprovisionable_sled_id);
        assert!(sled_id != expunged_sled_id);
        assert!(sled_id != decommissioned_sled_id);
        let zones_on_modified_sled = &modified_sled.diff_pair().zones;
        assert!(zones_on_modified_sled.removed.is_empty());
        let zones = &zones_on_modified_sled.added;
        for zone in zones {
            if ZoneKind::Nexus != zone.kind() {
                panic!("unexpectedly added a non-Nexus zone: {zone:?}");
            };
        }
        if zones.len() == 3 {
            total_new_nexus_zones += 3;
        } else {
            panic!(
                "unexpected number of zones added to {sled_id}: {}",
                zones.len()
            );
        }
    }
    assert_eq!(total_new_nexus_zones, 6);

    // ---

    // Also poke at some of the config by hand; we'll use this to test out
    // diff output. This isn't a real blueprint, just one that we're
    // creating to test diff output.
    //
    // Some of the things we're testing here:
    //
    // * modifying zones
    // * removing zones
    // * removing sleds
    // * for modified sleds' zone config generation, both a bump and the
    //   generation staying the same (the latter should produce a warning)
    let mut blueprint2a = Arc::clone(&blueprint2);
    let blueprint2a = Arc::make_mut(&mut blueprint2a);

    enum NextCrucibleMutate {
        Modify,
        Remove,
        Done,
    }
    let mut next = NextCrucibleMutate::Modify;

    // Leave the non-provisionable sled's generation alone.
    let zones = &mut blueprint2a
        .sleds
        .get_mut(&nonprovisionable_sled_id)
        .unwrap()
        .zones;

    zones.retain(|mut zone| {
        if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
            internal_address,
            ..
        }) = &mut zone.zone_type
        {
            // Change the internal address.
            let mut segments = internal_address.ip().segments();
            segments[0] = segments[0].wrapping_add(1);
            internal_address.set_ip(segments.into());
            true
        } else if let BlueprintZoneType::Crucible(_) = zone.zone_type {
            match next {
                NextCrucibleMutate::Modify => {
                    zone.disposition = BlueprintZoneDisposition::Expunged {
                        as_of_generation: Generation::new(),
                        ready_for_cleanup: false,
                    };
                    next = NextCrucibleMutate::Remove;
                    true
                }
                NextCrucibleMutate::Remove => {
                    next = NextCrucibleMutate::Done;
                    false
                }
                NextCrucibleMutate::Done => true,
            }
        } else if let BlueprintZoneType::InternalNtp(
            blueprint_zone_type::InternalNtp { address },
        ) = &mut zone.zone_type
        {
            // Change the underlay IP.
            let mut segments = address.ip().segments();
            segments[0] += 1;
            address.set_ip(segments.into());
            true
        } else {
            true
        }
    });

    let expunged_sled =
        &mut blueprint2a.sleds.get_mut(&expunged_sled_id).unwrap();
    expunged_sled.zones.clear();
    expunged_sled.sled_agent_generation =
        expunged_sled.sled_agent_generation.next();

    blueprint2a.sleds.remove(&decommissioned_sled_id);
    blueprint2a.external_dns_version = blueprint2a.external_dns_version.next();

    let diff = blueprint2a.diff_since_blueprint(&blueprint2);
    println!("2 -> 2a (manually modified zones):\n{}", diff.display());
    assert_contents(
        "tests/output/planner_nonprovisionable_2_2a.txt",
        &diff.display().to_string(),
    );

    // ---

    logctx.cleanup_successful();
}

#[track_caller]
fn assert_all_zones_expunged<'a>(
    summary: &BlueprintDiffSummary<'a>,
    expunged_sled_id: SledUuid,
    desc: &str,
) {
    assert!(
        summary.added_zones(&expunged_sled_id).is_none(),
        "for {desc}, no zones should have been added to blueprint"
    );

    // A zone disposition going to expunged *does not* mean that the
    // zone is actually removed, i.e. `zones_removed` is still 0. Any
    // zone removal will be part of some future garbage collection
    // process that isn't currently defined.

    assert!(
        summary.removed_zones(&expunged_sled_id).is_none(),
        "for {desc}, no zones should have been removed from blueprint"
    );

    // Run through all the common zones and ensure that all of them
    // have been marked expunged.
    let modified_sled = &summary
        .diff
        .sleds
        .get_modified(&expunged_sled_id)
        .unwrap()
        .diff_pair();
    assert_eq!(
        modified_sled.sled_agent_generation.before.next(),
        *modified_sled.sled_agent_generation.after,
        "for {desc}, generation should have been bumped"
    );

    for modified_zone in modified_sled.zones.modified_diff() {
        assert_eq!(
            *modified_zone.disposition.after,
            BlueprintZoneDisposition::Expunged {
                as_of_generation: *modified_sled.sled_agent_generation.after,
                ready_for_cleanup: true,
            },
            "for {desc}, zone {} should have been marked expunged",
            modified_zone.id.after
        );
    }
}

#[test]
fn planner_decommissions_sleds() {
    static TEST_NAME: &str = "planner_decommissions_sleds";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // Expunge one of the sleds.
    let expunged_sled_id = blueprint1.sleds().next().expect("at least 1 sled");
    sim.change_description("expunge sled", |desc| {
        desc.sled_expunge(expunged_sled_id)?;
        Ok(())
    })
    .unwrap();

    let mut blueprint2 = sim.run_planner().expect("planning succeeded");

    // Define a time_created for consistent output across runs.
    {
        let blueprint2 = Arc::make_mut(&mut blueprint2);
        blueprint2.time_created = DateTime::<Utc>::UNIX_EPOCH;
    }

    assert_contents(
        "tests/output/planner_decommissions_sleds_bp2.txt",
        &blueprint2.display().to_string(),
    );
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunged {expunged_sled_id}):\n{}", diff.display());
    assert_contents(
        "tests/output/planner_decommissions_sleds_1_2.txt",
        &diff.display().to_string(),
    );

    // All the zones of the expunged sled should be expunged, and the sled
    // itself should be decommissioned.
    assert!(blueprint2.sleds[&expunged_sled_id].are_all_zones_expunged());
    assert_eq!(
        blueprint2.sleds[&expunged_sled_id].state,
        SledState::Decommissioned
    );

    // Set the state of the expunged sled to decommissioned, and run the
    // planner again.
    sim.change_description("decommission sled", |desc| {
        desc.sled_set_state(expunged_sled_id, SledState::Decommissioned)?;
        Ok(())
    })
    .unwrap();

    // There should be no changes to the blueprint; we don't yet garbage
    // collect zones, so we should still have the sled's expunged zones
    // (even though the sled itself is no longer present in the list of
    // commissioned sleds).
    let blueprint3 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint3.diff_since_blueprint(&blueprint2);
    println!(
        "2 -> 3 (decommissioned {expunged_sled_id}):\n{}",
        summary.display()
    );
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 0);
    assert_eq!(summary.diff.sleds.unchanged().count(), blueprint1.sleds.len());

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    // Now remove the decommissioned sled from the input entirely. (This
    // should not happen in practice at the moment -- entries in the sled
    // table are kept forever -- but we need to test it.)
    //
    // Eventually, once zone and sled garbage collection is implemented,
    // we'll expect that the diff's `sleds.removed` will become
    // non-empty. At some point we may also want to remove entries from the
    // sled table, but that's a future concern that would come after
    // blueprint cleanup is implemented.
    sim.change_description("remove decommissioned sled", |desc| {
        desc.sled_remove(expunged_sled_id)?;
        Ok(())
    })
    .unwrap();

    let blueprint4 = sim.run_planner().expect("planning succeeded");
    let summary = blueprint4.diff_since_blueprint(&blueprint3);
    println!(
        "3 -> 4 (removed from input {expunged_sled_id}):\n{}",
        summary.display()
    );
    assert_eq!(summary.diff.sleds.added.len(), 0);
    assert_eq!(summary.diff.sleds.removed.len(), 0);
    assert_eq!(summary.diff.sleds.modified().count(), 0);
    assert_eq!(summary.diff.sleds.unchanged().count(), blueprint1.sleds.len(),);

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_ensure_preserve_downgrade_option() {
    static TEST_NAME: &str = "planner_ensure_preserve_downgrade_option";
    let logctx = test_setup_log(TEST_NAME);

    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let bp1 = sim.assert_latest_blueprint_is_blippy_clean();
    assert!(bp1.cockroachdb_fingerprint.is_empty());
    assert_eq!(
        bp1.cockroachdb_setting_preserve_downgrade,
        CockroachDbPreserveDowngrade::DoNotModify
    );

    // If `preserve_downgrade_option` is unset and the current cluster
    // version matches `POLICY`, we ensure it is set.
    sim.change_description("change crdb settings", |desc| {
        desc.set_cockroachdb_settings(CockroachDbSettings {
            state_fingerprint: "bp2".to_owned(),
            version: CockroachDbClusterVersion::POLICY.to_string(),
            preserve_downgrade: String::new(),
        });
        Ok(())
    })
    .unwrap();

    let bp2 = sim.run_planner().expect("planning succeeded");
    assert_eq!(bp2.cockroachdb_fingerprint, "bp2");
    assert_eq!(
        bp2.cockroachdb_setting_preserve_downgrade,
        CockroachDbClusterVersion::POLICY.into()
    );

    // If `preserve_downgrade_option` is unset and the current cluster
    // version is known to us and _newer_ than `POLICY`, we still ensure
    // it is set. (During a "tock" release, `POLICY == NEWLY_INITIALIZED`
    // and this won't be materially different than the above test, but it
    // shouldn't need to change when moving to a "tick" release.)
    sim.change_description("change crdb settings", |desc| {
        desc.set_cockroachdb_settings(CockroachDbSettings {
            state_fingerprint: "bp3".to_owned(),
            version: CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
            preserve_downgrade: String::new(),
        });
        Ok(())
    })
    .unwrap();

    let bp3 = sim.run_planner().expect("planning succeeded");
    assert_eq!(bp3.cockroachdb_fingerprint, "bp3");
    assert_eq!(
        bp3.cockroachdb_setting_preserve_downgrade,
        CockroachDbClusterVersion::NEWLY_INITIALIZED.into()
    );

    // When we run the planner again after setting the setting, the inputs
    // will change; we should still be ensuring the setting.
    sim.change_description("change crdb settings", |desc| {
        desc.set_cockroachdb_settings(CockroachDbSettings {
            state_fingerprint: "bp4".to_owned(),
            version: CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
            preserve_downgrade: CockroachDbClusterVersion::NEWLY_INITIALIZED
                .to_string(),
        });
        Ok(())
    })
    .unwrap();

    let bp4 = sim.run_planner().expect("planning succeeded");
    assert_eq!(bp4.cockroachdb_fingerprint, "bp4");
    assert_eq!(
        bp4.cockroachdb_setting_preserve_downgrade,
        CockroachDbClusterVersion::NEWLY_INITIALIZED.into()
    );

    // When `version` isn't recognized, do nothing regardless of the value
    // of `preserve_downgrade`.
    for preserve_downgrade in [
        String::new(),
        CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
        "definitely not a real cluster version".to_owned(),
    ] {
        sim.change_description("change crdb settings", |desc| {
            desc.set_cockroachdb_settings(CockroachDbSettings {
                state_fingerprint: "bp5".to_owned(),
                version: "definitely not a real cluster version".to_owned(),
                preserve_downgrade: preserve_downgrade.clone(),
            });
            Ok(())
        })
        .unwrap();

        let bp5 = sim.run_planner().expect("planning succeeded");
        assert_eq!(bp5.cockroachdb_fingerprint, "bp5");
        assert_eq!(
            bp5.cockroachdb_setting_preserve_downgrade,
            CockroachDbPreserveDowngrade::DoNotModify
        );
    }

    logctx.cleanup_successful();
}

#[test]
fn test_crucible_pantry() {
    static TEST_NAME: &str = "test_crucible_pantry";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // We should start with CRUCIBLE_PANTRY_REDUNDANCY pantries spread out
    // to at most 1 per sled. Find one of the sleds running one.
    let pantry_sleds = blueprint1
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(sled_id, zone)| {
            zone.zone_type.is_crucible_pantry().then_some(sled_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        pantry_sleds.len(),
        CRUCIBLE_PANTRY_REDUNDANCY,
        "expected {CRUCIBLE_PANTRY_REDUNDANCY} pantries, but found {}",
        pantry_sleds.len(),
    );

    // Expunge one of the pantry-hosting sleds and re-plan. The planner should immediately replace the zone with one on another
    // (non-expunged) sled.
    let expunged_sled_id = pantry_sleds[0];
    sim.sled_expunge("expunge first pantry sled", expunged_sled_id).unwrap();
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunged sled):\n{}", diff.display());
    assert_eq!(
        blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(sled_id, zone)| *sled_id != expunged_sled_id
                && zone.zone_type.is_crucible_pantry())
            .count(),
        CRUCIBLE_PANTRY_REDUNDANCY,
        "can't find replacement pantry zone"
    );

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Check that the planner can replace a single-node ClickHouse zone.
/// This is completely distinct from (and much simpler than) the replicated
/// (multi-node) case.
#[test]
fn test_single_node_clickhouse() {
    static TEST_NAME: &str = "test_single_node_clickhouse";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system as a starting point.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // We should start with one ClickHouse zone. Find out which sled it's on.
    let clickhouse_sleds = blueprint1
        .all_omicron_zones(BlueprintZoneDisposition::any)
        .filter_map(|(sled, zone)| {
            zone.zone_type.is_clickhouse().then(|| Some(sled))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        clickhouse_sleds.len(),
        1,
        "can't find ClickHouse zone in initial blueprint"
    );
    let clickhouse_sled = clickhouse_sleds[0].expect("missing sled id");

    // Expunge the sled hosting ClickHouse and re-plan. The planner should
    // immediately replace the zone with one on another (non-expunged) sled.
    sim.sled_expunge("expunge clickhouse sled", clickhouse_sled).unwrap();
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    println!("1 -> 2 (expunged sled):\n{}", diff.display());
    assert_eq!(
        blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(sled, zone)| *sled != clickhouse_sled
                && zone.zone_type.is_clickhouse())
            .count(),
        1,
        "can't find replacement ClickHouse zone"
    );

    // Test a no-op planning iteration.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Deploy all keeper nodes server nodes at once for a new cluster.
/// Then add keeper nodes 1 at a time.
#[test]
fn test_plan_deploy_all_clickhouse_cluster_nodes() {
    static TEST_NAME: &str = "planner_deploy_all_keeper_nodes";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // We shouldn't have a clickhouse cluster config, as we don't have a
    // clickhouse policy set yet
    assert!(blueprint1.clickhouse_cluster_config.is_none());
    let target_keepers = 3;
    let target_servers = 2;

    // Enable clickhouse clusters via policy
    sim.set_clickhouse_policy(
        "enable clustered",
        clickhouse_policy(ClickhouseMode::Both {
            target_servers,
            target_keepers,
        }),
    );

    let blueprint2 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    assert_contents(
        "tests/output/planner_deploy_all_keeper_nodes_1_2.txt",
        &diff.display().to_string(),
    );

    // We should see zones for 3 clickhouse keepers, and 2 servers created
    let active_zones: Vec<_> = blueprint2
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .map(|(_, z)| z.clone())
        .collect();

    let keeper_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_keeper())
        .map(|z| z.id)
        .collect();
    let server_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_server())
        .map(|z| z.id)
        .collect();

    assert_eq!(keeper_zone_ids.len(), target_keepers as usize);
    assert_eq!(server_zone_ids.len(), target_servers as usize);

    // We should be attempting to allocate all servers and keepers since
    // this the initial configuration
    {
        let clickhouse_cluster_config =
            blueprint2.clickhouse_cluster_config.as_ref().unwrap();
        assert_eq!(clickhouse_cluster_config.generation, 2.into());
        assert_eq!(
            clickhouse_cluster_config.max_used_keeper_id,
            (u64::from(target_keepers)).into()
        );
        assert_eq!(
            clickhouse_cluster_config.max_used_server_id,
            (u64::from(target_servers)).into()
        );
        assert_eq!(
            clickhouse_cluster_config.keepers.len(),
            target_keepers as usize
        );
        assert_eq!(
            clickhouse_cluster_config.servers.len(),
            target_servers as usize
        );

        // Ensure that the added keepers are in server zones
        for zone_id in clickhouse_cluster_config.keepers.keys() {
            assert!(keeper_zone_ids.contains(zone_id));
        }

        // Ensure that the added servers are in server zones
        for zone_id in clickhouse_cluster_config.servers.keys() {
            assert!(server_zone_ids.contains(zone_id));
        }
    }

    // Planning again without changing inventory should result in the same
    // state
    let blueprint3 = sim.run_planner().expect("planning succeeded");

    assert_eq!(
        blueprint2.clickhouse_cluster_config,
        blueprint3.clickhouse_cluster_config
    );

    // Updating the inventory to reflect the keepers
    // should result in the same state, except for the
    // `highest_seen_keeper_leader_committed_log_index`
    let (_, keeper_id) = blueprint3
        .clickhouse_cluster_config
        .as_ref()
        .unwrap()
        .keepers
        .first_key_value()
        .unwrap();
    let membership = ClickhouseKeeperClusterMembership {
        queried_keeper: *keeper_id,
        leader_committed_log_index: 1,
        raft_config: blueprint3
            .clickhouse_cluster_config
            .as_ref()
            .unwrap()
            .keepers
            .values()
            .cloned()
            .collect(),
    };
    sim.generate_inventory_customized("add membership", |builder| {
        builder.found_clickhouse_keeper_cluster_membership(membership);
        Ok(())
    })
    .unwrap();

    let blueprint4 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint4.diff_since_blueprint(&blueprint3);
    assert_contents(
        "tests/output/planner_deploy_all_keeper_nodes_3_4.txt",
        &diff.display().to_string(),
    );

    let bp3_config = blueprint3.clickhouse_cluster_config.as_ref().unwrap();
    let bp4_config = blueprint4.clickhouse_cluster_config.as_ref().unwrap();
    assert_eq!(bp4_config.generation, bp3_config.generation);
    assert_eq!(bp4_config.max_used_keeper_id, bp3_config.max_used_keeper_id);
    assert_eq!(bp4_config.max_used_server_id, bp3_config.max_used_server_id);
    assert_eq!(bp4_config.keepers, bp3_config.keepers);
    assert_eq!(bp4_config.servers, bp3_config.servers);
    // The blueprint shouldn't update solely because the log index changed
    assert_eq!(bp4_config.highest_seen_keeper_leader_committed_log_index, 0);

    // Let's bump the clickhouse target to 5 via policy so that we can add
    // more nodes one at a time. Initial configuration deploys all nodes,
    // but reconfigurations may only add or remove one node at a time.
    // Enable clickhouse clusters via policy
    let target_keepers = 5;
    sim.set_clickhouse_policy(
        "enable clickhouse cluster",
        clickhouse_policy(ClickhouseMode::Both {
            target_servers,
            target_keepers,
        }),
    );
    let blueprint5 = sim.run_planner().expect("planning succeeded");

    let diff = blueprint5.diff_since_blueprint(&blueprint4);
    assert_contents(
        "tests/output/planner_deploy_all_keeper_nodes_4_5.txt",
        &diff.display().to_string(),
    );

    let active_zones: Vec<_> = blueprint5
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .map(|(_, z)| z.clone())
        .collect();

    let new_keeper_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_keeper())
        .map(|z| z.id)
        .collect();

    // We should have allocated 2 new keeper zones
    assert_eq!(new_keeper_zone_ids.len(), target_keepers as usize);
    assert!(keeper_zone_ids.is_subset(&new_keeper_zone_ids));

    // We should be trying to provision one new keeper for a keeper zone
    let bp4_config = blueprint4.clickhouse_cluster_config.as_ref().unwrap();
    let bp5_config = blueprint5.clickhouse_cluster_config.as_ref().unwrap();
    assert_eq!(bp5_config.generation, bp4_config.generation.next());
    assert_eq!(
        bp5_config.max_used_keeper_id,
        bp4_config.max_used_keeper_id + 1.into()
    );
    assert_eq!(
        bp5_config.keepers.len(),
        bp5_config.max_used_keeper_id.0 as usize
    );

    // Planning again without updating inventory results in the same
    // `ClickhouseClusterConfig`
    let blueprint6 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint6.diff_since_blueprint(&blueprint5);
    assert_contents(
        "tests/output/planner_deploy_all_keeper_nodes_5_6.txt",
        &diff.display().to_string(),
    );

    let bp6_config = blueprint6.clickhouse_cluster_config.as_ref().unwrap();
    assert_eq!(bp5_config, bp6_config);

    // Updating the inventory to include the 4th node should add another
    // keeper node
    let membership = ClickhouseKeeperClusterMembership {
        queried_keeper: *keeper_id,
        leader_committed_log_index: 2,
        raft_config: blueprint6
            .clickhouse_cluster_config
            .as_ref()
            .unwrap()
            .keepers
            .values()
            .cloned()
            .collect(),
    };
    sim.generate_inventory_customized("add membership", |builder| {
        builder.found_clickhouse_keeper_cluster_membership(membership);
        Ok(())
    })
    .unwrap();

    let blueprint7 = sim.run_planner().expect("planning succeeded");
    let bp7_config = blueprint7.clickhouse_cluster_config.as_ref().unwrap();
    assert_eq!(bp7_config.generation, bp6_config.generation.next());
    assert_eq!(
        bp7_config.max_used_keeper_id,
        bp6_config.max_used_keeper_id + 1.into()
    );
    assert_eq!(
        bp7_config.keepers.len(),
        bp7_config.max_used_keeper_id.0 as usize
    );
    assert_eq!(bp7_config.keepers.len(), target_keepers as usize);
    assert_eq!(bp7_config.highest_seen_keeper_leader_committed_log_index, 2);

    // Updating the inventory to reflect the newest keeper node should not
    // increase the cluster size since we have reached the target.
    let membership = ClickhouseKeeperClusterMembership {
        queried_keeper: *keeper_id,
        leader_committed_log_index: 3,
        raft_config: blueprint7
            .clickhouse_cluster_config
            .as_ref()
            .unwrap()
            .keepers
            .values()
            .cloned()
            .collect(),
    };
    sim.generate_inventory_customized("add membership", |builder| {
        builder.found_clickhouse_keeper_cluster_membership(membership);
        Ok(())
    })
    .unwrap();

    let blueprint8 = sim.run_planner().expect("planning succeeded");
    let bp8_config = blueprint8.clickhouse_cluster_config.as_ref().unwrap();
    assert_eq!(bp8_config.generation, bp7_config.generation);
    assert_eq!(bp8_config.max_used_keeper_id, bp7_config.max_used_keeper_id);
    assert_eq!(bp8_config.keepers, bp7_config.keepers);
    assert_eq!(bp7_config.keepers.len(), target_keepers as usize);
    // The blueprint should not change solely due to the log index in inventory
    // changing
    assert_eq!(bp8_config.highest_seen_keeper_leader_committed_log_index, 2);

    logctx.cleanup_successful();
}

// Start with an existing clickhouse cluster and expunge a keeper. This
// models what will happen after an RSS deployment with clickhouse policy
// enabled or an existing system already running a clickhouse cluster.
#[test]
fn test_expunge_clickhouse_clusters() {
    static TEST_NAME: &str = "planner_expunge_clickhouse_clusters";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded example system");

    let target_keepers = 3;
    let target_servers = 2;

    // Enable clickhouse clusters via policy
    sim.set_clickhouse_policy(
        "enable clickhouse cluster",
        clickhouse_policy(ClickhouseMode::Both {
            target_servers,
            target_keepers,
        }),
    );

    // Create a new blueprint to deploy all our clickhouse zones
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    // We should see zones for 3 clickhouse keepers, and 2 servers created
    let active_zones: Vec<_> = blueprint2
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .map(|(_, z)| z.clone())
        .collect();

    let keeper_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_keeper())
        .map(|z| z.id)
        .collect();
    let server_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_server())
        .map(|z| z.id)
        .collect();

    assert_eq!(keeper_zone_ids.len(), target_keepers as usize);
    assert_eq!(server_zone_ids.len(), target_servers as usize);

    // Directly manipulate the blueprint and inventory so that the
    // clickhouse clusters are stable
    let blueprint2a = sim
        .blueprint_edit_latest_low_level("stable cluster", |blueprint2| {
            let config = blueprint2.clickhouse_cluster_config.as_mut().unwrap();
            config.max_used_keeper_id = (u64::from(target_keepers)).into();
            config.keepers = keeper_zone_ids
                .iter()
                .enumerate()
                .map(|(i, zone_id)| (*zone_id, KeeperId(i as u64)))
                .collect();
            config.highest_seen_keeper_leader_committed_log_index = 1;
            Ok(())
        })
        .unwrap();

    let raft_config: BTreeSet<_> = blueprint2a
        .clickhouse_cluster_config
        .as_ref()
        .unwrap()
        .keepers
        .values()
        .cloned()
        .collect();

    sim.change_description("adjust membership", |desc| {
        for keeper_id in blueprint2a
            .clickhouse_cluster_config
            .as_ref()
            .unwrap()
            .keepers
            .values()
        {
            desc.add_clickhouse_keeper_cluster_membership(
                ClickhouseKeeperClusterMembership {
                    queried_keeper: *keeper_id,
                    leader_committed_log_index: 1,
                    raft_config: raft_config.clone(),
                },
            );
        }
        Ok(())
    })
    .unwrap();
    sim.generate_inventory("inventory with new cluster membership").unwrap();

    let blueprint3 = sim.run_planner().expect("planning succeeded");

    assert_eq!(
        blueprint2a.clickhouse_cluster_config,
        blueprint3.clickhouse_cluster_config
    );

    // Find the sled containing one of the keeper zones and expunge it
    let (sled_id, bp_zone_config) = blueprint3
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .find(|(_, z)| z.zone_type.is_clickhouse_keeper())
        .unwrap();

    // What's the keeper id for this expunged zone?
    let expunged_keeper_id = blueprint3
        .clickhouse_cluster_config
        .as_ref()
        .unwrap()
        .keepers
        .get(&bp_zone_config.id)
        .unwrap();

    // Expunge a keeper zone
    sim.change_description("expunge keeper zone", |desc| {
        desc.sled_expunge(sled_id)?;
        Ok(())
    })
    .unwrap();

    let blueprint4 = sim.run_planner().expect("planning succeeded");
    let diff = blueprint4.diff_since_blueprint(&blueprint3);
    assert_contents(
        "tests/output/planner_expunge_clickhouse_clusters_3_4.txt",
        &diff.display().to_string(),
    );

    // The planner should expunge a zone based on the sled being expunged. Since
    // this is a clickhouse keeper zone, the clickhouse keeper configuration
    // should change to reflect this.
    let old_config = blueprint3.clickhouse_cluster_config.as_ref().unwrap();
    let config = blueprint4.clickhouse_cluster_config.as_ref().unwrap();
    assert_eq!(config.generation, old_config.generation.next());
    assert!(!config.keepers.contains_key(&bp_zone_config.id));
    // We've only removed one keeper from our desired state
    assert_eq!(config.keepers.len() + 1, old_config.keepers.len());
    // We haven't allocated any new keepers
    assert_eq!(config.max_used_keeper_id, old_config.max_used_keeper_id);

    // Planning again will not change the keeper state because we haven't
    // updated the inventory
    let blueprint5 = sim.run_planner().expect("planning succeeded");

    assert_eq!(
        blueprint4.clickhouse_cluster_config,
        blueprint5.clickhouse_cluster_config
    );

    // Updating the inventory to reflect the removed keeper results in a new one
    // being added
    sim.inventory_edit_latest_low_level("adjust membership", |collection| {
        println!("{:?}", collection.clickhouse_keeper_cluster_membership);
        println!("xxx {expunged_keeper_id}");
        // Remove the keeper for the expunged zone
        collection
            .clickhouse_keeper_cluster_membership
            .retain(|m| m.queried_keeper != *expunged_keeper_id);

        // Update the inventory on at least one of the remaining nodes.
        let mut existing = collection
            .clickhouse_keeper_cluster_membership
            .pop_first()
            .unwrap();
        existing.leader_committed_log_index = 3;
        existing.raft_config = config.keepers.values().cloned().collect();
        collection.clickhouse_keeper_cluster_membership.insert(existing);

        Ok(())
    })
    .unwrap();

    let blueprint6 = sim.run_planner().expect("planning succeeded");

    let diff = blueprint6.diff_since_blueprint(&blueprint5);
    assert_contents(
        "tests/output/planner_expunge_clickhouse_clusters_5_6.txt",
        &diff.display().to_string(),
    );

    let old_config = blueprint5.clickhouse_cluster_config.as_ref().unwrap();
    let config = blueprint6.clickhouse_cluster_config.as_ref().unwrap();

    // Our generation has changed to reflect the added keeper
    assert_eq!(config.generation, old_config.generation.next());
    assert!(!config.keepers.contains_key(&bp_zone_config.id));
    // We've only added one keeper from our desired state
    // This brings us back up to our target count
    assert_eq!(config.keepers.len(), old_config.keepers.len() + 1);
    assert_eq!(config.keepers.len(), target_keepers as usize);
    // We've allocated one new keeper
    assert_eq!(
        config.max_used_keeper_id,
        old_config.max_used_keeper_id + 1.into()
    );

    logctx.cleanup_successful();
}

#[test]
fn test_expunge_clickhouse_zones_after_policy_is_changed() {
    static TEST_NAME: &str =
        "planner_expunge_clickhouse_zones_after_policy_is_changed";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded example system");

    let target_keepers = 3;
    let target_servers = 2;

    // Enable clickhouse clusters via policy
    sim.set_clickhouse_policy(
        "enable clickhouse cluster",
        clickhouse_policy(ClickhouseMode::Both {
            target_servers,
            target_keepers,
        }),
    );

    // Create a new blueprint to deploy all our clickhouse zones
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    // We should see zones for 3 clickhouse keepers, and 2 servers created
    let active_zones: Vec<_> = blueprint2
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .map(|(_, z)| z.clone())
        .collect();

    let keeper_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_keeper())
        .map(|z| z.id)
        .collect();
    let server_zone_ids: BTreeSet<_> = active_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_server())
        .map(|z| z.id)
        .collect();

    assert_eq!(keeper_zone_ids.len(), target_keepers as usize);
    assert_eq!(server_zone_ids.len(), target_servers as usize);

    // Disable clickhouse single node via policy, and ensure the zone goes
    // away. First ensure we have one.
    assert_eq!(
        1,
        active_zones.iter().filter(|z| z.zone_type.is_clickhouse()).count()
    );
    sim.set_clickhouse_policy(
        "disable single-node",
        clickhouse_policy(ClickhouseMode::ClusterOnly {
            target_servers,
            target_keepers,
        }),
    );
    let blueprint3 = sim.run_planner().expect("planning succeeded");

    // We should have expunged our single-node clickhouse zone
    let expunged_zones: Vec<_> = blueprint3
        .all_omicron_zones(BlueprintZoneDisposition::is_expunged)
        .map(|(_, z)| z.clone())
        .collect();

    assert_eq!(1, expunged_zones.len());
    assert!(expunged_zones.first().unwrap().zone_type.is_clickhouse());

    // Disable clickhouse clusters via policy and restart single node
    sim.set_clickhouse_policy(
        "re-enable single-node",
        clickhouse_policy(ClickhouseMode::SingleNodeOnly),
    );
    let blueprint4 = sim.run_planner().expect("planning succeeded");

    let diff = blueprint4.diff_since_blueprint(&blueprint3);
    assert_contents(
        "tests/output/planner_expunge_clickhouse_zones_after_policy_is_changed_3_4.txt",
        &diff.display().to_string(),
    );

    // All our clickhouse keeper and server zones that we created when we
    // enabled our clickhouse policy should be expunged when we disable it.
    let expunged_zones: Vec<_> = blueprint4
        .all_omicron_zones(BlueprintZoneDisposition::is_expunged)
        .map(|(_, z)| z.clone())
        .collect();

    let expunged_keeper_zone_ids: BTreeSet<_> = expunged_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_keeper())
        .map(|z| z.id)
        .collect();
    let expunged_server_zone_ids: BTreeSet<_> = expunged_zones
        .iter()
        .filter(|z| z.zone_type.is_clickhouse_server())
        .map(|z| z.id)
        .collect();

    assert_eq!(keeper_zone_ids, expunged_keeper_zone_ids);
    assert_eq!(server_zone_ids, expunged_server_zone_ids);

    // We should have a new single-node clickhouse zone
    assert_eq!(
        1,
        blueprint4
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| z.zone_type.is_clickhouse())
            .count()
    );

    logctx.cleanup_successful();
}

#[test]
fn test_zones_marked_ready_for_cleanup_based_on_inventory() {
    static TEST_NAME: &str =
        "planner_zones_marked_ready_for_cleanup_based_on_inventory";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    //
    // Don't start internal DNS zones (not part of this test, and causes noise).
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| builder.internal_dns_count(0))
        .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // Find a Nexus zone we'll use for our test.
    let (sled_id, nexus_config) = blueprint1
        .sleds
        .iter()
        .find_map(|(sled_id, sled_config)| {
            let nexus =
                sled_config.zones.iter().find(|z| z.zone_type.is_nexus())?;
            Some((*sled_id, nexus.clone()))
        })
        .expect("found a Nexus zone");

    // Expunge the disk used by the Nexus zone.
    sim.change_description("expunge Nexus's disk", |desc| {
        desc.get_sled_mut(sled_id)
            .unwrap()
            .resources_mut()
            .zpools
            .get_mut(&nexus_config.filesystem_pool.id())
            .unwrap()
            .policy = PhysicalDiskPolicy::Expunged;
        Ok(())
    })
    .unwrap();

    // Run the planner. It should expunge all zones on the disk we just
    // expunged, including our Nexus zone, but not mark them as ready for
    // cleanup yet.
    let _pre_blueprint2 = sim.run_planner().expect("planning succeeded");

    // Mark the disk we expected as "ready for cleanup"; this isn't what
    // we're testing, and failing to do this will interfere with some of the
    // checks we do below.
    let blueprint2 = sim
        .blueprint_edit_latest_low_level(
            "manually mark disk ready for cleanup",
            |bp| {
                for mut disk in
                    bp.sleds.get_mut(&sled_id).unwrap().disks.iter_mut()
                {
                    match disk.disposition {
                        BlueprintPhysicalDiskDisposition::InService => (),
                        BlueprintPhysicalDiskDisposition::Expunged {
                            as_of_generation,
                            ..
                        } => {
                            disk.disposition =
                                BlueprintPhysicalDiskDisposition::Expunged {
                                    as_of_generation,
                                    ready_for_cleanup: true,
                                };
                        }
                    }
                }
                Ok(())
            },
        )
        .unwrap();

    // Helper to extract the Nexus zone's disposition in a blueprint.
    let get_nexus_disposition = |bp: &Blueprint| {
        bp.sleds.get(&sled_id).unwrap().zones.iter().find_map(|z| {
            if z.id == nexus_config.id { Some(z.disposition) } else { None }
        })
    };

    // This sled's config generation should have been bumped...
    let bp2_config = blueprint2.sleds.get(&sled_id).unwrap().clone();
    let bp2_sled_config = bp2_config.clone().into_in_service_sled_config();
    assert_eq!(
        blueprint1.sleds.get(&sled_id).unwrap().sled_agent_generation.next(),
        bp2_sled_config.generation
    );
    // ... and the Nexus should should have the disposition we expect.
    assert_eq!(
        get_nexus_disposition(&blueprint2),
        Some(BlueprintZoneDisposition::Expunged {
            as_of_generation: bp2_sled_config.generation,
            ready_for_cleanup: false,
        })
    );

    // Running the planner again should make no changes until the inventory
    // reports that the zone is not running and that the sled has reconciled
    // a new-enough generation. Try these variants:
    //
    // * same inventory as above (expect no changes)
    // * new config is ledgered but not reconciled (expect no changes)
    // * new config is reconciled, but zone is in an error state (expect
    //   no changes)
    eprintln!("planning with no inventory change...");
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    eprintln!("planning with config ledgered but not reconciled...");
    sim.inventory_edit_latest_low_level("ledger sled config", |collection| {
        collection
            .sled_agents
            .get_mut(&sled_id)
            .unwrap()
            .ledgered_sled_config = Some(bp2_sled_config.clone());
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    eprintln!("planning with config ledgered but zones failed to shut down...");
    sim.inventory_edit_latest_low_level("ledger sled config", |collection| {
        collection
            .sled_agents
            .get_mut(&sled_id)
            .unwrap()
            .ledgered_sled_config = Some(bp2_sled_config.clone());
        let mut reconciliation =
            ConfigReconcilerInventory::debug_assume_success(
                bp2_sled_config.clone(),
            );
        // For all the zones that are in bp2_config but not
        // bp2_sled_config (i.e., zones that should have been shut
        // down), insert an error result in the reconciliation.
        for zone in &bp2_config.zones {
            reconciliation.zones.entry(zone.id).or_insert_with(|| {
                ConfigReconcilerInventoryResult::Err {
                    message: "failed to shut down".to_string(),
                }
            });
        }
        collection.sled_agents.get_mut(&sled_id).unwrap().last_reconciliation =
            Some(reconciliation);
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Now make both changes to the inventory.
    sim.inventory_edit_latest_low_level("ledger sled config", |collection| {
        let mut config = collection.sled_agents.get_mut(&sled_id).unwrap();
        config.ledgered_sled_config = Some(bp2_sled_config.clone());
        config.last_reconciliation =
            Some(ConfigReconcilerInventory::debug_assume_success(
                bp2_sled_config.clone(),
            ));
        Ok(())
    })
    .unwrap();

    // Run the planner. It mark our Nexus zone as ready for cleanup now that
    // the inventory conditions are satisfied.
    let blueprint3 = sim.run_planner().expect("planning succeeded");

    assert_eq!(
        get_nexus_disposition(&blueprint3),
        Some(BlueprintZoneDisposition::Expunged {
            as_of_generation: bp2_sled_config.generation,
            ready_for_cleanup: true,
        })
    );

    // ready_for_cleanup changes should not bump the config generation,
    // since it doesn't affect what's sent to sled-agent.
    assert_eq!(
        blueprint3.sleds.get(&sled_id).unwrap().sled_agent_generation,
        bp2_sled_config.generation
    );

    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_internal_dns_zone_replaced_after_marked_for_cleanup() {
    static TEST_NAME: &str =
        "planner_internal_dns_zone_replaced_after_marked_for_cleanup";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example().expect("loaded default example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // Find a internal DNS zone we'll use for our test.
    let (sled_id, internal_dns_config) = blueprint1
        .sleds
        .iter()
        .find_map(|(sled_id, sled_config)| {
            let config = sled_config
                .zones
                .iter()
                .find(|z| z.zone_type.is_internal_dns())?;
            Some((*sled_id, config.clone()))
        })
        .expect("found an Internal DNS zone");

    // Expunge the disk used by the internal DNS zone.
    sim.change_description("expunge Nexus's disk", |desc| {
        desc.get_sled_mut(sled_id)
            .unwrap()
            .resources_mut()
            .zpools
            .get_mut(&internal_dns_config.filesystem_pool.id())
            .unwrap()
            .policy = PhysicalDiskPolicy::Expunged;
        Ok(())
    })
    .unwrap();

    // Run the planner. It should expunge all zones on the disk we just
    // expunged, including our DNS zone, but not mark them as ready for
    // cleanup yet.
    let blueprint2 = sim.run_planner().expect("planning succeeded");

    // Helper to extract the DNS zone's disposition in a blueprint.
    let get_dns_disposition = |bp: &Blueprint| {
        bp.sleds.get(&sled_id).unwrap().zones.iter().find_map(|z| {
            if z.id == internal_dns_config.id {
                Some(z.disposition)
            } else {
                None
            }
        })
    };

    // This sled's config generation should have been bumped...
    let bp2_config = blueprint2
        .sleds
        .get(&sled_id)
        .unwrap()
        .clone()
        .into_in_service_sled_config();
    assert_eq!(
        blueprint1.sleds.get(&sled_id).unwrap().sled_agent_generation.next(),
        bp2_config.generation
    );
    // ... and the DNS zone should should have the disposition we expect.
    assert_eq!(
        get_dns_disposition(&blueprint2),
        Some(BlueprintZoneDisposition::Expunged {
            as_of_generation: bp2_config.generation,
            ready_for_cleanup: false,
        })
    );

    // Running the planner again should make no changes until the inventory
    // reports that the zone is not running and that the sled has seen a
    // new-enough generation.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Make the inventory changes necessary for cleanup to proceed.
    sim_update_collection_from_blueprint(&mut sim, &blueprint2);

    // Run the planner. It should mark our internal DNS zone as ready for
    // cleanup now that the inventory conditions are satisfied, and also
    // place a new internal DNS zone now that the original subnet is free to
    // reuse.
    let blueprint3 = sim.run_planner().expect("planning succeeded");

    assert_eq!(
        get_dns_disposition(&blueprint3),
        Some(BlueprintZoneDisposition::Expunged {
            as_of_generation: bp2_config.generation,
            ready_for_cleanup: true,
        })
    );

    let summary = blueprint3.diff_since_blueprint(&blueprint2);
    eprintln!("{}", summary.display());

    let mut added_count = 0;
    for sled_cfg in summary.diff.sleds.modified_values_diff() {
        added_count += sled_cfg.zones.added.len();
        for z in &sled_cfg.zones.added {
            match &z.zone_type {
                BlueprintZoneType::InternalDns(internal_dns) => {
                    let BlueprintZoneType::InternalDns(InternalDns {
                        dns_address: orig_dns_address,
                        ..
                    }) = &internal_dns_config.zone_type
                    else {
                        unreachable!();
                    };

                    assert_eq!(internal_dns.dns_address, *orig_dns_address);
                }
                _ => panic!("unexpected added zone {z:?}"),
            }
        }
    }
    assert_eq!(added_count, 1);

    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

/// Deploy all configs (simulating blueprint execution) then generate a new
/// inventory collection that sees them.
fn sim_update_collection_from_blueprint(
    sim: &mut ReconfiguratorCliTestState,
    blueprint: &Blueprint,
) {
    sim.deploy_configs_to_active_sleds(
        "deploy latest configs to all sleds",
        &blueprint,
    )
    .expect("deployed configs");

    let active_nexus_zones =
        get_nexus_ids_at_generation(&blueprint, blueprint.nexus_generation);
    let not_yet_nexus_zones = get_nexus_ids_at_generation(
        &blueprint,
        blueprint.nexus_generation.next(),
    );
    sim.change_state("update active Nexus zones", |state| {
        // TODO-cleanup `sim.change_description()` also has
        // `set_{active,not_yet}_nexus_zones()` methods, but those don't take
        // effect. Why not?
        state
            .config_mut()
            .set_explicit_active_nexus_zones(Some(active_nexus_zones));
        state
            .config_mut()
            .set_explicit_not_yet_nexus_zones(Some(not_yet_nexus_zones));
        Ok(())
    })
    .unwrap();

    sim.generate_inventory("inventory with latest configs")
        .expect("generated inventory");
}

/// Manually update the example system's inventory collection's zones
/// from a blueprint.
fn update_collection_from_blueprint(
    example: &mut ExampleSystem,
    blueprint: &Blueprint,
) {
    for (&sled_id, config) in blueprint.sleds.iter() {
        example
            .system
            .sled_set_omicron_config(
                sled_id,
                config.clone().into_in_service_sled_config(),
            )
            .expect("can't set sled config");
    }
    example.collection =
        example.system.to_collection_builder().unwrap().build();

    update_input_with_nexus_at_generation(
        example,
        blueprint,
        blueprint.nexus_generation,
    )
}

fn update_input_with_nexus_at_generation(
    example: &mut ExampleSystem,
    blueprint: &Blueprint,
    active_generation: Generation,
) {
    let active_nexus_zones =
        get_nexus_ids_at_generation(&blueprint, active_generation);
    let not_yet_nexus_zones =
        get_nexus_ids_at_generation(&blueprint, active_generation.next());

    let mut input = std::mem::replace(
        &mut example.input,
        nexus_types::deployment::PlanningInputBuilder::empty_input(),
    )
    .into_builder();
    input.set_active_nexus_zones(active_nexus_zones);
    input.set_not_yet_nexus_zones(not_yet_nexus_zones);
    example.input = input.build();
}

macro_rules! fake_zone_artifact {
    ($kind: ident, $version: expr) => {
        TufArtifactMeta {
            id: ArtifactId {
                name: ZoneKind::$kind.artifact_id_name().to_string(),
                version: $version,
                kind: ArtifactKind::from_known(KnownArtifactKind::Zone),
            },
            hash: ArtifactHash([0; 32]),
            size: 0,
            board: None,
            sign: None,
        }
    };
}

fn create_zone_artifacts_at_version(
    version: &ArtifactVersion,
) -> Vec<TufArtifactMeta> {
    vec![
        // Omit `BoundaryNtp` because it has the same artifact name as
        // `InternalNtp`.
        fake_zone_artifact!(Clickhouse, version.clone()),
        fake_zone_artifact!(ClickhouseKeeper, version.clone()),
        fake_zone_artifact!(ClickhouseServer, version.clone()),
        fake_zone_artifact!(CockroachDb, version.clone()),
        fake_zone_artifact!(Crucible, version.clone()),
        fake_zone_artifact!(CruciblePantry, version.clone()),
        fake_zone_artifact!(ExternalDns, version.clone()),
        fake_zone_artifact!(InternalDns, version.clone()),
        fake_zone_artifact!(InternalNtp, version.clone()),
        fake_zone_artifact!(Nexus, version.clone()),
        fake_zone_artifact!(Oximeter, version.clone()),
        // We create artifacts with the versions (or hash) set to those of
        // the example system to simulate an environment that does not need
        // SP component updates.
        TufArtifactMeta {
            id: ArtifactId {
                name: "host-os-phase-1".to_string(),
                version: version.clone(),
                kind: ArtifactKind::GIMLET_HOST_PHASE_1,
            },
            hash: ArtifactHash([1; 32]),
            size: 0,
            board: None,
            sign: None,
        },
        TufArtifactMeta {
            id: ArtifactId {
                name: "host-os-phase-2".to_string(),
                version: version.clone(),
                kind: ArtifactKind::HOST_PHASE_2,
            },
            hash: ArtifactHash(hex_literal::hex!(
                "7cd830e1682d50620de0f5c24b8cca15937eb10d2a415ade6ad28c0d314408eb"
            )),
            size: 0,
            board: None,
            sign: None,
        },
        TufArtifactMeta {
            id: ArtifactId {
                name: sp_sim::SIM_GIMLET_BOARD.to_string(),
                version: ArtifactVersion::new("0.0.1").unwrap(),
                kind: KnownArtifactKind::GimletSp.into(),
            },
            hash: ArtifactHash([0; 32]),
            size: 0,
            board: Some(sp_sim::SIM_GIMLET_BOARD.to_string()),
            sign: None,
        },
        TufArtifactMeta {
            id: ArtifactId {
                name: sp_sim::SIM_ROT_BOARD.to_string(),
                version: ArtifactVersion::new("0.0.1").unwrap(),
                kind: ArtifactKind::GIMLET_ROT_IMAGE_B,
            },
            hash: ArtifactHash([0; 32]),
            size: 0,
            board: Some(sp_sim::SIM_ROT_BOARD.to_string()),
            sign: Some("sign-gimlet".into()),
        },
        TufArtifactMeta {
            id: ArtifactId {
                name: sp_sim::SIM_ROT_BOARD.to_string(),
                version: ArtifactVersion::new("0.0.1").unwrap(),
                kind: ArtifactKind::GIMLET_ROT_STAGE0,
            },
            hash: ArtifactHash([0; 32]),
            size: 0,
            board: Some(sp_sim::SIM_ROT_BOARD.to_string()),
            sign: Some("sign-gimlet".into()),
        },
    ]
}

/// Ensure that dependent zones (here just Crucible Pantry) are updated
/// before Nexus.
#[test]
fn test_update_crucible_pantry_before_nexus() {
    static TEST_NAME: &str = "update_crucible_pantry_before_nexus";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| builder.with_target_release_0_0_1())
        .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // We should start with nothing to do.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    // All zones should be sourced from the initial 0.0.1 target release by
    // default.
    assert!(
        blueprint1
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .all(|(_, z)| matches!(
                &z.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            ))
    );

    // Manually specify a TUF repo with fake zone images.
    let version = ArtifactVersion::new_static("1.0.0-freeform")
        .expect("can't parse artifact version");
    let fake_hash = ArtifactHash([0; 32]);
    let image_source = BlueprintZoneImageSource::Artifact {
        version: BlueprintArtifactVersion::Available {
            version: version.clone(),
        },
        hash: fake_hash,
    };
    let artifacts = create_zone_artifacts_at_version(&version);
    let description = TargetReleaseDescription::TufRepo(TufRepoDescription {
        repo: TufRepoMeta {
            hash: fake_hash,
            targets_role_version: 0,
            valid_until: Utc::now(),
            system_version: Version::new(1, 0, 0),
            file_name: String::from(""),
        },
        artifacts,
    });
    sim.change_description("set new target release", |desc| {
        desc.set_target_release_and_old_repo(description);
        Ok(())
    })
    .unwrap();

    // Some helper predicates for the assertions below.
    let is_old_nexus = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_nexus()
            && matches!(
                &zone.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            )
    };
    let is_up_to_date_nexus = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_nexus() && zone.image_source == image_source
    };
    let is_old_pantry = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_crucible_pantry()
            && matches!(
                &zone.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            )
    };
    let is_up_to_date_pantry = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_crucible_pantry() && zone.image_source == image_source
    };

    // Manually update all zones except CruciblePantry and Nexus.
    let blueprint2 = sim
        .blueprint_edit_latest_low_level(
            "update zones other than Nexus and pantry",
            |bp| {
                for mut zone in bp
                    .sleds
                    .values_mut()
                    .flat_map(|config| config.zones.iter_mut())
                    .filter(|z| {
                        !z.zone_type.is_nexus()
                            && !z.zone_type.is_crucible_pantry()
                    })
                {
                    zone.image_source = BlueprintZoneImageSource::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: version.clone(),
                        },
                        hash: fake_hash,
                    };
                }
                Ok(())
            },
        )
        .unwrap();

    let expected_new_nexus_zones =
        sim.current_description().target_nexus_zone_count();
    let expected_pantries =
        sim.current_description().target_crucible_pantry_zone_count();

    // We should now have iterations of expunge/cleanup/add iterations for
    // the Crucible Pantry zones.
    let mut parent = blueprint2;

    let mut old_pantries = expected_pantries;
    let mut expunging_pantries = 0;
    let mut added_pantries = 0;
    let mut i = 0;

    while old_pantries > 0
        || expunging_pantries > 0
        || added_pantries != expected_pantries
    {
        let blueprint_name = format!("expunging_crucible_pantry_{i}");
        i += 1;

        sim_update_collection_from_blueprint(&mut sim, &parent);
        let blueprint = sim.run_planner().expect("planning succeeded");

        {
            let summary = blueprint.diff_since_blueprint(&parent);
            eprintln!("diff to {blueprint_name}: {}", summary.display());
            for sled in summary.diff.sleds.modified_values_diff() {
                assert!(sled.zones.removed.is_empty());

                for modified_zone in sled.zones.modified_diff() {
                    assert!(matches!(
                        *modified_zone.zone_type.before,
                        BlueprintZoneType::CruciblePantry(_)
                    ));
                    // If the zone was previously in-service, it gets
                    // expunged.
                    if modified_zone.disposition.before.is_in_service() {
                        assert!(matches!(
                            modified_zone.image_source.before,
                            BlueprintZoneImageSource::Artifact {
                                version,
                                hash: _,
                            } if version == &BlueprintArtifactVersion::Available {
                                version: ArtifactVersion::new_const("0.0.1")
                            }
                        ));
                        assert!(modified_zone.disposition.after.is_expunged(),);
                        old_pantries -= 1;
                        expunging_pantries += 1;
                    }

                    // If the zone was previously expunged and not ready for
                    // cleanup, it should be marked ready-for-cleanup
                    if modified_zone.disposition.before.is_expunged()
                        && !modified_zone
                            .disposition
                            .before
                            .is_ready_for_cleanup()
                    {
                        assert!(
                            modified_zone
                                .disposition
                                .after
                                .is_ready_for_cleanup(),
                        );

                        expunging_pantries -= 1;
                    }
                }

                for zone in &sled.zones.added {
                    match zone.zone_type {
                        BlueprintZoneType::CruciblePantry(_) => {
                            assert_eq!(zone.image_source, image_source);
                            added_pantries += 1;
                        }
                        _ => panic!("Unexpected zone add: {zone:?}"),
                    }
                }
            }
        }

        parent = blueprint;
    }
    let mut blueprint = parent;

    // All Crucible Pantries should now be updated.
    assert_eq!(
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| is_up_to_date_pantry(z))
            .count(),
        CRUCIBLE_PANTRY_REDUNDANCY
    );

    // All old Pantry zones should now be expunged.
    assert_eq!(
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_expunged)
            .filter(|(_, z)| is_old_pantry(z))
            .count(),
        CRUCIBLE_PANTRY_REDUNDANCY
    );

    // Nexus should deploy new zones, but keep the old ones running.
    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        let mut modified_sleds = 0;
        for sled in summary.diff.sleds.modified_values_diff() {
            assert!(sled.zones.removed.is_empty());
            assert_eq!(sled.zones.added.len(), 1);
            let added = sled.zones.added.first().unwrap();
            let BlueprintZoneType::Nexus(nexus_zone) = &added.zone_type else {
                panic!("Unexpected zone type: {:?}", added.zone_type);
            };
            assert_eq!(nexus_zone.nexus_generation, Generation::new().next());
            assert_eq!(&added.image_source, &image_source);
            modified_sleds += 1;
        }
        assert_eq!(modified_sleds, expected_new_nexus_zones);
    }
    blueprint = new_blueprint;

    // Now we can update Nexus, because all of its dependent zones
    // are up-to-date w/r/t the new repo.
    //
    // First, we'll expect the nexus generation to get bumped.
    let active_nexus_zones =
        get_nexus_ids_at_generation(&blueprint, Generation::new());
    let not_yet_nexus_zones =
        get_nexus_ids_at_generation(&blueprint, Generation::new().next());

    assert_eq!(active_nexus_zones.len(), NEXUS_REDUNDANCY);
    assert_eq!(not_yet_nexus_zones.len(), NEXUS_REDUNDANCY);

    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        assert!(summary.has_changes(), "Should have bumped nexus generation");
        assert_eq!(
            summary.diff.nexus_generation.before.next(),
            *summary.diff.nexus_generation.after,
        );
        assert!(summary.diff.sleds.modified_values_diff().next().is_none());
    }
    blueprint = new_blueprint;

    let mut parent = blueprint;
    for i in 9..=12 {
        sim_update_collection_from_blueprint(&mut sim, &parent);
        let blueprint = sim.run_planner().expect("planning succeeded");

        {
            let summary = blueprint.diff_since_blueprint(&parent);
            assert!(summary.has_changes(), "No changes at iteration {i}");
            for sled in summary.diff.sleds.modified_values_diff() {
                assert!(sled.zones.added.is_empty());
                assert!(sled.zones.removed.is_empty());
                for modified_zone in sled.zones.modified_diff() {
                    // We're only modifying Nexus zones on the old image
                    assert!(matches!(
                        *modified_zone.zone_type.before,
                        BlueprintZoneType::Nexus(_)
                    ));
                    assert!(matches!(
                        modified_zone.image_source.before,
                        BlueprintZoneImageSource::Artifact {
                            version,
                            hash: _,
                        } if version == &BlueprintArtifactVersion::Available {
                            version: ArtifactVersion::new_const("0.0.1")
                        }
                    ));

                    // If the zone was previously in-service, it gets
                    // expunged.
                    if modified_zone.disposition.before.is_in_service() {
                        assert!(modified_zone.disposition.after.is_expunged(),);
                    }

                    // If the zone was previously expunged and not ready for
                    // cleanup, it should be marked ready-for-cleanup
                    if modified_zone.disposition.before.is_expunged()
                        && !modified_zone
                            .disposition
                            .before
                            .is_ready_for_cleanup()
                    {
                        assert!(
                            modified_zone
                                .disposition
                                .after
                                .is_ready_for_cleanup(),
                        );
                    }
                }
            }
        }
        parent = blueprint;
    }

    // Everything's up-to-date in Kansas City!
    let blueprint12 = parent;
    assert_eq!(
        blueprint12
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| is_up_to_date_nexus(z))
            .count(),
        NEXUS_REDUNDANCY,
    );
    assert_eq!(
        blueprint12
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| is_old_nexus(z))
            .count(),
        0,
    );

    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_update_cockroach() {
    static TEST_NAME: &str = "update_cockroach";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| builder.with_target_release_0_0_1())
        .expect("loaded example system");
    let mut blueprint = sim.assert_latest_blueprint_is_blippy_clean();

    // Update the example system and blueprint, as a part of test set-up.
    //
    // Ask for COCKROACHDB_REDUNDANCY cockroach nodes

    // If this assertion breaks - which would be okay - we should delete all
    // these planning steps explicitly including a base set of CRDB zones.
    sim.change_description("ask for cockroach nodes", |desc| {
        assert_eq!(
            desc.target_cockroachdb_zone_count(),
            0,
            "We expect the system is initialized without cockroach zones"
        );
        desc.set_target_cockroachdb_zone_count(COCKROACHDB_REDUNDANCY);
        Ok(())
    })
    .unwrap();

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        assert_eq!(summary.total_zones_added(), COCKROACHDB_REDUNDANCY);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 0);
    }
    blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);

    // We should have started with no specified TUF repo and nothing to do.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    // All zones should be sourced from the initial 0.0.1 target release by
    // default.
    eprintln!("{}", blueprint.display());
    assert!(
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .all(|(_, z)| matches!(
                &z.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            ))
    );

    // This test "starts" here -- we specify a new TUF repo with an updated
    // CockroachDB image. We create a new TUF repo where version of
    // CockroachDB has been updated out of the 0.0.1 repo.
    //
    // The planner should avoid doing this update until it has confirmation
    // from inventory that the cluster is healthy.

    let version = ArtifactVersion::new_static("1.0.0-freeform")
        .expect("can't parse artifact version");
    let fake_hash = ArtifactHash([0; 32]);
    let image_source = BlueprintZoneImageSource::Artifact {
        version: BlueprintArtifactVersion::Available {
            version: version.clone(),
        },
        hash: fake_hash,
    };
    let artifacts = create_zone_artifacts_at_version(&version);
    let description = TargetReleaseDescription::TufRepo(TufRepoDescription {
        repo: TufRepoMeta {
            hash: fake_hash,
            targets_role_version: 0,
            valid_until: Utc::now(),
            system_version: Version::new(1, 0, 0),
            file_name: String::from(""),
        },
        artifacts,
    });
    sim.change_description("set new target release", |desc| {
        desc.set_target_release_and_old_repo(description);
        Ok(())
    })
    .unwrap();

    // Manually update all zones except Cockroach
    //
    // We just specified a new TUF repo, everything is going to shift from
    // the initial 0.0.1 repo to this new repo.
    blueprint = sim
        .blueprint_edit_latest_low_level("update non-crdb zones", |bp| {
            for mut zone in bp
                .sleds
                .values_mut()
                .flat_map(|config| config.zones.iter_mut())
                .filter(|z| !z.zone_type.is_cockroach())
            {
                zone.image_source = BlueprintZoneImageSource::Artifact {
                    version: BlueprintArtifactVersion::Available {
                        version: version.clone(),
                    },
                    hash: fake_hash,
                };
            }
            Ok(())
        })
        .unwrap();
    sim_update_collection_from_blueprint(&mut sim, &blueprint);

    // Some helper predicates for the assertions below.
    const GOAL_REDUNDANCY: u64 = COCKROACHDB_REDUNDANCY as u64;
    let is_old_cockroach = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_cockroach()
            && matches!(
                &zone.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            )
    };
    let is_up_to_date_cockroach = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_cockroach() && zone.image_source == image_source
    };
    let create_valid_looking_status = || {
        let mut result = BTreeMap::new();
        for i in 1..=COCKROACHDB_REDUNDANCY {
            result.insert(
                cockroach_admin_types::NodeId(i.to_string()),
                CockroachStatus {
                    ranges_underreplicated: Some(0),
                    liveness_live_nodes: Some(GOAL_REDUNDANCY),
                },
            );
        }
        result
    };

    // If we have missing info in our inventory, the
    // planner will not update any Cockroach zones.
    sim.inventory_edit_latest_low_level("no cockroach status", |collection| {
        collection.cockroach_status = BTreeMap::new();
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we don't have valid statuses from enough internal DNS zones, we
    // will refuse to update.
    sim.inventory_edit_latest_low_level(
        "too few cockroach statuses",
        |collection| {
            collection.cockroach_status = create_valid_looking_status();
            collection.cockroach_status.pop_first();
            Ok(())
        },
    )
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we have any non-zero "ranges_underreplicated" in in our inventory,
    // the planner will not update any Cockroach zones.
    sim.inventory_edit_latest_low_level(
        "ranges underreplicated",
        |collection| {
            collection.cockroach_status = create_valid_looking_status();
            *collection.cockroach_status.values_mut().next().unwrap() =
                CockroachStatus {
                    ranges_underreplicated: Some(1),
                    liveness_live_nodes: Some(GOAL_REDUNDANCY),
                };
            Ok(())
        },
    )
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we don't have enough live nodes, we won't update Cockroach zones.
    sim.inventory_edit_latest_low_level("too few live nodes", |collection| {
        collection.cockroach_status = create_valid_looking_status();
        *collection.cockroach_status.values_mut().next().unwrap() =
            CockroachStatus {
                ranges_underreplicated: Some(0),
                liveness_live_nodes: Some(GOAL_REDUNDANCY - 1),
            };
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Once we have zero underreplicated ranges, we can start to update
    // Cockroach zones.
    //
    // We'll update one zone at a time, from the initial 0.0.1 artifact to
    // the new TUF repo artifact.
    for i in 1..=COCKROACHDB_REDUNDANCY {
        // Keep setting this value in a loop;
        // "sim_update_collection_from_blueprint" resets it.
        sim.inventory_edit_latest_low_level(
            "valid cockroach status",
            |collection| {
                collection.cockroach_status = create_valid_looking_status();
                Ok(())
            },
        )
        .unwrap();

        println!("Updating cockroach {i} of {COCKROACHDB_REDUNDANCY}");
        blueprint = sim.run_planner().expect("planning succeeded");

        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_old_cockroach(z))
                .count(),
            COCKROACHDB_REDUNDANCY - i
        );
        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_up_to_date_cockroach(z))
                .count(),
            i
        );
        sim_update_collection_from_blueprint(&mut sim, &blueprint);
    }

    // Validate that we have no further changes to make, once all Cockroach
    // zones have been updated.
    sim.inventory_edit_latest_low_level(
        "valid cockroach status",
        |collection| {
            collection.cockroach_status = create_valid_looking_status();
            Ok(())
        },
    )
    .unwrap();

    // Note that we use `InputUnchanged` here because we just updated the
    // collection.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Validate that we do not flip back to the 0.0.1 artifact after
    // performing the update.
    sim.inventory_edit_latest_low_level(
        "invalid cockroach status",
        |collection| {
            collection.cockroach_status = create_valid_looking_status();
            collection
                .cockroach_status
                .values_mut()
                .next()
                .unwrap()
                .ranges_underreplicated = Some(1);
            Ok(())
        },
    )
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_update_boundary_ntp() {
    static TEST_NAME: &str = "update_boundary_ntp";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| builder.with_target_release_0_0_1())
        .expect("loaded example system");

    // The example system creates three internal NTP zones, and zero
    // boundary NTP zones. This is a little arbitrary, but we're checking it
    // here: the lack of boundary NTP zones means we need to perform some
    // manual promotion of "internal -> boundary NTP", as documented below.

    let collection = sim.inventory(CollectionId::Latest).unwrap();
    assert_eq!(
        collection
            .all_running_omicron_zones()
            .filter(|zone_config| { zone_config.zone_type.is_ntp() })
            .count(),
        3,
    );
    assert_eq!(
        collection
            .all_running_omicron_zones()
            .filter(|zone_config| { zone_config.zone_type.is_boundary_ntp() })
            .count(),
        0,
    );

    // Update the example system and blueprint, as a part of test set-up.
    //
    // Ask for BOUNDARY_NTP_REDUNDANCY boundary NTP zones.
    //
    // To pull this off, we need to have AT LEAST ONE boundary NTP zone
    // that already exists. We'll perform a manual promotion first, then
    // ask for the other boundary NTP zones.

    let blueprint = sim
        .blueprint_edit_latest_low_level("manually promote NTP zone", |bp| {
            let mut zone = bp
                .sleds
                .values_mut()
                .flat_map(|config| config.zones.iter_mut())
                .find(|z| z.zone_type.is_ntp())
                .unwrap();
            let address = match zone.zone_type {
                BlueprintZoneType::InternalNtp(
                    blueprint_zone_type::InternalNtp { address },
                ) => address,
                _ => panic!("should be internal NTP?"),
            };
            let ip_config = PrivateIpConfig::new_ipv6(
                Ipv6Addr::LOCALHOST,
                Ipv6Net::new(Ipv6Addr::LOCALHOST, 64).unwrap(),
            )
            .unwrap();

            // The contents here are all lies, but it's just stored
            // as plain-old-data for the purposes of this test, so
            // it doesn't need to be real.
            zone.zone_type = BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp {
                    address,
                    ntp_servers: vec![],
                    dns_servers: vec![],
                    domain: None,
                    nic: NetworkInterface {
                        id: Uuid::new_v4(),
                        kind: NetworkInterfaceKind::Service {
                            id: Uuid::new_v4(),
                        },
                        name: "ntp-0".parse().unwrap(),
                        ip_config,
                        mac: MacAddr::random_system(),
                        vni: Vni::SERVICES_VNI,
                        primary: true,
                        slot: 0,
                    },
                    external_ip: OmicronZoneExternalSnatIp {
                        id: ExternalIpUuid::new_v4(),
                        snat_cfg: SourceNatConfig::new(
                            IpAddr::V6(Ipv6Addr::LOCALHOST),
                            0,
                            0x4000 - 1,
                        )
                        .unwrap(),
                    },
                },
            );
            Ok(())
        })
        .unwrap();
    sim_update_collection_from_blueprint(&mut sim, &blueprint);

    // We should have one boundary NTP zone now.
    assert_eq!(
        sim.inventory(CollectionId::Latest)
            .unwrap()
            .all_running_omicron_zones()
            .filter(|zone_config| { zone_config.zone_type.is_boundary_ntp() })
            .count(),
        1,
    );

    // Use that boundary NTP zone to promote others.
    sim.change_description("make more boundary NTP zones", |desc| {
        desc.set_target_boundary_ntp_zone_count(BOUNDARY_NTP_REDUNDANCY);
        Ok(())
    })
    .unwrap();
    let new_blueprint = sim.run_planner().expect("planning succeeded");

    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        assert_eq!(summary.total_zones_added(), BOUNDARY_NTP_REDUNDANCY - 1);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), BOUNDARY_NTP_REDUNDANCY - 1);
    }
    let blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);

    assert_eq!(
        sim.inventory(CollectionId::Latest)
            .unwrap()
            .all_running_omicron_zones()
            .filter(|zone_config| { zone_config.zone_type.is_boundary_ntp() })
            .count(),
        BOUNDARY_NTP_REDUNDANCY
    );

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), BOUNDARY_NTP_REDUNDANCY - 1);
    }
    let blueprint = new_blueprint;

    // We should have started with no specified TUF repo and nothing to do.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::DeployLatestConfigs,
    );

    // All zones should be sourced from the 0.0.1 repo by default.
    assert!(
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .all(|(_, z)| matches!(
                &z.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            ))
    );

    // This test "starts" here -- we specify a new TUF repo with an updated
    // Boundary NTP image. We create a new TUF repo where version of
    // Boundary NTP has been updated out of the 0.0.1 repo.
    //
    // The planner should avoid doing this update until it has confirmation
    // from inventory that the cluster is healthy.

    let version = ArtifactVersion::new_static("1.0.0-freeform")
        .expect("can't parse artifact version");
    let fake_hash = ArtifactHash([0; 32]);
    let image_source = BlueprintZoneImageSource::Artifact {
        version: BlueprintArtifactVersion::Available {
            version: version.clone(),
        },
        hash: fake_hash,
    };
    let artifacts = create_zone_artifacts_at_version(&version);
    let description = TargetReleaseDescription::TufRepo(TufRepoDescription {
        repo: TufRepoMeta {
            hash: fake_hash,
            targets_role_version: 0,
            valid_until: Utc::now(),
            system_version: Version::new(1, 0, 0),
            file_name: String::from(""),
        },
        artifacts,
    });
    sim.change_description("set new target release", |desc| {
        desc.set_target_release_and_old_repo(description);
        Ok(())
    })
    .unwrap();

    // Manually update all zones except boundary NTP
    //
    // We just specified a new TUF repo, everything is going to shift from
    // the 0.0.1 repo to this new repo.
    let blueprint = sim
        .blueprint_edit_latest_low_level("manually update zones", |bp| {
            for mut zone in bp
                .sleds
                .values_mut()
                .flat_map(|config| config.zones.iter_mut())
                .filter(|z| !z.zone_type.is_boundary_ntp())
            {
                zone.image_source = BlueprintZoneImageSource::Artifact {
                    version: BlueprintArtifactVersion::Available {
                        version: version.clone(),
                    },
                    hash: fake_hash,
                };
            }
            Ok(())
        })
        .unwrap();
    sim_update_collection_from_blueprint(&mut sim, &blueprint);

    // Some helper predicates for the assertions below.
    let is_old_boundary_ntp = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_boundary_ntp()
            && matches!(
                &zone.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            )
    };
    let old_boundary_ntp_count = |blueprint: &Blueprint| -> usize {
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| is_old_boundary_ntp(z))
            .count()
    };
    let is_up_to_date_boundary_ntp = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_boundary_ntp() && zone.image_source == image_source
    };
    let up_to_date_boundary_ntp_count = |blueprint: &Blueprint| -> usize {
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter(|(_, z)| is_up_to_date_boundary_ntp(z))
            .count()
    };

    let set_valid_looking_timesync = |collection: &mut Collection| {
        let mut ntp_timesync = IdOrdMap::<TimeSync>::new();

        for sled in &collection.sled_agents {
            let config = &sled
                .last_reconciliation
                .as_ref()
                .expect("Sled missing ledger? {sled:?}")
                .last_reconciled_config;

            let Some(zone_id) =
                config.zones.iter().find_map(|zone| match zone.zone_type {
                    OmicronZoneType::BoundaryNtp { .. }
                    | OmicronZoneType::InternalNtp { .. } => Some(zone.id),
                    _ => None,
                })
            else {
                // Sled without NTP
                continue;
            };

            ntp_timesync
                .insert_unique(TimeSync { zone_id, synced: true })
                .expect("NTP zone with same zone ID seen repeatedly");
        }
        collection.ntp_timesync = ntp_timesync;
    };

    let sim_set_valid_looking_timesync =
        |sim: &mut ReconfiguratorCliTestState| {
            sim.inventory_edit_latest_low_level(
                "valid NTP status",
                |collection| {
                    set_valid_looking_timesync(collection);
                    Ok(())
                },
            )
            .unwrap();
        };

    // If we have missing info in our inventory, the
    // planner will not update any boundary NTP zones.
    sim.inventory_edit_latest_low_level("no timesync status", |collection| {
        collection.ntp_timesync = IdOrdMap::new();
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we don't have enough info from boundary NTP nodes, we'll refuse to
    // update.
    sim.inventory_edit_latest_low_level("too few NTP zones", |collection| {
        set_valid_looking_timesync(collection);
        let boundary_ntp_zone = collection
            .all_running_omicron_zones()
            .find_map(|z| {
                if let OmicronZoneType::BoundaryNtp { .. } = z.zone_type {
                    Some(z.id)
                } else {
                    None
                }
            })
            .unwrap();
        collection.ntp_timesync.remove(&boundary_ntp_zone);
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we don't have enough explicitly synced nodes, we'll refuse to
    // update.
    sim.inventory_edit_latest_low_level("unsynced NTP zones", |collection| {
        set_valid_looking_timesync(collection);
        let boundary_ntp_zone = collection
            .all_running_omicron_zones()
            .find_map(|z| {
                if let OmicronZoneType::BoundaryNtp { .. } = z.zone_type {
                    Some(z.id)
                } else {
                    None
                }
            })
            .unwrap();
        collection.ntp_timesync.get_mut(&boundary_ntp_zone).unwrap().synced =
            false;
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Once all nodes are timesync'd, we can start to update boundary NTP
    // zones.
    //
    // We'll update one zone at a time, from the 0.0.1 artifact to the new
    // TUF repo artifact.
    sim_set_valid_looking_timesync(&mut sim);

    //
    // Step 1:
    //
    // * Expunge old boundary NTP. This is treated as a "modified zone", here
    // and below.
    //
    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        eprintln!(
            "diff between blueprints (should be expunging \
                 boundary NTP using 0.0.1 artifact):\n{}",
            summary.display()
        );
        eprintln!("{}", new_blueprint.source);

        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 1);
    }
    let blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    sim_set_valid_looking_timesync(&mut sim);

    // NOTE: This is a choice! The current planner is opting to reduce the
    // redundancy count of boundary NTP zones for the duration of the
    // upgrade.
    assert_eq!(old_boundary_ntp_count(&blueprint), BOUNDARY_NTP_REDUNDANCY - 1);
    assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 0);

    //
    // Step 2:
    //
    // On one sled:
    // * Finish expunging the boundary NTP zone (started in prior step)
    // + Add an internal NTP zone on the sled where that boundary NTP zone was
    //   expunged.
    // Since NTP is a non-discretionary zone, this is the default behavior.
    //
    // On another sled, do promotion to try to restore boundary NTP redundancy:
    // * Expunge an internal NTP zone
    // + Add it back as a boundary NTP zone
    //

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        eprintln!(
            "diff between blueprints \
             (should be adding one internal NTP and \
              promoting another to boundary):\n{}",
            summary.display()
        );
        eprintln!("{}", new_blueprint.source);

        assert_eq!(summary.total_zones_added(), 2);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 2);
    }
    let blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    sim_set_valid_looking_timesync(&mut sim);

    assert_eq!(old_boundary_ntp_count(&blueprint), 1);
    assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 1);

    //
    // Step 3:
    //
    // Now that the sum of "old + new" boundary NTP zones ==
    // BOUNDARY_NTP_REDUNDANCY, we can finish the upgrade process.
    //
    // * Start expunging the remaining old boundary NTP zone
    // * Finish expunging the internal NTP zone (started in prior step)
    //

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        eprintln!(
            "diff between blueprints \
             (should be expunging another boundary NTP):\n{}",
            summary.display()
        );
        eprintln!("{}", new_blueprint.source);

        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 2);
    }
    let blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    sim_set_valid_looking_timesync(&mut sim);

    assert_eq!(old_boundary_ntp_count(&blueprint), 0);
    assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 1);

    //
    // Step 4:
    //
    // Promotion:
    // + Add a boundary NTP on a sled where there was an internal NTP
    // + Start expunging an internal NTP
    //
    // Cleanup:
    // * Finish expunging the boundary NTP running off of the 0.0.1
    //   artifact
    //

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        eprintln!(
            "diff between blueprints \
             (should be adding promoting internal -> boundary NTP):\n{}",
            summary.display()
        );
        eprintln!("{}", new_blueprint.source);

        assert_eq!(summary.total_zones_added(), 2);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 1);
    }
    let blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    sim_set_valid_looking_timesync(&mut sim);

    assert_eq!(old_boundary_ntp_count(&blueprint), 0);
    assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 2);

    //
    // Step 5:
    //
    // Cleanup:
    // * Finish clearing out expunged internal NTP zones (added in prior step)
    //

    let new_blueprint = sim.run_planner().expect("planning succeeded");
    {
        let summary = new_blueprint.diff_since_blueprint(&blueprint);
        eprintln!(
            "diff between blueprints \
             (should be adding wrapping up internal NTP expungement):\n{}",
            summary.display()
        );
        eprintln!("{}", new_blueprint.source);

        assert_eq!(summary.total_zones_added(), 0);
        assert_eq!(summary.total_zones_removed(), 0);
        assert_eq!(summary.total_zones_modified(), 1);
    }
    let blueprint = new_blueprint;
    sim_update_collection_from_blueprint(&mut sim, &blueprint);
    sim_set_valid_looking_timesync(&mut sim);

    assert_eq!(old_boundary_ntp_count(&blueprint), 0);
    assert_eq!(up_to_date_boundary_ntp_count(&blueprint), 2);

    // Validate that we have no further changes to make, once all Boundary
    // NTP zones have been updated.
    //
    // Note that we use `InputUnchanged` here because we just updated the
    // collection.
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Validate that we do not flip back to the 0.0.1 artifact after
    // performing the update, even if we lose timesync data.
    sim.inventory_edit_latest_low_level("no timesync status", |collection| {
        collection.ntp_timesync = IdOrdMap::new();
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    logctx.cleanup_successful();
}

#[test]
fn test_update_internal_dns() {
    static TEST_NAME: &str = "update_internal_dns";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| builder.with_target_release_0_0_1())
        .expect("loaded example system");
    let blueprint = sim.assert_latest_blueprint_is_blippy_clean();

    // All zones should be sourced from the initial TUF repo by default.
    assert!(
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .all(|(_, z)| matches!(
                &z.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            ))
    );

    // This test "starts" here -- we specify a new TUF repo with an updated
    // Internal DNS image. We create a new TUF repo where version of
    // Internal DNS has been updated to "1.0.0-freeform".
    //
    // The planner should avoid doing this update until it has confirmation
    // from inventory that the Internal DNS servers are ready.

    let version = ArtifactVersion::new_static("1.0.0-freeform")
        .expect("can't parse artifact version");
    let fake_hash = ArtifactHash([0; 32]);
    let image_source = BlueprintZoneImageSource::Artifact {
        version: BlueprintArtifactVersion::Available {
            version: version.clone(),
        },
        hash: fake_hash,
    };
    let artifacts = create_zone_artifacts_at_version(&version);
    let description = TargetReleaseDescription::TufRepo(TufRepoDescription {
        repo: TufRepoMeta {
            hash: fake_hash,
            targets_role_version: 0,
            valid_until: Utc::now(),
            system_version: Version::new(1, 0, 0),
            file_name: String::from(""),
        },
        artifacts,
    });
    sim.change_description("set new target release", |desc| {
        desc.set_target_release_and_old_repo(description);
        Ok(())
    })
    .unwrap();

    // Manually update all zones except Internal DNS
    //
    // We just specified a new TUF repo, everything is going to shift from
    // the 0.0.1 repo to this new repo.
    let mut blueprint = sim
        .blueprint_edit_latest_low_level("manually update zones", |bp| {
            for mut zone in bp
                .sleds
                .values_mut()
                .flat_map(|config| config.zones.iter_mut())
                .filter(|z| !z.zone_type.is_internal_dns())
            {
                zone.image_source = BlueprintZoneImageSource::Artifact {
                    version: BlueprintArtifactVersion::Available {
                        version: version.clone(),
                    },
                    hash: fake_hash,
                };
            }
            Ok(())
        })
        .unwrap();
    sim_update_collection_from_blueprint(&mut sim, &blueprint);

    // Some helper predicates for the assertions below.
    let is_old_internal_dns = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_internal_dns()
            && matches!(
                &zone.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            )
    };
    let is_up_to_date_internal_dns = |zone: &BlueprintZoneConfig| -> bool {
        zone.zone_type.is_internal_dns() && zone.image_source == image_source
    };
    let create_valid_looking_status = |bp: &Blueprint| {
        let mut result = IdOrdMap::new();
        for sled in bp.sleds.values() {
            for zone in &sled.zones {
                if zone.zone_type.is_internal_dns() {
                    result
                        .insert_unique(InternalDnsGenerationStatus {
                            zone_id: zone.id,
                            generation: bp.internal_dns_version,
                        })
                        .expect("Observed duplicate Internal DNS zones");
                }
            }
        }
        result
    };

    // If we have missing info in our inventory, the
    // planner will not update any Internal DNS zones.
    sim.inventory_edit_latest_low_level("no status", |collection| {
        collection.internal_dns_generation_status = IdOrdMap::new();
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we're missing info from even a single zone, we
    // will still refuse to update.
    sim.inventory_edit_latest_low_level("missing node status", |collection| {
        collection.internal_dns_generation_status =
            create_valid_looking_status(&blueprint);
        let first_zone = collection
            .internal_dns_generation_status
            .iter()
            .next()
            .unwrap()
            .zone_id;
        collection
            .internal_dns_generation_status
            .remove(&first_zone)
            .expect("Could not remove one status");
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // If we have any out-of-sync generations in our inventory,
    // the planner will not update Internal DNS zones.
    sim.inventory_edit_latest_low_level("out of sync gen", |collection| {
        collection.internal_dns_generation_status =
            create_valid_looking_status(&blueprint);
        // I'd rather have the generation be "too low", but we also reject
        // generations that are "too far ahead", so this works.
        collection
            .internal_dns_generation_status
            .iter_mut()
            .next()
            .unwrap()
            .generation = blueprint.internal_dns_version.next();
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Once we have valid DNS statuses, we can start to update Internal DNS
    // zones.
    //
    // We'll update one zone at a time, from the 0.0.1 artifact to the new
    // TUF repo artifact.
    for i in 1..=INTERNAL_DNS_REDUNDANCY {
        sim.inventory_edit_latest_low_level("valid status", |collection| {
            collection.internal_dns_generation_status =
                create_valid_looking_status(&blueprint);
            Ok(())
        })
        .unwrap();

        // First blueprint: Remove an internal DNS zone

        println!(
            "Updating internal DNS {i} of {INTERNAL_DNS_REDUNDANCY} (expunge)"
        );
        let new_blueprint = sim.run_planner().expect("planning succeeded");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            assert_eq!(summary.total_zones_added(), 0);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 1);
        }
        blueprint = new_blueprint;
        sim_update_collection_from_blueprint(&mut sim, &blueprint);

        // Next blueprint: Add an (updated) internal DNS zone back

        println!(
            "Updating internal DNS {i} of {INTERNAL_DNS_REDUNDANCY} (add)"
        );
        let new_blueprint = sim.run_planner().expect("planning succeeded");
        {
            let summary = new_blueprint.diff_since_blueprint(&blueprint);
            assert_eq!(summary.total_zones_added(), 1);
            assert_eq!(summary.total_zones_removed(), 0);
            assert_eq!(summary.total_zones_modified(), 1);
        }
        blueprint = new_blueprint;
        sim_update_collection_from_blueprint(&mut sim, &blueprint);

        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_old_internal_dns(z))
                .count(),
            INTERNAL_DNS_REDUNDANCY - i
        );
        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter(|(_, z)| is_up_to_date_internal_dns(z))
                .count(),
            i
        );
    }

    // Validate that we have no further changes to make, once all Internal
    // DNS zones have been updated.
    sim.inventory_edit_latest_low_level("valid status", |collection| {
        collection.internal_dns_generation_status =
            create_valid_looking_status(&blueprint);
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    // Validate that we do not flip back to the 0.0.1 artifact after
    // performing the update.
    sim.inventory_edit_latest_low_level("empty status", |collection| {
        collection.internal_dns_generation_status = IdOrdMap::new();
        Ok(())
    })
    .unwrap();
    sim_assert_planning_makes_no_changes(
        &mut sim,
        AssertPlanningMakesNoChangesMode::InputUnchanged,
    );

    logctx.cleanup_successful();
}

/// Ensure that planning to update all zones terminates.
#[test]
fn test_update_all_zones() {
    static TEST_NAME: &str = "update_all_zones";
    let logctx = test_setup_log(TEST_NAME);

    // Use our example system.
    let mut sim = ReconfiguratorCliTestState::new(TEST_NAME, &logctx.log);
    sim.load_example_customized(|builder| builder.with_target_release_0_0_1())
        .expect("loaded example system");
    let blueprint1 = sim.assert_latest_blueprint_is_blippy_clean();

    // All zones should be sourced from the 0.0.1 repo by default.
    assert!(
        blueprint1
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .all(|(_, z)| matches!(
                &z.image_source,
                BlueprintZoneImageSource::Artifact { version, hash: _ }
                    if version == &BlueprintArtifactVersion::Available {
                        version: ArtifactVersion::new_const("0.0.1")
                    }
            ))
    );

    // Manually specify a TUF repo with fake images for all zones.
    // Only the name and kind of the artifacts matter.
    let version = ArtifactVersion::new_static("2.0.0-freeform")
        .expect("can't parse artifact version");
    let fake_hash = ArtifactHash([0; 32]);
    let image_source = BlueprintZoneImageSource::Artifact {
        version: BlueprintArtifactVersion::Available {
            version: version.clone(),
        },
        hash: fake_hash,
    };
    // We use generation 2 to represent the first generation with a TUF repo
    // attached.
    let description = TargetReleaseDescription::TufRepo(TufRepoDescription {
        repo: TufRepoMeta {
            hash: fake_hash,
            targets_role_version: 0,
            valid_until: Utc::now(),
            system_version: Version::new(1, 0, 0),
            file_name: String::from(""),
        },
        artifacts: create_zone_artifacts_at_version(&version),
    });

    sim.change_description("set new target release", |desc| {
        desc.set_target_release_and_old_repo(description);
        Ok(())
    })
    .unwrap();

    /// Expected number of planner iterations required to converge.
    /// If incidental planner work changes this value occasionally,
    /// that's fine; but if we find we're changing it all the time,
    /// we should probably drop it and keep just the maximum below.
    const EXP_PLANNING_ITERATIONS: usize = 57;

    /// Planning must not take more than this number of iterations.
    const MAX_PLANNING_ITERATIONS: usize = 100;
    assert!(EXP_PLANNING_ITERATIONS < MAX_PLANNING_ITERATIONS);

    let mut parent = blueprint1;
    for i in 2..=MAX_PLANNING_ITERATIONS {
        sim_update_collection_from_blueprint(&mut sim, &parent);

        let blueprint = sim.run_planner().expect("planning succeeded");

        let BlueprintSource::Planner(report) = &blueprint.source else {
            panic!("unexpected source: {:?}", blueprint.source);
        };
        eprintln!("{report}\n");
        // TODO: more report testing

        {
            let summary = blueprint.diff_since_blueprint(&parent);
            if summary.total_zones_added() == 0
                && summary.total_zones_removed() == 0
                && summary.total_zones_modified() == 0
                && summary.before.nexus_generation
                    == summary.after.nexus_generation
            {
                assert!(
                    blueprint
                        .all_omicron_zones(
                            BlueprintZoneDisposition::is_in_service
                        )
                        .all(|(_, zone)| zone.image_source == image_source),
                    "failed to update all zones"
                );

                assert_eq!(
                    i, EXP_PLANNING_ITERATIONS,
                    "expected {EXP_PLANNING_ITERATIONS} iterations \
                     but converged in {i}"
                );
                println!("planning converged after {i} iterations");

                logctx.cleanup_successful();
                return;
            }
        }
        parent = blueprint;
    }

    panic!("did not converge after {MAX_PLANNING_ITERATIONS} iterations");
}
