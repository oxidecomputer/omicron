// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod common;

use crate::common::reconfigurator::blueprint_load_target_enabled;
use crate::common::reconfigurator::blueprint_wait_sled_configs_propagated;
use anyhow::Context;
use common::LiveTestContext;
use common::reconfigurator::blueprint_edit_current_target;
use live_tests_macros::live_test;
use nexus_client::types::QuiesceState;
use nexus_db_model::DbMetadataNexusState;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::blueprint_zone_type;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::debug;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::time::Duration;

#[live_test]
async fn test_nexus_handoff(lc: &LiveTestContext) {
    // Test setup
    let log = lc.log();
    let opctx = lc.opctx();
    let datastore = lc.datastore();
    let initial_nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
    let nexus = initial_nexus_clients.first().expect("internal Nexus client");

    // Make sure we're starting from a known-normal state.
    // First, we have an enabled target blueprint.
    let blueprint1 = blueprint_load_target_enabled(log, nexus)
        .await
        .expect("loading initial target blueprint");
    // That blueprint should be propagated to all sleds.  We wait just a bit
    // here to deal with races set up by other tests failing or other ongoing
    // activity.
    let collection = blueprint_wait_sled_configs_propagated(
        opctx,
        datastore,
        &blueprint1,
        nexus,
        Duration::from_secs(15),
    )
    .await
    .expect("verifying initial blueprints' sled configs propagated");
    // Check that there's no Nexus handoff already pending.  That means that
    // there exist no Nexus zones with a generation newer than the blueprint's
    // `nexus_generation`.
    let new_zones = blueprint1
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, z)| {
            let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nexus_generation,
                ..
            }) = &z.zone_type
            else {
                return None;
            };
            (*nexus_generation > blueprint1.nexus_generation).then_some(z.id)
        })
        .collect::<Vec<_>>();
    if !new_zones.is_empty() {
        panic!(
            "handoff in progress!  found zones with generation newer than \
             current blueprint generation ({}): {}",
            blueprint1.nexus_generation,
            new_zones
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Identify the current generation of Nexus zones.
    struct CurrentNexusZone<'a> {
        sled_id: SledUuid,
        image_source: &'a BlueprintZoneImageSource,
        cfg: &'a blueprint_zone_type::Nexus,
    }
    let current_nexus_zones: BTreeMap<OmicronZoneUuid, _> = blueprint1
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(sled_id, z)| {
            let BlueprintZoneType::Nexus(
                cfg @ blueprint_zone_type::Nexus { nexus_generation, .. },
            ) = &z.zone_type
            else {
                return None;
            };
            (*nexus_generation == blueprint1.nexus_generation).then(|| {
                (
                    z.id,
                    CurrentNexusZone {
                        sled_id,
                        image_source: &z.image_source,
                        cfg,
                    },
                )
            })
        })
        .collect();
    assert!(
        !current_nexus_zones.is_empty(),
        "found no Nexus instances at current nexus generation"
    );
    let nexus_clients: BTreeMap<_, _> = current_nexus_zones
        .iter()
        .map(|(id, current)| {
            (
                id,
                lc.specific_internal_nexus_client(current.cfg.internal_address),
            )
        })
        .collect();

    // All Nexus instances ought to be running normally, not quiesced.
    info!(log, "checking current Nexus status");
    for (id, nexus) in &nexus_clients {
        let qq = nexus
            .quiesce_get()
            .await
            .expect("fetching quiesce state")
            .into_inner();
        let QuiesceState::Running = qq.state else {
            panic!("Nexus {id} is in unexpected quiesce state: {qq:?}");
        };
    }
    info!(log, "all test prerequisites complete");

    // We're finally ready to start the test!
    //
    // For each zone in the current generation, create a replacement at the next
    // generation.
    let next_generation = blueprint1.nexus_generation.next();
    let planner_config = datastore
        .reconfigurator_config_get_latest(opctx)
        .await
        .expect("obtained latest reconfigurator config")
        .map_or_else(PlannerConfig::default, |cs| cs.config.planner_config);
    let planning_input =
        PlanningInputFromDb::assemble(opctx, datastore, planner_config)
            .await
            .expect("planning input");
    let (_blueprint1, blueprint2) = blueprint_edit_current_target(
        log,
        &planning_input,
        &collection,
        &nexus,
        &|builder: &mut BlueprintBuilder| {
            for current_nexus in current_nexus_zones.values() {
                builder
                    .sled_add_zone_nexus(
                        current_nexus.sled_id,
                        current_nexus.image_source.clone(),
                        next_generation,
                    )
                    .context("adding Nexus zone")?;
            }
            Ok(())
        },
    )
    .await
    .expect("editing blueprint to add zones");
    info!(
        log,
        "wrote new target blueprint with new Nexus zones";
        "blueprint_id" => %blueprint2.id
    );

    // Find the new Nexus zones and make clients for them.
    let new_nexus_clients = blueprint2
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, z)| {
            let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nexus_generation,
                internal_address,
                ..
            }) = &z.zone_type
            else {
                return None;
            };
            (*nexus_generation == next_generation).then(|| {
                (z.id, lc.specific_internal_nexus_client(*internal_address))
            })
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(new_nexus_clients.len(), current_nexus_zones.len());

    // Wait for the zones to be running.
    // (This does not mean that their Nexus instances are running.)
    let collection = blueprint_wait_sled_configs_propagated(
        opctx,
        datastore,
        &blueprint2,
        nexus,
        Duration::from_secs(180),
    )
    .await
    .expect("three new Nexus zones running");
    info!(log, "blueprint configs propagated"; "blueprint_id" => %blueprint2.id);

    // Check that the db_metadata_nexus records for the new Nexus instances
    // exist.
    let new_records = datastore
        .database_nexus_access_all(
            opctx,
            &new_nexus_clients.keys().cloned().collect(),
        )
        .await
        .expect("fetching db_metadata_nexus records");
    assert_eq!(new_records.len(), new_nexus_clients.len());
    assert!(
        new_records.iter().all(|r| r.state() == DbMetadataNexusState::NotYet)
    );

    // Create a demo saga that will hold up quiescing.
    let demo_nexus =
        nexus_clients.values().next().expect("at least one Nexus client");
    let demo_saga =
        demo_nexus.saga_demo_create().await.expect("demo saga create");
    info!(log, "created demo saga"; "demo_saga" => ?demo_saga);

    // Now update the target blueprint to trigger a handoff.
    let planning_input =
        PlanningInputFromDb::assemble(opctx, datastore, planner_config)
            .await
            .expect("planning input");
    let (_blueprint2, blueprint3) = blueprint_edit_current_target(
        log,
        &planning_input,
        &collection,
        &nexus,
        &|builder: &mut BlueprintBuilder| {
            builder.set_nexus_generation(next_generation);
            Ok(())
        },
    )
    .await
    .expect("editing blueprint to bump nexus_generation");
    info!(
        log,
        "wrote new target blueprint with new nexus generation";
        "blueprint_id" => %blueprint3.id
    );

    // The old Nexus zones should pretty soon report that they're quiescing, but
    // not quiesced because of the one outstanding saga.
    for (id, nexus) in &nexus_clients {
        wait_for_condition(
            || async {
                let qq = nexus
                    .quiesce_get()
                    .await
                    .expect("fetching quiesce status")
                    .into_inner();
                debug!(
                    log,
                    "Nexus quiesce state";
                    "nexus_id" => %id,
                    "state" => ?qq.state
                );
                match qq.state {
                    QuiesceState::DrainingSagas { .. } => Ok(()),
                    _ => Err(CondCheckError::<()>::NotYet),
                }
            },
            &Duration::from_secs(1),
            &Duration::from_secs(90),
        )
        .await
        .expect("waiting for Nexus to report quiescing");

        info!(log, "Nexus reports quiescing"; "nexus_id" => %id);
    }

    // At this point, we should not yet be able to reach the new Nexus instances
    // because they're sitting waiting for the database to be ready for handoff.
    for (id, nexus) in &new_nexus_clients {
        if let Ok(_) = nexus.quiesce_get().await {
            panic!("unexpectedly reached Nexus {id}");
        }
    }
    info!(log, "new Nexus instances are still not reachable (good)");

    // Complete the demo saga to unblock quiescing.
    demo_nexus
        .saga_demo_complete(&demo_saga.demo_saga_id)
        .await
        .expect("complete demos aga");

    // Now wait for the old Nexus zones to report being fully quiesced.
    for (id, nexus) in &nexus_clients {
        wait_for_condition(
            || async {
                let qq = nexus
                    .quiesce_get()
                    .await
                    .expect("fetching quiesce status")
                    .into_inner();
                debug!(
                    log,
                    "Nexus quiesce state";
                    "nexus_id" => %id,
                    "state" => ?qq.state
                );
                match qq.state {
                    QuiesceState::Quiesced { .. } => Ok(()),
                    _ => Err(CondCheckError::<()>::NotYet),
                }
            },
            &Duration::from_secs(1),
            &Duration::from_secs(90),
        )
        .await
        .expect("waiting for old Nexus to report quiesced");

        info!(log, "Nexus reports quiesced"; "nexus_id" => %id);
    }
    info!(log, "all old Nexus instances report quiescing!");

    // Now wait for the new Nexus zones to report being online.
    for (id, nexus) in &new_nexus_clients {
        wait_for_condition(
            || async {
                match nexus.quiesce_get().await {
                    Ok(qq) => {
                        debug!(
                            log,
                            "new Nexus quiesce state";
                            "state" => ?qq.state
                        );
                        match qq.state {
                            QuiesceState::Undetermined => {
                                Err(CondCheckError::<()>::NotYet)
                            }
                            QuiesceState::Running => Ok(()),
                            _ => panic!("unexpected new Nexus quiesce state"),
                        }
                    }
                    Err(error) => {
                        debug!(
                            log,
                            "error fetching new Nexus quiesce state";
                            InlineErrorChain::new(&error),
                        );
                        Err(CondCheckError::NotYet)
                    }
                }
            },
            &Duration::from_secs(1),
            &Duration::from_secs(90),
        )
        .await
        .expect("waiting for old Nexus to report quiesced");
        info!(log, "new Nexus reports running"; "id" => %id);
    }
    info!(log, "all new Nexus instances report running!");

    // Clean up: expunge the old Nexus instances.
    let planning_input =
        PlanningInputFromDb::assemble(opctx, datastore, planner_config)
            .await
            .expect("planning input");
    let new_nexus =
        new_nexus_clients.values().next().expect("one new Nexus client");
    let (_blueprint3, blueprint4) = blueprint_edit_current_target(
        log,
        &planning_input,
        &collection,
        new_nexus,
        &|builder: &mut BlueprintBuilder| {
            for (id, current_zone) in &current_nexus_zones {
                builder
                    .sled_expunge_zone(current_zone.sled_id, *id)
                    .context("expunging zone")?;
            }

            Ok(())
        },
    )
    .await
    .expect("editing blueprint to expunge old Nexus zones");
    info!(
        log,
        "wrote new target blueprint with expunged zones";
        "blueprint_id" => %blueprint4.id
    );

    // Wait for this to get propagated everywhere.
    let _latest_collection = blueprint_wait_sled_configs_propagated(
        opctx,
        datastore,
        &blueprint4,
        new_nexus,
        Duration::from_secs(120),
    )
    .await
    .expect("waiting for blueprint4 sled configs");
}
