// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod common;

use anyhow::Context;
use common::LiveTestContext;
use common::reconfigurator::blueprint_edit_current_target;
use futures::TryStreamExt;
use live_tests_macros::live_test;
use nexus_client::types::BackgroundTasksActivateRequest;
use nexus_client::types::BlueprintTargetSet;
use nexus_client::types::Saga;
use nexus_client::types::SagaState;
use nexus_inventory::CollectionBuilder;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::Planner;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::PlannerChickenSwitches;
use nexus_types::deployment::SledFilter;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use slog::{debug, info};
use std::net::SocketAddrV6;
use std::time::Duration;

// TODO-coverage This test could check other stuff:
//
// - that after adding:
//   - the new Nexus appears in external DNS
//   - we can _use_ the new Nexus from the outside
//     (e.g., using an `oxide_client` using a custom reqwest resolver that
//     points only at that one IP so that we can make sure we're always getting
//     that one)
// - that after expungement, it doesn't appear in external DNS any more
//
#[live_test]
async fn test_nexus_add_remove(lc: &LiveTestContext) {
    // Test setup
    let log = lc.log();
    let opctx = lc.opctx();
    let datastore = lc.datastore();

    let chicken_switches = datastore
        .reconfigurator_chicken_switches_get_latest(opctx)
        .await
        .expect("obtained latest chicken switches")
        .map_or_else(PlannerChickenSwitches::default, |cs| {
            cs.switches.planner_switches
        });
    let planning_input =
        PlanningInputFromDb::assemble(&opctx, &datastore, chicken_switches)
            .await
            .expect("planning input");
    let collection = datastore
        .inventory_get_latest_collection(opctx)
        .await
        .expect("latest inventory collection")
        .unwrap_or_else(|| CollectionBuilder::new("test").build());
    let initial_nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
    let nexus = initial_nexus_clients.first().expect("internal Nexus client");

    // First, deploy a new Nexus zone to an arbitrary sled.
    let commissioned_sled_ids = planning_input
        .all_sled_ids(SledFilter::Commissioned)
        .collect::<Vec<_>>();
    let sled_id = *commissioned_sled_ids.first().expect("any sled id");
    let (blueprint1, blueprint2) = blueprint_edit_current_target(
        log,
        &planning_input,
        &collection,
        &nexus,
        &|builder: &mut BlueprintBuilder| {
            // We have to tell the builder what image source to use for the new
            // Nexus zone. If we were the planner, we'd check whether we have a
            // TUF repo (or two) then decide whether to use the image from one
            // of those or the install dataset. Instead of duplicating all of
            // that logic, we'll just find an existing Nexus zone and copy its
            // image source. This should always be right in this context; it
            // would only be wrong if there are existing Nexus zones with
            // different image sources, which would only be true in the middle
            // of an update.
            let image_source = commissioned_sled_ids
                .iter()
                .find_map(|&sled_id| {
                    builder
                        .current_sled_zones(
                            sled_id,
                            BlueprintZoneDisposition::is_in_service,
                        )
                        .find_map(|zone| {
                            if zone.zone_type.is_nexus() {
                                Some(zone.image_source.clone())
                            } else {
                                None
                            }
                        })
                })
                .context(
                    "could not find in-service Nexus in parent blueprint",
                )?;

            builder
                .sled_add_zone_nexus(sled_id, image_source)
                .context("adding Nexus zone")?;

            Ok(())
        },
    )
    .await
    .expect("editing blueprint to add zone");

    // Figure out which zone is new and make a new client for it.
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    let zones = diff.added_zones(&sled_id).expect("sled has added zones");
    let new_zone =
        zones.zones.first().expect("at least one added zone on that sled");
    assert_eq!(new_zone.kind(), ZoneKind::Nexus);
    let new_zone_addr = new_zone.underlay_ip();
    let new_zone_sockaddr =
        SocketAddrV6::new(new_zone_addr, NEXUS_INTERNAL_PORT, 0, 0);
    let new_zone_client = lc.specific_internal_nexus_client(new_zone_sockaddr);

    // Wait for the new Nexus zone to show up and be usable.
    let initial_sagas_list = wait_for_condition(
        || async {
            list_sagas(&new_zone_client).await.map_err(|e| {
                debug!(log,
                    "waiting for new Nexus to be available: listing sagas: {e:#}"
                );
                CondCheckError::<()>::NotYet
            })
        },
        &Duration::from_millis(50),
        &Duration::from_secs(60),
    )
    .await
    .expect("new Nexus to be usable");
    assert!(initial_sagas_list.is_empty());
    info!(log, "new Nexus is online");

    // Create a demo saga from the new Nexus zone.  We'll use this to test that
    // when the zone is expunged, its saga gets moved to a different Nexus.
    let demo_saga = new_zone_client
        .saga_demo_create()
        .await
        .expect("new Nexus saga demo create");
    let saga_id = demo_saga.saga_id;
    let sagas_list =
        list_sagas(&new_zone_client).await.expect("new Nexus sagas_list");
    assert_eq!(sagas_list.len(), 1);
    assert_eq!(sagas_list[0].id, saga_id);
    info!(log, "created demo saga"; "demo_saga" => ?demo_saga);

    // Now expunge the zone we just created.
    let _ = blueprint_edit_current_target(
        log,
        &planning_input,
        &collection,
        &nexus,
        &|builder: &mut BlueprintBuilder| {
            builder
                .sled_expunge_zone(sled_id, new_zone.id)
                .context("expunging zone")?;
            Ok(())
        },
    )
    .await
    .expect("editing blueprint to expunge zone");

    // At some point, we should be unable to reach this Nexus any more.
    wait_for_condition(
        || async {
            match new_zone_client.saga_list(None, None, None).await {
                Err(nexus_client::Error::CommunicationError(error)) => {
                    info!(log, "expunged Nexus no longer reachable";
                        "error" => slog_error_chain::InlineErrorChain::new(&error),
                    );
                    Ok(())
                }
                Ok(_) => {
                    debug!(log, "expunged Nexus is still reachable");
                    Err(CondCheckError::<()>::NotYet)
                }
                Err(error) => {
                    debug!(log, "expunged Nexus is still reachable";
                        "error" => slog_error_chain::InlineErrorChain::new(&error),
                    );
                    Err(CondCheckError::NotYet)
                }
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(60),
    )
    .await
    .unwrap();

    // We want to see another Nexus instance pick up the saga.
    //
    // For that to happen, inventory must first reflect that the Nexus we
    // expunged is really gone.  Then we must run through another planning
    // round.
    //
    // First, kick one Nexus instance's inventory collector.  Otherwise, it
    // might take a while for the system to notice this zone is gone.  Having
    // activated the task, it shouldn't take too long to get an inventory
    info!(log, "activating inventory collector");
    nexus
        .bgtask_activate(&BackgroundTasksActivateRequest {
            bgtask_names: vec![String::from("inventory_collection")],
        })
        .await
        .expect("activating inventory background task");
    let latest_collection = wait_for_condition(
        || async {
            let latest_collection = datastore
                .inventory_get_latest_collection(opctx)
                .await
                .expect("latest inventory collection")
                .expect("have a latest inventory collection");
            debug!(log, "got inventory"; "id" => %latest_collection.id);
            let agent = latest_collection.sled_agents.get(&sled_id).expect(
                "collection information for the sled we added a Nexus to",
            );
            if let Some(config) = &agent.ledgered_sled_config {
                if config.zones.iter().any(|z| z.id == new_zone.id) {
                    debug!(log, "zone still present in ledger");
                    return Err(CondCheckError::<()>::NotYet);
                }
            }
            if let Some(config) = agent
                .last_reconciliation
                .as_ref()
                .map(|lr| &lr.last_reconciled_config)
            {
                if config.zones.iter().any(|z| z.id == new_zone.id) {
                    debug!(log, "zone still present in inventory");
                    return Err(CondCheckError::<()>::NotYet);
                }
            }
            return Ok(latest_collection);
        },
        &Duration::from_millis(3000),
        &Duration::from_secs(90),
    )
    .await
    .expect("waiting for zone to be gone from inventory");

    // Now run through the planner.
    info!(log, "running through planner");
    let planning_input =
        PlanningInputFromDb::assemble(&opctx, &datastore, chicken_switches)
            .await
            .expect("planning input");
    let (_, parent_blueprint) = datastore
        .blueprint_target_get_current_full(opctx)
        .await
        .expect("getting latest target blueprint");
    let planner = Planner::new_based_on(
        log.clone(),
        &parent_blueprint,
        &planning_input,
        "live test suite",
        &latest_collection,
        PlannerRng::from_entropy(),
    )
    .expect("constructing planner");
    let new_blueprint = planner.plan().expect("creating blueprint");

    // The new blueprint ought to have our zone expunged and ready for cleanup.
    // We don't need to check this here.  It just provides a better error
    // message if something has gone wrong up to this point.
    let (_, expunged_zone_config) = new_blueprint
        .all_omicron_zones(|_| true)
        .find(|(_sled_id, zone_config)| zone_config.id == new_zone.id)
        .expect("expunged zone in new blueprint");
    assert!(expunged_zone_config.disposition.is_ready_for_cleanup());

    // Now make this the current target.
    info!(
        log,
        "setting new blueprint target";
        "blueprint_id" => ?new_blueprint.id
    );
    nexus
        .blueprint_import(&new_blueprint)
        .await
        .expect("importing new blueprint");
    nexus
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: true,
            target_id: new_blueprint.id,
        })
        .await
        .expect("setting target blueprint");

    // At this point, blueprint execution should re-assign the saga.
    // Wait for that to happen and then for another Nexus instance to pick up
    // the saga.
    let nexus_found = wait_for_condition(
        || async {
            for nexus_client in &initial_nexus_clients {
                assert!(nexus_client.baseurl() != new_zone_client.baseurl());
                let Ok(sagas) = list_sagas(&nexus_client).await else {
                    continue;
                };

                debug!(log, "found sagas (last): {:?}", sagas);
                if sagas.into_iter().any(|s| s.id == saga_id) {
                    return Ok(nexus_client);
                }
            }

            return Err(CondCheckError::<()>::NotYet);
        },
        &Duration::from_millis(1000),
        &Duration::from_secs(120),
    )
    .await
    .unwrap();

    info!(log, "found saga in a different Nexus instance";
        "saga_id" => %saga_id,
        "found_nexus" => nexus_found.baseurl(),
    );
    assert!(nexus_found.baseurl() != new_zone_client.baseurl());

    // Now, complete the demo saga on whichever instance is running it now.
    // `saga_demo_complete` is not synchronous.  It just unblocks the saga.
    // We'll need to poll a bit to wait for it to finish.
    nexus_found
        .saga_demo_complete(&demo_saga.demo_saga_id)
        .await
        .expect("complete demo saga");
    let found = wait_for_condition(
        || async {
            let sagas = list_sagas(&nexus_found).await.expect("listing sagas");
            debug!(log, "found sagas (last): {:?}", sagas);
            let found = sagas.into_iter().find(|s| s.id == saga_id).unwrap();
            if matches!(found.state, SagaState::Succeeded) {
                Ok(found)
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(30),
    )
    .await
    .unwrap();

    assert_eq!(found.id, saga_id);
    assert!(matches!(found.state, SagaState::Succeeded));
}

async fn list_sagas(
    client: &nexus_client::Client,
) -> Result<Vec<Saga>, anyhow::Error> {
    client
        .saga_list_stream(None, None)
        .try_collect::<Vec<_>>()
        .await
        .context("listing sagas")
}
