// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod common;

use anyhow::Context;
use assert_matches::assert_matches;
use common::LiveTestContext;
use futures::TryStreamExt;
use nexus_client::types::BlueprintTargetSet;
use nexus_client::types::Saga;
use nexus_client::types::SagaState;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::blueprint_builder::EnsureMultiple;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::SledFilter;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_test_utils::dev::poll::CondCheckError;
use slog::{debug, info, o};
use std::net::SocketAddrV6;
use std::time::Duration;

// XXX-dap clean up in general
// XXX-dap clean up logging
#[tokio::test]
async fn test_nexus_add_remove() {
    let lc = LiveTestContext::new("test_nexus_add_remove").await.unwrap();
    let log = lc.log();
    let mylog = lc.log().new(o!("top-level" => true));

    let nexus =
        lc.any_internal_nexus_client().await.expect("internal Nexus client");
    info!(mylog, "have Nexus client");

    // Fetch the current target blueprint.
    let target_blueprint = nexus
        .blueprint_target_view()
        .await
        .expect("fetch target config")
        .into_inner();
    if !target_blueprint.enabled {
        panic!("refusing to modify a system with target blueprint disabled");
    }

    let blueprint1 = nexus
        .blueprint_view(&target_blueprint.target_id)
        .await
        .expect("fetch target blueprint")
        .into_inner();
    info!(mylog, "have current blueprint");

    // Assemble a new blueprint based on the current target that adds a Nexus
    // zone on some sled.
    let opctx = lc.opctx();
    let datastore = lc.datastore();
    let planning_input = PlanningInputFromDb::assemble(&opctx, &datastore)
        .await
        .expect("planning input");
    info!(mylog, "have PlanningInput");

    let mut builder = BlueprintBuilder::new_based_on(
        log,
        &blueprint1,
        &planning_input,
        "test-suite",
    )
    .expect("BlueprintBuilder");

    let sled_id = planning_input
        .all_sled_ids(SledFilter::Commissioned)
        .next()
        .expect("any sled id");

    let nnexus =
        builder.sled_num_running_zones_of_kind(sled_id, ZoneKind::Nexus);
    let count = builder
        .sled_ensure_zone_multiple_nexus(sled_id, nnexus + 1)
        .expect("adding Nexus");
    assert_matches!(count, EnsureMultiple::Changed { added: 1, removed: 0 });
    let blueprint2 = builder.build();
    info!(mylog, "built new blueprint";
        "blueprint1_id" => %blueprint1.id,
        "blueprint2_id" => %blueprint2.id
    );

    // Make this the new target.
    nexus.blueprint_import(&blueprint2).await.expect("importing new blueprint");
    nexus
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: true,
            target_id: blueprint2.id,
        })
        .await
        .expect("setting new target");
    info!(mylog, "imported blueprint"; "blueprint_id" => %blueprint2.id);

    // Figure out which zone is new and make a new client for it.
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    let new_zone = diff
        .zones
        .added
        .values()
        .next()
        .expect("at least one sled with added zones")
        .zones
        .iter()
        .next()
        .expect("at least one added zone on that sled");
    assert_eq!(new_zone.kind(), ZoneKind::Nexus);
    let new_zone_addr = new_zone.underlay_address();
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
    info!(mylog, "new Nexus is online");

    // Create a demo saga.
    let demo_saga = new_zone_client
        .saga_demo_create()
        .await
        .expect("new Nexus saga demo create");
    let saga_id = demo_saga.saga_id;

    let sagas_list =
        list_sagas(&new_zone_client).await.expect("new Nexus sagas_list");
    assert_eq!(sagas_list.len(), 1);
    assert_eq!(sagas_list[0].id, saga_id);
    info!(mylog, "ready to expunge");

    let mut builder = BlueprintBuilder::new_based_on(
        log,
        &blueprint2,
        &planning_input,
        "test-suite",
    )
    .expect("BlueprintBuilder");
    builder.sled_expunge_zone(sled_id, new_zone.id()).expect("expunge zone");
    let blueprint3 = builder.build();
    nexus.blueprint_import(&blueprint3).await.expect("importing new blueprint");
    nexus
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: true,
            target_id: blueprint3.id,
        })
        .await
        .expect("setting new target");
    info!(mylog, "imported blueprint with Nexus expunged and set target";
        "blueprint_id" => %blueprint3.id
    );

    // Wait for some other Nexus instance to pick up the saga.
    let nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
    let nexus_found = wait_for_condition(
        || async {
            for nexus_client in &nexus_clients {
                let Ok(sagas) = list_sagas(&nexus_client).await else {
                    continue;
                };

                debug!(mylog, "found sagas (last): {:?}", sagas);
                if sagas.into_iter().any(|s| s.id == saga_id) {
                    return Ok(nexus_client);
                }
            }

            return Err(CondCheckError::<()>::NotYet);
        },
        &Duration::from_millis(50),
        &Duration::from_secs(60),
    )
    .await
    .unwrap();

    info!(mylog, "found saga in previous Nexus instance");
    // Now, complete it on this instance.
    nexus_found
        .saga_demo_complete(&demo_saga.demo_saga_id)
        .await
        .expect("complete demo saga");

    // Completion is not synchronous -- that just unblocked the saga.  So we
    // need to poll a bit to wait for it to actually finish.
    let found = wait_for_condition(
        || async {
            let sagas = list_sagas(&nexus_found).await.expect("listing sagas");
            debug!(mylog, "found sagas (last): {:?}", sagas);
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

    lc.cleanup_successful();
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
