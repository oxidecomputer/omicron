// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod common;

use anyhow::Context;
use assert_matches::assert_matches;
use common::reconfigurator::blueprint_edit_current_target;
use common::LiveTestContext;
use futures::TryStreamExt;
use live_tests_macros::live_test;
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
    let planning_input = PlanningInputFromDb::assemble(&opctx, &datastore)
        .await
        .expect("planning input");
    let initial_nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
    let nexus = initial_nexus_clients.first().expect("internal Nexus client");

    // First, deploy a new Nexus zone to an arbitrary sled.
    let sled_id = planning_input
        .all_sled_ids(SledFilter::Commissioned)
        .next()
        .expect("any sled id");
    let (blueprint1, blueprint2) = blueprint_edit_current_target(
        log,
        &planning_input,
        &nexus,
        &|builder: &mut BlueprintBuilder| {
            let nnexus = builder
                .sled_num_running_zones_of_kind(sled_id, ZoneKind::Nexus);
            let count = builder
                .sled_ensure_zone_multiple_nexus(sled_id, nnexus + 1)
                .context("adding Nexus zone")?;
            assert_matches!(
                count,
                EnsureMultiple::Changed { added: 1, removed: 0 }
            );
            Ok(())
        },
    )
    .await
    .expect("editing blueprint to add zone");

    // Figure out which zone is new and make a new client for it.
    let diff = blueprint2.diff_since_blueprint(&blueprint1);
    let new_zone = diff
        .zones
        .added
        .values()
        .next()
        .expect("at least one sled with added zones")
        .zones
        .first()
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
        &nexus,
        &|builder: &mut BlueprintBuilder| {
            builder
                .sled_expunge_zone(sled_id, new_zone.id())
                .context("expunging zone")
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

    // Wait for some other Nexus instance to pick up the saga.
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
        &Duration::from_millis(50),
        &Duration::from_secs(60),
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
