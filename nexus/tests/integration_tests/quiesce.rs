// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, anyhow};
use nexus_auth::context::OpContext;
use nexus_client::types::QuiesceState;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_reconfigurator_preparation::PlanningInputFromDb;
use nexus_test_interface::NexusServer;
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::PlannerConfig;
use omicron_common::api::external::Error;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::GenericUuid;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// Tests that Nexus quiesces when the blueprint says that it should
#[nexus_test]
async fn test_quiesce(cptestctx: &ControlPlaneTestContext) {
    let log = &cptestctx.logctx.log;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());
    let nexus_internal_url = format!(
        "http://{}",
        cptestctx.server.get_http_server_internal_address().await
    );
    let nexus_client =
        nexus_client::Client::new(&nexus_internal_url, log.clone());

    // Collect what we need to modify the blueprint.
    let collection = wait_for_condition(
        || async {
            let collection = datastore
                .inventory_get_latest_collection(&opctx)
                .await
                .map_err(CondCheckError::Failed)?;
            match collection {
                Some(s) => Ok(s),
                None => Err(CondCheckError::<Error>::NotYet),
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await
    .expect("initial inventory collection");

    let chicken_switches = datastore
        .reconfigurator_chicken_switches_get_latest(&opctx)
        .await
        .expect("obtained latest chicken switches")
        .map_or_else(PlannerConfig::default, |cs| cs.config.planner_config);
    let planning_input =
        PlanningInputFromDb::assemble(&opctx, &datastore, chicken_switches)
            .await
            .expect("planning input");
    let target_blueprint = nexus
        .blueprint_target_view(&opctx)
        .await
        .expect("fetch current target config");
    let blueprint1 = nexus
        .blueprint_view(&opctx, *target_blueprint.target_id.as_untyped_uuid())
        .await
        .expect("fetch current target blueprint");

    // Now, update the target blueprint to reflect that Nexus should quiesce.
    // We don't need it to be enabled to still reflect quiescing.
    let mut builder = BlueprintBuilder::new_based_on(
        log,
        &blueprint1,
        &planning_input,
        &collection,
        "test-suite",
        PlannerRng::from_entropy(),
    )
    .expect("creating BlueprintBuilder");
    builder.set_nexus_generation(blueprint1.nexus_generation.next());
    let blueprint2 = builder.build();
    nexus
        .blueprint_import(&opctx, blueprint2.clone())
        .await
        .expect("importing new blueprint");
    nexus
        .blueprint_target_set(
            &opctx,
            BlueprintTargetSet { enabled: false, target_id: blueprint2.id },
        )
        .await
        .expect("setting new target");

    // Wait for Nexus to quiesce.
    let _ = wait_for_condition(
        || async {
            let quiesce = nexus_client
                .quiesce_get()
                .await
                .context("fetching quiesce state")
                .map_err(CondCheckError::Failed)?
                .into_inner();
            eprintln!("quiesce state: {:#?}\n", quiesce);
            match quiesce.state {
                QuiesceState::Undetermined => {
                    Err(CondCheckError::Failed(anyhow!(
                        "quiesce state should have been determined before \
                         test started"
                    )))
                }
                QuiesceState::Running => Err(CondCheckError::NotYet),
                QuiesceState::DrainingSagas { .. }
                | QuiesceState::DrainingDb { .. }
                | QuiesceState::RecordingQuiesce { .. }
                | QuiesceState::Quiesced { .. } => Ok(()),
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(30),
    )
    .await
    .expect("Nexus should have quiesced");
}
