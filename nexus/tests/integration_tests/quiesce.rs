// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, anyhow};
use nexus_auth::context::OpContext;
use nexus_lockstep_client::types::QuiesceState;
use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
use nexus_reconfigurator_planning::planner::PlannerRng;
use nexus_test_interface::NexusServer;
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintTargetSet;
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
    let nexus_lockstep_url = format!(
        "http://{}",
        cptestctx.server.get_http_server_lockstep_address().await
    );
    let nexus_client =
        nexus_lockstep_client::Client::new(&nexus_lockstep_url, log.clone());

    // Load the current target blueprint.
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
        "test-suite",
        PlannerRng::from_entropy(),
    )
    .expect("creating BlueprintBuilder");
    builder.set_nexus_generation(blueprint1.nexus_generation.next());
    let blueprint2 = builder.build(BlueprintSource::Test);
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
