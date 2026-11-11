// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, anyhow};
use nexus_lockstep_client::types::QuiesceState;
use nexus_test_interface::NexusServer;
use nexus_test_utils_macros::nexus_test;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// Tests that Nexus quiesces when the blueprint says that it should
#[nexus_test]
async fn test_quiesce(cptestctx: &ControlPlaneTestContext) {
    let log = &cptestctx.logctx.log;
    let nexus_lockstep_url = format!(
        "http://{}",
        cptestctx.server.get_http_server_lockstep_address(),
    );
    let nexus_client =
        nexus_lockstep_client::Client::new(&nexus_lockstep_url, log.clone());

    // Now, update the target blueprint to reflect that Nexus should quiesce.
    // We don't need it to be enabled to still reflect quiescing.
    cptestctx
        .blueprint_edit_current_target(|builder| {
            builder.set_nexus_generation(builder.nexus_generation().next());
            builder.comment("quiesce Nexus");
            Ok(())
        })
        .await
        .expect("edited blueprint to quiesce Nexus");

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
                QuiesceState::Running => {
                    Err(CondCheckError::NotYet { status: None })
                }
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
