// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for the expunge precheck's control plane check
//!
//! Tests for the distributed storage check are with the Crucible replacement
//! tests.  Tests for recovery from sled and disk expungement are unit tests in
//! `nexus/src/app/expunge_precheck/control_plane.rs`.  Those use a simulated
//! system because the test environment here does not have enough zones to
//! satisfy the control plane check.

use crate::integration_tests::crucible_replacements::expunge_precheck_get;
use nexus_test_utils::background::run_blueprint_planner;
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::ReconfiguratorConfig;
use nexus_types::deployment::ReconfiguratorConfigParam;
use nexus_types::internal_api::background::BlueprintPlannerStatus;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_expunge_precheck_control_plane(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // The planner is disabled by default in the test context.  Until it has
    // run with its config loaded, it reports a different status, so keep
    // activating it until it reports that it is disabled.
    wait_for_condition(
        || async {
            match run_blueprint_planner(&cptestctx.lockstep_client).await {
                BlueprintPlannerStatus::Disabled => Ok(()),
                other => Err(CondCheckError::<()>::NotYet {
                    status: Some(format!("planner status: {other:?}")),
                }),
            }
        },
        &Duration::from_millis(500),
        &Duration::from_secs(60),
    )
    .await
    .expect("planner should have reported being disabled within 60s");

    // With the planner disabled, there's no way to know whether the control
    // plane has recovered, so the check does not pass.
    let status = expunge_precheck_get(client).await;
    assert!(!status.ok, "{status:#?}");
    assert_eq!(
        status.control_plane.risks,
        [String::from(
            "the most recent blueprint planning attempt failed (blueprint \
             planning is disabled), so it is not known whether the control \
             plane has recovered"
        )],
    );

    // Enable the planner.
    let nexus_client = cptestctx.lockstep_client();
    let config_initial = nexus_client
        .reconfigurator_config_show_current()
        .await
        .expect("load initial reconfigurator config")
        .into_inner();
    nexus_client
        .reconfigurator_config_set(&ReconfiguratorConfigParam {
            version: config_initial.version + 1,
            config: ReconfiguratorConfig {
                planner_enabled: true,
                ..config_initial.config
            },
        })
        .await
        .expect("enable blueprint planner");

    // Once the planner has run, the check evaluates the planner's blueprint
    // against the database and inventory.  The simulated system has fewer
    // CockroachDB and internal DNS zones than a real system needs, so the check
    // still does not pass.  But it should no longer report that planning
    // failed.
    let status = wait_for_condition(
        || async {
            let status = expunge_precheck_get(client).await;
            if status.control_plane.risks.iter().any(|risk| {
                risk.contains("planning attempt failed")
                    || risk.contains("has not yet run")
            }) {
                Err(CondCheckError::<()>::NotYet {
                    status: Some(format!("expunge precheck: {status:?}")),
                })
            } else {
                Ok(status)
            }
        },
        &Duration::from_millis(500),
        &Duration::from_secs(60),
    )
    .await
    .expect("planner results should have appeared within 60s");
    assert!(!status.ok, "{status:#?}");
    assert!(!status.control_plane.risks.is_empty(), "{status:#?}");
}
