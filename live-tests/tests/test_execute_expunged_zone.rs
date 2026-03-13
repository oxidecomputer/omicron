// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod common;

use crate::common::reconfigurator::blueprint_load_target_enabled;
use anyhow::Context;
use common::LiveTestContext;
use common::reconfigurator::blueprint_edit_current_target_disabled;
use live_tests_macros::live_test;
use nexus_lockstep_client::types::BlueprintTargetSet;
use nexus_lockstep_client::types::LastResult;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::ReconfiguratorConfig;
use nexus_types::deployment::ReconfiguratorConfigParam;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use sled_agent_types::inventory::ZoneKind;
use slog::Logger;
use slog::info;
use std::collections::BTreeSet;
use std::time::Duration;
use strum::IntoEnumIterator;
use update_engine::EventBuffer;
use update_engine::ExecutionStatus;
use update_engine::NestedError;
use update_engine::NestedSpec;
use update_engine::TerminalKind;
use update_engine::events::EventReport;
use update_engine::events::StepOutcome;

#[live_test]
async fn test_execute_expunged_zone(lc: &LiveTestContext) {
    let log = lc.log();

    for zone_kind in ZoneKind::iter() {
        match zone_kind {
            // Skip multinode Clickhouse for now - we don't deploy it and have
            // no plans to change this in the near future.
            ZoneKind::ClickhouseKeeper | ZoneKind::ClickhouseServer => {
                continue;
            }

            // We expect all these zone types to exist, and we want to test what
            // happens if we execute a zone of this type with the `expunged`
            // disposition when we never executed that same zone with the
            // `in-service` disposition.
            ZoneKind::BoundaryNtp
            | ZoneKind::Clickhouse
            | ZoneKind::CockroachDb
            | ZoneKind::Crucible
            | ZoneKind::CruciblePantry
            | ZoneKind::ExternalDns
            | ZoneKind::InternalDns
            | ZoneKind::InternalNtp
            | ZoneKind::Nexus
            | ZoneKind::Oximeter => {
                let log =
                    log.new(slog::o!("zone-kind" => format!("{zone_kind:?}")));
                info!(log, "starting inner test");
                test_execute_expunged_zone_of_kind(zone_kind, lc).await;
            }
        }
    }
}

async fn test_execute_expunged_zone_of_kind(
    zone_kind: ZoneKind,
    lc: &LiveTestContext,
) {
    // Test setup
    let log = lc.log();

    let initial_nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
    let nexus = initial_nexus_clients.first().expect("internal Nexus client");

    // Safety check: We expect the going-in state to be "blueprint execution is
    // enabled". If it isn't, bail out and don't attempt to run this test.
    blueprint_load_target_enabled(log, nexus)
        .await
        .expect("blueprint execution should be enabled");

    // The high-level sequence here is:
    //
    // 1. Pause blueprint execution and planning
    // 2. Expunge a zone of kind `zone_kind`
    // 3. Run the planner; it should place a new zone of `zone_kind`
    // 4. Expunge that new zone
    // 5. Run the planner; it should place another new zone of `zone_kind`
    // 6. Enable execution
    //
    // Execution in step 6 should succeed. It will be executing a blueprint that
    // has the following changes since the last time execution ran:
    //
    // * One zone that had been in-service the last time a blueprint was
    //   executed is now expunged
    // * One zone that did not exist the last time a blueprint was executed
    //   exists in the blueprint but with the expunged disposition
    // * One zone that did not exist the last time a blueprint was executed
    //   exists and is should be put into service

    // Disable planning and execution.
    disable_blueprint_planning(nexus, log).await;
    disable_blueprint_execution(nexus, log).await;

    let mut orig_zones_of_interest = None;
    blueprint_edit_current_target_disabled(log, nexus, |builder| {
        let zones_of_interest = ZonesOfInterest::from_zones(
            builder.current_in_service_zones(),
            zone_kind,
        );

        let (sled_id, zone_id) = zones_of_interest.pick_any_zone_to_expunge();

        builder
            .sled_expunge_zone(sled_id, zone_id)
            .context("expunging zone")?;

        orig_zones_of_interest = Some(zones_of_interest);

        Ok(())
    })
    .await
    .expect("edited blueprint to expunge a zone");
    let orig_zones_of_interest = orig_zones_of_interest.unwrap();

    // Run the planner.
    let planned_bp_1 = nexus.blueprint_regenerate().await.expect("ran planner");

    // It should have added a new zone of the kind we care about.
    let plan1_zones_of_interest =
        ZonesOfInterest::from_zones(planned_bp_1.in_service_zones(), zone_kind);
    let plan1_zones_added =
        plan1_zones_of_interest.zones_added_since(&orig_zones_of_interest);
    assert_eq!(
        plan1_zones_added.len(),
        1,
        "expected planner to add exactly 1 new zone, \
         but got {plan1_zones_added:?}"
    );

    // Make that blueprint the target, but keep execution disabled.
    nexus
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: false,
            target_id: planned_bp_1.id,
        })
        .await
        .expect("set new target");

    // Edit the newly-planned blueprint and expunge the zone it just added.
    blueprint_edit_current_target_disabled(log, nexus, |builder| {
        let (sled_id, zone_id) = plan1_zones_added.first().copied().unwrap();
        builder
            .sled_expunge_zone(sled_id, zone_id)
            .context("expunging zone")?;
        Ok(())
    })
    .await
    .expect("edited blueprint to expunge a zone");

    // Run the planner again.
    let planned_bp_2 = nexus.blueprint_regenerate().await.expect("ran planner");

    // It should have added a new zone of the kind we care about.
    let plan2_zones_of_interest =
        ZonesOfInterest::from_zones(planned_bp_2.in_service_zones(), zone_kind);
    let plan2_zones_added =
        plan2_zones_of_interest.zones_added_since(&orig_zones_of_interest);
    assert_eq!(
        plan2_zones_added.len(),
        1,
        "expected planner to add exactly 1 new zone, \
         but got {plan2_zones_added:?}"
    );

    // Make that blueprint the target, but and finally enable execution.
    nexus
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: true,
            target_id: planned_bp_2.id,
        })
        .await
        .expect("set new target");

    // Wait until we see execution of this latest blueprint.
    wait_for_condition(
        || async {
            let status = nexus
                .bgtask_view("blueprint_executor")
                .await
                .expect("got status for `blueprint_executor` bgtask")
                .into_inner()
                .last;

            let details = match status {
                LastResult::NeverCompleted => {
                    return Err(CondCheckError::NotYet);
                }
                LastResult::Completed(completed) => completed.details,
            };

            #[derive(Deserialize)]
            struct BlueprintExecutorStatus {
                target_id: BlueprintUuid,
                execution_error: Option<NestedError>,
                event_report: EventReport<NestedSpec>,
            }

            let details: BlueprintExecutorStatus =
                serde_json::from_value(details)
                    .expect("parsed details as JSON");

            // Make sure we're looking at the execution of the blueprint we care
            // about; otherwise, keep waiting.
            if details.target_id != planned_bp_2.id {
                return Err(CondCheckError::NotYet);
            }

            // Check that execution completely cleanly.
            if let Some(err) = details.execution_error {
                return Err(CondCheckError::Failed(format!(
                    "execution error: {err}"
                )));
            }

            if let Some(err) = event_report_has_problems(details.event_report) {
                return Err(CondCheckError::Failed(format!(
                    "execution problem: {err}"
                )));
            }

            Ok(())
        },
        &Duration::from_secs(1),
        &Duration::from_secs(120),
    )
    .await
    .expect("waited for successful execution");

    // Log the IDs of the zone in this state.
    let (sled_id, zone_id) = plan1_zones_added.first().copied().unwrap();
    info!(
        log,
        "got successful execution of blueprint with \
         expunged-and-never-in-service zone";
        "sled_id" => %sled_id,
        "zone_id" => %zone_id,
    );
}

async fn disable_blueprint_planning(
    nexus: &nexus_lockstep_client::Client,
    log: &Logger,
) {
    let current_config = nexus
        .reconfigurator_config_show_current()
        .await
        .expect("got current reconfigurator config")
        .into_inner();

    if !current_config.config.planner_enabled {
        info!(log, "skipping disable of blueprint planning: already disabled");
    }

    let new_config = ReconfiguratorConfigParam {
        version: current_config.version + 1,
        config: ReconfiguratorConfig {
            planner_enabled: false,
            planner_config: current_config.config.planner_config,
            tuf_repo_pruner_enabled: current_config
                .config
                .tuf_repo_pruner_enabled,
        },
    };

    nexus
        .reconfigurator_config_set(&new_config)
        .await
        .expect("applied config to disable planner");

    info!(log, "disabled blueprint planning");
}

async fn disable_blueprint_execution(
    nexus: &nexus_lockstep_client::Client,
    log: &Logger,
) {
    let target_blueprint = nexus
        .blueprint_target_view()
        .await
        .expect("got current target blueprint");

    if !target_blueprint.enabled {
        info!(log, "blueprint execution already disabled");
        return;
    }

    nexus
        .blueprint_target_set_enabled(&BlueprintTargetSet {
            enabled: false,
            target_id: target_blueprint.target_id,
        })
        .await
        .expect("disabled blueprint execution");

    info!(log, "disabled blueprint execution");
}

struct ZonesOfInterest {
    in_service_zones: BTreeSet<(SledUuid, OmicronZoneUuid)>,
}

impl ZonesOfInterest {
    fn from_zones<'a>(
        zones: impl Iterator<Item = (SledUuid, &'a BlueprintZoneConfig)>,
        zone_kind: ZoneKind,
    ) -> Self {
        let mut in_service_zones = BTreeSet::new();

        for (sled_id, zone_cfg) in zones {
            if zone_cfg.kind() != zone_kind {
                continue;
            }

            in_service_zones.insert((sled_id, zone_cfg.id));
        }

        Self { in_service_zones }
    }

    fn pick_any_zone_to_expunge(&self) -> (SledUuid, OmicronZoneUuid) {
        self.in_service_zones
            .first()
            .copied()
            .expect("no zones of relevant kind found")
    }

    fn zones_added_since(
        &self,
        other: &Self,
    ) -> BTreeSet<(SledUuid, OmicronZoneUuid)> {
        self.in_service_zones
            .difference(&other.in_service_zones)
            .copied()
            .collect()
    }
}

fn event_report_has_problems(
    event_report: EventReport<NestedSpec>,
) -> Option<&'static str> {
    let mut buf = EventBuffer::default();
    buf.add_event_report(event_report);

    // Check for outright failure.
    let Some(summary) = buf.root_execution_summary() else {
        return Some("missing root_execution_summary");
    };
    match summary.execution_status {
        ExecutionStatus::Terminal(info) => match info.kind {
            TerminalKind::Completed => (),
            TerminalKind::Failed => return Some("execution failed"),
            TerminalKind::Aborted => return Some("execution aborted"),
        },
        ExecutionStatus::NotStarted => return Some("execution not started"),
        ExecutionStatus::Running { .. } => {
            return Some("execution still running");
        }
    }

    // Also look for warnings.
    for (_, step_data) in buf.iter_steps_recursive() {
        if let Some(reason) = step_data.step_status().completion_reason() {
            if let Some(info) = reason.step_completed_info() {
                match info.outcome {
                    StepOutcome::Success { .. }
                    | StepOutcome::Skipped { .. } => (),
                    StepOutcome::Warning { .. } => {
                        return Some("execution has warnings");
                    }
                }
            }
        }
    }

    None
}
