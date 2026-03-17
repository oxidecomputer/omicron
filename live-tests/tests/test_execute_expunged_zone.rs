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
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use sled_agent_types::inventory::ZoneKind;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
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
    let opctx = lc.opctx();
    let datastore = lc.datastore();

    // Safety check: If running this test multiple times, we may leave behind
    // underreplicated cockroach ranges. We shouldn't attempt to proceed if
    // those haven't been repair yet. Get the latest inventory collection and
    // check.
    match datastore.inventory_get_latest_collection(opctx).await {
        Ok(Some(collection)) => {
            let expected_nodes = COCKROACHDB_REDUNDANCY;
            let db_status = collection.cockroach_status;
            if db_status.len() < expected_nodes {
                panic!(
                    "refusing to run live test: \
                    latest inventory only has status from {} cockroach nodes; \
                    expected {expected_nodes}",
                    db_status.len()
                );
            }
            for (node_id, status) in db_status {
                match status.ranges_underreplicated {
                    Some(0) => (),
                    _ => panic!(
                        "refusing to run live test: inventory reports {:?} \
                         for ranges underreplicated on CRDB node {node_id}",
                        status.ranges_underreplicated,
                    ),
                }
                match status.liveness_live_nodes {
                    Some(n) if n == expected_nodes as u64 => (),
                    _ => panic!(
                        "refusing to run live test: inventory reports {:?} \
                         for live nodes on CRDB node {node_id}",
                        status.liveness_live_nodes,
                    ),
                }
            }
        }
        Ok(None) => panic!(
            "refusing to run live test: no inventory collections exist yet"
        ),
        Err(err) => panic!(
            "refusing to run live test: \
             error fetching inventory collection: {}",
            InlineErrorChain::new(&err),
        ),
    }

    for zone_kind in ZoneKind::iter() {
        match zone_kind {
            // Skip multinode Clickhouse for now - we don't deploy it and have
            // no plans to change this in the near future.
            ZoneKind::ClickhouseKeeper | ZoneKind::ClickhouseServer => {
                continue;
            }

            // Skip internal DNS for now - our test relies on the planner
            // immediately replacing a zone we expunge. The planner (correctly!)
            // does not do this for internal DNS: it has to wait for inventory
            // to reflect that the zone has been expunged, which can't happen
            // during our test because we've disabled execution.
            ZoneKind::InternalDns => continue,

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
    let (_, edit1) =
        blueprint_edit_current_target_disabled(log, nexus, |builder| {
            let zones_of_interest = ZonesOfInterest::from_zones(
                builder.current_in_service_zones(),
                zone_kind,
            );

            let (sled_id, zone_id) =
                zones_of_interest.pick_any_zone_to_expunge();

            builder
                .sled_expunge_zone(sled_id, zone_id)
                .context("expunging zone")?;

            orig_zones_of_interest = Some(zones_of_interest);

            Ok(())
        })
        .await
        .expect("edited blueprint to expunge a zone");
    let orig_zones_of_interest = orig_zones_of_interest.unwrap();
    info!(
        log, "edited blueprint first time: expunged 1 zone";
        "blueprint_id" => %edit1.id,
    );

    // Run the planner.
    let planned_bp_1 = nexus.blueprint_regenerate().await.expect("ran planner");
    info!(
        log, "ran planner first time; expected one new zone to be placed";
        "blueprint_id" => %planned_bp_1.id,
    );

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
    let (_, edit2) =
        blueprint_edit_current_target_disabled(log, nexus, |builder| {
            let (sled_id, zone_id) =
                plan1_zones_added.first().copied().unwrap();
            builder
                .sled_expunge_zone(sled_id, zone_id)
                .context("expunging zone")?;
            Ok(())
        })
        .await
        .expect("edited blueprint to expunge a zone");
    info!(
        log, "edited blueprint second time: expunged 1 zone";
        "blueprint_id" => %edit2.id,
    );

    // Run the planner again.
    let planned_bp_2 = nexus.blueprint_regenerate().await.expect("ran planner");
    info!(
        log, "ran planner second time; expected one new zone to be placed";
        "blueprint_id" => %planned_bp_2.id,
    );

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

    // Make that blueprint the target, and finally enable execution.
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
                    info!(
                        log,
                        "still waiting for execution: task never completed"
                    );
                    return Err(CondCheckError::NotYet);
                }
                LastResult::Completed(completed) => completed.details,
            };

            #[derive(Deserialize)]
            struct BlueprintExecutorStatus {
                target_id: BlueprintUuid,
                execution_error: Option<NestedError>,
                event_report: Result<EventReport<NestedSpec>, NestedError>,
            }

            // We won't be able to parse the `details` we expect if the
            // `bgtask_view()` we just executed is still returning the status
            // from a previous execution attempt of a disabled blueprint, so
            // return `NotYet` until we can parse them.
            let details: BlueprintExecutorStatus = match serde_json::from_value(
                details,
            ) {
                Ok(details) => details,
                Err(err) => {
                    info!(
                        log,
                        "still waiting for execution: failed to parse details";
                        InlineErrorChain::new(&err),
                    );
                    return Err(CondCheckError::NotYet);
                }
            };

            // Make sure we're looking at the execution of the blueprint we care
            // about; otherwise, keep waiting.
            if details.target_id != planned_bp_2.id {
                info!(
                    log,
                    "still waiting for execution: executed blueprint ID \
                     does not match ID we're waiting for";
                    "executed_id" => %details.target_id,
                    "waiting_for_id" => %planned_bp_2.id,
                );
                return Err(CondCheckError::NotYet);
            }

            // Check that execution completely cleanly.
            if let Some(err) = details.execution_error {
                return Err(CondCheckError::Failed(format!(
                    "execution error: {}",
                    InlineErrorChain::new(&err),
                )));
            }

            match details.event_report {
                Ok(event_report) => {
                    match event_report_problems(event_report, log) {
                        // Success!
                        None => Ok(()),
                        // Some warnings when executing new zones are expected
                        // (e.g., failure to talk to a zone immediately after
                        // starting it), but we expect those to clear up
                        // quickly.
                        Some(EventReportProblems::HadWarnings) => {
                            Err(CondCheckError::NotYet)
                        }
                        Some(EventReportProblems::Fatal(err)) => {
                            Err(CondCheckError::Failed(format!(
                                "execution problem: {err}"
                            )))
                        }
                    }
                }
                Err(err) => Err(CondCheckError::Failed(format!(
                    "no available event report: {}",
                    InlineErrorChain::new(&err),
                ))),
            }
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
        return;
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

enum EventReportProblems {
    Fatal(&'static str),
    HadWarnings,
}

fn event_report_problems(
    event_report: EventReport<NestedSpec>,
    log: &Logger,
) -> Option<EventReportProblems> {
    let mut buf = EventBuffer::default();
    buf.add_event_report(event_report);

    // Check for outright failure.
    let Some(summary) = buf.root_execution_summary() else {
        return Some(EventReportProblems::Fatal(
            "missing root_execution_summary",
        ));
    };
    match summary.execution_status {
        ExecutionStatus::Terminal(info) => match info.kind {
            TerminalKind::Completed => (),
            TerminalKind::Failed => {
                return Some(EventReportProblems::Fatal("execution failed"));
            }
            TerminalKind::Aborted => {
                return Some(EventReportProblems::Fatal("execution aborted"));
            }
        },
        ExecutionStatus::NotStarted => {
            return Some(EventReportProblems::Fatal("execution not started"));
        }
        ExecutionStatus::Running { .. } => {
            return Some(EventReportProblems::Fatal("execution still running"));
        }
    }

    // Also look for warnings.
    let mut had_warnings = false;
    for (_, step_data) in buf.iter_steps_recursive() {
        if let Some(reason) = step_data.step_status().completion_reason() {
            if let Some(info) = reason.step_completed_info() {
                match &info.outcome {
                    StepOutcome::Success { .. }
                    | StepOutcome::Skipped { .. } => (),
                    StepOutcome::Warning { message, .. } => {
                        warn!(
                            log,
                            "exeucution warning";
                            "message" => %message,
                            "step" => %step_data.step_info().description,
                        );
                        had_warnings = true;
                    }
                }
            }
        }
    }

    if had_warnings { Some(EventReportProblems::HadWarnings) } else { None }
}
