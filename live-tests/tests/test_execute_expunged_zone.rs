// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod common;

use crate::common::reconfigurator::blueprint_load_target_enabled;
use anyhow::Context;
use anyhow::bail;
use chrono::Utc;
use common::LiveTestContext;
use common::reconfigurator::blueprint_edit_current_target_disabled;
use dns_service_client::ClientInfo;
use live_tests_macros::live_test;
use nexus_lockstep_client::types::BackgroundTasksActivateRequest;
use nexus_lockstep_client::types::BlueprintTargetSet;
use nexus_lockstep_client::types::LastResult;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ReconfiguratorConfig;
use nexus_types::deployment::ReconfiguratorConfigParam;
use nexus_types::inventory::Collection;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::BlueprintUuid;
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
            if let Err(err) =
                validate_cockroach_is_healthy_according_to_inventory(
                    &collection,
                )
            {
                panic!("refusing to run live test: {err:#}");
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
    let mut nexus =
        initial_nexus_clients.first().expect("internal Nexus client");

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

            let (sled_id, zone) = zones_of_interest.pick_any_zone_to_expunge();

            builder
                .sled_expunge_zone(sled_id, zone.id)
                .context("expunging zone")?;

            // If we're expunging a Nexus, check and see if we should switch to
            // a different Nexus for the remainder of this test. This is pretty
            // hacky.
            if let BlueprintZoneType::Nexus(nexus_cfg) = &zone.zone_type {
                let client_baseurl = nexus.baseurl();
                let expunged_host =
                    format!("[{}]", nexus_cfg.lockstep_address().ip());
                info!(
                    log,
                    "checking current Nexus client's `baseurl` \
                     against the Nexus zone we're expunging";
                    "client_baseurl" => %client_baseurl,
                    "expunged_host" => %expunged_host,
                );

                if client_baseurl.contains(&expunged_host) {
                    nexus = initial_nexus_clients
                        .get(1)
                        .expect("at least two Nexus clients available");
                    info!(log, "swapping to another Nexus client");
                }
            }

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
            let (sled_id, zone) = plan1_zones_added.first().unwrap();
            builder
                .sled_expunge_zone(*sled_id, zone.id)
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
            let status = match nexus.bgtask_view("blueprint_executor").await {
                Ok(task_view) => task_view.into_inner().last,
                Err(err) => {
                    // We don't generally expect this to fail, but it could if
                    // we're testing Nexus or Cockroach (or recently did), since
                    // either of those being expunged may cause transient errors
                    // attempting to fetch task status from an arbitrary Nexus
                    // present in DNS.
                    warn!(
                        log,
                        "failed to get blueprint_executor status from Nexus";
                        InlineErrorChain::new(&err),
                    );
                    return Err(CondCheckError::NotYet);
                }
            };

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
                warn!(
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
                warn!(
                    log, "execution had an error";
                    InlineErrorChain::new(&err),
                );
                return Err(CondCheckError::NotYet);
            }

            match details.event_report {
                Ok(event_report) => {
                    // If the event report indicates any problems, we'll try
                    // again - we expect to see some transient problems and
                    // warnings, but also expect them to clear up on their own
                    // pretty quickly.
                    if event_report_has_problems(event_report, log) {
                        Err(CondCheckError::NotYet)
                    } else {
                        Ok(())
                    }
                }
                Err(err) => Err(CondCheckError::Failed(format!(
                    "no available event report: {}",
                    InlineErrorChain::new(&err),
                ))),
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(180),
    )
    .await
    .expect("waited for successful execution");

    // Log the IDs of the zone in this state.
    let (sled_id, zone) = plan1_zones_added.first().unwrap();
    info!(
        log,
        "got successful execution of blueprint with \
         expunged-and-never-in-service zone";
        "sled_id" => %sled_id,
        "zone_id" => %zone.id,
    );

    // If we just expunged a cockroach node, wait for the cluster to be healthy
    // again before we return.
    if zone_kind == ZoneKind::CockroachDb {
        wait_for_cockroach_cluster_to_be_healthy(nexus, lc, log).await;
    }
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
    in_service_zones: BTreeSet<(SledUuid, BlueprintZoneConfig)>,
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

            in_service_zones.insert((sled_id, zone_cfg.clone()));
        }

        Self { in_service_zones }
    }

    fn pick_any_zone_to_expunge(&self) -> (SledUuid, BlueprintZoneConfig) {
        self.in_service_zones
            .first()
            .cloned()
            .expect("no zones of relevant kind found")
    }

    fn zones_added_since(
        &self,
        other: &Self,
    ) -> BTreeSet<(SledUuid, BlueprintZoneConfig)> {
        self.in_service_zones
            .difference(&other.in_service_zones)
            .cloned()
            .collect()
    }
}

fn event_report_has_problems(
    event_report: EventReport<NestedSpec>,
    log: &Logger,
) -> bool {
    let mut buf = EventBuffer::default();
    buf.add_event_report(event_report);

    // Check for outright failure.
    let Some(summary) = buf.root_execution_summary() else {
        warn!(log, "event report missing root_execution_summary");
        return true;
    };
    match summary.execution_status {
        ExecutionStatus::Terminal(info) => match info.kind {
            TerminalKind::Completed => (),
            TerminalKind::Failed | TerminalKind::Aborted => {
                warn!(
                    log, "execution ended with unexpected terminal status";
                    "stats" => ?info.kind,
                );
                return true;
            }
        },
        ExecutionStatus::NotStarted | ExecutionStatus::Running { .. } => {
            warn!(
                log, "execution has unexpected execution status";
                "stats" => ?summary.execution_status,
            );
            return true;
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
                            log, "execution warning";
                            "message" => %message,
                            "step" => %step_data.step_info().description,
                        );
                        had_warnings = true;
                    }
                }
            }
        }
    }

    had_warnings
}

async fn wait_for_cockroach_cluster_to_be_healthy(
    nexus: &nexus_lockstep_client::Client,
    lc: &LiveTestContext,
    log: &Logger,
) {
    let wait_start_time = Utc::now();
    let inventory_collector_bgtask = vec!["inventory_collection".to_string()];

    info!(log, "starting to wait for cockroachdb to be reported healthy");
    wait_for_condition(
        || async {
            // Attempt to activate inventory collection; ignore any errors here,
            // since activation might fail transiently.
            match nexus
                .bgtask_activate(&BackgroundTasksActivateRequest {
                    bgtask_names: inventory_collector_bgtask.clone(),
                })
                .await
            {
                Ok(_) => (),
                Err(err) => {
                    warn!(
                        log, "failed to activate inventory collector bg task";
                        InlineErrorChain::new(&err),
                    );
                }
            }

            // Get a new datastore - if we're waiting for cockroach to be
            // healthy, it's possible we expunged the cockroach that `lc` cached
            // and returns via its `.datastore()` method. Look up cockroach in
            // DNS again in every iteration of this check.
            let (opctx, datastore) = match lc.new_datastore_connection().await {
                Ok((opctx, datastore)) => (opctx, datastore),
                Err(err) => {
                    warn!(
                        log, "failed to establish new datastore connection";
                        InlineErrorChain::new(&*err),
                    );
                    return Err(CondCheckError::NotYet);
                }
            };

            // Load the latest inventory collection.
            let collection_result =
                datastore.inventory_get_latest_collection(&opctx).await;

            // Cleanly terminate this newly-created datastore.
            datastore.terminate().await;

            let collection = match collection_result {
                Ok(Some(collection)) => collection,
                // This should never happen: we know from starting the test
                // that at least one inventory collection already existed.
                Ok(None) => {
                    return Err(CondCheckError::Failed(
                        "no inventory collections exist?!",
                    ));
                }
                Err(err) => {
                    warn!(
                        log, "failed to load latest inventory collection";
                        InlineErrorChain::new(&err),
                    );
                    return Err(CondCheckError::NotYet);
                }
            };

            // Wait until the inventory collection is sufficiently new.
            if collection.time_started < wait_start_time {
                info!(
                    log, "waiting for new inventory collection";
                    "latest-collection-started-at" => %collection.time_started,
                    "must-be-newer-than" => %wait_start_time,
                );
                return Err(CondCheckError::NotYet);
            }

            // Is cockroach healthy?
            match validate_cockroach_is_healthy_according_to_inventory(
                &collection,
            ) {
                Ok(()) => Ok(()),
                Err(err) => {
                    warn!(
                        log, "cockroach cluster is not yet healthy";
                        InlineErrorChain::new(&*err),
                    );
                    Err(CondCheckError::NotYet)
                }
            }
        },
        &Duration::from_secs(1),
        // Even on freshly-set-up systems, it can take more than 10 minutes for
        // CRDB to become healthy after an expungement. Is 20 generous enough?
        &Duration::from_secs(20 * 60),
    )
    .await
    .expect("cockroach cluster failed to become healthy sufficently quickly");
}

fn validate_cockroach_is_healthy_according_to_inventory(
    collection: &Collection,
) -> anyhow::Result<()> {
    let expected_nodes = COCKROACHDB_REDUNDANCY;

    let db_status = &collection.cockroach_status;
    if db_status.len() < expected_nodes {
        bail!(
            "latest inventory only has status from {} cockroach nodes; \
             expected {expected_nodes}",
            db_status.len()
        );
    }

    for (node_id, status) in db_status {
        match status.ranges_underreplicated {
            Some(0) => (),
            _ => bail!(
                "inventory reports {:?} for ranges underreplicated on \
                 CRDB node {node_id}",
                status.ranges_underreplicated,
            ),
        }
        match status.liveness_live_nodes {
            Some(n) if n == expected_nodes as u64 => (),
            _ => bail!(
                "inventory reports {:?} for live nodes on CRDB node {node_id}",
                status.liveness_live_nodes,
            ),
        }
    }

    Ok(())
}
