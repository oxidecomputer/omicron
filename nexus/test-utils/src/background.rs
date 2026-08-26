// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions related to Nexus background tasks

use crate::http_testing::NexusRequest;
use dropshot::test_util::ClientTestContext;
use nexus_lockstep_client::types::BackgroundTask;
use nexus_lockstep_client::types::CurrentStatus;
use nexus_lockstep_client::types::LastResult;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSetError;
use nexus_types::internal_api::background::*;
use omicron_test_utils::dev::poll::{self, CondCheckError, wait_for_condition};
use omicron_uuid_kinds::BlueprintUuid;
use slog::info;
use slog::warn;
use std::time::Duration;

/// Given the name of a background task, wait for it to complete if it's
/// running, then return the last polled `BackgroundTask` object. Panics if the
/// task has never been activated.
pub async fn wait_background_task(
    lockstep_client: &ClientTestContext,
    task_name: &str,
) -> BackgroundTask {
    // Wait for the task to finish
    let last_task_poll = wait_for_condition(
        || async {
            let task = NexusRequest::object_get(
                lockstep_client,
                &format!("/bgtasks/view/{task_name}"),
            )
            .execute_and_parse_unwrap::<BackgroundTask>()
            .await;

            // Wait until the task has actually run and then is idle
            if matches!(&task.current, CurrentStatus::Idle) {
                match &task.last {
                    LastResult::Completed(_) => Ok(task),
                    LastResult::NeverCompleted => {
                        panic!("task never activated")
                    }
                }
            } else {
                Err(CondCheckError::<()>::NotYet { status: None })
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(60),
    )
    .await
    .unwrap();

    last_task_poll
}

/// Given the name of a background task, activate it, then wait for it to
/// complete. Return the `BackgroundTask` object from this invocation.
///
/// The `timeout` parameter controls how long to wait for the task to go idle
/// before activating it, and how long to wait for it to complete after
/// activation. Defaults to 10 seconds if not specified.
pub async fn activate_background_task(
    lockstep_client: &ClientTestContext,
    task_name: &str,
) -> BackgroundTask {
    activate_background_task_with_timeout(
        lockstep_client,
        task_name,
        Duration::from_secs(10),
    )
    .await
}

/// Like `activate_background_task`, but with a configurable timeout.
///
/// Use this variant when you need a longer timeout.
pub async fn activate_background_task_with_timeout(
    lockstep_client: &ClientTestContext,
    task_name: &str,
    timeout: Duration,
) -> BackgroundTask {
    // If it is running, wait for an existing task to complete - this function
    // has to wait for _this_ activation to finish.
    //
    // If it has never run, this function will return straight away.
    let previous_task = wait_for_condition(
        || async {
            let task = NexusRequest::object_get(
                lockstep_client,
                &format!("/bgtasks/view/{task_name}"),
            )
            .execute_and_parse_unwrap::<BackgroundTask>()
            .await;

            if matches!(task.current, CurrentStatus::Idle) {
                return Ok(task);
            }

            info!(
                lockstep_client.client_log,
                "waiting for {task_name} to go idle",
            );

            Err(CondCheckError::<()>::NotYet { status: None })
        },
        &Duration::from_millis(50),
        &timeout,
    )
    .await
    .expect("task never went to idle");

    lockstep_client
        .make_request(
            http::Method::POST,
            "/bgtasks/activate",
            Some(serde_json::json!({
                "bgtask_names": vec![String::from(task_name)]
            })),
            http::StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // Wait for the task to finish
    //
    // Note: if another request to activate this background task occurred
    // concurrently, this loop will wait for that to complete, not our
    // activation (which would have been queued). This is ok: this function's
    // intention is to have an activation of the background task occur _after_
    // the call, and it doesn't matter which one lands first.
    let last_task_poll = wait_for_condition(
        || async {
            let task = NexusRequest::object_get(
                lockstep_client,
                &format!("/bgtasks/view/{task_name}"),
            )
            .execute_and_parse_unwrap::<BackgroundTask>()
            .await;

            // Wait until the task has actually run and then is idle
            if matches!(&task.current, CurrentStatus::Idle) {
                match (&previous_task.last, &task.last) {
                    (
                        LastResult::NeverCompleted,
                        LastResult::NeverCompleted,
                    ) => {
                        // task hasn't started yet
                        Err(CondCheckError::<()>::NotYet { status: None })
                    }

                    // task was activated for the first time by this function
                    // call (or a concurrent one!), and it's done now (because
                    // the task is idle)
                    (LastResult::NeverCompleted, LastResult::Completed(_)) => {
                        Ok(task)
                    }

                    // the task first reported that it completed, but now
                    // reports that it has never completed
                    (LastResult::Completed(_), LastResult::NeverCompleted) => {
                        panic!("completed, then never completed?!");
                    }

                    (
                        LastResult::Completed(last),
                        LastResult::Completed(current),
                    ) => {
                        if last.iteration < current.iteration {
                            Ok(task)
                        } else if last.iteration == current.iteration {
                            // task hasn't started yet
                            Err(CondCheckError::<()>::NotYet { status: None })
                        } else {
                            // last.iteration > current.iteration
                            panic!(
                                "last iteration {}, current iteration {}",
                                last.iteration, current.iteration,
                            );
                        }
                    }
                }
            } else {
                Err(CondCheckError::<()>::NotYet { status: None })
            }
        },
        &Duration::from_millis(50),
        &timeout,
    )
    .await
    .unwrap();

    last_task_poll
}

/// Run the abandoned_vmm_reaper background task, panicking if the activation
/// reported any errors.
pub async fn run_abandoned_vmm_reaper(
    lockstep_client: &ClientTestContext,
) -> AbandonedVmmReaperStatus {
    let last_background_task =
        activate_background_task(&lockstep_client, "abandoned_vmm_reaper")
            .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from abandoned_vmm_reaper task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<AbandonedVmmReaperStatus>(
        last_result_completed.details.clone(),
    )
    .unwrap_or_else(|error| {
        panic!(
            "abandoned_vmm_reaper status should deserialize as \
             AbandonedVmmReaperStatus: {error} (raw value: {:?})",
            last_result_completed.details,
        )
    });

    assert!(
        status.errors.is_empty(),
        "abandoned_vmm_reaper task should complete without errors: {status:?}",
    );

    status
}

/// Run the region_replacement background task, returning how many actions
/// were taken
pub async fn run_region_replacement(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task =
        activate_background_task(&lockstep_client, "region_replacement").await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from region_replacement task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<RegionReplacementStatus>(
        last_result_completed.details,
    )
    .unwrap();

    assert!(status.errors.is_empty());

    status.requests_created_ok.len()
        + status.start_invoked_ok.len()
        + status.requests_completed_ok.len()
}

/// Run the region_replacement_driver background task, returning how many actions
/// were taken
pub async fn run_region_replacement_driver(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task =
        activate_background_task(&lockstep_client, "region_replacement_driver")
            .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from region_replacement_driver task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<RegionReplacementDriverStatus>(
        last_result_completed.details,
    )
    .unwrap();

    assert!(status.errors.is_empty());

    status.drive_invoked_ok.len() + status.finish_invoked_ok.len()
}

/// Run the region_snapshot_replacement_start background task, returning how many
/// actions were taken
pub async fn run_region_snapshot_replacement_start(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &lockstep_client,
        "region_snapshot_replacement_start",
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from region_snapshot_replacement_start \
            task",
            last_background_task.last,
        );
    };

    let status =
        serde_json::from_value::<RegionSnapshotReplacementStartStatus>(
            last_result_completed.details,
        )
        .unwrap();

    assert!(status.errors.is_empty());

    status.requests_created_ok.len() + status.start_invoked_ok.len()
}

/// Run the region_snapshot_replacement_garbage_collection background task,
/// returning how many actions were taken
pub async fn run_region_snapshot_replacement_garbage_collection(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &lockstep_client,
        "region_snapshot_replacement_garbage_collection",
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from \
            region_snapshot_replacement_garbage_collection task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<
        RegionSnapshotReplacementGarbageCollectStatus,
    >(last_result_completed.details)
    .unwrap();

    assert!(status.errors.is_empty());

    status.garbage_collect_requested.len()
}

/// Run the region_snapshot_replacement_step background task, returning how many
/// actions were taken
pub async fn run_region_snapshot_replacement_step(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &lockstep_client,
        "region_snapshot_replacement_step",
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from region_snapshot_replacement_step \
            task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<RegionSnapshotReplacementStepStatus>(
        last_result_completed.details,
    )
    .unwrap();

    assert!(status.errors.is_empty());

    status.step_records_created_ok.len()
        + status.step_garbage_collect_invoked_ok.len()
        + status.step_invoked_ok.len()
}

/// Run the region_snapshot_replacement_finish background task, returning how many
/// actions were taken
pub async fn run_region_snapshot_replacement_finish(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &lockstep_client,
        "region_snapshot_replacement_finish",
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from region_snapshot_replacement_finish \
            task",
            last_background_task.last,
        );
    };

    let status =
        serde_json::from_value::<RegionSnapshotReplacementFinishStatus>(
            last_result_completed.details,
        )
        .unwrap();

    assert!(status.errors.is_empty());

    status.finish_invoked_ok.len()
}

/// Run the read_only_region_replacement_start background task, returning how
/// many actions were taken
pub async fn run_read_only_region_replacement_start(
    lockstep_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &lockstep_client,
        "read_only_region_replacement_start",
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from read_only_region_replacement_start \
            task",
            last_background_task.last,
        );
    };

    let status =
        serde_json::from_value::<ReadOnlyRegionReplacementStartStatus>(
            last_result_completed.details,
        )
        .unwrap();

    assert!(status.errors.is_empty());

    status.requests_created_ok.len()
}

/// Run all replacement related background tasks and return how many actions
/// were taken.
pub async fn run_all_crucible_replacement_tasks(
    lockstep_client: &ClientTestContext,
) -> usize {
    // region replacement related
    run_region_replacement(lockstep_client).await +
    run_region_replacement_driver(lockstep_client).await +
    // region snapshot replacement related
    run_region_snapshot_replacement_start(lockstep_client).await +
    run_region_snapshot_replacement_garbage_collection(lockstep_client).await +
    run_region_snapshot_replacement_step(lockstep_client).await +
    run_region_snapshot_replacement_finish(lockstep_client).await +
    run_read_only_region_replacement_start(lockstep_client).await
}

pub async fn wait_tuf_artifact_replication_step(
    lockstep_client: &ClientTestContext,
) -> TufArtifactReplicationStatus {
    let last_background_task =
        wait_background_task(&lockstep_client, "tuf_artifact_replication")
            .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from tuf_artifact_replication task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<TufArtifactReplicationStatus>(
        last_result_completed.details,
    )
    .unwrap();
    assert_eq!(status.last_run_counters.err(), 0);
    status
}

pub async fn run_tuf_artifact_replication_step(
    lockstep_client: &ClientTestContext,
) -> TufArtifactReplicationStatus {
    let last_background_task =
        activate_background_task(&lockstep_client, "tuf_artifact_replication")
            .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from tuf_artifact_replication task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<TufArtifactReplicationStatus>(
        last_result_completed.details,
    )
    .unwrap();
    assert_eq!(status.last_run_counters.err(), 0);
    status
}

/// Run the `blueprint_loader` background task.
///
/// The loader's watch channel is populated after this completes.
///
/// Panics if activation fails.
pub async fn run_blueprint_loader(
    lockstep_client: &ClientTestContext,
) -> BlueprintLoaded {
    run_blueprint_loader_with_timeout(lockstep_client, Duration::from_secs(10))
        .await
}

/// Like `run_blueprint_loader`, but with a configurable timeout for the
/// activation.
pub async fn run_blueprint_loader_with_timeout(
    lockstep_client: &ClientTestContext,
    timeout: Duration,
) -> BlueprintLoaded {
    let last_background_task = activate_background_task_with_timeout(
        &lockstep_client,
        "blueprint_loader",
        timeout,
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from blueprint_loader task",
            last_background_task.last,
        );
    };

    let details = last_result_completed.details;
    let status =
        serde_json::from_value::<BlueprintLoaderStatus>(details.clone())
            .unwrap_or_else(|error| {
                panic!(
                    "blueprint_loader status did not parse as \
             BlueprintLoaderStatus: {error}; details: {details:#}"
                )
            });
    match status {
        BlueprintLoaderStatus::Loaded(loaded) => loaded,
        BlueprintLoaderStatus::Error(error) => {
            panic!("blueprint_loader activation failed: {error}");
        }
        BlueprintLoaderStatus::ImmutableBlueprintChanged { target_id } => {
            panic!(
                "blueprint_loader found that target blueprint {target_id} \
                 changed, but blueprints are immutable"
            );
        }
    }
}

/// Activate the `blueprint_planner` background task, and return the status of
/// the next activation to complete.
///
/// This does not guarantee that exactly one activation runs, nor that the
/// returned status is from the activation requested by this call. That's
/// because the planner is also activated through other sources, and
/// `activate_background_task` returns whichever activation completes next.
pub async fn run_blueprint_planner(
    lockstep_client: &ClientTestContext,
) -> BlueprintPlannerStatus {
    run_blueprint_planner_with_timeout(lockstep_client, Duration::from_secs(10))
        .await
}

/// Like `run_blueprint_planner`, but with a configurable timeout for the
/// activation.
pub async fn run_blueprint_planner_with_timeout(
    lockstep_client: &ClientTestContext,
    timeout: Duration,
) -> BlueprintPlannerStatus {
    let last_background_task = activate_background_task_with_timeout(
        &lockstep_client,
        "blueprint_planner",
        timeout,
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from blueprint_planner task",
            last_background_task.last,
        );
    };

    let details = last_result_completed.details;
    serde_json::from_value::<BlueprintPlannerStatus>(details.clone())
        .unwrap_or_else(|error| {
            panic!(
                "blueprint_planner status did not parse as \
                 BlueprintPlannerStatus: {error}; details: {details:#}"
            )
        })
}

/// Repeatedly activate the `blueprint_planner` background task until it makes
/// a target blueprint other than `prior_target_id`, then run the
/// `blueprint_loader` task so that the new target is loaded.
///
/// Callers must:
///
/// * Enable the planner.
/// * Make a change that planning will act on (i.e., the planner should not
///   result in a no-op). Otherwise the planner reports `Unchanged`, which is
///   treated as a permanent failure.
/// * Pass in `prior_target_id` as the current target.
///
/// Returns the new target as the planner set it. Note that if another blueprint
/// is somehow generated and something else sets the new target in the meantime,
/// the loader holds that newer one instead. The new target should always be a
/// descendant of the returned target, though.
pub async fn run_blueprint_planner_until_new_target(
    lockstep_client: &ClientTestContext,
    prior_target_id: BlueprintUuid,
    timeout: Duration,
) -> BlueprintTarget {
    let result = wait_for_condition(
        || async {
            let status =
                run_blueprint_planner_with_timeout(lockstep_client, timeout)
                    .await;
            match status {
                BlueprintPlannerStatus::Targeted { target, .. } => Ok(target),
                BlueprintPlannerStatus::Unchanged { parent, .. }
                    if parent.target_id != prior_target_id =>
                {
                    // A watcher-triggered activation already made a new
                    // target, and the loader has loaded it.
                    Ok(parent)
                }
                BlueprintPlannerStatus::Disabled => {
                    // Either the caller forgot to enable the planner, or the
                    // config loader hasn't observed the caller's change yet.
                    // The former is not fixed here (it is documented as a
                    // precondition), but the latter will be fixed if the
                    // corresponding task is activated.
                    activate_planner_input_loader(
                        lockstep_client,
                        BlueprintPlannerSkipReason::ConfigNotYetLoaded,
                        timeout,
                    )
                    .await;
                    Err(CondCheckError::NotYet {
                        status: Some(
                            "planner disabled: either the caller has not \
                             enabled it, or the reconfigurator config loader \
                             has not yet observed the change"
                                .to_string(),
                        ),
                    })
                }
                BlueprintPlannerStatus::Unchanged {
                    parent, report, ..
                } => {
                    // This activation started after the caller's change landed,
                    // so if the change was visible to the planner, it really
                    // had nothing to do.
                    Err(CondCheckError::Failed(format!(
                        "planner found nothing to change from prior target \
                         {}; planning report:\n{report}",
                        parent.target_id,
                    )))
                }
                BlueprintPlannerStatus::Skipped(reason) => {
                    // Skipped means that something hasn't been loaded yet, so
                    // activate that loader and retry.
                    activate_planner_input_loader(
                        lockstep_client,
                        reason,
                        timeout,
                    )
                    .await;
                    Err(CondCheckError::NotYet {
                        status: Some(format!(
                            "planner skipped: {reason}; activated the \"{}\" \
                             task",
                            reason.loader_task_name(),
                        )),
                    })
                }
                BlueprintPlannerStatus::Planned {
                    parent,
                    error: BlueprintTargetSetError::ParentNotTarget { .. },
                    ..
                } => {
                    // This means the loader fell behind and the planner ran
                    // against a stale parent. Try again, hoping that this
                    // process comes to rest.
                    run_blueprint_loader_with_timeout(lockstep_client, timeout)
                        .await;
                    Err(CondCheckError::NotYet {
                        status: Some(format!(
                            "planner produced a blueprint from {}, but the \
                             target moved underneath it; loaded the current \
                             target",
                            parent.target_id,
                        )),
                    })
                }
                BlueprintPlannerStatus::Planned {
                    parent,
                    error:
                        error @ (BlueprintTargetSetError::NoSuchBlueprint { .. }
                        | BlueprintTargetSetError::Other(_)),
                    ..
                } => Err(CondCheckError::Failed(format!(
                    "planner produced a blueprint from {} but could not make \
                     it the target: {error}",
                    parent.target_id,
                ))),
                BlueprintPlannerStatus::Error(error) => {
                    Err(CondCheckError::Failed(format!(
                        "blueprint planner failed: {error}"
                    )))
                }
                BlueprintPlannerStatus::LimitReached { limit, .. } => {
                    Err(CondCheckError::Failed(format!(
                        "blueprint limit ({limit}) reached"
                    )))
                }
            }
        },
        &Duration::from_secs(1),
        &timeout,
    )
    .await;

    let new_target = match result {
        Ok(new_target) => new_target,
        Err(poll::Error::TimedOut { elapsed, last_status }) => {
            panic!(
                "blueprint planner did not make a new target blueprint \
                 (prior target {prior_target_id}) within {elapsed:?}; \
                 last status: {}",
                last_status.as_deref().unwrap_or("(none)"),
            );
        }
        Err(poll::Error::PermanentError(message)) => {
            panic!("blueprint planner cannot make a new target: {message}");
        }
    };

    // The planner notifies the loader through a watch channel, but only
    // asynchronously. Load the new target now so that the caller's next planner
    // activation plans from it. (In principle, this might load a different
    // target that's a descendant of this one -- log if so.)
    let loaded =
        run_blueprint_loader_with_timeout(lockstep_client, timeout).await;
    if loaded.target.target_id != new_target.target_id {
        warn!(
            lockstep_client.client_log,
            "blueprint loader loaded a different target than the planner \
             just set -- something else set a target in the meantime";
            "planner_target_id" => %new_target.target_id,
            "loaded_target_id" => %loaded.target.target_id,
        );
    } else if loaded.target != new_target {
        // Weird. (Was a `BlueprintTarget` constructed with a timestamp that
        // isn't at database precision?) We should detect this.
        panic!(
            "blueprint target {} did not round-trip through the database: \
             planner set {new_target:?}, loader loaded {:?}",
            new_target.target_id, loaded.target,
        );
    }
    new_target
}

/// Activate the task named by `reason`, waiting for that activation to
/// complete.
async fn activate_planner_input_loader(
    lockstep_client: &ClientTestContext,
    reason: BlueprintPlannerSkipReason,
    timeout: Duration,
) {
    activate_background_task_with_timeout(
        lockstep_client,
        reason.loader_task_name(),
        timeout,
    )
    .await;
}

/// Run the blueprint_executor background task
pub async fn run_blueprint_executor(lockstep_client: &ClientTestContext) {
    let last_background_task =
        activate_background_task(&lockstep_client, "blueprint_executor").await;

    let LastResult::Completed(_last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from blueprint_executor task",
            last_background_task.last,
        );
    };
}

/// Run the blueprint_rendezvous background task
pub async fn run_blueprint_rendezvous(lockstep_client: &ClientTestContext) {
    let last_background_task =
        activate_background_task(&lockstep_client, "blueprint_rendezvous")
            .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from blueprint_rendezvous task",
            last_background_task.last,
        );
    };

    let _status = serde_json::from_value::<BlueprintRendezvousStatus>(
        last_result_completed.details,
    )
    .unwrap();
}
