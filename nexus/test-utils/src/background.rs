// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions related to Nexus background tasks

use crate::http_testing::NexusRequest;
use dropshot::test_util::ClientTestContext;
use nexus_lockstep_client::types::BackgroundTask;
use nexus_lockstep_client::types::CurrentStatus;
use nexus_lockstep_client::types::LastResult;
use nexus_types::internal_api::background::*;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::CollectionUuid;
use slog::info;
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

/// Run the inventory_collection background task and return the new collection's
/// ID.
///
/// Panics on encountering an error, or if no collection ID was returned from
/// the task.
pub async fn run_inventory_collection(
    lockstep_client: &ClientTestContext,
) -> CollectionUuid {
    // Use a longer 60s timeout here.
    //
    // The wait covers an in-flight iteration too, and a single iteration can
    // hit the collector's 15s cockroach-admin and the 60s Sled Agent client
    // timeouts. In general we expect Sled Agent to be present, but
    // cockroach-admin to not currently be present in tests (see
    // https://github.com/oxidecomputer/omicron/issues/8496 -- we use an
    // ephemeral port in tests but Nexus attempts to reach a hardcoded port). We
    // may need to bump this timeout further if we continue to see flakes.
    let last_background_task = activate_background_task_with_timeout(
        &lockstep_client,
        "inventory_collection",
        Duration::from_secs(60),
    )
    .await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from inventory_collection task",
            last_background_task.last,
        );
    };

    let details = last_result_completed.details;
    if let Some(error) = details.get("error") {
        panic!("inventory_collection task failed: {error}");
    }
    let Some(collection_id) =
        details.get("collection_id").and_then(serde_json::Value::as_str)
    else {
        panic!("inventory_collection details have no collection_id: {details}");
    };
    collection_id.parse().expect("parsed collection_id as a CollectionUuid")
}

/// Run the inventory_loader background task and return the ID of the collection
/// it has loaded.
///
/// Panics on encountering an error, or if there are no collections.
pub async fn run_inventory_loader(
    lockstep_client: &ClientTestContext,
) -> CollectionUuid {
    let last_background_task =
        activate_background_task(&lockstep_client, "inventory_loader").await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from inventory_loader task",
            last_background_task.last,
        );
    };

    let status = serde_json::from_value::<InventoryLoadStatus>(
        last_result_completed.details,
    )
    .expect("parsed inventory_loader details as InventoryLoadStatus");
    match status {
        InventoryLoadStatus::Loaded { collection_id, .. } => collection_id,
        InventoryLoadStatus::Error(_) | InventoryLoadStatus::NoCollections => {
            panic!("inventory_loader did not load a collection: {status:?}")
        }
    }
}

/// Run the blueprint_loader background task
pub async fn run_blueprint_loader(lockstep_client: &ClientTestContext) {
    let last_background_task =
        activate_background_task(&lockstep_client, "blueprint_loader").await;

    let LastResult::Completed(_last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from blueprint_loader task",
            last_background_task.last,
        );
    };
}

/// Run the blueprint_planner background task
pub async fn run_blueprint_planner(
    lockstep_client: &ClientTestContext,
) -> BlueprintPlannerStatus {
    let last_background_task =
        activate_background_task(&lockstep_client, "blueprint_planner").await;

    let LastResult::Completed(last_result_completed) =
        last_background_task.last
    else {
        panic!(
            "unexpected {:?} returned from blueprint_planner task",
            last_background_task.last,
        );
    };

    serde_json::from_value::<BlueprintPlannerStatus>(
        last_result_completed.details,
    )
    .unwrap()
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
pub async fn run_blueprint_rendezvous(
    lockstep_client: &ClientTestContext,
) -> BlueprintRendezvousStatus {
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

    let status = serde_json::from_value::<BlueprintRendezvousStatus>(
        last_result_completed.details,
    )
    .expect("parsed blueprint_rendezvous task status");

    match &status.sled_blueprint_availability {
        SledBlueprintAvailabilityRendezvousOutcome::Reconciled(_) => (),
        SledBlueprintAvailabilityRendezvousOutcome::Error(error) => {
            panic!("blueprint_rendezvous sled availability failed: {error}")
        }
    }
    match &status.datasets {
        DatasetRendezvousOutcome::NoInventoryCollection
        | DatasetRendezvousOutcome::Reconciled { .. } => (),
        DatasetRendezvousOutcome::Error { inventory_collection_id, error } => {
            panic!(
                "blueprint_rendezvous dataset reconciliation against \
                 inventory collection {inventory_collection_id} failed: {error}"
            )
        }
    }

    status
}
