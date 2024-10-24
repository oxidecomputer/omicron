// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions related to Nexus background tasks

use crate::http_testing::NexusRequest;
use dropshot::test_util::ClientTestContext;
use nexus_client::types::BackgroundTask;
use nexus_client::types::CurrentStatus;
use nexus_client::types::LastResult;
use nexus_client::types::LastResultCompleted;
use nexus_types::internal_api::background::*;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use std::time::Duration;

/// Return the most recent activate time for a background task, returning None
/// if it has never been started or is currently running.
fn most_recent_activate_time(
    task: &BackgroundTask,
) -> Option<chrono::DateTime<chrono::Utc>> {
    match task.current {
        CurrentStatus::Idle => match task.last {
            LastResult::Completed(LastResultCompleted {
                start_time, ..
            }) => Some(start_time),

            LastResult::NeverCompleted => None,
        },

        CurrentStatus::Running(..) => None,
    }
}

/// Given the name of a background task, activate it, then wait for it to
/// complete. Return the last polled `BackgroundTask` object.
pub async fn activate_background_task(
    internal_client: &ClientTestContext,
    task_name: &str,
) -> BackgroundTask {
    let task = NexusRequest::object_get(
        internal_client,
        &format!("/bgtasks/view/{task_name}"),
    )
    .execute_and_parse_unwrap::<BackgroundTask>()
    .await;

    let last_activate = most_recent_activate_time(&task);

    internal_client
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
    let last_task_poll = wait_for_condition(
        || async {
            let task = NexusRequest::object_get(
                internal_client,
                &format!("/bgtasks/view/{task_name}"),
            )
            .execute_and_parse_unwrap::<BackgroundTask>()
            .await;

            // Wait until the task has actually run and then is idle
            if matches!(&task.current, CurrentStatus::Idle) {
                let current_activate = most_recent_activate_time(&task);
                match (current_activate, last_activate) {
                    (None, None) => {
                        // task is idle but it hasn't started yet, and it was
                        // never previously activated
                        Err(CondCheckError::<()>::NotYet)
                    }

                    (Some(_), None) => {
                        // task was activated for the first time by this
                        // function call, and it's done now (because the task is
                        // idle)
                        Ok(task)
                    }

                    (None, Some(_)) => {
                        // the task is idle (due to the check above) but
                        // `most_recent_activate_time` returned None, implying
                        // that the LastResult is NeverCompleted? the Some in
                        // the second part of the tuple means this ran before,
                        // so panic here.
                        panic!("task is idle, but there's no activate time?!");
                    }

                    (Some(current_activation), Some(last_activation)) => {
                        // the task is idle, it started ok, and it was
                        // previously activated: compare times to make sure we
                        // didn't observe the same BackgroundTask object
                        if current_activation > last_activation {
                            Ok(task)
                        } else {
                            // the task hasn't started yet, we observed the same
                            // BackgroundTask object
                            Err(CondCheckError::<()>::NotYet)
                        }
                    }
                }
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_millis(500),
        &Duration::from_secs(60),
    )
    .await
    .unwrap();

    last_task_poll
}

/// Run the region_replacement background task, returning how many actions
/// were taken
pub async fn run_region_replacement(
    internal_client: &ClientTestContext,
) -> usize {
    let last_background_task =
        activate_background_task(&internal_client, "region_replacement").await;

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
    internal_client: &ClientTestContext,
) -> usize {
    let last_background_task =
        activate_background_task(&internal_client, "region_replacement_driver")
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
    internal_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &internal_client,
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
    internal_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &internal_client,
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
    internal_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &internal_client,
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

    eprintln!("{:?}", &status.errors);

    assert!(status.errors.is_empty());

    status.step_records_created_ok.len()
        + status.step_garbage_collect_invoked_ok.len()
        + status.step_invoked_ok.len()
}

/// Run the region_snapshot_replacement_finish background task, returning how many
/// actions were taken
pub async fn run_region_snapshot_replacement_finish(
    internal_client: &ClientTestContext,
) -> usize {
    let last_background_task = activate_background_task(
        &internal_client,
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

    status.records_set_to_done.len()
}

/// Run all replacement related background tasks until they aren't doing
/// anything anymore.
pub async fn run_replacement_tasks_to_completion(
    internal_client: &ClientTestContext,
) {
    wait_for_condition(
        || async {
            let actions_taken =
                // region replacement related
                run_region_replacement(internal_client).await +
                run_region_replacement_driver(internal_client).await +
                // region snapshot replacement related
                run_region_snapshot_replacement_start(internal_client).await +
                run_region_snapshot_replacement_garbage_collection(internal_client).await +
                run_region_snapshot_replacement_step(internal_client).await +
                run_region_snapshot_replacement_finish(internal_client).await;

            if actions_taken > 0 {
                Err(CondCheckError::<()>::NotYet)
            } else {
                Ok(())
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(20),
    )
    .await
    .unwrap();
}
