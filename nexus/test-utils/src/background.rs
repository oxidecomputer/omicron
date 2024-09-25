// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions related to Nexus background tasks

use crate::http_testing::NexusRequest;
use dropshot::test_util::ClientTestContext;
use nexus_client::types::BackgroundTask;
use nexus_client::types::CurrentStatus;
use nexus_client::types::CurrentStatusRunning;
use nexus_client::types::LastResult;
use nexus_client::types::LastResultCompleted;
use nexus_types::internal_api::background::*;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use std::time::Duration;

/// Return the most recent start time for a background task
fn most_recent_start_time(
    task: &BackgroundTask,
) -> Option<chrono::DateTime<chrono::Utc>> {
    match task.current {
        CurrentStatus::Idle => match task.last {
            LastResult::Completed(LastResultCompleted {
                start_time, ..
            }) => Some(start_time),
            LastResult::NeverCompleted => None,
        },
        CurrentStatus::Running(CurrentStatusRunning { start_time, .. }) => {
            Some(start_time)
        }
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

    let last_start = most_recent_start_time(&task);

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

            if matches!(&task.current, CurrentStatus::Idle)
                && most_recent_start_time(&task) > last_start
            {
                Ok(task)
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
        &Duration::from_secs(10),
    )
    .await
    .unwrap();
}
