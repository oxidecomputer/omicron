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
