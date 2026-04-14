// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{collections::BTreeSet, time::Duration};

use semver::Version;

use dropshot::HttpError;
use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::inventory::SpIdentifier;

#[derive(Clone, Debug, Default, JsonSchema, Deserialize, Serialize)]
pub struct StartUpdateOptions {
    /// If passed in, fails the update with a simulated error.
    pub test_error: Option<UpdateTestError>,

    /// If passed in, creates a test step that lasts these many seconds long.
    ///
    /// This is used for testing.
    pub test_step_seconds: Option<u64>,

    /// If passed in, simulates a result for the RoT Bootloader update.
    ///
    /// This is used for testing.
    pub test_simulate_rot_bootloader_result: Option<UpdateSimulatedResult>,

    /// If passed in, simulates a result for the RoT update.
    ///
    /// This is used for testing.
    pub test_simulate_rot_result: Option<UpdateSimulatedResult>,

    /// If passed in, simulates a result for the SP update.
    ///
    /// This is used for testing.
    pub test_simulate_sp_result: Option<UpdateSimulatedResult>,

    /// If true, skip the check on the current RoT version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub skip_rot_bootloader_version_check: bool,

    /// If true, skip the check on the current RoT version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub skip_rot_version_check: bool,

    /// If true, skip the check on the current SP version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub skip_sp_version_check: bool,
}

/// A simulated result for a component update.
///
/// Used by [`StartUpdateOptions`].
#[derive(Clone, Debug, JsonSchema, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateSimulatedResult {
    Success,
    Warning,
    Skipped,
    Failure,
}

#[derive(Clone, Debug, JsonSchema, Deserialize, Serialize)]
pub struct ClearUpdateStateOptions {
    /// If passed in, fails the clear update state operation with a simulated
    /// error.
    pub test_error: Option<UpdateTestError>,
}

#[derive(Clone, Debug, JsonSchema, Deserialize, Serialize)]
pub struct AbortUpdateOptions {
    /// The message to abort the update with.
    pub message: String,

    /// If passed in, fails the force cancel update operation with a simulated
    /// error.
    pub test_error: Option<UpdateTestError>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
pub struct ClearUpdateStateResponse {
    /// The SPs for which update data was cleared.
    pub cleared: BTreeSet<SpIdentifier>,

    /// The SPs that had no update state to clear.
    pub no_update_data: BTreeSet<SpIdentifier>,
}

#[derive(
    Copy, Clone, Debug, JsonSchema, Deserialize, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case", tag = "kind", content = "content")]
pub enum UpdateTestError {
    /// Simulate an error where the operation fails to complete.
    Fail,

    /// Simulate an issue where the operation times out.
    Timeout {
        /// The number of seconds to time out after.
        secs: u64,
    },
}

impl UpdateTestError {
    pub async fn into_http_error(
        self,
        log: &slog::Logger,
        reason: &str,
    ) -> HttpError {
        let message = self.into_error_string(log, reason).await;
        HttpError::for_bad_request(None, message)
    }

    pub async fn into_error_string(
        self,
        log: &slog::Logger,
        reason: &str,
    ) -> String {
        match self {
            UpdateTestError::Fail => {
                format!("Simulated failure while {reason}")
            }
            UpdateTestError::Timeout { secs } => {
                slog::info!(log, "Simulating timeout while {reason}");
                // 15 seconds should be enough to cause a timeout.
                tokio::time::sleep(Duration::from_secs(secs)).await;
                "XXX request should time out before this is hit".into()
            }
        }
    }
}

/// The status of a rack update.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RackUpdateStatus {
    /// The overall update state, rolled up across all components.
    pub state: UpdateState,
    /// The version of the top-level TUF archive.
    pub system_version: Option<Version>,
    /// The artifacts included in the TUF archive.
    pub artifacts: Vec<ArtifactId>,
    /// The update status of each of the target components.
    pub components: Vec<ComponentUpdateStatus>,
}

/// The status of an update for a component within a rack.
/// Here, a component means a Sled, Switch, or PSC.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ComponentUpdateStatus {
    /// The ID of the component.
    pub id: SpIdentifier,
    /// The state of the component update.
    pub state: UpdateState,
    /// The index of the current step (if in progress) or the last
    /// step (if terminal).
    pub step_index: Option<usize>,
    /// The total number of steps in the update.
    pub total_steps: Option<usize>,
    /// The time elapsed since starting the update.
    pub elapsed_secs: Option<f64>,
}

/// The state of a rack or component update.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UpdateState {
    NotStarted,
    InProgress,
    Completed,
    Failed,
    Aborted,
}

pub fn rollup_update_state(states: &[UpdateState]) -> UpdateState {
    if states.is_empty() {
        // An empty list is treated as "not started".
        UpdateState::NotStarted
    } else if states.iter().any(|s| matches!(s, UpdateState::Failed)) {
        // If *any* component failed, the update failed.
        UpdateState::Failed
    } else if states.iter().any(|s| matches!(s, UpdateState::Aborted)) {
        // If *any* component was aborted (and none failed),
        // the update is aborted.
        UpdateState::Aborted
    } else if states.iter().all(|s| matches!(s, UpdateState::Completed)) {
        // If *all* components are completed, the update is completed.
        UpdateState::Completed
    } else if states.iter().all(|s| matches!(s, UpdateState::NotStarted)) {
        // If *all* components are not started, the update is not started.
        UpdateState::NotStarted
    } else 
        // Here, some components have started, none have failed or been aborted,
        // and not all are completed. Therefore, the update is in progress.
        UpdateState::InProgress
    }
}

impl UpdateState {
    /// The exit code corresponding to this state.
    ///
    /// State-specific codes start at 4: exit code 1 tends to be used for
    /// generic errors, and 2 and 3 tend to be reserved for things like
    /// incorrect CLI args.
    ///
    /// - 0: Completed
    /// - 4: NotStarted
    /// - 5: InProgress
    /// - 6: Failed
    /// - 7: Aborted
    pub fn exit_code(self) -> u8 {
        match self {
            UpdateState::Completed => 0,
            UpdateState::NotStarted => 4,
            UpdateState::InProgress => 5,
            UpdateState::Failed => 6,
            UpdateState::Aborted => 7,
        }
    }
}

impl std::fmt::Display for UpdateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateState::NotStarted => write!(f, "not started"),
            UpdateState::InProgress => write!(f, "in progress"),
            UpdateState::Completed => write!(f, "completed"),
            UpdateState::Failed => write!(f, "failed"),
            UpdateState::Aborted => write!(f, "aborted"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rollup_update_state() {
        use UpdateState::*;

        // Empty is treated as NotStarted.
        assert_eq!(rollup_update_state(&[]), NotStarted);

        // Single states roll up to themselves.
        assert_eq!(rollup_update_state(&[NotStarted]), NotStarted);
        assert_eq!(rollup_update_state(&[InProgress]), InProgress);
        assert_eq!(rollup_update_state(&[Completed]), Completed);
        assert_eq!(rollup_update_state(&[Failed]), Failed);
        assert_eq!(rollup_update_state(&[Aborted]), Aborted);

        // Failed / Aborted take priority
        assert_eq!(rollup_update_state(&[Completed, Failed]), Failed);
        assert_eq!(rollup_update_state(&[InProgress, Failed]), Failed);
        assert_eq!(rollup_update_state(&[Aborted, Completed]), Aborted);
        assert_eq!(rollup_update_state(&[Aborted, Failed]), Failed);

        // Complete if all Completed.
        assert_eq!(rollup_update_state(&[Completed, Completed]), Completed);

        // Otherwise ... InProgress
        assert_eq!(rollup_update_state(&[Completed, InProgress]), InProgress);
        assert_eq!(rollup_update_state(&[NotStarted, InProgress]), InProgress);
        assert_eq!(rollup_update_state(&[NotStarted, Completed]), InProgress);
    }
}
