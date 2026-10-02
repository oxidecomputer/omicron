// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for `wicket rack-update status`.

use std::fmt;

use oxide_versioned_envelope::Versioned;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use wicketd_commission_types_versions::{latest, v1, v4, v5};

/// The version of [`RackUpdateStatus`]'s data.
///
/// Bump this whenever the schema of [`RackUpdateStatus`] changes.
pub const RACK_UPDATE_STATUS_CLI_VERSION: u32 = 1;

/// The data inside the JSON emitted by `wicket rack-update status --json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RackUpdateStatus {
    /// The update progress reported by wicketd.
    pub update_progress: v5::update::GetUpdateProgressResponse,
}

impl Versioned for RackUpdateStatus {
    const VERSION: u32 = RACK_UPDATE_STATUS_CLI_VERSION;
}

impl RackUpdateStatus {
    /// Converts the floating `latest::` type into the pinned one.
    ///
    /// (If this conversion becomes fallible or needs ancillary data, update the
    /// signature of this method.)
    pub fn from_latest(
        update_progress: latest::update::GetUpdateProgressResponse,
    ) -> Self {
        let update_progress: v5::update::GetUpdateProgressResponse =
            update_progress;
        Self { update_progress }
    }

    /// Returns how many components are in each state.
    ///
    /// Only SPs present in `self.update_progress.sps` are counted. An SP
    /// appears there only once an update has been started for it.
    pub fn state_counts(&self) -> RackUpdateStateCounts {
        RackUpdateStateCounts::from_states(
            self.update_progress.sps.iter().map(|sp| &sp.progress.state),
        )
    }

    pub fn rollup(&self) -> RackUpdateStateRollup {
        self.state_counts().rollup()
    }
}

mod private {
    pub trait Sealed {}

    impl Sealed for super::v4::update::UpdateState {}
    impl Sealed for super::v4::update::UpdateProgress {}
}

/// Extension methods on [`v4::update::UpdateState`] for use by clients of this
/// crate.
pub trait UpdateStateExt: private::Sealed {
    // If and when a new version of UpdateState is added, add an impl for
    // `elapsed` here.

    /// Returns the terminal message corresponding to a given update state.
    fn terminal_message(&self) -> Option<&str>;

    /// Returns a human-readable label for this update state.
    fn label(&self) -> &'static str;
}

impl UpdateStateExt for v4::update::UpdateState {
    fn terminal_message(&self) -> Option<&str> {
        match self {
            v4::update::UpdateState::Waiting
            | v4::update::UpdateState::Running { elapsed: _ }
            | v4::update::UpdateState::Completed { elapsed: _ } => None,
            v4::update::UpdateState::Failed { message, elapsed: _ } => {
                Some(message)
            }
            v4::update::UpdateState::Aborted { message, elapsed: _ } => {
                Some(message)
            }
        }
    }

    fn label(&self) -> &'static str {
        match self {
            v4::update::UpdateState::Waiting => "not started",
            v4::update::UpdateState::Running { elapsed: _ } => "in progress",
            v4::update::UpdateState::Completed { elapsed: _ } => "completed",
            v4::update::UpdateState::Failed { message: _, elapsed: _ } => {
                "failed"
            }
            v4::update::UpdateState::Aborted { message: _, elapsed: _ } => {
                "aborted"
            }
        }
    }
}

/// How far through its top-level steps an update has got, in the form `current
/// of total`.
///
/// (This does not include nested executions.)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct StepPosition {
    /// The 1-based index of the current or last step.
    pub current: usize,
    /// The total number of top-level steps.
    pub total: usize,
}

/// Extension methods on [`v4::update::UpdateProgress`] for use by clients of
/// this crate.
pub trait UpdateProgressExt: private::Sealed {
    /// Returns the position of an update within its top-level steps.
    fn step_position(&self) -> Option<StepPosition>;
}

impl UpdateProgressExt for v4::update::UpdateProgress {
    fn step_position(&self) -> Option<StepPosition> {
        let total = self.steps.len();
        if total == 0 {
            return None;
        }

        let current = self
            .steps
            .iter()
            .position(|step| !step_is_completed(step))
            .map_or(total, |index| index + 1);
        Some(StepPosition { current, total })
    }
}

fn step_is_completed(step: &v4::update::UpdateStep) -> bool {
    match &step.status {
        v1::update::UpdateStepStatus::Completed { outcome: _ } => true,
        v1::update::UpdateStepStatus::NotStarted
        | v1::update::UpdateStepStatus::Running { progress: _ }
        | v1::update::UpdateStepStatus::Failed { message: _, causes: _ }
        | v1::update::UpdateStepStatus::Aborted { message: _ }
        | v1::update::UpdateStepStatus::WillNotBeRun { reason: _ } => false,
    }
}

/// Counts of component updates by state.
///
/// (This is derived from the JSON.)
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct RackUpdateStateCounts {
    pub not_started: usize,
    pub in_progress: usize,
    pub completed: usize,
    pub failed: usize,
    pub aborted: usize,
}

impl RackUpdateStateCounts {
    pub fn from_states<'a>(
        states: impl IntoIterator<Item = &'a v4::update::UpdateState>,
    ) -> Self {
        let mut counts = Self::default();
        for state in states {
            match state {
                v4::update::UpdateState::Waiting => counts.not_started += 1,
                v4::update::UpdateState::Running { elapsed: _ } => {
                    counts.in_progress += 1
                }
                v4::update::UpdateState::Completed { elapsed: _ } => {
                    counts.completed += 1
                }
                v4::update::UpdateState::Failed { message: _, elapsed: _ } => {
                    counts.failed += 1
                }
                v4::update::UpdateState::Aborted { message: _, elapsed: _ } => {
                    counts.aborted += 1
                }
            }
        }
        counts
    }

    /// Returns the total number of update states in this rollup.
    pub fn total(&self) -> usize {
        let Self { not_started, in_progress, completed, failed, aborted } =
            *self;
        not_started + in_progress + completed + failed + aborted
    }

    /// Returns the rollup state.
    pub fn rollup(&self) -> RackUpdateStateRollup {
        let Self { not_started, in_progress: _, completed, failed, aborted } =
            *self;
        let total = self.total();
        if failed > 0 {
            // A single failure makes the whole update a failure.
            RackUpdateStateRollup::Failed
        } else if aborted > 0 {
            RackUpdateStateRollup::Aborted
        } else if not_started == total {
            // This also covers the empty case.
            RackUpdateStateRollup::NotStarted
        } else if completed == total {
            RackUpdateStateRollup::Completed
        } else {
            RackUpdateStateRollup::InProgress
        }
    }
}

/// The state of the rack update as a whole, as reported by the CLI's exit code
/// and human-readable output.
///
/// (This is derived from the JSON.)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RackUpdateStateRollup {
    NotStarted,
    InProgress,
    Completed,
    Failed,
    Aborted,
}

impl RackUpdateStateRollup {
    /// Return the exit code corresponding to this state.
    ///
    /// Other than 0 for success, the state-specific exit codes start at 4,
    /// because 1 stands for a generic error and 2/3 are conventionally CLI
    /// usage errors.
    pub fn exit_code(self) -> u8 {
        match self {
            RackUpdateStateRollup::Completed => 0,
            RackUpdateStateRollup::NotStarted => 4,
            RackUpdateStateRollup::InProgress => 5,
            RackUpdateStateRollup::Failed => 6,
            RackUpdateStateRollup::Aborted => 7,
        }
    }
}

impl fmt::Display for RackUpdateStateRollup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RackUpdateStateRollup::NotStarted => f.write_str("not started"),
            RackUpdateStateRollup::InProgress => f.write_str("in progress"),
            RackUpdateStateRollup::Completed => f.write_str("completed"),
            RackUpdateStateRollup::Failed => f.write_str("failed"),
            RackUpdateStateRollup::Aborted => f.write_str("aborted"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use oxide_versioned_envelope::WriteEnvelope;

    fn waiting() -> v4::update::UpdateState {
        v4::update::UpdateState::Waiting
    }

    fn running() -> v4::update::UpdateState {
        v4::update::UpdateState::Running { elapsed: Duration::from_secs(5) }
    }

    fn completed() -> v4::update::UpdateState {
        v4::update::UpdateState::Completed {
            elapsed: Some(Duration::from_secs(9)),
        }
    }

    fn failed() -> v4::update::UpdateState {
        v4::update::UpdateState::Failed {
            message: "writing the RoT image failed".to_owned(),
            elapsed: Some(Duration::from_secs(2)),
        }
    }

    fn aborted() -> v4::update::UpdateState {
        v4::update::UpdateState::Aborted {
            message: "the update was aborted by the operator".to_owned(),
            elapsed: None,
        }
    }

    #[test]
    fn rollup_table() {
        let cases: Vec<(
            &str,
            Vec<v4::update::UpdateState>,
            RackUpdateStateRollup,
        )> = vec![
            ("empty", vec![], RackUpdateStateRollup::NotStarted),
            ("waiting", vec![waiting()], RackUpdateStateRollup::NotStarted),
            ("running", vec![running()], RackUpdateStateRollup::InProgress),
            ("completed", vec![completed()], RackUpdateStateRollup::Completed),
            ("failed", vec![failed()], RackUpdateStateRollup::Failed),
            ("aborted", vec![aborted()], RackUpdateStateRollup::Aborted),
            (
                "completed + failed",
                vec![completed(), failed()],
                RackUpdateStateRollup::Failed,
            ),
            (
                "running + failed",
                vec![running(), failed()],
                RackUpdateStateRollup::Failed,
            ),
            (
                "running + aborted",
                vec![running(), aborted()],
                RackUpdateStateRollup::Aborted,
            ),
            (
                "waiting + aborted",
                vec![waiting(), aborted()],
                RackUpdateStateRollup::Aborted,
            ),
            (
                "aborted + completed",
                vec![aborted(), completed()],
                RackUpdateStateRollup::Aborted,
            ),
            (
                "aborted + failed",
                vec![aborted(), failed()],
                RackUpdateStateRollup::Failed,
            ),
            (
                "completed + completed",
                vec![completed(), completed()],
                RackUpdateStateRollup::Completed,
            ),
            (
                "waiting + waiting",
                vec![waiting(), waiting()],
                RackUpdateStateRollup::NotStarted,
            ),
            (
                "completed + running",
                vec![completed(), running()],
                RackUpdateStateRollup::InProgress,
            ),
            (
                "waiting + running",
                vec![waiting(), running()],
                RackUpdateStateRollup::InProgress,
            ),
            (
                "waiting + completed",
                vec![waiting(), completed()],
                RackUpdateStateRollup::InProgress,
            ),
            (
                "one of each",
                vec![waiting(), running(), completed(), failed(), aborted()],
                RackUpdateStateRollup::Failed,
            ),
        ];

        for (name, states, expected) in cases {
            let counts = RackUpdateStateCounts::from_states(&states);
            assert_eq!(
                counts.total(),
                states.len(),
                "{name}: every state was counted exactly once, in {counts:?}"
            );
            assert_eq!(
                counts.rollup(),
                expected,
                "{name}: {counts:?} rolls up to {expected}"
            );
        }
    }

    fn progress_with_steps(
        statuses: Vec<v1::update::UpdateStepStatus>,
    ) -> v4::update::UpdateProgress {
        v4::update::UpdateProgress {
            state: waiting(),
            steps: statuses
                .into_iter()
                .enumerate()
                .map(|(index, status)| v4::update::UpdateStep {
                    description: format!("step {index}"),
                    status,
                    children: Vec::new(),
                })
                .collect(),
        }
    }

    fn not_started_status() -> v1::update::UpdateStepStatus {
        v1::update::UpdateStepStatus::NotStarted
    }

    fn running_status() -> v1::update::UpdateStepStatus {
        v1::update::UpdateStepStatus::Running {
            progress: v1::update::RunningProgress::WaitingForProgress,
        }
    }

    fn completed_status() -> v1::update::UpdateStepStatus {
        v1::update::UpdateStepStatus::Completed {
            outcome: v1::update::StepOutcome::Success { message: None },
        }
    }

    fn failed_status() -> v1::update::UpdateStepStatus {
        v1::update::UpdateStepStatus::Failed {
            message: "the SP did not respond".to_owned(),
            causes: Vec::new(),
        }
    }

    fn aborted_status() -> v1::update::UpdateStepStatus {
        v1::update::UpdateStepStatus::Aborted {
            message: "aborted by the operator".to_owned(),
        }
    }

    fn will_not_be_run_status() -> v1::update::UpdateStepStatus {
        v1::update::UpdateStepStatus::WillNotBeRun {
            reason: "a prior step failed".to_owned(),
        }
    }

    #[test]
    fn step_position_table() {
        let cases: Vec<(
            &str,
            Vec<v1::update::UpdateStepStatus>,
            Option<StepPosition>,
        )> = vec![
            ("no steps", vec![], None),
            (
                "all not started",
                vec![not_started_status(), not_started_status()],
                Some(StepPosition { current: 1, total: 2 }),
            ),
            (
                "completed, running, not started",
                vec![
                    completed_status(),
                    running_status(),
                    not_started_status(),
                ],
                Some(StepPosition { current: 2, total: 3 }),
            ),
            (
                "all completed",
                vec![completed_status(), completed_status()],
                Some(StepPosition { current: 2, total: 2 }),
            ),
            (
                "completed, failed, will not be run",
                vec![
                    completed_status(),
                    failed_status(),
                    will_not_be_run_status(),
                ],
                Some(StepPosition { current: 2, total: 3 }),
            ),
            (
                "completed, aborted, not started",
                vec![
                    completed_status(),
                    aborted_status(),
                    not_started_status(),
                ],
                Some(StepPosition { current: 2, total: 3 }),
            ),
            (
                "completed, will not be run, not started",
                vec![
                    completed_status(),
                    will_not_be_run_status(),
                    not_started_status(),
                ],
                Some(StepPosition { current: 2, total: 3 }),
            ),
        ];

        for (name, statuses, expected) in cases {
            let progress = progress_with_steps(statuses);
            assert_eq!(
                progress.step_position(),
                expected,
                "{name}: the position is the first top-level step that has \
                 not completed"
            );
        }

        // StepPosition only considers top-level count.
        let mut progress = progress_with_steps(vec![
            completed_status(),
            running_status(),
            not_started_status(),
        ]);
        progress.steps[1].children = vec![v4::update::UpdateProgress {
            state: running(),
            ..progress_with_steps(vec![completed_status(), completed_status()])
        }];
        assert_eq!(
            progress.step_position(),
            Some(StepPosition { current: 2, total: 3 }),
            "a nested execution under the first step that has not completed \
             doesn't alter the top-level count"
        );
    }

    #[test]
    fn elapsed_table() {
        let cases: Vec<(&str, v4::update::UpdateState, Option<Duration>)> = vec![
            ("waiting", waiting(), None),
            ("running", running(), Some(Duration::from_secs(5))),
            ("completed", completed(), Some(Duration::from_secs(9))),
            ("failed", failed(), Some(Duration::from_secs(2))),
            ("aborted", aborted(), None),
        ];

        for (name, state, expected) in cases {
            assert_eq!(
                state.elapsed(),
                expected,
                "{name}: the elapsed time is read off the state"
            );
        }
    }

    #[test]
    fn label_table() {
        let cases: Vec<(&str, v4::update::UpdateState, &str)> = vec![
            ("waiting", waiting(), "not started"),
            ("running", running(), "in progress"),
            ("completed", completed(), "completed"),
            ("failed", failed(), "failed"),
            ("aborted", aborted(), "aborted"),
        ];

        for (name, state, expected) in cases {
            assert_eq!(
                state.label(),
                expected,
                "{name}: the label names the state of this one update"
            );
        }
    }

    #[test]
    fn terminal_message_table() {
        let cases: Vec<(&str, v4::update::UpdateState, Option<&str>)> = vec![
            ("waiting", waiting(), None),
            ("running", running(), None),
            ("completed", completed(), None),
            ("failed", failed(), Some("writing the RoT image failed")),
            (
                "aborted",
                aborted(),
                Some("the update was aborted by the operator"),
            ),
        ];

        for (name, state, expected) in cases {
            assert_eq!(
                state.terminal_message(),
                expected,
                "{name}: only failed and aborted states carry a message"
            );
        }
    }

    #[test]
    fn schema_snapshot() {
        let schema = schemars::schema_for!(WriteEnvelope<RackUpdateStatus>);
        let json = serde_json::to_string_pretty(&schema)
            .expect("the schema serialized to JSON");
        expectorate::assert_contents(
            "tests/output/rack-update-status.json",
            &json,
        );
    }
}
