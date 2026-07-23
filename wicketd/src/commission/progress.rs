// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

use oxide_update_engine_types::buffer::{
    AbortReason, CompletionReason, ExecutionStatus, ExecutionSummary,
    FailureReason, StepKey, StepStatus, TerminalKind, WillNotBeRunReason,
};
use oxide_update_engine_types::events::{
    ExecutionUuid, LeafProgress, ProgressCounter, ProgressEventKind,
    StepOutcome as EngineStepOutcome,
};
use oxide_update_engine_types::spec::{EngineSpec, GenericSpec};
use wicket_common::update_events::{
    EventBuffer, EventBufferStepData, EventReport,
};
use wicketd_commission_types::inventory::SpIdentifier;
use wicketd_commission_types::update::{
    RunningProgress, SpUpdateProgress, StepOutcome, StepProgress,
    UpdateProgress, UpdateState, UpdateStep, UpdateStepStatus,
};

fn completed_outcome(reason: &CompletionReason) -> StepOutcome {
    match reason {
        CompletionReason::StepCompleted(info) => engine_outcome(&info.outcome),
        // The engine can't record an outcome for a step completed only because
        // a later step started. Report success but say that the outcome was
        // inferred.
        CompletionReason::SubsequentStarted { .. } => StepOutcome::Success {
            message: Some(
                "step completed; exact outcome not recorded (inferred from a \
                 subsequent step starting)"
                    .to_string(),
            ),
        },
        // Same for a step being marked completed because the parent was
        // completed.
        CompletionReason::ParentCompleted { parent_info, .. } => {
            StepOutcome::Success {
                message: Some(format!(
                    "step completed; exact outcome not recorded (inferred \
                     from parent completion; {})",
                    parent_outcome_detail(&parent_info.outcome),
                )),
            }
        }
    }
}

fn engine_outcome(outcome: &EngineStepOutcome<GenericSpec>) -> StepOutcome {
    match outcome {
        EngineStepOutcome::Success { message, .. } => {
            StepOutcome::Success { message: message.clone().map(Into::into) }
        }
        EngineStepOutcome::Warning { message, .. } => {
            StepOutcome::Warning { message: message.to_string() }
        }
        EngineStepOutcome::Skipped { message, .. } => {
            StepOutcome::Skipped { message: message.to_string() }
        }
    }
}

fn parent_outcome_detail(outcome: &EngineStepOutcome<GenericSpec>) -> String {
    match outcome {
        EngineStepOutcome::Success { message: Some(message), .. } => {
            format!("parent succeeded: {message}")
        }
        EngineStepOutcome::Success { message: None, .. } => {
            "parent succeeded".to_string()
        }
        EngineStepOutcome::Warning { message, .. } => {
            format!("parent completed with warning: {message}")
        }
        EngineStepOutcome::Skipped { message, .. } => {
            format!("parent was skipped: {message}")
        }
    }
}

fn terminal_message(
    description: &str,
    detail: String,
    empty_fallback: &str,
) -> String {
    let line = if description.is_empty() {
        detail
    } else if detail.is_empty() {
        description.to_string()
    } else {
        format!("{description}: {detail}")
    };
    if line.is_empty() { empty_fallback.to_string() } else { line }
}

fn failed_message(event_buffer: &EventBuffer, step_key: &StepKey) -> String {
    let data = event_buffer
        .get(step_key)
        .expect("terminal step key is present in the buffer it came from");
    let reason = data
        .step_status()
        .failure_reason()
        .expect("step with TerminalKind::Failed has StepStatus::Failed");
    terminal_message(
        data.step_info().description.as_ref(),
        reason.message_display(event_buffer).to_string(),
        "update failed; no failure details were recorded in the event buffer",
    )
}

fn aborted_message(event_buffer: &EventBuffer, step_key: &StepKey) -> String {
    let data = event_buffer
        .get(step_key)
        .expect("terminal step key is present in the buffer it came from");
    let reason = data
        .step_status()
        .abort_reason()
        .expect("step with TerminalKind::Aborted has StepStatus::Aborted");
    terminal_message(
        data.step_info().description.as_ref(),
        reason.message_display(event_buffer).to_string(),
        "update aborted; no abort details were recorded in the event buffer",
    )
}

fn step_progress_from_counter(counter: &ProgressCounter) -> StepProgress {
    StepProgress {
        current: counter.current,
        total: counter.total,
        units: counter.units.0.to_string(),
    }
}

fn running_progress<S: EngineSpec>(
    kind: &ProgressEventKind<S>,
) -> RunningProgress {
    match kind.leaf_progress() {
        LeafProgress::Progress(counter) => RunningProgress::Progress {
            progress: counter.map(step_progress_from_counter),
        },
        LeafProgress::WaitingForProgress => RunningProgress::WaitingForProgress,
        // Unknown means an event couldn't be deserialized. The EventBuffer
        // drops these events so this branch is unreachable in practice.
        LeafProgress::Unknown => RunningProgress::Progress { progress: None },
    }
}

fn step_description(
    event_buffer: &EventBuffer,
    step_key: &StepKey,
) -> Option<String> {
    event_buffer
        .get(step_key)
        .map(|data| data.step_info().description.to_string())
}

fn will_not_be_run_message(
    event_buffer: &EventBuffer,
    reason: &WillNotBeRunReason,
) -> String {
    let (relation, outcome, step) = match reason {
        WillNotBeRunReason::PreviousStepFailed { step } => {
            ("a previous", "failed", step)
        }
        WillNotBeRunReason::ParentStepFailed { step } => {
            ("a parent", "failed", step)
        }
        WillNotBeRunReason::PreviousStepAborted { step } => {
            ("a previous", "was aborted", step)
        }
        WillNotBeRunReason::ParentAborted { step } => {
            ("a parent", "was aborted", step)
        }
    };
    match step_description(event_buffer, step) {
        Some(description) => format!(
            "will not run because {relation} step (\"{description}\") {outcome}"
        ),
        None => format!("will not run because {relation} step {outcome}"),
    }
}

struct ProgressBuilder<'a> {
    event_buffer: &'a EventBuffer,
    // A cache of the execution summary for each execution ID.
    summaries: HashMap<ExecutionUuid, ExecutionSummary>,
}

impl<'a> ProgressBuilder<'a> {
    fn new(event_buffer: &'a EventBuffer) -> Self {
        let summaries: HashMap<_, _> =
            event_buffer.steps().summarize().into_iter().collect();
        Self { event_buffer, summaries }
    }

    fn build_execution(&self, execution_id: ExecutionUuid) -> UpdateProgress {
        let state = self.execution_state(execution_id);
        let steps = self
            .event_buffer
            .iter_steps_for_execution(execution_id)
            .map(|(_, data)| self.build_step(data))
            .collect();
        UpdateProgress { state, steps }
    }

    fn build_step(&self, data: &EventBufferStepData) -> UpdateStep {
        let description = data.step_info().description.to_string();
        let status = self.map_status(data.step_status());
        let children = data
            .child_execution_ids()
            .iter()
            .map(|child_exec| self.build_execution(*child_exec))
            .collect();
        UpdateStep { description, status, children }
    }

    /// Maps a [`StepStatus`] to a published [`UpdateStepStatus`].
    fn map_status<S: EngineSpec>(
        &self,
        status: &StepStatus<S>,
    ) -> UpdateStepStatus {
        match status {
            StepStatus::NotStarted => UpdateStepStatus::NotStarted,
            StepStatus::Running { progress_event, .. } => {
                UpdateStepStatus::Running {
                    progress: running_progress(&progress_event.kind),
                }
            }
            StepStatus::Completed { reason } => UpdateStepStatus::Completed {
                outcome: completed_outcome(reason),
            },
            StepStatus::Failed { reason } => match reason {
                FailureReason::StepFailed(info) => UpdateStepStatus::Failed {
                    message: info.message.clone(),
                    causes: info.causes.clone(),
                },
                FailureReason::ParentFailed { .. } => {
                    UpdateStepStatus::Failed {
                        // message_display names the parent and provides its
                        // causes inline, so leave the `causes` list empty in
                        // this case.
                        message: reason
                            .message_display(self.event_buffer)
                            .to_string(),
                        causes: Vec::new(),
                    }
                }
            },
            StepStatus::Aborted { reason, .. } => {
                let message = match reason {
                    AbortReason::StepAborted(info) => info.message.clone(),
                    AbortReason::ParentAborted { .. } => {
                        reason.message_display(self.event_buffer).to_string()
                    }
                };
                UpdateStepStatus::Aborted { message }
            }
            StepStatus::WillNotBeRun { reason } => {
                UpdateStepStatus::WillNotBeRun {
                    reason: will_not_be_run_message(self.event_buffer, reason),
                }
            }
        }
    }

    fn execution_state(&self, execution_id: ExecutionUuid) -> UpdateState {
        let Some(summary) = self.summaries.get(&execution_id) else {
            return UpdateState::Waiting;
        };
        match &summary.execution_status {
            ExecutionStatus::NotStarted => UpdateState::Waiting,
            ExecutionStatus::Running { .. } => UpdateState::Running,
            ExecutionStatus::Terminal(info) => match info.kind {
                TerminalKind::Completed => UpdateState::Completed,
                TerminalKind::Failed => UpdateState::Failed {
                    message: failed_message(self.event_buffer, &info.step_key),
                },
                TerminalKind::Aborted => UpdateState::Aborted {
                    message: aborted_message(self.event_buffer, &info.step_key),
                },
            },
        }
    }
}

pub(crate) fn sp_update_progress(
    sp: SpIdentifier,
    report: EventReport,
) -> SpUpdateProgress {
    SpUpdateProgress { sp, progress: update_progress(report) }
}

fn update_progress(report: EventReport) -> UpdateProgress {
    let mut event_buffer = EventBuffer::default();
    event_buffer.add_event_report(report);

    let Some(root_id) = event_buffer.root_execution_id() else {
        return UpdateProgress {
            state: UpdateState::Waiting,
            steps: Vec::new(),
        };
    };

    ProgressBuilder::new(&event_buffer).build_execution(root_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_update_engine_types::events::ProgressUnits;
    use wicketd_commission_types::inventory::SpType;

    #[test]
    fn empty_report_projects_waiting() {
        let sp = SpIdentifier { typ: SpType::Sled, slot: 0 };
        let result = sp_update_progress(sp, EventReport::default());
        assert_eq!(result.sp, sp);
        assert_eq!(result.progress.state, UpdateState::Waiting);
        assert!(
            result.progress.steps.is_empty(),
            "an empty report doesn't have any steps, but these were found: {:?}",
            result.progress.steps,
        );
    }

    #[test]
    fn counter_projects_to_step_progress() {
        let counter = ProgressCounter {
            current: 3,
            total: Some(10),
            units: ProgressUnits::new("bytes"),
        };
        assert_eq!(
            step_progress_from_counter(&counter),
            StepProgress {
                current: 3,
                total: Some(10),
                units: "bytes".to_string(),
            },
        );
    }

    fn fake_step_key() -> StepKey {
        StepKey { execution_id: ExecutionUuid::nil(), index: 0 }
    }

    #[test]
    fn will_not_be_run_messages_distinguish_reason_variants() {
        let buffer = EventBuffer::default();
        let step = fake_step_key();

        assert_eq!(
            will_not_be_run_message(
                &buffer,
                &WillNotBeRunReason::PreviousStepFailed { step },
            ),
            "will not run because a previous step failed",
        );
        assert_eq!(
            will_not_be_run_message(
                &buffer,
                &WillNotBeRunReason::ParentStepFailed { step },
            ),
            "will not run because a parent step failed",
        );
        assert_eq!(
            will_not_be_run_message(
                &buffer,
                &WillNotBeRunReason::PreviousStepAborted { step },
            ),
            "will not run because a previous step was aborted",
        );
        assert_eq!(
            will_not_be_run_message(
                &buffer,
                &WillNotBeRunReason::ParentAborted { step },
            ),
            "will not run because a parent step was aborted",
        );
    }
}
