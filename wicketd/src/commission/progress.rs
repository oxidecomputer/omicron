// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

use oxide_update_engine_types::buffer::{
    ExecutionStatus, ExecutionSummary, StepKey, StepStatus, TerminalKind,
};
use oxide_update_engine_types::events::{
    ExecutionUuid, ProgressEventKind, StepOutcome as EngineStepOutcome,
};
use oxide_update_engine_types::spec::EngineSpec;
use wicket_common::update_events::{
    EventBuffer, EventBufferStepData, EventReport, StepEventKind,
};
use wicketd_commission_types::inventory::SpIdentifier;
use wicketd_commission_types::update::{
    SpUpdateProgress, StepOutcome, StepProgress, UpdateProgress, UpdateState,
    UpdateStep, UpdateStepStatus,
};

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

/// Maps a [`StepStatus`] to a published [`UpdateStepStatus`].
fn map_status<S: EngineSpec>(status: &StepStatus<S>) -> UpdateStepStatus {
    match status {
        StepStatus::NotStarted => UpdateStepStatus::NotStarted,
        StepStatus::Running { progress_event, .. } => {
            let progress = match &progress_event.kind {
                ProgressEventKind::Progress {
                    progress: Some(counter), ..
                } => Some(StepProgress {
                    current: counter.current,
                    total: counter.total,
                    units: counter.units.0.to_string(),
                }),
                ProgressEventKind::Progress { progress: None, .. }
                | ProgressEventKind::WaitingForProgress { .. }
                | ProgressEventKind::Nested { .. }
                | ProgressEventKind::Unknown => None,
            };
            UpdateStepStatus::Running { progress }
        }
        StepStatus::Completed { reason } => {
            let outcome = reason
                .step_completed_info()
                .map(|info| match &info.outcome {
                    EngineStepOutcome::Success { message, .. } => {
                        StepOutcome::Success {
                            message: message.clone().map(Into::into),
                        }
                    }
                    EngineStepOutcome::Warning { message, .. } => {
                        StepOutcome::Warning { message: message.to_string() }
                    }
                    EngineStepOutcome::Skipped { message, .. } => {
                        StepOutcome::Skipped { message: message.to_string() }
                    }
                })
                // A step completed via SubsequentStarted or ParentCompleted
                // doesn't carry an explicit outcome, but is still successful.
                // Treat it as a plain success.
                .unwrap_or(StepOutcome::Success { message: None });
            UpdateStepStatus::Completed { outcome }
        }
        StepStatus::Failed { reason } => {
            let (message, causes) = reason
                .step_failed_info()
                .map(|info| (info.message.clone(), info.causes.clone()))
                .unwrap_or_else(|| {
                    (
                        "step failed; no failure details were recorded in \
                         the event buffer"
                            .to_string(),
                        Vec::new(),
                    )
                });
            UpdateStepStatus::Failed { message, causes }
        }
        StepStatus::Aborted { reason, .. } => {
            let message = reason
                .step_aborted_info()
                .map(|info| info.message.clone())
                .unwrap_or_else(|| {
                    "step aborted; no abort details were recorded in the \
                     event buffer"
                        .to_string()
                });
            UpdateStepStatus::Aborted { message }
        }
        StepStatus::WillNotBeRun { .. } => UpdateStepStatus::WillNotBeRun,
    }
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
        // The root execution can legitimately be absent here, since an
        // `ExecutionStarted` with an empty steps list sets root_execution_id
        // without adding any steps.
        let steps = self
            .event_buffer
            .iter_steps_for_execution(execution_id)
            .map(|(_, data)| self.build_step(data))
            .collect();
        UpdateProgress { state, steps }
    }

    fn build_step(&self, data: &EventBufferStepData) -> UpdateStep {
        let description = data.step_info().description.to_string();
        let status = map_status(data.step_status());
        let children = data
            .child_execution_ids()
            .iter()
            .map(|child_exec| self.build_execution(*child_exec))
            .collect();
        UpdateStep { description, status, children }
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
    // EventBuffer ignores NoStepsDefined, so we would report Waiting; preserve
    // the prior Skipped classification with a narrow pre-check on that one
    // variant.
    let has_no_steps_defined = report.step_events.iter().any(|event| {
        if let StepEventKind::NoStepsDefined = event.kind {
            true
        } else {
            false
        }
    });
    if has_no_steps_defined {
        return UpdateProgress {
            state: UpdateState::Skipped,
            steps: Vec::new(),
        };
    }

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
    use wicketd_commission_types::inventory::SpType;

    // TODO-RAINCLAUDE: an empty report has no root execution, so the projection
    // TODO-RAINCLAUDE: reports Waiting (an update was started but nothing has
    // TODO-RAINCLAUDE: happened yet) with an empty step tree.
    #[test]
    fn empty_report_projects_waiting() {
        let sp = SpIdentifier { typ: SpType::Sled, slot: 0 };
        let result = sp_update_progress(sp, EventReport::default());
        assert_eq!(result.sp, sp);
        assert_eq!(result.progress.state, UpdateState::Waiting);
        assert!(
            result.progress.steps.is_empty(),
            "an empty report projects no steps: {:?}",
            result.progress.steps,
        );
    }
}
