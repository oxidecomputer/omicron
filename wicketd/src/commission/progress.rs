// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use update_engine::events::StepOutcome;
use update_engine::{ExecutionStatus, StepKey, TerminalKind};
use wicket_common::update_events::{EventBuffer, EventReport, StepEventKind};
use wicketd_commission_types::update::SpUpdateProgress;

// TODO-RAINCLAUDE: combine the terminal step's description with its message into
// TODO-RAINCLAUDE: the single most useful line, mirroring what the wicket TUI
// TODO-RAINCLAUDE: prints (the step description followed by the message text).
fn terminal_message(description: &str, message: &str) -> String {
    // TODO-RAINCLAUDE: an empty description would otherwise yield a stray
    // TODO-RAINCLAUDE: leading ": " prefix on the message.
    if description.is_empty() {
        return message.to_string();
    }
    if message.is_empty() {
        description.to_string()
    } else {
        format!("{description}: {message}")
    }
}

// TODO-RAINCLAUDE: description of the step recorded against a terminal event,
// TODO-RAINCLAUDE: falling back to empty if the buffer no longer has the step.
fn terminal_description(
    event_buffer: &EventBuffer,
    step_key: &StepKey,
) -> String {
    event_buffer
        .get(step_key)
        .map(|data| data.step_info().description.to_string())
        .unwrap_or_default()
}

// TODO-RAINCLAUDE: combine a terminal step's description with its detail
// TODO-RAINCLAUDE: message, guaranteeing a non-empty operator-facing string by
// TODO-RAINCLAUDE: falling back to a canned message when the buffer recorded
// TODO-RAINCLAUDE: neither a description nor a detail for the step.
fn finalize_terminal_message(
    event_buffer: &EventBuffer,
    step_key: &StepKey,
    detail: Option<String>,
    empty_fallback: &str,
) -> String {
    let description = terminal_description(event_buffer, step_key);
    let message = terminal_message(&description, &detail.unwrap_or_default());
    if message.is_empty() { empty_fallback.to_string() } else { message }
}

// TODO-RAINCLAUDE: the innermost running step's description; the last running
// TODO-RAINCLAUDE: step in buffer order is the deepest nested one.
fn running_step_description(event_buffer: &EventBuffer) -> String {
    event_buffer
        .steps()
        .as_slice()
        .iter()
        .rfind(|(_key, data)| data.step_status().is_running())
        .map(|(_key, data)| data.step_info().description.to_string())
        .unwrap_or_default()
}

#[derive(Debug, Default)]
struct StepOutcomeSummary {
    did_real_work: bool,
    warnings: Vec<String>,
}

// TODO-RAINCLAUDE: walk every step (root and nested) that recorded an explicit
// TODO-RAINCLAUDE: completion outcome. Collect skip and warning messages, and
// TODO-RAINCLAUDE: note whether any step did real work (succeeded or warned).
fn collect_step_outcomes(event_buffer: &EventBuffer) -> StepOutcomeSummary {
    let mut summary = StepOutcomeSummary::default();

    let steps = event_buffer.steps();
    for (_key, data) in steps.as_slice() {
        let Some(info) = data
            .step_status()
            .completion_reason()
            .and_then(|reason| reason.step_completed_info())
        else {
            continue;
        };
        match &info.outcome {
            StepOutcome::Success { .. } => summary.did_real_work = true,
            StepOutcome::Warning { message, .. } => {
                summary.did_real_work = true;
                summary.warnings.push(message.to_string());
            }
            StepOutcome::Skipped { message, .. } => {
                summary.warnings.push(message.to_string());
            }
        }
    }

    summary
}

pub(crate) fn sp_update_progress(report: EventReport) -> SpUpdateProgress {
    // TODO-RAINCLAUDE: EventBuffer ignores NoStepsDefined (update-engine
    // TODO-RAINCLAUDE: buffer.rs), so root_execution_summary() would be None and
    // TODO-RAINCLAUDE: we would report Waiting. Preserve the prior Skipped
    // TODO-RAINCLAUDE: classification with a narrow pre-check on that one variant.
    let has_no_steps_defined = report.step_events.iter().any(|event| {
        if let StepEventKind::NoStepsDefined = event.kind {
            true
        } else {
            false
        }
    });
    if has_no_steps_defined {
        return SpUpdateProgress::Skipped;
    }

    let mut event_buffer = EventBuffer::default();
    event_buffer.add_event_report(report);

    let Some(summary) = event_buffer.root_execution_summary() else {
        return SpUpdateProgress::Waiting;
    };

    match summary.execution_status {
        ExecutionStatus::NotStarted => SpUpdateProgress::Waiting,
        ExecutionStatus::Running { step_key, .. } => {
            SpUpdateProgress::InProgress {
                step: step_key.index as u32 + 1,
                total_steps: summary.total_steps as u32,
                description: running_step_description(&event_buffer),
            }
        }
        ExecutionStatus::Terminal(info) => match info.kind {
            TerminalKind::Aborted => {
                let detail = event_buffer
                    .get(&info.step_key)
                    .and_then(|data| data.step_status().abort_reason())
                    .and_then(|reason| reason.step_aborted_info())
                    .map(|info| info.message.clone());
                let message = finalize_terminal_message(
                    &event_buffer,
                    &info.step_key,
                    detail,
                    "update aborted; no abort details were recorded in the \
                     event buffer",
                );
                SpUpdateProgress::Aborted { message }
            }
            TerminalKind::Failed => {
                // TODO-RAINCLAUDE: fold FailureInfo.causes into the one-line
                // TODO-RAINCLAUDE: detail so the operator sees the whole error
                // TODO-RAINCLAUDE: chain, not just the top-level message. AbortInfo
                // TODO-RAINCLAUDE: (below) carries no causes, so the Aborted arm has
                // TODO-RAINCLAUDE: nothing analogous to fold.
                let detail = event_buffer
                    .get(&info.step_key)
                    .and_then(|data| data.step_status().failure_reason())
                    .and_then(|reason| reason.step_failed_info())
                    .map(|failure| {
                        let mut detail = failure.message.clone();
                        for cause in &failure.causes {
                            detail.push_str(": ");
                            detail.push_str(cause);
                        }
                        detail
                    });
                let message = finalize_terminal_message(
                    &event_buffer,
                    &info.step_key,
                    detail,
                    "update failed; no failure details were recorded in the \
                     event buffer",
                );
                SpUpdateProgress::Failed { message }
            }
            TerminalKind::Completed => {
                let StepOutcomeSummary { did_real_work, warnings } =
                    collect_step_outcomes(&event_buffer);
                if did_real_work {
                    SpUpdateProgress::Completed { warnings }
                } else {
                    SpUpdateProgress::Skipped
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO-RAINCLAUDE: an empty report has no root execution, so the projection
    // TODO-RAINCLAUDE: reports Waiting (an update was started but nothing has
    // TODO-RAINCLAUDE: happened yet).
    #[test]
    fn empty_report_projects_waiting() {
        assert_eq!(
            sp_update_progress(EventReport::default()),
            SpUpdateProgress::Waiting,
        );
    }
}
