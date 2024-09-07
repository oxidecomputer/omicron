// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Types and code shared between `LineDisplay` and `GroupDisplay`.

use std::{
    collections::HashMap,
    fmt::{self, Write as _},
    sync::LazyLock,
    time::Duration,
};

use chrono::{DateTime, Utc};
use owo_colors::OwoColorize;
use swrite::{swrite, SWrite as _};

use crate::{
    display::ProgressRatioDisplay,
    events::{
        ProgressCounter, ProgressEvent, ProgressEventKind, StepEvent,
        StepEventKind, StepInfo, StepOutcome,
    },
    EventBuffer, ExecutionId, ExecutionTerminalInfo, StepKey, StepSpec,
    TerminalKind,
};

use super::LineDisplayStyles;

// This is chosen to leave enough room for all possible headers: "Completed" at
// 9 characters is the longest.
pub(super) const HEADER_WIDTH: usize = 9;

#[derive(Debug, Default)]
pub(super) struct LineDisplayShared {
    // The start time, if provided.
    start_time: Option<DateTime<Utc>>,
    // This is a map from root execution ID to data about it.
    execution_data: HashMap<ExecutionId, ExecutionData>,
}

impl LineDisplayShared {
    pub(super) fn with_context<'a>(
        &'a mut self,
        prefix: &'a str,
        formatter: &'a LineDisplayFormatter,
    ) -> LineDisplaySharedContext<'a> {
        LineDisplaySharedContext { shared: self, prefix, formatter }
    }

    pub(super) fn set_start_time(&mut self, start_time: DateTime<Utc>) {
        self.start_time = Some(start_time);
    }
}

#[derive(Debug)]
pub(super) struct LineDisplaySharedContext<'a> {
    shared: &'a mut LineDisplayShared,
    prefix: &'a str,
    formatter: &'a LineDisplayFormatter,
}

impl<'a> LineDisplaySharedContext<'a> {
    /// Produces a generic line from the prefix and message.
    ///
    /// This line does not have a trailing newline; adding one is the caller's
    /// responsibility.
    pub(super) fn format_generic(&self, message: &str) -> String {
        let mut line = self.formatter.start_line(
            self.prefix,
            self.shared.start_time,
            None,
        );
        line.push_str(message);
        line
    }

    /// Produces lines for this event buffer, and advances internal state.
    ///
    /// Returned lines do not have a trailing newline; adding them is the
    /// caller's responsibility.
    pub(super) fn format_event_buffer<S: StepSpec>(
        &mut self,
        buffer: &EventBuffer<S>,
        out: &mut LineDisplayOutput,
    ) {
        let Some(execution_id) = buffer.root_execution_id() else {
            // No known events, so nothing to display.
            return;
        };
        let execution_data =
            self.shared.execution_data.entry(execution_id).or_default();
        let prev_progress_event_at = execution_data.last_progress_event_at;
        let mut current_progress_event_at = prev_progress_event_at;

        let report =
            buffer.generate_report_since(&mut execution_data.last_seen);

        for event in &report.step_events {
            self.format_step_event(buffer, event, out);
        }

        // Update progress events.
        for event in &report.progress_events {
            if Some(event.total_elapsed) > prev_progress_event_at {
                self.format_progress_event(buffer, event, out);
                current_progress_event_at =
                    current_progress_event_at.max(Some(event.total_elapsed));
            }
        }

        // Finally, write to last_progress_event_at. (Need to re-fetch execution data.)
        let execution_data = self
            .shared
            .execution_data
            .get_mut(&execution_id)
            .expect("we created this execution data above");
        execution_data.last_progress_event_at = current_progress_event_at;
    }

    /// Format this step event.
    fn format_step_event<S: StepSpec>(
        &self,
        buffer: &EventBuffer<S>,
        step_event: &StepEvent<S>,
        out: &mut LineDisplayOutput,
    ) {
        self.format_step_event_impl(
            buffer,
            step_event,
            Default::default(),
            step_event.total_elapsed,
            out,
        );
    }

    fn format_step_event_impl<S: StepSpec, S2: StepSpec>(
        &self,
        buffer: &EventBuffer<S>,
        step_event: &StepEvent<S2>,
        mut nest_data: NestData,
        root_total_elapsed: Duration,
        out: &mut LineDisplayOutput,
    ) {
        match &step_event.kind {
            StepEventKind::NoStepsDefined => {
                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(step_event.total_elapsed),
                );
                swrite!(
                    line,
                    "{}",
                    "No steps defined"
                        .style(self.formatter.styles.progress_style),
                );
                out.add_line(line);
            }
            StepEventKind::ExecutionStarted { first_step, .. } => {
                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &first_step.info,
                    &nest_data,
                );
                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Running".style(self.formatter.styles.progress_style),
                );
                self.formatter.add_step_info(&mut line, ld_step_info);
                out.add_line(line);
            }
            StepEventKind::AttemptRetry {
                step,
                next_attempt,
                attempt_elapsed,
                message,
                ..
            } => {
                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &step.info,
                    &nest_data,
                );

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Retry".style(self.formatter.styles.warning_style)
                );
                self.formatter.add_step_info(&mut line, ld_step_info);
                swrite!(
                    line,
                    ": after {:.2?}",
                    attempt_elapsed.style(self.formatter.styles.meta_style),
                );
                if *next_attempt > 1 {
                    swrite!(
                        line,
                        " (at attempt {})",
                        next_attempt
                            .saturating_sub(1)
                            .style(self.formatter.styles.meta_style),
                    );
                }
                swrite!(
                    line,
                    " with message: {}",
                    message.style(self.formatter.styles.warning_message_style)
                );

                out.add_line(line);
            }
            StepEventKind::ProgressReset {
                step,
                attempt,
                attempt_elapsed,
                message,
                ..
            } => {
                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &step.info,
                    &nest_data,
                );

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Reset".style(self.formatter.styles.warning_style)
                );
                self.formatter.add_step_info(&mut line, ld_step_info);
                swrite!(
                    line,
                    ": after {:.2?}",
                    attempt_elapsed.style(self.formatter.styles.meta_style),
                );
                if *attempt > 1 {
                    swrite!(
                        line,
                        " (at attempt {})",
                        attempt.style(self.formatter.styles.meta_style),
                    );
                }
                swrite!(
                    line,
                    " with message: {}",
                    message.style(self.formatter.styles.warning_message_style)
                );

                out.add_line(line);
            }
            StepEventKind::StepCompleted {
                step,
                attempt,
                outcome,
                next_step,
                attempt_elapsed,
                ..
            } => {
                // --- Add completion info about this step.

                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &step.info,
                    &nest_data,
                );
                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                self.formatter.add_completion_and_step_info(
                    &mut line,
                    ld_step_info,
                    *attempt_elapsed,
                    *attempt,
                    outcome,
                );

                out.add_line(line);

                // --- Add information about the next step.

                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &next_step.info,
                    &nest_data,
                );

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                self.format_step_running(&mut line, ld_step_info);

                out.add_line(line);
            }
            StepEventKind::ExecutionCompleted {
                last_step,
                last_attempt,
                last_outcome,
                attempt_elapsed,
                ..
            } => {
                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &last_step.info,
                    &nest_data,
                );

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                self.formatter.add_completion_and_step_info(
                    &mut line,
                    ld_step_info,
                    *attempt_elapsed,
                    *last_attempt,
                    last_outcome,
                );

                out.add_line(line);
            }
            StepEventKind::ExecutionFailed {
                failed_step,
                total_attempts,
                attempt_elapsed,
                message,
                causes,
                ..
            } => {
                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &failed_step.info,
                    &nest_data,
                );

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );
                // The prefix is used for "Caused by" lines below. Add
                // the requisite amount of spacing here.
                let mut caused_by_prefix = line.clone();
                swrite!(caused_by_prefix, "{:>HEADER_WIDTH$} ", "");
                nest_data.add_prefix(&mut caused_by_prefix);

                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Failed".style(self.formatter.styles.error_style)
                );

                self.formatter.add_step_info(&mut line, ld_step_info);
                line.push_str(": ");

                self.formatter.add_failure_info(
                    &mut line,
                    &caused_by_prefix,
                    *attempt_elapsed,
                    *total_attempts,
                    message,
                    causes,
                );

                out.add_line(line);
            }
            StepEventKind::ExecutionAborted {
                aborted_step,
                attempt,
                attempt_elapsed,
                message,
                ..
            } => {
                let ld_step_info = LineDisplayStepInfo::new(
                    buffer,
                    step_event.execution_id,
                    &aborted_step.info,
                    &nest_data,
                );

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Aborted".style(self.formatter.styles.error_style)
                );
                self.formatter.add_step_info(&mut line, ld_step_info);
                line.push_str(": ");

                self.formatter.add_abort_info(
                    &mut line,
                    *attempt_elapsed,
                    *attempt,
                    message,
                );

                out.add_line(line);
            }
            StepEventKind::Nested { step, event, .. } => {
                // Look up the child event's ID to add to the nest data.
                let child_step_key = StepKey {
                    execution_id: event.execution_id,
                    // XXX: we currently look up index 0 because that should
                    // always exist (unless no steps are defined, in which case
                    // we skip this). The child index is actually shared by all
                    // steps within an execution. Fix this by changing
                    // EventBuffer to also track general per-execution data.
                    index: 0,
                };
                let Some(child_step_data) = buffer.get(&child_step_key) else {
                    // This should only happen if no steps are defined. See TODO
                    // above.
                    return;
                };
                let (_, child_index) = child_step_data
                    .parent_key_and_child_index()
                    .expect("child steps should have a child index");

                nest_data.add_nest_level(step.info.index, child_index);

                self.format_step_event_impl(
                    buffer,
                    &**event,
                    nest_data,
                    root_total_elapsed,
                    out,
                );
            }
            StepEventKind::Unknown => {}
        }
    }

    fn format_step_running<S: StepSpec>(
        &self,
        line: &mut String,
        ld_step_info: LineDisplayStepInfo<'_, S>,
    ) {
        swrite!(
            line,
            "{:>HEADER_WIDTH$} ",
            "Running".style(self.formatter.styles.progress_style),
        );
        self.formatter.add_step_info(line, ld_step_info);
    }

    /// Formats this terminal information.
    ///
    /// This line does not have a trailing newline; adding one is the caller's
    /// responsibility.
    pub(super) fn format_terminal_info(
        &self,
        info: &ExecutionTerminalInfo,
    ) -> String {
        let mut line = self.formatter.start_line(
            self.prefix,
            self.shared.start_time,
            info.leaf_total_elapsed,
        );
        match info.kind {
            TerminalKind::Completed => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} Execution {}",
                    "Terminal".style(self.formatter.styles.progress_style),
                    "completed".style(self.formatter.styles.progress_style),
                );
            }
            TerminalKind::Failed => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} Execution {}",
                    "Terminal".style(self.formatter.styles.error_style),
                    "failed".style(self.formatter.styles.error_style),
                );
            }
            TerminalKind::Aborted => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} Execution {}",
                    "Terminal".style(self.formatter.styles.error_style),
                    "aborted".style(self.formatter.styles.error_style),
                );
            }
        }
        line
    }

    fn format_progress_event<S: StepSpec, S2: StepSpec>(
        &self,
        buffer: &EventBuffer<S>,
        progress_event: &ProgressEvent<S2>,
        out: &mut LineDisplayOutput,
    ) {
        self.format_progress_event_impl(
            buffer,
            progress_event,
            NestData::default(),
            progress_event.total_elapsed,
            out,
        )
    }

    fn format_progress_event_impl<S: StepSpec, S2: StepSpec>(
        &self,
        buffer: &EventBuffer<S>,
        progress_event: &ProgressEvent<S2>,
        mut nest_data: NestData,
        root_total_elapsed: Duration,
        out: &mut LineDisplayOutput,
    ) {
        match &progress_event.kind {
            ProgressEventKind::WaitingForProgress { .. } => {
                // Don't need to show this because "Running" is shown within
                // step events.
            }
            ProgressEventKind::Progress {
                step,
                progress,
                attempt_elapsed,
                ..
            } => {
                let step_key = StepKey {
                    execution_id: progress_event.execution_id,
                    index: step.info.index,
                };
                let step_data =
                    buffer.get(&step_key).expect("step key must exist");
                let ld_step_info = LineDisplayStepInfo {
                    step_info: &step.info,
                    total_steps: step_data.total_steps(),
                    nest_data: &nest_data,
                };

                let mut line = self.formatter.start_line(
                    self.prefix,
                    self.shared.start_time,
                    Some(root_total_elapsed),
                );

                let (before, after) = match progress {
                    Some(counter) => {
                        let progress_str = format_progress_counter(counter);
                        (
                            format!(
                                "{:>HEADER_WIDTH$} ",
                                "Progress".style(
                                    self.formatter.styles.progress_style
                                )
                            ),
                            format!(
                                "{progress_str} after {:.2?}",
                                attempt_elapsed
                                    .style(self.formatter.styles.meta_style),
                            ),
                        )
                    }
                    None => {
                        let before = format!(
                            "{:>HEADER_WIDTH$} ",
                            "Running"
                                .style(self.formatter.styles.progress_style),
                        );

                        // If the attempt elapsed is non-zero, show it.
                        let after = if *attempt_elapsed > Duration::ZERO {
                            format!(
                                "after {:.2?}",
                                attempt_elapsed
                                    .style(self.formatter.styles.meta_style),
                            )
                        } else {
                            String::new()
                        };

                        (before, after)
                    }
                };

                swrite!(line, "{}", before);
                self.formatter.add_step_info(&mut line, ld_step_info);
                if !after.is_empty() {
                    swrite!(line, ": {}", after);
                }

                out.add_line(line);
            }
            ProgressEventKind::Nested { step, event, .. } => {
                // Look up the child event's ID to add to the nest data.
                let child_step_key = StepKey {
                    execution_id: event.execution_id,
                    // XXX: we currently look up index 0 because that should
                    // always exist (unless no steps are defined, in which case
                    // we skip this). The child index is actually shared by all
                    // steps within an execution. Fix this by changing
                    // EventBuffer to also track general per-execution data.
                    index: 0,
                };
                let Some(child_step_data) = buffer.get(&child_step_key) else {
                    // This should only happen if no steps are defined. See TODO
                    // above.
                    return;
                };
                let (_, child_index) = child_step_data
                    .parent_key_and_child_index()
                    .expect("child steps should have a child index");

                nest_data.add_nest_level(step.info.index, child_index);

                self.format_progress_event_impl(
                    buffer,
                    &**event,
                    nest_data,
                    root_total_elapsed,
                    out,
                );
            }
            ProgressEventKind::Unknown => {}
        }
    }
}

fn format_progress_counter(counter: &ProgressCounter) -> String {
    match counter.total {
        Some(total) => {
            // Show a percentage value. Correct alignment requires converting to
            // a string in the middle like this.
            let percent = (counter.current as f64 / total as f64) * 100.0;
            // <12.34> is 5 characters wide.
            let percent_width = 5;
            format!(
                "{:>percent_width$.2}% ({} {})",
                percent,
                ProgressRatioDisplay::current_and_total(counter.current, total)
                    .padded(true),
                counter.units,
            )
        }
        None => format!("{} {}", counter.current, counter.units),
    }
}

/// State that tracks line display formatting.
///
/// Each `LineDisplay` and `GroupDisplay` has one of these.
#[derive(Debug)]
pub(super) struct LineDisplayFormatter {
    styles: LineDisplayStyles,
    progress_interval: Duration,
}

impl LineDisplayFormatter {
    pub(super) fn new() -> Self {
        Self {
            styles: LineDisplayStyles::default(),
            progress_interval: Duration::from_secs(1),
        }
    }

    #[inline]
    pub(super) fn styles(&self) -> &LineDisplayStyles {
        &self.styles
    }

    #[inline]
    pub(super) fn set_styles(&mut self, styles: LineDisplayStyles) {
        self.styles = styles;
    }

    #[inline]
    pub(super) fn set_progress_interval(&mut self, interval: Duration) {
        self.progress_interval = interval;
    }

    // ---
    // Internal helpers
    // ---

    pub(super) fn start_line(
        &self,
        prefix: &str,
        start_time: Option<DateTime<Utc>>,
        total_elapsed: Option<Duration>,
    ) -> String {
        let mut line = format!("[{}", prefix.style(self.styles.prefix_style));

        if !prefix.is_empty() {
            line.push(' ');
        }

        // Show total elapsed time in an hh:mm:ss format.
        match (start_time, total_elapsed) {
            (Some(start_time), Some(total_elapsed)) => {
                // Add the offset from the start time.
                let current_time = start_time + total_elapsed;
                swrite!(
                    line,
                    "{}",
                    current_time.format_with_items(DATETIME_FORMAT.iter())
                );
            }
            (None, Some(total_elapsed)) => {
                let total_elapsed_secs = total_elapsed.as_secs();
                let hours = total_elapsed_secs / 3600;
                let minutes = (total_elapsed_secs % 3600) / 60;
                let seconds = total_elapsed_secs % 60;
                swrite!(line, "{:02}:{:02}:{:02}", hours, minutes, seconds);
                // To show total_elapsed more accurately, use:
                // swrite!(line, "{:.2?}", total_elapsed);
            }
            (Some(_), None) => {
                line.push_str(DATETIME_FORMAT_INDENT);
            }
            (None, None) => {
                line.push_str(ELAPSED_FORMAT_INDENT);
            }
        }

        line.push_str("] ");

        line
    }

    fn add_step_info<S: StepSpec>(
        &self,
        line: &mut String,
        ld_step_info: LineDisplayStepInfo<'_, S>,
    ) {
        ld_step_info.nest_data.add_prefix(line);

        // Print out "(<current>/<total>)" in a padded way, so that successive
        // steps are vertically aligned.
        swrite!(
            line,
            "({}) ",
            ProgressRatioDisplay::index_and_total(
                ld_step_info.step_info.index,
                ld_step_info.total_steps
            )
            .padded(true),
        );

        swrite!(
            line,
            "{}",
            ld_step_info
                .step_info
                .description
                .style(self.styles.step_name_style)
        );
    }

    pub(super) fn add_completion_and_step_info<S: StepSpec>(
        &self,
        line: &mut String,
        ld_step_info: LineDisplayStepInfo<'_, S>,
        attempt_elapsed: Duration,
        attempt: usize,
        outcome: &StepOutcome<S>,
    ) {
        let mut meta = format!(
            "after {:.2?}",
            attempt_elapsed.style(self.styles.meta_style)
        );
        if attempt > 1 {
            swrite!(
                meta,
                " (at attempt {})",
                attempt.style(self.styles.meta_style)
            );
        }

        match &outcome {
            StepOutcome::Success { message, .. } => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Completed".style(self.styles.progress_style),
                );
                self.add_step_info(line, ld_step_info);
                match message {
                    Some(message) => {
                        swrite!(
                            line,
                            ": {meta} with message: {}",
                            message.style(self.styles.progress_message_style)
                        );
                    }
                    None => {
                        swrite!(line, ": {meta}");
                    }
                }
            }
            StepOutcome::Warning { message, .. } => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Completed".style(self.styles.warning_style),
                );
                self.add_step_info(line, ld_step_info);
                swrite!(
                    line,
                    ": {meta} with warning: {}",
                    message.style(self.styles.warning_message_style)
                );
            }
            StepOutcome::Skipped { message, .. } => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Skipped".style(self.styles.skipped_style),
                );
                self.add_step_info(line, ld_step_info);
                swrite!(
                    line,
                    ": {}",
                    message.style(self.styles.warning_message_style)
                );
            }
        };
    }

    pub(super) fn add_failure_info(
        &self,
        line: &mut String,
        line_prefix: &str,
        attempt_elapsed: Duration,
        total_attempts: usize,
        message: &str,
        causes: &[String],
    ) {
        let mut meta = format!(
            "after {:.2?}",
            attempt_elapsed.style(self.styles.meta_style)
        );
        if total_attempts > 1 {
            swrite!(
                meta,
                " (after {} attempts)",
                total_attempts.style(self.styles.meta_style)
            );
        }

        swrite!(
            line,
            "{meta}: {}",
            message.style(self.styles.error_message_style)
        );
        if !causes.is_empty() {
            swrite!(
                line,
                "\n{line_prefix}{}",
                "  Caused by:".style(self.styles.meta_style)
            );
            for cause in causes {
                swrite!(line, "\n{line_prefix}  - {}", cause);
            }
        }

        // The last newline is added by the caller.
    }

    pub(super) fn add_abort_info(
        &self,
        line: &mut String,
        attempt_elapsed: Duration,
        attempt: usize,
        message: &str,
    ) {
        let mut meta = format!(
            "after {:.2?}",
            attempt_elapsed.style(self.styles.meta_style)
        );
        if attempt > 1 {
            swrite!(
                meta,
                " (at attempt {})",
                attempt.style(self.styles.meta_style)
            );
        }

        swrite!(line, "{meta} with message \"{}\"", message);
    }
}

static DATETIME_FORMAT: LazyLock<Vec<chrono::format::Item<'static>>> =
    LazyLock::new(|| {
        // The format is "Jan 01 00:00:00".
        //
        // We can add customization in the future, but we want to restrict
        // formats to fixed-width so we know how to align them.
        chrono::format::StrftimeItems::new("%b %d %H:%M:%S")
            .parse()
            .expect("datetime format is valid")
    });

// "Jan 01 00:00:00" is 15 characters wide.
const DATETIME_FORMAT_INDENT: &str = "               ";

// "00:00:00" is 8 characters wide.
const ELAPSED_FORMAT_INDENT: &str = "        ";

#[derive(Clone, Debug)]
pub(super) struct LineDisplayOutput {
    lines: Vec<String>,
}

impl LineDisplayOutput {
    pub(super) fn new() -> Self {
        Self { lines: Vec::new() }
    }

    pub(super) fn add_line(&mut self, line: String) {
        self.lines.push(line);
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &str> {
        self.lines.iter().map(|line| line.as_str())
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LineDisplayStepInfo<'a, S: StepSpec> {
    pub(super) step_info: &'a StepInfo<S>,
    pub(super) total_steps: usize,
    pub(super) nest_data: &'a NestData,
}

impl<'a, S: StepSpec> LineDisplayStepInfo<'a, S> {
    fn new<S2: StepSpec>(
        buffer: &'a EventBuffer<S2>,
        execution_id: ExecutionId,
        step_info: &'a StepInfo<S>,
        nest_data: &'a NestData,
    ) -> Self {
        let step_key = StepKey { execution_id, index: step_info.index };
        let step_data = buffer.get(&step_key).expect("step key must exist");
        LineDisplayStepInfo {
            step_info,
            total_steps: step_data.total_steps(),
            nest_data,
        }
    }
}

/// Per-step stateful data tracked by the line displayer.
#[derive(Debug, Default)]
struct ExecutionData {
    /// The last seen root event index.
    ///
    /// This is used to avoid displaying the same event twice.
    last_seen: Option<usize>,

    /// The last `root_total_elapsed` at which a progress event was displayed for
    /// this execution.
    last_progress_event_at: Option<Duration>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct NestData {
    nest_indexes: Vec<NestIndex>,
}

impl NestData {
    fn add_nest_level(&mut self, parent_step_index: usize, child_index: usize) {
        self.nest_indexes.push(NestIndex { parent_step_index, child_index });
    }

    fn add_prefix(&self, line: &mut String) {
        if !self.nest_indexes.is_empty() {
            line.push_str(&"..".repeat(self.nest_indexes.len()));
            line.push_str(" ");
        }

        for nest_index in &self.nest_indexes {
            swrite!(
                line,
                "{}{} ",
                // Add 1 to the index to make it 1-based.
                nest_index.parent_step_index + 1,
                AsLetters(nest_index.child_index)
            );
        }
    }
}

#[derive(Clone, Debug)]
struct NestIndex {
    parent_step_index: usize,
    // If a parent has multiple nested executions, this counts which execution
    // this is, up from 0.
    child_index: usize,
}

/// A display impl that converts a 0-based index into a letter or a series of
/// letters.
///
/// This is effectively a conversion to base 26.
struct AsLetters(usize);

impl fmt::Display for AsLetters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut index = self.0;
        loop {
            let letter = (b'a' + (index % 26) as u8) as char;
            f.write_char(letter)?;
            index /= 26;
            if index == 0 {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_format_progress_counter() {
        let tests = vec![
            (ProgressCounter::new(5, 20, "units"), "25.00% ( 5/20 units)"),
            (ProgressCounter::new(0, 20, "bytes"), " 0.00% ( 0/20 bytes)"),
            (ProgressCounter::new(20, 20, "cubes"), "100.00% (20/20 cubes)"),
            // NaN is a weird case that is a buggy update engine impl in practice
            (ProgressCounter::new(0, 0, "units"), "  NaN% (0/0 units)"),
            (ProgressCounter::current(5, "units"), "5 units"),
        ];
        for (input, output) in tests {
            assert_eq!(
                format_progress_counter(&input),
                output,
                "format matches for input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_start_line() {
        let formatter = LineDisplayFormatter::new();
        let prefix = "prefix";
        let start_time = Utc.with_ymd_and_hms(2023, 2, 8, 3, 40, 56).unwrap();

        assert_eq!(
            formatter.start_line(prefix, None, None),
            "[prefix         ] ",
        );
        assert_eq!(
            formatter.start_line(prefix, None, Some(Duration::from_secs(5))),
            "[prefix 00:00:05] ",
        );
        assert_eq!(
            formatter.start_line(prefix, Some(start_time), None),
            "[prefix                ] ",
        );
        assert_eq!(
            formatter.start_line(
                prefix,
                Some(start_time),
                Some(Duration::from_secs(3600)),
            ),
            "[prefix Feb 08 04:40:56] ",
        );
    }
}
