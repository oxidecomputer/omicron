// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Types and code shared between `LineDisplay` and `GroupDisplay`.

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{self, Write as _},
    time::Duration,
};

use owo_colors::OwoColorize;
use swrite::{swrite, SWrite as _};

use crate::{
    events::{
        LowPriorityStepEventKind, ProgressCounter, ProgressEvent, StepEvent,
        StepInfo, StepOutcome,
    },
    AbortInfo, AbortReason, CompletionInfo, EventBuffer, EventBufferStepData,
    ExecutionTerminalInfo, FailureInfo, FailureReason, NestedSpec,
    RootEventIndex, StepKey, StepSpec, StepStatus, TerminalKind,
};

use super::LineDisplayStyles;

// This is chosen to leave enough room for all possible headers: "Completed" at
// 9 characters is the longest.
pub(super) const HEADER_WIDTH: usize = 9;

#[derive(Debug)]
pub(super) struct LineDisplayShared {
    step_data: HashMap<StepKey, StepData>,
}

impl LineDisplayShared {
    pub(super) fn new() -> Self {
        Self { step_data: HashMap::new() }
    }

    pub(super) fn with_context<'a>(
        &'a mut self,
        prefix: &'a str,
        formatter: &'a LineDisplayFormatter,
    ) -> LineDisplaySharedContext<'_> {
        LineDisplaySharedContext { shared: self, prefix, formatter }
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
        let mut line = self.formatter.start_println(self.prefix);
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
        out: &mut Vec<String>,
    ) {
        let steps = buffer.steps();
        for (step_key, data) in steps.as_slice() {
            self.format_step_and_update(buffer, *step_key, data, out);
        }
    }

    /// Produces lines corresponding to this step, and advances internal state.
    ///
    /// Returned lines do not have a trailing newline; adding them is the
    /// caller's responsibility.
    pub(super) fn format_step_and_update<S: StepSpec>(
        &mut self,
        buffer: &EventBuffer<S>,
        step_key: StepKey,
        data: &EventBufferStepData<S>,
        out: &mut Vec<String>,
    ) {
        let ld_step_info = LineDisplayStepInfo {
            step_info: data.step_info(),
            parent_key_and_child_index: data.parent_key_and_child_index(),
            total_steps: data.total_steps(),
        };
        let nest_level = data.nest_level();
        let step_status = data.step_status();

        match step_status {
            StepStatus::NotStarted => {}
            StepStatus::Running { progress_event, low_priority } => {
                for event in low_priority {
                    self.format_low_priority_event(
                        event,
                        step_key,
                        ld_step_info,
                        nest_level,
                        out,
                    );
                }

                self.format_progress_event(
                    progress_event,
                    step_key,
                    data,
                    ld_step_info,
                    nest_level,
                    out,
                );

                self.insert_or_update_index(
                    step_key,
                    data.last_root_event_index(),
                );
            }
            StepStatus::Completed { info } => {
                if let Some(sd) = self.shared.step_data.get(&step_key) {
                    if sd.last_root_event_index >= data.last_root_event_index()
                    {
                        // We've already displayed this event.
                        return;
                    }
                }

                match info {
                    Some(info) => {
                        let mut line = self.formatter.start_line(
                            self.prefix,
                            Some(info.root_total_elapsed),
                        );
                        self.formatter.add_completion_and_step_info(
                            &mut line,
                            NestLevel::Regular(nest_level),
                            ld_step_info,
                            info,
                        );
                        out.push(line);
                    }
                    None => {
                        // This means that we don't know what happened to the step
                        // but it did complete.
                        let mut line =
                            self.formatter.start_line(self.prefix, None);
                        swrite!(
                            line,
                            "{:>HEADER_WIDTH$} ",
                            "Completed"
                                .style(self.formatter.styles.progress_style),
                        );
                        self.formatter.add_step_info(
                            &mut line,
                            ld_step_info,
                            NestLevel::Regular(nest_level),
                        );
                        swrite!(
                            line,
                            ": with {}",
                            "unknown outcome"
                                .style(self.formatter.styles.meta_style),
                        );
                        out.push(line);
                    }
                }

                self.insert_or_update_index(
                    step_key,
                    data.last_root_event_index(),
                );
            }
            StepStatus::Failed { reason } => {
                if let Some(ld) = self.shared.step_data.get(&step_key) {
                    if ld.last_root_event_index >= data.last_root_event_index()
                    {
                        // We've already displayed this event.
                        return;
                    }
                }

                match reason {
                    FailureReason::StepFailed(info) => {
                        let mut line = self.formatter.start_line(
                            self.prefix,
                            Some(info.root_total_elapsed),
                        );
                        let nest_level = NestLevel::Regular(nest_level);

                        // The prefix is used for "Caused by" lines below. Add
                        // the requisite amount of spacing here.
                        let mut caused_by_prefix = line.clone();
                        swrite!(caused_by_prefix, "{:>HEADER_WIDTH$} ", "");
                        nest_level.add_prefix(&mut caused_by_prefix);

                        swrite!(
                            line,
                            "{:>HEADER_WIDTH$} ",
                            "Failed".style(self.formatter.styles.error_style)
                        );
                        self.formatter.add_step_info(
                            &mut line,
                            ld_step_info,
                            nest_level,
                        );
                        line.push_str(": ");
                        self.formatter.add_failure_info(
                            &mut line,
                            &caused_by_prefix,
                            info,
                        );
                        out.push(line);
                    }
                    FailureReason::ParentFailed { parent_step } => {
                        let parent_step_info = buffer
                            .get(&parent_step)
                            .expect("parent step must exist");
                        let mut line =
                            self.formatter.start_line(self.prefix, None);
                        swrite!(
                            line,
                            "{:>HEADER_WIDTH$} ",
                            "Failed".style(self.formatter.styles.error_style)
                        );
                        self.formatter.add_step_info(
                            &mut line,
                            ld_step_info,
                            NestLevel::Regular(nest_level),
                        );
                        swrite!(
                            line,
                            ": because parent step {} failed",
                            parent_step_info
                                .step_info()
                                .description
                                .style(self.formatter.styles.step_name_style)
                        );
                        out.push(line);
                    }
                }

                self.insert_or_update_index(
                    step_key,
                    data.last_root_event_index(),
                );
            }
            StepStatus::Aborted { reason, .. } => {
                if let Some(ld) = self.shared.step_data.get(&step_key) {
                    if ld.last_root_event_index >= data.last_root_event_index()
                    {
                        // We've already displayed this event.
                        return;
                    }
                }

                match reason {
                    AbortReason::StepAborted(info) => {
                        let mut line = self.formatter.start_line(
                            self.prefix,
                            Some(info.root_total_elapsed),
                        );
                        swrite!(
                            line,
                            "{:>HEADER_WIDTH$} ",
                            "Aborted".style(self.formatter.styles.error_style)
                        );
                        self.formatter.add_step_info(
                            &mut line,
                            ld_step_info,
                            NestLevel::Regular(nest_level),
                        );
                        line.push_str(": ");
                        self.formatter.add_abort_info(&mut line, info);
                        out.push(line);
                    }
                    AbortReason::ParentAborted { parent_step } => {
                        let parent_step_info = buffer
                            .get(&parent_step)
                            .expect("parent step must exist");
                        let mut line =
                            self.formatter.start_line(self.prefix, None);
                        swrite!(
                            line,
                            "{:>HEADER_WIDTH$} ",
                            "Aborted".style(self.formatter.styles.error_style)
                        );
                        self.formatter.add_step_info(
                            &mut line,
                            ld_step_info,
                            NestLevel::Regular(nest_level),
                        );
                        swrite!(
                            line,
                            ": because parent step {} aborted",
                            parent_step_info
                                .step_info()
                                .description
                                .style(self.formatter.styles.step_name_style)
                        );
                        out.push(line);
                    }
                }

                self.insert_or_update_index(
                    step_key,
                    data.last_root_event_index(),
                );
            }
            StepStatus::WillNotBeRun { .. } => {
                // We don't print "will not be run". (TODO: maybe add an
                // extended mode which does do so?)
            }
        }
    }

    /// Formats this terminal information.
    ///
    /// This line does not have a trailing newline; adding one is the caller's
    /// responsibility.
    pub(super) fn format_terminal_info(
        &self,
        info: &ExecutionTerminalInfo,
    ) -> String {
        let mut line =
            self.formatter.start_line(self.prefix, info.leaf_total_elapsed);
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

    fn format_low_priority_event<S: StepSpec>(
        &mut self,
        event: &StepEvent<S>,
        step_key: StepKey,
        ld_step_info: LineDisplayStepInfo<'_>,
        nest_level: usize,
        out: &mut Vec<String>,
    ) {
        if let Some(sd) = self.shared.step_data.get(&step_key) {
            if sd.last_root_event_index >= RootEventIndex(event.event_index) {
                // We've already displayed this event.
                return;
            }
        }

        let Some(lowpri_event) = event.to_low_priority() else {
            // Can't show anything for unknown events.
            return;
        };

        let mut line =
            self.formatter.start_line(self.prefix, Some(event.total_elapsed));

        match lowpri_event.kind {
            LowPriorityStepEventKind::ProgressReset {
                attempt,
                attempt_elapsed,
                message,
                ..
            } => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Reset".style(self.formatter.styles.warning_style)
                );
                self.formatter.add_step_info(
                    &mut line,
                    ld_step_info,
                    NestLevel::Regular(nest_level),
                );
                swrite!(
                    line,
                    ": after {:.2?}",
                    attempt_elapsed.style(self.formatter.styles.meta_style),
                );
                if attempt > 1 {
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
            }
            LowPriorityStepEventKind::AttemptRetry {
                next_attempt,
                attempt_elapsed,
                message,
                ..
            } => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Retry".style(self.formatter.styles.warning_style)
                );
                self.formatter.add_step_info(
                    &mut line,
                    ld_step_info,
                    NestLevel::Regular(nest_level),
                );
                swrite!(
                    line,
                    ": after {:.2?}",
                    attempt_elapsed.style(self.formatter.styles.meta_style),
                );
                swrite!(
                    line,
                    " (at attempt {})",
                    next_attempt
                        .saturating_sub(1)
                        .style(self.formatter.styles.meta_style),
                );
                swrite!(
                    line,
                    " with message: {}",
                    message.style(self.formatter.styles.warning_message_style)
                );
            }
        }

        out.push(line);
    }

    fn format_progress_event<S: StepSpec>(
        &mut self,
        progress_event: &ProgressEvent<S>,
        step_key: StepKey,
        data: &EventBufferStepData<S>,
        ld_step_info: LineDisplayStepInfo<'_>,
        nest_level: usize,
        out: &mut Vec<String>,
    ) {
        let Some(leaf_step_elapsed) = progress_event.kind.leaf_step_elapsed()
        else {
            // Can't show anything for unknown events.
            return;
        };
        let leaf_attempt = progress_event
            .kind
            .leaf_attempt()
            .expect("if leaf_step_elapsed is Some, leaf_attempt must be Some");
        let leaf_attempt_elapsed = progress_event
            .kind
            .leaf_attempt_elapsed()
            .expect(
            "if leaf_step_elapsed is Some, leaf_attempt_elapsed must be Some",
        );

        let sd = self
            .shared
            .step_data
            .entry(step_key)
            .or_insert_with(|| StepData::new(data.last_root_event_index()));

        let (is_first_event, should_display) = match sd.last_progress_event_at {
            Some(last_progress_event_at) => {
                // Don't show events with zero attempt elapsed time unless
                // they're the first (the others will be shown as part of
                // low-priority step events).
                let should_display = if leaf_attempt > 1 {
                    leaf_attempt_elapsed > Duration::ZERO
                } else {
                    true
                };
                // Show further progress events only after the progress interval
                // has elapsed.
                let should_display = should_display
                    && leaf_step_elapsed
                        > last_progress_event_at
                            + self.formatter.progress_interval;
                (false, should_display)
            }
            None => (true, true),
        };

        if should_display {
            let mut line = self
                .formatter
                .start_line(self.prefix, Some(progress_event.total_elapsed));
            let nest_level = if is_first_event {
                NestLevel::Regular(nest_level)
            } else {
                // Add an extra half-indent for non-first progress events.
                NestLevel::ExtraHalf(nest_level)
            };

            let (before, after) = match progress_event.kind.progress_counter() {
                Some(counter) => {
                    let progress_str = format_progress_counter(counter);
                    (
                        format!(
                            "{:>HEADER_WIDTH$} ",
                            "Progress"
                                .style(self.formatter.styles.progress_style)
                        ),
                        format!(
                            "{progress_str} after {:.2?}",
                            leaf_attempt_elapsed
                                .style(self.formatter.styles.meta_style),
                        ),
                    )
                }
                None => {
                    let before = format!(
                        "{:>HEADER_WIDTH$} ",
                        "Running".style(self.formatter.styles.progress_style),
                    );

                    // If the leaf attempt elapsed is non-zero, show it.
                    let after = if leaf_attempt_elapsed > Duration::ZERO {
                        format!(
                            "after {:.2?}",
                            leaf_attempt_elapsed
                                .style(self.formatter.styles.meta_style),
                        )
                    } else {
                        String::new()
                    };

                    (before, after)
                }
            };

            swrite!(line, "{}", before);
            self.formatter.add_step_info(&mut line, ld_step_info, nest_level);
            if !after.is_empty() {
                swrite!(line, ": {}", after);
            }

            out.push(line);

            sd.update_progress_event(leaf_step_elapsed);
        }
    }

    fn insert_or_update_index(
        &mut self,
        step_key: StepKey,
        last_root_index: RootEventIndex,
    ) {
        match self.shared.step_data.entry(step_key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().update_last_root_event_index(last_root_index);
            }
            Entry::Vacant(entry) => {
                entry.insert(StepData::new(last_root_index));
            }
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
            let counter_width = total.to_string().len();
            format!(
                "{:>percent_width$.2}% ({:>counter_width$}/{} {})",
                percent, counter.current, total, counter.units,
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

    fn start_println(&self, prefix: &str) -> String {
        if !prefix.is_empty() {
            format!("[{}] ", prefix.style(self.styles.prefix_style))
        } else {
            String::new()
        }
    }

    fn start_line(
        &self,
        prefix: &str,
        total_elapsed: Option<Duration>,
    ) -> String {
        let mut line = format!("[{}", prefix.style(self.styles.prefix_style));

        if !prefix.is_empty() {
            line.push(' ');
        }

        // Show total elapsed time in an hh:mm:ss format.
        if let Some(total_elapsed) = total_elapsed {
            let total_elapsed = total_elapsed.as_secs();
            let hours = total_elapsed / 3600;
            let minutes = (total_elapsed % 3600) / 60;
            let seconds = total_elapsed % 60;
            swrite!(&mut line, "{:02}:{:02}:{:02}", hours, minutes, seconds);
        } else {
            // Add 8 spaces to align with hh:mm:ss.
            line.push_str("        ");
        }

        line.push_str("] ");

        line
    }

    fn add_step_info(
        &self,
        line: &mut String,
        ld_step_info: LineDisplayStepInfo<'_>,
        nest_level: NestLevel,
    ) {
        nest_level.add_prefix(line);

        match ld_step_info.parent_key_and_child_index {
            Some((parent_key, child_index)) => {
                // Print e.g. (6a .
                swrite!(
                    line,
                    "({}{} ",
                    // Add 1 to the index to make it 1-based.
                    parent_key.index + 1,
                    AsLetters(child_index)
                );
            }
            None => {
                swrite!(line, "(");
            }
        };

        // Print out "<step index>/<total steps>)". Leave space such that we
        // print out e.g. "1/8)" and " 3/14)".
        // Add 1 to the index to make it 1-based.
        let step_index = ld_step_info.step_info.index + 1;
        let step_index_width = ld_step_info.total_steps.to_string().len();
        swrite!(
            line,
            "{:width$}/{:width$}) ",
            step_index,
            ld_step_info.total_steps,
            width = step_index_width
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

    pub(super) fn add_completion_and_step_info(
        &self,
        line: &mut String,
        nest_level: NestLevel,
        ld_step_info: LineDisplayStepInfo<'_>,
        info: &CompletionInfo,
    ) {
        let mut meta = format!(
            "after {:.2?}",
            info.step_elapsed.style(self.styles.meta_style)
        );
        if info.attempt > 1 {
            swrite!(
                meta,
                " (at attempt {})",
                info.attempt.style(self.styles.meta_style)
            );
        }

        match &info.outcome {
            StepOutcome::Success { message, .. } => {
                swrite!(
                    line,
                    "{:>HEADER_WIDTH$} ",
                    "Completed".style(self.styles.progress_style),
                );
                self.add_step_info(line, ld_step_info, nest_level);
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
                self.add_step_info(line, ld_step_info, nest_level);
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
                self.add_step_info(line, ld_step_info, nest_level);
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
        info: &FailureInfo,
    ) {
        let mut meta = format!(
            "after {:.2?}",
            info.step_elapsed.style(self.styles.meta_style)
        );
        if info.total_attempts > 1 {
            swrite!(
                meta,
                " (after {} attempts)",
                info.total_attempts.style(self.styles.meta_style)
            );
        }

        swrite!(
            line,
            "{meta}: {}",
            info.message.style(self.styles.error_message_style)
        );
        if !info.causes.is_empty() {
            swrite!(
                line,
                "\n{line_prefix}{}",
                "  Caused by:".style(self.styles.meta_style)
            );
            for cause in &info.causes {
                swrite!(line, "\n{line_prefix}  - {}", cause);
            }
        }

        // The last newline is added by the caller.
    }

    pub(super) fn add_abort_info(&self, line: &mut String, info: &AbortInfo) {
        let mut meta = format!(
            "after {:.2?}",
            info.step_elapsed.style(self.styles.meta_style)
        );
        if info.attempt > 1 {
            swrite!(
                meta,
                " (at attempt {})",
                info.attempt.style(self.styles.meta_style)
            );
        }

        swrite!(line, "{meta} with message \"{}\"", info.message,);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LineDisplayStepInfo<'a> {
    pub(super) step_info: &'a StepInfo<NestedSpec>,
    pub(super) parent_key_and_child_index: Option<(StepKey, usize)>,
    pub(super) total_steps: usize,
}

/// Per-step stateful data tracked by the line displayer.
#[derive(Debug)]
struct StepData {
    /// The last root event index that was displayed for this step.
    ///
    /// This is used to avoid displaying the same event twice.
    last_root_event_index: RootEventIndex,

    /// The last `leaf_step_elapsed` at which a progress event was displayed for
    /// this step.
    last_progress_event_at: Option<Duration>,
}

impl StepData {
    fn new(last_root_event_index: RootEventIndex) -> Self {
        Self { last_root_event_index, last_progress_event_at: None }
    }

    fn update_progress_event(&mut self, leaf_step_elapsed: Duration) {
        self.last_progress_event_at = Some(leaf_step_elapsed);
    }

    fn update_last_root_event_index(
        &mut self,
        root_event_index: RootEventIndex,
    ) {
        self.last_root_event_index = root_event_index;
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) enum NestLevel {
    /// Regular nest level.
    Regular(usize),

    /// These many nest levels, except also add an extra half indent.
    ExtraHalf(usize),
}

impl NestLevel {
    fn add_prefix(self, line: &mut String) {
        match self {
            NestLevel::Regular(0) => {}
            NestLevel::Regular(nest_level) => {
                line.push_str(&"....".repeat(nest_level));
                line.push_str(" ");
            }
            NestLevel::ExtraHalf(nest_level) => {
                line.push_str(&"....".repeat(nest_level));
                line.push_str(".. ");
            }
        }
    }
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
}
