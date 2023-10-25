// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use owo_colors::{OwoColorize, Style};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{self, Write as _},
    time::Duration,
};
use swrite::{swrite, SWrite};

use crate::{
    events::{StepInfo, StepOutcome},
    AbortInfo, AbortReason, CompletionInfo, EventBuffer, EventBufferStepData,
    ExecutionTerminalStatus, FailureInfo, FailureReason, NestedSpec,
    RootEventIndex, StepKey, StepSpec, StepStatus,
};

/// A line-oriented display.
///
/// This display produces output to the provided writer.
#[derive_where(Debug)]
pub struct LineDisplay<W> {
    writer: DebugIgnore<W>,
    step_data: HashMap<StepKey, LineDisplayStepData>,
    // Inner state that's immutable during `display_event_buffer` calls.
    inner: LineDisplayInner,
}

impl<W: std::io::Write> LineDisplay<W> {
    /// Creates a new LineDisplay.
    pub fn new(writer: W) -> Self {
        Self {
            writer: DebugIgnore(writer),
            step_data: HashMap::new(),
            inner: LineDisplayInner {
                prefix: String::new(),
                styles: Default::default(),
                progress_interval: Duration::from_secs(1),
            },
        }

        // TODO: separate out set_prefix/set_styles/set_progress_interval into
        // its own struct, figure out how to harmonize this with
        // GroupLineDisplay.
    }

    /// Sets the prefix for all future lines.
    pub fn set_prefix(&mut self, prefix: impl Into<String>) {
        self.inner.prefix = prefix.into();
    }

    /// Sets the styles for all future lines.
    pub fn set_styles(&mut self, styles: LineDisplayStyles) {
        self.inner.styles = styles;
    }

    /// Sets the amount of time before the next progress event is shown.
    pub fn set_progress_interval(&mut self, interval: Duration) {
        self.inner.progress_interval = interval;
    }

    /// Writes an event buffer to the display.
    ///
    /// This is a stateful method that will only display events that have not
    /// been displayed before.
    pub fn display_event_buffer<S: StepSpec>(
        &mut self,
        buffer: &EventBuffer<S>,
    ) -> std::io::Result<()> {
        let steps = buffer.steps();
        for (step_key, data) in steps.as_slice() {
            self.display_step(buffer, *step_key, data)?;
        }

        Ok(())
    }

    /// Writes a terminal status to the display.
    pub fn print_terminal_status(
        &mut self,
        status: &ExecutionTerminalStatus,
        total_elapsed: Option<Duration>,
    ) -> std::io::Result<()> {
        let mut line = self.inner.start_line(total_elapsed);
        match status {
            ExecutionTerminalStatus::Completed { .. } => {
                swrite!(
                    line,
                    "Execution {}",
                    "completed".style(self.inner.styles.progress_style),
                );
            }
            ExecutionTerminalStatus::Failed { .. } => {
                swrite!(
                    line,
                    "Execution {}",
                    "failed".style(self.inner.styles.error_style),
                );
            }
            ExecutionTerminalStatus::Aborted { .. } => {
                swrite!(
                    line,
                    "Execution {}",
                    "aborted".style(self.inner.styles.warning_style),
                );
            }
        }
        writeln!(self.writer, "{line}")?;

        Ok(())
    }

    /// Writes a line to the writer, with prefix attached if provided.
    pub fn println(&mut self, line: impl Into<String>) -> std::io::Result<()> {
        let mut out = self.inner.start_println();
        out.push_str(&line.into());
        writeln!(self.writer, "{}", out)
    }

    /// Writes step data to the display.
    fn display_step<S: StepSpec>(
        &mut self,
        buffer: &EventBuffer<S>,
        step_key: StepKey,
        data: &EventBufferStepData<S>,
    ) -> std::io::Result<()> {
        let parent_key_and_child_index = data.parent_key_and_child_index();
        let nest_level = data.nest_level();
        let total_steps = data.total_steps();
        let step_info = data.step_info();
        let step_status = data.step_status();

        match step_status {
            StepStatus::NotStarted => {}
            StepStatus::Running { progress_event, .. } => {
                let Some(leaf_step_elapsed) =
                    progress_event.kind.leaf_step_elapsed()
                else {
                    // Can't show anything for unknown events.
                    return Ok(());
                };

                let ld = self.step_data.entry(step_key).or_insert_with(|| {
                    LineDisplayStepData::new(data.last_root_event_index())
                });

                let (is_first_event, should_display) =
                    match ld.last_progress_event_at {
                        Some(last_progress_event_at) => {
                            let should_display = leaf_step_elapsed
                                > last_progress_event_at
                                    + self.inner.progress_interval;
                            (false, should_display)
                        }
                        None => (true, true),
                    };

                if should_display {
                    let mut line = self.inner.start_line(
                        // Add extra half-indent for non-first progress events.
                        Some(progress_event.total_elapsed),
                    );
                    let nest_level = if is_first_event {
                        NestLevel::Regular(nest_level)
                    } else {
                        NestLevel::ExtraHalf(nest_level)
                    };

                    self.inner.add_step_info(
                        &mut line,
                        parent_key_and_child_index,
                        nest_level,
                        step_info,
                        total_steps,
                    );
                    match progress_event.kind.progress_counter() {
                        Some(counter) => {
                            let progress_str = match counter.total {
                                Some(total) => {
                                    format!("{}/{}", counter.current, total)
                                }
                                None => format!("{}", counter.current),
                            };
                            swrite!(
                                line,
                                "{}: {progress_str} {} after {:.2?}",
                                "Progress"
                                    .style(self.inner.styles.progress_style),
                                counter.units,
                                leaf_step_elapsed
                                    .style(self.inner.styles.meta_style),
                            );
                        }
                        None => {
                            swrite!(
                                line,
                                "{}",
                                "Running"
                                    .style(self.inner.styles.progress_style),
                            );

                            // If the leaf step elapsed is non-zero, show it.
                            if leaf_step_elapsed > Duration::ZERO {
                                swrite!(
                                    line,
                                    " after {:.2?}",
                                    leaf_step_elapsed
                                        .style(self.inner.styles.meta_style),
                                );
                            }
                        }
                    }

                    writeln!(self.writer, "{line}")?;

                    ld.update_progress_event(leaf_step_elapsed);
                }

                // TODO: show low-priority events (retries and resets).
            }
            StepStatus::Completed { info } => {
                if let Some(ld) = self.step_data.get(&step_key) {
                    if ld.last_root_event_index >= data.last_root_event_index()
                    {
                        // We've already displayed this event.
                        return Ok(());
                    }
                }

                match info {
                    Some(info) => {
                        let mut line = self
                            .inner
                            .start_line(Some(info.root_total_elapsed));
                        self.inner.add_step_info(
                            &mut line,
                            parent_key_and_child_index,
                            NestLevel::Regular(nest_level),
                            step_info,
                            total_steps,
                        );
                        self.inner.add_completion_info(&mut line, info);
                        writeln!(self.writer, "{line}")?;
                    }
                    None => {
                        // This means that we don't know what happened to the step
                        // but it did complete.
                        let mut line = self.inner.start_line(None);
                        self.inner.add_step_info(
                            &mut line,
                            parent_key_and_child_index,
                            NestLevel::Regular(nest_level),
                            step_info,
                            total_steps,
                        );
                        swrite!(
                            line,
                            "{} with {}",
                            "Completed".style(self.inner.styles.progress_style),
                            "unknown outcome"
                                .style(self.inner.styles.meta_style),
                        );
                        writeln!(self.writer, "{line}")?;
                    }
                }

                self.insert_or_update_index(
                    step_key,
                    data.last_root_event_index(),
                );
            }
            StepStatus::Failed { reason } => {
                if let Some(ld) = self.step_data.get(&step_key) {
                    if ld.last_root_event_index >= data.last_root_event_index()
                    {
                        // We've already displayed this event.
                        return Ok(());
                    }
                }

                match reason {
                    FailureReason::StepFailed(info) => {
                        let mut line = self
                            .inner
                            .start_line(Some(info.root_total_elapsed));
                        self.inner.add_step_info(
                            &mut line,
                            parent_key_and_child_index,
                            NestLevel::Regular(nest_level),
                            step_info,
                            total_steps,
                        );
                        self.inner.add_failure_info(&mut line, info);
                        writeln!(self.writer, "{line}")?;
                    }
                    FailureReason::ParentFailed { parent_step } => {
                        let parent_step_info = buffer
                            .get(&parent_step)
                            .expect("parent step must exist");
                        let mut line = self.inner.start_line(None);
                        self.inner.add_step_info(
                            &mut line,
                            parent_key_and_child_index,
                            NestLevel::Regular(nest_level),
                            step_info,
                            total_steps,
                        );
                        swrite!(
                            line,
                            "{} because parent step {} failed",
                            "Failed".style(self.inner.styles.error_style),
                            parent_step_info
                                .step_info()
                                .description
                                .style(self.inner.styles.step_name_style)
                        );
                        writeln!(self.writer, "{line}")?;
                    }
                }

                self.insert_or_update_index(
                    step_key,
                    data.last_root_event_index(),
                );
            }
            StepStatus::Aborted { reason, .. } => {
                if let Some(ld) = self.step_data.get(&step_key) {
                    if ld.last_root_event_index >= data.last_root_event_index()
                    {
                        // We've already displayed this event.
                        return Ok(());
                    }
                }

                match reason {
                    AbortReason::StepAborted(info) => {
                        let mut line = self
                            .inner
                            .start_line(Some(info.root_total_elapsed));
                        self.inner.add_step_info(
                            &mut line,
                            parent_key_and_child_index,
                            NestLevel::Regular(nest_level),
                            step_info,
                            total_steps,
                        );
                        self.inner.add_abort_info(&mut line, info);
                        writeln!(self.writer, "{line}")?;
                    }
                    AbortReason::ParentAborted { parent_step } => {
                        let parent_step_info = buffer
                            .get(&parent_step)
                            .expect("parent step must exist");
                        let mut line = self.inner.start_line(None);
                        self.inner.add_step_info(
                            &mut line,
                            parent_key_and_child_index,
                            NestLevel::Regular(nest_level),
                            step_info,
                            total_steps,
                        );
                        swrite!(
                            line,
                            "{} because parent step {} aborted",
                            "Aborted".style(self.inner.styles.error_style),
                            parent_step_info
                                .step_info()
                                .description
                                .style(self.inner.styles.step_name_style)
                        );
                        writeln!(self.writer, "{line}")?;
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

        Ok(())
    }

    fn insert_or_update_index(
        &mut self,
        step_key: StepKey,
        last_root_index: RootEventIndex,
    ) {
        match self.step_data.entry(step_key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().update_last_root_event_index(last_root_index);
            }
            Entry::Vacant(entry) => {
                entry.insert(LineDisplayStepData::new(last_root_index));
            }
        }
    }
}

#[derive(Debug)]
struct LineDisplayInner {
    prefix: String,
    styles: LineDisplayStyles,
    progress_interval: Duration,
}

impl LineDisplayInner {
    fn start_println(&self) -> String {
        if !self.prefix.is_empty() {
            format!("[{}] ", self.prefix.style(self.styles.prefix_style))
        } else {
            String::new()
        }
    }

    fn start_line(&self, total_elapsed: Option<Duration>) -> String {
        let mut line =
            format!("[{}", self.prefix.style(self.styles.prefix_style));

        if !self.prefix.is_empty() {
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
        mut line: &mut String,
        parent_key_and_child_index: Option<(StepKey, usize)>,
        nest_level: NestLevel,
        step_info: &StepInfo<NestedSpec>,
        total_steps: usize,
    ) {
        match nest_level {
            NestLevel::Regular(nest_level) => {
                line.push_str(&"    ".repeat(nest_level));
            }
            NestLevel::ExtraHalf(nest_level) => {
                line.push_str(&"    ".repeat(nest_level));
                line.push_str("  ");
            }
        }

        match parent_key_and_child_index {
            Some((parent_key, child_index)) => {
                // Print e.g. (6a .
                swrite!(
                    &mut line,
                    "({}{} ",
                    // Add 1 to the index to make it 1-based.
                    parent_key.index + 1,
                    AsLetters(child_index)
                );
            }
            None => {
                swrite!(&mut line, "(");
            }
        };

        // Print out "<step index>/<total steps>)". Leave space such that we
        // print out e.g. "1/8)" and " 3/14)".
        // Add 1 to the index to make it 1-based.
        let step_index = step_info.index + 1;
        let step_index_width = total_steps.to_string().len();
        swrite!(
            &mut line,
            "{:width$}/{:width$}) ",
            step_index,
            total_steps,
            width = step_index_width
        );

        swrite!(
            &mut line,
            "{}",
            step_info.description.style(self.styles.step_name_style)
        );
        line.push_str(": ");
    }

    fn add_completion_info(&self, line: &mut String, info: &CompletionInfo) {
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
            StepOutcome::Success { message, .. } => match message {
                Some(message) => {
                    swrite!(
                        line,
                        "{} {meta} with message: {}",
                        "Completed".style(self.styles.progress_style),
                        message
                    );
                }
                None => {
                    swrite!(
                        line,
                        "{} {meta}",
                        "Completed".style(self.styles.progress_style),
                    );
                }
            },
            StepOutcome::Warning { message, .. } => {
                swrite!(
                    line,
                    "{} {meta} {}: {}",
                    "Completed".style(self.styles.warning_style),
                    "with warning".style(self.styles.warning_style),
                    message,
                );
            }
            StepOutcome::Skipped { message, .. } => {
                swrite!(
                    line,
                    "{}: {}",
                    "Skipped".style(self.styles.skipped_style),
                    message,
                );
            }
        };
    }

    fn add_failure_info(&self, line: &mut String, info: &FailureInfo) {
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
            "{} {meta}: {}",
            "Failed".style(self.styles.error_style),
            info.message,
        );
        if !info.causes.is_empty() {
            swrite!(line, "\n{}", "  Caused by:".style(self.styles.meta_style));
            for cause in &info.causes {
                swrite!(line, "\n  - {}", cause);
            }
        }

        // The last newline is added by the caller.
    }

    fn add_abort_info(&self, line: &mut String, info: &AbortInfo) {
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

        swrite!(
            line,
            "{} {meta} with message \"{}\"",
            "Aborted".style(self.styles.warning_style),
            info.message,
        );
    }
}

#[derive(Copy, Clone, Debug)]
enum NestLevel {
    /// Regular nest level.
    Regular(usize),

    /// These many nest levels, except also add an extra half indent.
    ExtraHalf(usize),
}

/// Per-step stateful data tracked by the line displayer.
#[derive(Debug)]
struct LineDisplayStepData {
    /// The last root event index that was displayed for this step.
    ///
    /// This is used to avoid displaying the same event twice.
    last_root_event_index: RootEventIndex,

    /// The last `total_elapsed` at which a progress event was displayed for
    /// this step.
    last_progress_event_at: Option<Duration>,
}

impl LineDisplayStepData {
    fn new(last_root_event_index: RootEventIndex) -> Self {
        Self { last_root_event_index, last_progress_event_at: None }
    }

    fn update_progress_event(&mut self, event: Duration) {
        self.last_progress_event_at = Some(event);
    }

    fn update_last_root_event_index(&mut self, event: RootEventIndex) {
        self.last_root_event_index = event;
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

/// Styles for [`LineDisplay`].
///
/// By default this isn't colorized, but it can be if so chosen.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct LineDisplayStyles {
    pub prefix_style: Style,
    pub meta_style: Style,
    pub step_name_style: Style,
    pub progress_style: Style,
    pub warning_style: Style,
    pub error_style: Style,
    pub skipped_style: Style,
    pub retry_style: Style,
}

impl LineDisplayStyles {
    /// Returns a default set of colorized styles with ANSI colors.
    pub fn colorized() -> Self {
        let mut ret = Self::default();
        ret.prefix_style = Style::new().bold();
        ret.meta_style = Style::new().bold();
        ret.step_name_style = Style::new();
        ret.progress_style = Style::new().bold().green();
        ret.warning_style = Style::new().bold().yellow();
        ret.error_style = Style::new().bold().red();
        ret.skipped_style = Style::new().bold().yellow();
        ret.retry_style = Style::new().bold().yellow();

        ret
    }
}
