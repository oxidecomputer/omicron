// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use debug_ignore::DebugIgnore;
use owo_colors::{OwoColorize, Style};
use std::{
    collections::HashMap,
    fmt::{self, Write as _},
};

use crate::{
    events::{StepInfo, StepOutcome},
    AbortInfo, AbortReason, CompletionInfo, EventBuffer, EventBufferStepData,
    FailureInfo, FailureReason, NestedSpec, StepKey, StepSpec, StepStatus,
};

/// A line-oriented display.
///
/// This display produces output to the provided writer.
#[derive(Debug)]
pub struct LineDisplay<W> {
    writer: DebugIgnore<W>,
    prefix: String,
    styles: LineDisplayStyles,
    // A map from the step key to the index of the last event we reported for
    // that step. This is used to determine which events are new and should be
    // displayed.
    last_event_index: HashMap<StepKey, usize>,
}

impl<W: std::io::Write> LineDisplay<W> {
    /// Creates a new LineDisplay.
    pub fn new(writer: W) -> Self {
        Self {
            writer: DebugIgnore(writer),
            prefix: String::new(),
            styles: Default::default(),
            last_event_index: HashMap::new(),
        }
    }

    /// Sets the prefix for all future lines.
    pub fn set_prefix(&mut self, prefix: impl Into<String>) {
        self.prefix = prefix.into();
    }

    /// Sets the styles for all future lines.
    pub fn set_styles(&mut self, styles: LineDisplayStyles) {
        self.styles = styles;
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
            if let Some(event_index) = self.last_event_index.get(step_key) {
                if *event_index >= data.last_root_event_index() {
                    // We've already displayed these events.
                    continue;
                }
            }
            // There are new events to display.
            self.display_step_event(buffer, data)?;
            self.last_event_index
                .insert(*step_key, data.last_root_event_index());
        }

        Ok(())
    }

    /// Writes step data to the display.
    fn display_step_event<S: StepSpec>(
        &mut self,
        buffer: &EventBuffer<S>,
        data: &EventBufferStepData<S>,
    ) -> std::io::Result<()> {
        let nest_level = data.nest_level();
        let total_steps = data.total_steps();
        let step_info = data.step_info();
        let step_status = data.step_status();

        match step_status {
            StepStatus::NotStarted => {}
            StepStatus::Running { low_priority, progress_event } => {
                // TODO: report low-priority and progress events here.
            }
            StepStatus::Completed { info } => match info {
                Some(info) => {
                    let mut line =
                        self.start_line(nest_level, step_info, total_steps);
                    self.add_completion_info(&mut line, info)
                        .expect("String::write_fmt is infallible");
                    writeln!(self.writer, "{line}")?;
                }
                None => {
                    // This means that we don't know what happened to the step
                    // but it did complete.
                    let mut line =
                        self.start_line(nest_level, step_info, total_steps);
                    write!(
                        line,
                        "{} with {}",
                        "Completed".style(self.styles.progress_style),
                        "unknown outcome".style(self.styles.meta_style),
                    )
                    .expect("String::write_fmt is infallible");
                    writeln!(self.writer, "{line}")?;
                }
            },
            StepStatus::Failed { reason } => match reason {
                FailureReason::StepFailed(info) => {
                    let mut line =
                        self.start_line(nest_level, step_info, total_steps);
                    self.add_failure_info(&mut line, info)
                        .expect("String::write_fmt is infallible");
                    writeln!(self.writer, "{line}")?;
                }
                FailureReason::ParentFailed { parent_step } => {
                    let parent_step_info = buffer
                        .get(&parent_step)
                        .expect("parent step must exist");
                    let mut line =
                        self.start_line(nest_level, step_info, total_steps);
                    write!(
                        line,
                        "{} because parent step {} failed",
                        "Failed".style(self.styles.error_style),
                        parent_step_info
                            .step_info()
                            .description
                            .style(self.styles.step_name_style)
                    )
                    .expect("String::write_fmt is infallible");
                    writeln!(self.writer, "{line}")?;
                }
            },
            StepStatus::Aborted { reason, .. } => match reason {
                AbortReason::StepAborted(info) => {
                    let mut line =
                        self.start_line(nest_level, step_info, total_steps);
                    self.add_abort_info(&mut line, info)
                        .expect("String::write_fmt is infallible");
                    writeln!(self.writer, "{line}")?;
                }
                AbortReason::ParentAborted { parent_step } => {
                    let parent_step_info = buffer
                        .get(&parent_step)
                        .expect("parent step must exist");
                    let mut line =
                        self.start_line(nest_level, step_info, total_steps);
                    write!(
                        line,
                        "{} because parent step {} aborted",
                        "Aborted".style(self.styles.error_style),
                        parent_step_info
                            .step_info()
                            .description
                            .style(self.styles.step_name_style)
                    )
                    .expect("String::write_fmt is infallible");
                    writeln!(self.writer, "{line}")?;
                }
            },
            StepStatus::WillNotBeRun { .. } => {
                // We don't print "will not be run". (TODO: maybe add an
                // extended mode which does do so?)
            }
        }

        Ok(())
    }

    fn start_line(
        &self,
        nest_level: usize,
        step_info: &StepInfo<NestedSpec>,
        total_steps: usize,
    ) -> String {
        let mut line =
            format!("{}", self.prefix.style(self.styles.prefix_style));

        if !self.prefix.is_empty() {
            line.push(' ');
        }
        line.push_str(&" ".repeat(nest_level * 2));

        // Print out "(<step index>/<total steps>)". Leave space such that we
        // print out e.g. "(1/8)" and "( 3/14)".
        let step_index = step_info.index + 1;
        let step_index_width = total_steps.to_string().len();
        write!(
            &mut line,
            "({:width$}/{:width$}) ",
            step_index,
            total_steps,
            width = step_index_width
        )
        .expect("String::write_fmt is infallible");

        write!(
            &mut line,
            "{}",
            step_info.description.style(self.styles.step_name_style)
        )
        .expect("String::write_fmt is infallible");
        line.push_str(": ");

        line
    }

    fn add_completion_info(
        &self,
        line: &mut String,
        info: &CompletionInfo,
    ) -> fmt::Result {
        let mut meta = format!(
            "after {:.2?}",
            info.step_elapsed.style(self.styles.meta_style)
        );
        if info.attempt > 1 {
            write!(
                meta,
                " (at attempt {})",
                info.attempt.style(self.styles.meta_style)
            )?;
        }

        match &info.outcome {
            StepOutcome::Success { message, .. } => match message {
                Some(message) => {
                    write!(
                        line,
                        "{} {meta} with message: {}",
                        "Completed".style(self.styles.progress_style),
                        message
                    )?;
                }
                None => {
                    write!(
                        line,
                        "{} {meta}",
                        "Completed".style(self.styles.progress_style),
                    )?;
                }
            },
            StepOutcome::Warning { message, .. } => {
                write!(
                    line,
                    "{} {meta} {}: {}",
                    "Completed".style(self.styles.warning_style),
                    "with warning".style(self.styles.warning_style),
                    message,
                )?;
            }
            StepOutcome::Skipped { message, .. } => {
                write!(
                    line,
                    "{}: {}",
                    "Skipped".style(self.styles.skipped_style),
                    message,
                )?;
            }
        };

        Ok(())
    }

    fn add_failure_info(
        &self,
        line: &mut String,
        info: &FailureInfo,
    ) -> fmt::Result {
        let mut meta = format!(
            "after {:.2?}",
            info.step_elapsed.style(self.styles.meta_style)
        );
        if info.total_attempts > 1 {
            write!(
                meta,
                " (after {} attempts)",
                info.total_attempts.style(self.styles.meta_style)
            )?;
        }

        write!(
            line,
            "{} {meta}: {}",
            "Failed".style(self.styles.error_style),
            info.message,
        )?;
        if !info.causes.is_empty() {
            write!(line, "\n{}", "  Caused by:".style(self.styles.meta_style))?;
            for cause in &info.causes {
                write!(line, "\n  - {}", cause)?;
            }
        }

        // The last newline is added by the caller.

        Ok(())
    }

    fn add_abort_info(
        &self,
        line: &mut String,
        info: &AbortInfo,
    ) -> fmt::Result {
        let mut meta = format!(
            "after {:.2?}",
            info.step_elapsed.style(self.styles.meta_style)
        );
        if info.attempt > 1 {
            write!(
                meta,
                " (at attempt {})",
                info.attempt.style(self.styles.meta_style)
            )?;
        }

        write!(
            line,
            "{} {meta} with message \"{}\"",
            "Aborted".style(self.styles.warning_style),
            info.message,
        )?;

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
        ret.step_name_style = Style::new().underline();
        ret.progress_style = Style::new().bold().green();
        ret.warning_style = Style::new().bold().yellow();
        ret.error_style = Style::new().bold().red();
        ret.skipped_style = Style::new().bold().yellow();
        ret.retry_style = Style::new().bold().yellow();

        ret
    }
}
