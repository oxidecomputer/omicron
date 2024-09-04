// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use chrono::{DateTime, Utc};
use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use owo_colors::Style;
use std::time::Duration;

use crate::{EventBuffer, ExecutionTerminalInfo, StepSpec};

use super::{
    line_display_shared::LineDisplayOutput, LineDisplayFormatter,
    LineDisplayShared,
};

/// A line-oriented display.
///
/// This display produces output to the provided writer.
#[derive_where(Debug)]
pub struct LineDisplay<W> {
    writer: DebugIgnore<W>,
    shared: LineDisplayShared,
    formatter: LineDisplayFormatter,
    prefix: String,
}

impl<W: std::io::Write> LineDisplay<W> {
    /// Creates a new LineDisplay.
    pub fn new(writer: W) -> Self {
        Self {
            writer: DebugIgnore(writer),
            shared: LineDisplayShared::default(),
            formatter: LineDisplayFormatter::new(),
            prefix: String::new(),
        }
    }

    /// Sets the prefix for all future lines.
    #[inline]
    pub fn set_prefix(&mut self, prefix: impl Into<String>) {
        self.prefix = prefix.into();
    }

    /// Sets the styles for all future lines.
    #[inline]
    pub fn set_styles(&mut self, styles: LineDisplayStyles) {
        self.formatter.set_styles(styles);
    }

    /// Sets the start time for all future lines.
    ///
    /// If the start time is set, then the progress display will be relative to
    /// that time. Otherwise, only the offset from the start of the job will be
    /// displayed.
    #[inline]
    pub fn set_start_time(&mut self, start_time: DateTime<Utc>) {
        self.shared.set_start_time(start_time);
    }

    /// Sets the amount of time before the next progress event is shown.
    #[inline]
    pub fn set_progress_interval(&mut self, interval: Duration) {
        self.formatter.set_progress_interval(interval);
    }

    /// Writes an event buffer to the writer, incrementally.
    ///
    /// This is a stateful method that will only display events that have not
    /// been displayed before.
    pub fn write_event_buffer<S: StepSpec>(
        &mut self,
        buffer: &EventBuffer<S>,
    ) -> std::io::Result<()> {
        let mut out = LineDisplayOutput::new();
        self.shared
            .with_context(&self.prefix, &self.formatter)
            .format_event_buffer(buffer, &mut out);
        for line in out.iter() {
            writeln!(self.writer, "{line}")?;
        }

        Ok(())
    }

    /// Writes terminal information to the writer.
    pub fn write_terminal_info(
        &mut self,
        info: &ExecutionTerminalInfo,
    ) -> std::io::Result<()> {
        let line = self
            .shared
            .with_context(&self.prefix, &self.formatter)
            .format_terminal_info(info);
        writeln!(self.writer, "{line}")
    }

    /// Writes a generic line to the writer, with prefix attached if provided.
    pub fn write_generic(&mut self, message: &str) -> std::io::Result<()> {
        let line = self
            .shared
            .with_context(&self.prefix, &self.formatter)
            .format_generic(message);
        writeln!(self.writer, "{line}")
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
    pub progress_message_style: Style,
    pub warning_style: Style,
    pub warning_message_style: Style,
    pub error_style: Style,
    pub error_message_style: Style,
    pub skipped_style: Style,
    pub retry_style: Style,
}

impl LineDisplayStyles {
    /// Returns a default set of colorized styles with ANSI colors.
    pub fn colorized() -> Self {
        let mut ret = Self::default();
        ret.prefix_style = Style::new().bold();
        ret.meta_style = Style::new().bold();
        ret.step_name_style = Style::new().cyan();
        ret.progress_style = Style::new().bold().green();
        ret.progress_message_style = Style::new().green();
        ret.warning_style = Style::new().bold().yellow();
        ret.warning_message_style = Style::new().yellow();
        ret.error_style = Style::new().bold().red();
        ret.error_message_style = Style::new().red();
        ret.skipped_style = Style::new().bold().yellow();
        ret.retry_style = Style::new().bold().yellow();

        ret
    }
}
