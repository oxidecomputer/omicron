// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use owo_colors::Style;
use std::time::Duration;

use crate::{EventBuffer, ExecutionTerminalInfo, StepSpec};

use super::{LineDisplayFormatter, LineDisplayShared};

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
            shared: LineDisplayShared::new(),
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
        let mut lines = Vec::new();
        self.shared.format_event_buffer(
            &self.prefix,
            buffer,
            &self.formatter,
            &mut lines,
        );
        for line in lines {
            writeln!(self.writer, "{line}")?;
        }

        Ok(())
    }

    /// Writes terminal information to the writer.
    pub fn write_terminal_info(
        &mut self,
        info: &ExecutionTerminalInfo,
    ) -> std::io::Result<()> {
        let line = self.shared.format_terminal_info(
            &self.prefix,
            info,
            &self.formatter,
        );
        writeln!(self.writer, "{line}")
    }

    /// Writes a generic line to the writer, with prefix attached if provided.
    pub fn write_generic(&mut self, message: &str) -> std::io::Result<()> {
        let line =
            self.shared.format_generic(&self.prefix, message, &self.formatter);
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
        ret.progress_message_style = Style::new().green();
        ret.warning_style = Style::new().bold().yellow();
        ret.warning_message_style = Style::new().yellow();
        ret.error_style = Style::new().bold().red();
        ret.skipped_style = Style::new().bold().yellow();
        ret.retry_style = Style::new().bold().yellow();

        ret
    }
}
