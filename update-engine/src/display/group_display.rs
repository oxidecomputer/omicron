// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{borrow::Borrow, collections::BTreeMap, fmt, time::Duration};

use owo_colors::OwoColorize;
use swrite::{swrite, SWrite};
use unicode_width::UnicodeWidthStr;

use crate::{
    errors::UnknownReportKey, events::EventReport, EventBuffer,
    ExecutionTerminalInfo, StepSpec, TerminalKind,
};

use super::{
    line_display_shared::LineDisplayFormatter, LineDisplayShared,
    LineDisplayStyles, HEADER_WIDTH,
};

/// A displayer that simultaneously manages and shows line-based output for
/// several event buffers.
///
/// `K` is the key type for each element in the group. Its [`fmt::Display`] impl
/// is called to obtain the prefix, and `Eq + Ord` is used for keys.
#[derive(Debug)]
pub struct GroupDisplay<K, W, S: StepSpec> {
    // We don't need to add any buffering here because we already write data to
    // the writer in a line-buffered fashion (see Self::write_events).
    writer: W,
    max_width: usize,
    single_states: BTreeMap<K, SingleState<S>>,
    formatter: LineDisplayFormatter,
    stats: GroupDisplayStats,
}

impl<K: Eq + Ord, W: std::io::Write, S: StepSpec> GroupDisplay<K, W, S> {
    /// Creates a new `GroupDisplay` with the provided report keys and
    /// prefixes.
    ///
    /// The function passed in is expected to create a writer.
    pub fn new<Str>(
        keys_and_prefixes: impl IntoIterator<Item = (K, Str)>,
        writer: W,
    ) -> Self
    where
        Str: Into<String>,
    {
        // Right-align prefixes to their maximum width -- this helps keep the
        // output organized.
        let mut max_width = 0;
        let keys_and_prefixes: Vec<_> = keys_and_prefixes
            .into_iter()
            .map(|(k, prefix)| {
                let prefix = prefix.into();
                max_width =
                    max_width.max(UnicodeWidthStr::width(prefix.as_str()));
                (k, prefix)
            })
            .collect();
        let single_states: BTreeMap<_, _> = keys_and_prefixes
            .into_iter()
            .map(|(k, prefix)| (k, SingleState::new(prefix, max_width)))
            .collect();

        let not_started = single_states.len();
        Self {
            writer,
            max_width,
            single_states,
            formatter: LineDisplayFormatter::new(),
            stats: GroupDisplayStats::new(not_started),
        }
    }

    /// Creates a new `GroupDisplay` with the provided report keys, using the
    /// `Display` impl to obtain the respective prefixes.
    pub fn new_with_display(
        keys: impl IntoIterator<Item = K>,
        writer: W,
    ) -> Self
    where
        K: fmt::Display,
    {
        Self::new(
            keys.into_iter().map(|k| {
                let prefix = k.to_string();
                (k, prefix)
            }),
            writer,
        )
    }

    /// Sets the styles for all future lines.
    #[inline]
    pub fn set_styles(&mut self, styles: LineDisplayStyles) {
        self.formatter.set_styles(styles);
    }

    /// Sets the amount of time before new progress events are shown.
    #[inline]
    pub fn set_progress_interval(&mut self, interval: Duration) {
        self.formatter.set_progress_interval(interval);
    }

    /// Returns true if this `GroupDisplay` is producing reports corresponding
    /// to the given key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.single_states.contains_key(key)
    }

    /// Adds an event report to the display, keyed by the index, and updates
    /// internal state.
    ///
    /// Returns `Ok(())` if the report was accepted because the key was
    /// known to this `GroupDisplay`, and an error if it was not.
    pub fn add_event_report<Q>(
        &mut self,
        key: &Q,
        event_report: EventReport<S>,
    ) -> Result<(), UnknownReportKey>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        if let Some(state) = self.single_states.get_mut(key) {
            let result = state.add_event_report(event_report);
            self.stats.apply_result(result);
            Ok(())
        } else {
            Err(UnknownReportKey {})
        }
    }

    /// Writes a "Status" or "Summary" line to the writer with statistics.
    pub fn write_stats(&mut self, header: &str) -> std::io::Result<()> {
        // Add a prefix which is equal to the maximum width of the prefixes.
        // [prefix 00:00:00] takes up self.max_width + 9 characters inside the
        // brackets.
        let total_width = self.max_width + 9;
        let mut line = format!("[{:total_width$}] ", "");
        self.stats.format_line(&mut line, header, &self.formatter);
        writeln!(self.writer, "{line}")
    }

    /// Writes all pending events to the writer.
    pub fn write_events(&mut self) -> std::io::Result<()> {
        let mut lines = Vec::new();
        for state in self.single_states.values_mut() {
            state.format_events(&self.formatter, &mut lines);
        }
        for line in lines {
            writeln!(self.writer, "{line}")?;
        }
        Ok(())
    }

    /// Returns the current statistics for this `GroupDisplay`.
    pub fn stats(&self) -> &GroupDisplayStats {
        &self.stats
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GroupDisplayStats {
    /// The total number of reports.
    pub total: usize,

    /// The number of reports that have not yet started.
    pub not_started: usize,

    /// The number of reports that are currently running.
    pub running: usize,

    /// The number of reports that indicate successful completion.
    pub completed: usize,

    /// The number of reports that indicate failure.
    pub failed: usize,

    /// The number of reports that indicate being aborted.
    pub aborted: usize,

    /// The number of reports where we didn't receive a final state and it got
    /// overwritten by another report.
    ///
    /// Overwritten reports are considered failures since we don't know what
    /// happened.
    pub overwritten: usize,
}

impl GroupDisplayStats {
    fn new(total: usize) -> Self {
        Self {
            total,
            not_started: total,
            completed: 0,
            failed: 0,
            aborted: 0,
            overwritten: 0,
            running: 0,
        }
    }

    /// Returns the number of terminal reports.
    pub fn terminal_count(&self) -> usize {
        self.completed + self.failed + self.aborted + self.overwritten
    }

    /// Returns true if all reports have reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        self.not_started == 0 && self.running == 0
    }

    /// Returns true if there are any failures.
    pub fn has_failures(&self) -> bool {
        self.failed > 0 || self.aborted > 0 || self.overwritten > 0
    }

    fn apply_result(&mut self, result: AddEventReportResult) {
        // Process result.after first to avoid integer underflow.
        match result.after {
            SingleStateTag::NotStarted => self.not_started += 1,
            SingleStateTag::Running => self.running += 1,
            SingleStateTag::Terminal(TerminalKind::Completed) => {
                self.completed += 1
            }
            SingleStateTag::Terminal(TerminalKind::Failed) => self.failed += 1,
            SingleStateTag::Terminal(TerminalKind::Aborted) => {
                self.aborted += 1
            }
            SingleStateTag::Overwritten => self.overwritten += 1,
        }

        match result.before {
            SingleStateTag::NotStarted => self.not_started -= 1,
            SingleStateTag::Running => self.running -= 1,
            SingleStateTag::Terminal(TerminalKind::Completed) => {
                self.completed -= 1
            }
            SingleStateTag::Terminal(TerminalKind::Failed) => self.failed -= 1,
            SingleStateTag::Terminal(TerminalKind::Aborted) => {
                self.aborted -= 1
            }
            SingleStateTag::Overwritten => self.overwritten -= 1,
        }
    }

    fn format_line(
        &self,
        line: &mut String,
        header: &str,
        formatter: &LineDisplayFormatter,
    ) {
        let header_style = if self.has_failures() {
            formatter.styles().error_style
        } else {
            formatter.styles().progress_style
        };

        swrite!(line, "{:>HEADER_WIDTH$} ", header.style(header_style));
        let terminal_count = self.terminal_count();
        swrite!(
            line,
            "{terminal_count}/{}: {} running, {} {}",
            self.total,
            self.running.style(formatter.styles().meta_style),
            self.completed.style(formatter.styles().meta_style),
            "completed".style(formatter.styles().progress_style),
        );
        if self.failed > 0 {
            swrite!(
                line,
                ", {} {}",
                self.failed.style(formatter.styles().meta_style),
                "failed".style(formatter.styles().error_style),
            );
        }
        if self.aborted > 0 {
            swrite!(
                line,
                ", {} {}",
                self.aborted.style(formatter.styles().meta_style),
                "aborted".style(formatter.styles().error_style),
            );
        }
        if self.overwritten > 0 {
            swrite!(
                line,
                ", {} {}",
                self.overwritten.style(formatter.styles().meta_style),
                "overwritten".style(formatter.styles().error_style),
            );
        }
    }
}

#[derive(Debug)]
struct SingleState<S: StepSpec> {
    shared: LineDisplayShared,
    kind: SingleStateKind<S>,
    prefix: String,
}

impl<S: StepSpec> SingleState<S> {
    fn new(prefix: String, max_width: usize) -> Self {
        // Right-align the prefix to the maximum width.
        let prefix = format!("{:>max_width$}", prefix);
        Self {
            shared: LineDisplayShared::new(),
            kind: SingleStateKind::NotStarted { displayed: false },
            prefix,
        }
    }

    /// Adds an event report and updates the internal state.
    fn add_event_report(
        &mut self,
        event_report: EventReport<S>,
    ) -> AddEventReportResult {
        let before = match &self.kind {
            SingleStateKind::NotStarted { .. } => {
                self.kind = SingleStateKind::Running {
                    event_buffer: EventBuffer::new(8),
                };
                SingleStateTag::NotStarted
            }
            SingleStateKind::Running { .. } => SingleStateTag::Running,

            SingleStateKind::Terminal { info, .. } => {
                // Once we've reached a terminal state, we don't record any more
                // events.
                return AddEventReportResult::unchanged(
                    SingleStateTag::Terminal(info.kind),
                );
            }
            SingleStateKind::Overwritten { .. } => {
                // This update has already completed -- assume that the event
                // buffer is for a new update, which we don't show.
                return AddEventReportResult::unchanged(
                    SingleStateTag::Overwritten,
                );
            }
        };

        let SingleStateKind::Running { event_buffer } = &mut self.kind else {
            unreachable!("other branches were handled above");
        };

        if let Some(root_execution_id) = event_buffer.root_execution_id() {
            if event_report.root_execution_id != Some(root_execution_id) {
                // The report is for a different execution ID -- assume that
                // this event is completed and mark our current execution as
                // completed.
                self.kind = SingleStateKind::Overwritten { displayed: false };
                return AddEventReportResult {
                    before,
                    after: SingleStateTag::Overwritten,
                };
            }
        }

        event_buffer.add_event_report(event_report);
        let after = if let Some(info) = event_buffer.root_terminal_info() {
            // Grab the event buffer to store it in the terminal state.
            let event_buffer =
                std::mem::replace(event_buffer, EventBuffer::new(0));
            let terminal_kind = info.kind;
            self.kind = SingleStateKind::Terminal {
                info,
                pending_event_buffer: Some(event_buffer),
            };
            SingleStateTag::Terminal(terminal_kind)
        } else {
            SingleStateTag::Running
        };

        AddEventReportResult { before, after }
    }

    pub(super) fn format_events(
        &mut self,
        formatter: &LineDisplayFormatter,
        out: &mut Vec<String>,
    ) {
        match &mut self.kind {
            SingleStateKind::NotStarted { displayed } => {
                if !*displayed {
                    let line = self.shared.format_generic(
                        &self.prefix,
                        "Update not started, waiting...",
                        formatter,
                    );
                    out.push(line);
                    *displayed = true;
                }
            }
            SingleStateKind::Running { event_buffer } => {
                self.shared.format_event_buffer(
                    &self.prefix,
                    event_buffer,
                    formatter,
                    out,
                );
            }
            SingleStateKind::Terminal { info, pending_event_buffer } => {
                // Are any remaining events left? This also sets pending_event_buffer
                // to None after displaying remaining events.
                if let Some(event_buffer) = pending_event_buffer.take() {
                    self.shared.format_event_buffer(
                        &self.prefix,
                        &event_buffer,
                        formatter,
                        out,
                    );
                    // Also show a line to wrap up the terminal status.
                    let line = self.shared.format_terminal_info(
                        &self.prefix,
                        info,
                        formatter,
                    );
                    out.push(line);
                }

                // Nothing to do, the terminal status was already printed above.
            }
            SingleStateKind::Overwritten { displayed } => {
                if !*displayed {
                    let line = self.shared.format_generic(
                        &self.prefix,
                        "Update overwritten (a different update was started)\
                                assuming failure",
                        formatter,
                    );
                    out.push(line);
                    *displayed = true;
                }
            }
        }
    }
}

#[derive(Debug)]
enum SingleStateKind<S: StepSpec> {
    NotStarted {
        displayed: bool,
    },
    Running {
        event_buffer: EventBuffer<S>,
    },
    Terminal {
        info: ExecutionTerminalInfo,
        // The event buffer is kept around so that we can display any remaining
        // lines.
        pending_event_buffer: Option<EventBuffer<S>>,
    },
    Overwritten {
        displayed: bool,
    },
}

struct AddEventReportResult {
    before: SingleStateTag,
    after: SingleStateTag,
}

impl AddEventReportResult {
    fn unchanged(tag: SingleStateTag) -> Self {
        Self { before: tag, after: tag }
    }
}

#[derive(Copy, Clone, Debug)]
enum SingleStateTag {
    NotStarted,
    Running,
    Terminal(TerminalKind),
    Overwritten,
}
