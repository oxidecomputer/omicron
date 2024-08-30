// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{borrow::Borrow, collections::BTreeMap, fmt, time::Duration};

use libsw::TokioSw;
use owo_colors::OwoColorize;
use swrite::{swrite, SWrite};
use unicode_width::UnicodeWidthStr;

use crate::{
    display::ProgressRatioDisplay, errors::UnknownReportKey,
    events::EventReport, EventBuffer, ExecutionStatus, ExecutionTerminalInfo,
    StepSpec, TerminalKind,
};

use super::{
    line_display_shared::{LineDisplayFormatter, LineDisplayOutput},
    LineDisplayShared, LineDisplayStyles, HEADER_WIDTH,
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
    log: slog::Logger,
    writer: W,
    max_width: usize,
    // This is set to the highest value of root_total_elapsed seen from any event reports.
    start_sw: TokioSw,
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
        log: &slog::Logger,
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
            log: log.new(slog::o!("component" => "GroupDisplay")),
            writer,
            max_width,
            // This creates the stopwatch in the stopped state with duration 0 -- i.e. a minimal
            // value that will be replaced as soon as an event comes in.
            start_sw: TokioSw::new(),
            single_states,
            formatter: LineDisplayFormatter::new(),
            stats: GroupDisplayStats::new(not_started),
        }
    }

    /// Creates a new `GroupDisplay` with the provided report keys, using the
    /// `Display` impl to obtain the respective prefixes.
    pub fn new_with_display(
        log: &slog::Logger,
        keys: impl IntoIterator<Item = K>,
        writer: W,
    ) -> Self
    where
        K: fmt::Display,
    {
        Self::new(
            log,
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
            // Set self.start_sw to the max of root_total_elapsed and the current value.
            if let Some(root_total_elapsed) = result.root_total_elapsed {
                if self.start_sw.elapsed() < root_total_elapsed {
                    self.start_sw =
                        TokioSw::with_elapsed_started(root_total_elapsed);
                }
            }

            self.stats.apply_result(result);

            if result.before != result.after {
                slog::debug!(
                    self.log,
                    "add_event_report caused state transition";
                    "prefix" => &state.prefix,
                    "before" => %result.before,
                    "after" => %result.after,
                    "current_stats" => ?self.stats,
                    "root_total_elapsed" => ?result.root_total_elapsed,
                );
            } else {
                slog::trace!(
                    self.log,
                    "add_event_report called, state did not change";
                    "prefix" => &state.prefix,
                    "state" => %result.before,
                    "current_stats" => ?self.stats,
                    "root_total_elapsed" => ?result.root_total_elapsed,
                );
            }

            Ok(())
        } else {
            Err(UnknownReportKey {})
        }
    }

    /// Writes a "Status" or "Summary" line to the writer with statistics.
    pub fn write_stats(&mut self, header: &str) -> std::io::Result<()> {
        // Add a blank prefix which is equal to the maximum width of known prefixes.
        let prefix = " ".repeat(self.max_width);
        let mut line =
            self.formatter.start_line(&prefix, Some(self.start_sw.elapsed()));
        self.stats.format_line(&mut line, header, &self.formatter);
        writeln!(self.writer, "{line}")
    }

    /// Writes all pending events to the writer.
    pub fn write_events(&mut self) -> std::io::Result<()> {
        let mut out = LineDisplayOutput::new();
        for state in self.single_states.values_mut() {
            state.format_events(&self.formatter, &mut out);
        }
        for line in out.iter() {
            writeln!(self.writer, "{line}")?;
        }
        Ok(())
    }

    /// Returns the current statistics for this `GroupDisplay`.
    pub fn stats(&self) -> &GroupDisplayStats {
        &self.stats
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
        if result.before == result.after {
            // Nothing to do.
            return;
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
        swrite!(
            line,
            "{}: {} running, {} {}",
            ProgressRatioDisplay::current_and_total(
                self.terminal_count(),
                self.total
            ),
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
            shared: LineDisplayShared::default(),
            kind: SingleStateKind::NotStarted { displayed: false },
            prefix,
        }
    }

    /// Adds an event report and updates the internal state.
    fn add_event_report(
        &mut self,
        event_report: EventReport<S>,
    ) -> AddEventReportResult {
        match &mut self.kind {
            SingleStateKind::NotStarted { .. } => {
                // We're starting a new update.
                let before = SingleStateTag::NotStarted;
                let mut event_buffer = EventBuffer::default();
                let (after, root_total_elapsed) =
                    match Self::apply_report(&mut event_buffer, event_report) {
                        ApplyReportResult::NotStarted => {
                            // This means that the event report was empty. Don't
                            // update `self.kind`.
                            (SingleStateTag::NotStarted, None)
                        }
                        ApplyReportResult::Running(root_total_elapsed) => {
                            self.kind =
                                SingleStateKind::Running { event_buffer };
                            (SingleStateTag::Running, Some(root_total_elapsed))
                        }
                        ApplyReportResult::Terminal(info) => {
                            let terminal_kind = info.kind;
                            let root_total_elapsed = info.root_total_elapsed;

                            self.kind = SingleStateKind::Terminal {
                                info,
                                pending_event_buffer: Some(event_buffer),
                            };
                            (
                                SingleStateTag::Terminal(terminal_kind),
                                root_total_elapsed,
                            )
                        }
                        ApplyReportResult::Overwritten => {
                            self.kind = SingleStateKind::Overwritten {
                                displayed: false,
                            };
                            (SingleStateTag::Overwritten, None)
                        }
                    };

                AddEventReportResult { before, after, root_total_elapsed }
            }
            SingleStateKind::Running { event_buffer } => {
                // We're in the middle of an update.
                let before = SingleStateTag::Running;
                let (after, root_total_elapsed) = match Self::apply_report(
                    event_buffer,
                    event_report,
                ) {
                    ApplyReportResult::NotStarted => {
                        // This is an illegal state transition: once a
                        // non-empty event report has been received, the
                        // event buffer never goes back to the NotStarted
                        // state.
                        unreachable!("illegal state transition from Running to NotStarted")
                    }
                    ApplyReportResult::Running(root_total_elapsed) => {
                        (SingleStateTag::Running, Some(root_total_elapsed))
                    }
                    ApplyReportResult::Terminal(info) => {
                        let terminal_kind = info.kind;
                        let root_total_elapsed = info.root_total_elapsed;

                        // Grab the event buffer so we can store it in the
                        // Terminal state below.
                        let event_buffer = std::mem::replace(
                            event_buffer,
                            EventBuffer::new(0),
                        );

                        self.kind = SingleStateKind::Terminal {
                            info,
                            pending_event_buffer: Some(event_buffer),
                        };
                        (
                            SingleStateTag::Terminal(terminal_kind),
                            root_total_elapsed,
                        )
                    }
                    ApplyReportResult::Overwritten => {
                        self.kind =
                            SingleStateKind::Overwritten { displayed: false };
                        (SingleStateTag::Overwritten, None)
                    }
                };
                AddEventReportResult { before, after, root_total_elapsed }
            }
            SingleStateKind::Terminal { info, .. } => {
                // Once we've reached a terminal state, we don't record any more
                // events.
                AddEventReportResult::unchanged(
                    SingleStateTag::Terminal(info.kind),
                    info.root_total_elapsed,
                )
            }
            SingleStateKind::Overwritten { .. } => {
                // This update has already completed -- assume that the event
                // buffer is for a new update, which we don't show.
                AddEventReportResult::unchanged(
                    SingleStateTag::Overwritten,
                    None,
                )
            }
        }
    }

    /// The internal logic used by [`Self::add_event_report`].
    fn apply_report(
        event_buffer: &mut EventBuffer<S>,
        event_report: EventReport<S>,
    ) -> ApplyReportResult {
        if let Some(root_execution_id) = event_buffer.root_execution_id() {
            if event_report.root_execution_id != Some(root_execution_id) {
                // The report is for a different execution ID -- assume that
                // this event is completed and mark our current execution as
                // completed.
                return ApplyReportResult::Overwritten;
            }
        }

        event_buffer.add_event_report(event_report);
        match event_buffer.root_execution_summary() {
            Some(summary) => match summary.execution_status {
                ExecutionStatus::NotStarted => ApplyReportResult::NotStarted,
                ExecutionStatus::Running { root_total_elapsed, .. } => {
                    ApplyReportResult::Running(root_total_elapsed)
                }
                ExecutionStatus::Terminal(info) => {
                    ApplyReportResult::Terminal(info)
                }
            },
            None => {
                // We don't have a summary yet.
                ApplyReportResult::NotStarted
            }
        }
    }

    pub(super) fn format_events(
        &mut self,
        formatter: &LineDisplayFormatter,
        out: &mut LineDisplayOutput,
    ) {
        let mut cx = self.shared.with_context(&self.prefix, formatter);
        match &mut self.kind {
            SingleStateKind::NotStarted { displayed } => {
                if !*displayed {
                    let line =
                        cx.format_generic("Update not started, waiting...");
                    out.add_line(line);
                    *displayed = true;
                }
            }
            SingleStateKind::Running { event_buffer } => {
                cx.format_event_buffer(event_buffer, out);
            }
            SingleStateKind::Terminal { info, pending_event_buffer } => {
                // Are any remaining events left? This also sets pending_event_buffer
                // to None after displaying remaining events.
                if let Some(event_buffer) = pending_event_buffer.take() {
                    cx.format_event_buffer(&event_buffer, out);
                    // Also show a line to wrap up the terminal status.
                    let line = cx.format_terminal_info(info);
                    out.add_line(line);
                }

                // Nothing to do, the terminal status was already printed above.
            }
            SingleStateKind::Overwritten { displayed } => {
                if !*displayed {
                    let line = cx.format_generic(
                        "Update overwritten (a different update was started): \
                         assuming failure",
                    );
                    out.add_line(line);
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AddEventReportResult {
    before: SingleStateTag,
    after: SingleStateTag,
    root_total_elapsed: Option<Duration>,
}

impl AddEventReportResult {
    fn unchanged(
        tag: SingleStateTag,
        root_total_elapsed: Option<Duration>,
    ) -> Self {
        Self { before: tag, after: tag, root_total_elapsed }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SingleStateTag {
    NotStarted,
    Running,
    Terminal(TerminalKind),
    Overwritten,
}

impl fmt::Display for SingleStateTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => write!(f, "not started"),
            Self::Running => write!(f, "running"),
            Self::Terminal(kind) => write!(f, "{kind}"),
            Self::Overwritten => write!(f, "overwritten"),
        }
    }
}

#[derive(Clone, Debug)]
enum ApplyReportResult {
    NotStarted,
    Running(Duration),
    Terminal(ExecutionTerminalInfo),
    Overwritten,
}

#[cfg(test)]
mod tests {
    use omicron_test_utils::dev::test_setup_log;

    use super::*;

    use crate::test_utils::{generate_test_events, GenerateTestEventsKind};

    #[tokio::test]
    async fn test_stats() {
        let logctx = test_setup_log("test_stats");
        // Generate three sets of events, one for each kind.
        let generated_completed = generate_test_events(
            &logctx.log,
            GenerateTestEventsKind::Completed,
        )
        .await;
        let generated_failed =
            generate_test_events(&logctx.log, GenerateTestEventsKind::Failed)
                .await;
        let generated_aborted =
            generate_test_events(&logctx.log, GenerateTestEventsKind::Aborted)
                .await;

        // Set up a `GroupDisplay` with three keys.
        let mut group_display = GroupDisplay::new_with_display(
            &logctx.log,
            vec![
                GroupDisplayKey::Completed,
                GroupDisplayKey::Failed,
                GroupDisplayKey::Aborted,
                GroupDisplayKey::Overwritten,
            ],
            std::io::stdout(),
        );

        let mut expected_stats = GroupDisplayStats {
            total: 4,
            not_started: 4,
            running: 0,
            completed: 0,
            failed: 0,
            aborted: 0,
            overwritten: 0,
        };
        assert_eq!(group_display.stats(), &expected_stats);
        assert!(!expected_stats.is_terminal());
        assert!(!expected_stats.has_failures());

        // Pass in an empty EventReport -- ensure that this doesn't move it to
        // a Running state.

        group_display
            .add_event_report(
                &GroupDisplayKey::Completed,
                EventReport::default(),
            )
            .unwrap();
        assert_eq!(group_display.stats(), &expected_stats);

        // Pass in events one by one -- ensure that we're always in the running
        // state until we've completed.
        {
            expected_stats.not_started -= 1;
            expected_stats.running += 1;

            let n = generated_completed.len();

            let mut buffer = EventBuffer::default();
            let mut last_seen = None;

            for (i, event) in
                generated_completed.clone().into_iter().enumerate()
            {
                buffer.add_event(event);
                let report = buffer.generate_report_since(&mut last_seen);
                group_display
                    .add_event_report(&GroupDisplayKey::Completed, report)
                    .unwrap();
                if i == n - 1 {
                    // The last event should have moved us to the completed
                    // state.
                    expected_stats.running -= 1;
                    expected_stats.completed += 1;
                } else {
                    // We should still be in the running state.
                }
                assert_eq!(group_display.stats(), &expected_stats);
                assert!(!expected_stats.is_terminal());
                assert!(!expected_stats.has_failures());
            }
        }

        // Pass in failed events, this time using buffer.generate_report()
        // rather than buffer.generate_report_since().
        {
            expected_stats.not_started -= 1;
            expected_stats.running += 1;

            let n = generated_failed.len();

            let mut buffer = EventBuffer::default();
            for (i, event) in generated_failed.clone().into_iter().enumerate() {
                buffer.add_event(event);
                let report = buffer.generate_report();
                group_display
                    .add_event_report(&GroupDisplayKey::Failed, report)
                    .unwrap();
                if i == n - 1 {
                    // The last event should have moved us to the failed state.
                    expected_stats.running -= 1;
                    expected_stats.failed += 1;
                    assert!(expected_stats.has_failures());
                } else {
                    // We should still be in the running state.
                    assert!(!expected_stats.has_failures());
                }
                assert_eq!(group_display.stats(), &expected_stats);
            }
        }

        // Pass in aborted events all at once.
        {
            expected_stats.not_started -= 1;
            expected_stats.running += 1;

            let mut buffer = EventBuffer::default();
            for event in generated_aborted {
                buffer.add_event(event);
            }
            let report = buffer.generate_report();
            group_display
                .add_event_report(&GroupDisplayKey::Aborted, report)
                .unwrap();
            // The aborted events should have moved us to the aborted state.
            expected_stats.running -= 1;
            expected_stats.aborted += 1;
            assert_eq!(group_display.stats(), &expected_stats);

            // Try passing in one of the events that, if we were running, would
            // cause us to move to an overwritten state. Ensure that that does
            // not happen (i.e. expected_stats stays the same)
            let mut buffer = EventBuffer::default();
            buffer.add_event(generated_failed.first().unwrap().clone());
            let report = buffer.generate_report();
            group_display
                .add_event_report(&GroupDisplayKey::Aborted, report)
                .unwrap();
            assert_eq!(group_display.stats(), &expected_stats);
        }

        // For the overwritten state, pass in half of the completed events, and
        // then pass in all of the failed events.

        {
            expected_stats.not_started -= 1;
            expected_stats.running += 1;

            let mut buffer = EventBuffer::default();
            let n = generated_completed.len() / 2;
            for event in generated_completed.into_iter().take(n) {
                buffer.add_event(event);
            }
            let report = buffer.generate_report();
            group_display
                .add_event_report(&GroupDisplayKey::Overwritten, report)
                .unwrap();
            assert_eq!(group_display.stats(), &expected_stats);

            // Now pass in a single failed event, which has a different
            // execution ID.
            let mut buffer = EventBuffer::default();
            buffer.add_event(generated_failed.first().unwrap().clone());
            let report = buffer.generate_report();
            group_display
                .add_event_report(&GroupDisplayKey::Overwritten, report)
                .unwrap();
            // The overwritten event should have moved us to the overwritten
            // state.
            expected_stats.running -= 1;
            expected_stats.overwritten += 1;
        }

        assert!(expected_stats.has_failures());
        assert!(expected_stats.is_terminal());

        logctx.cleanup_successful();
    }

    #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
    enum GroupDisplayKey {
        Completed,
        Failed,
        Aborted,
        Overwritten,
    }

    impl fmt::Display for GroupDisplayKey {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Completed => write!(f, "completed"),
                Self::Failed => write!(f, "failed"),
                Self::Aborted => write!(f, "aborted"),
                Self::Overwritten => write!(f, "overwritten"),
            }
        }
    }
}
