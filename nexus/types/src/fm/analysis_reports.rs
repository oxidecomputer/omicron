// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Human-readable reports summarizing what occurred during fault management
//! analysis.

use super::case;
use super::display;
use super::ereport::EreportId;
use iddqd::IdOrdMap;
use omicron_uuid_kinds::{
    AlertUuid, CaseUuid, CollectionUuid, PhysicalDiskUuid, SitrepUuid,
    SupportBundleUuid,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnalysisReport {
    pub sitrep_id: SitrepUuid,
    pub comment: String,
    pub cases: IdOrdMap<CaseReport>,
    #[serde(default)]
    pub log: DebugLog,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaseReport {
    pub id: CaseUuid,
    #[serde(flatten)]
    pub metadata: case::Metadata,
    pub log: DebugLog,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DebugLog(Vec<LogEntry>);

/// An entry in an analysis report's debug log.
///
/// This type is somewhat intentionally very "stringly-typed": through the use
/// of `#[serde(skip_serializing_if = "Option::is_none")]`, `#[serde(default)]`,
/// and `#[serde(flatten)]`, we are essentially saying that this consists of a
/// string field named `event`, and any number of other string fields, which are
/// just thrown in a bag of key-value pairs. If one is named 'comment', we
/// handle it separately."
///
/// If we add additional fields that are handled specially, they will still go
/// in the bag of `kvs` if they are parsed by an OMDB version that doesn't
/// understand them. Conversely, any new special-cased fields should be
/// `Option`al, so that OMDB can still display event log entries emitted by
/// older versions of Nexus.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    level: LogLevel,
    #[serde(flatten)]
    kvs: BTreeMap<String, serde_json::Value>,
}

impl slog::KV for LogEntry {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> Result<(), slog::Error> {
        for (k, v) in &self.kvs {
            let key = slog::Key::from(k.clone());
            match v {
                serde_json::Value::String(s) => {
                    serializer.emit_str(key, s)?;
                }
                serde_json::Value::Null => serializer.emit_none(key)?,
                serde_json::Value::Bool(b) => {
                    serializer.emit_bool(key, *b)?;
                }
                serde_json::Value::Number(n) => {
                    if let Some(n) = n.as_i64() {
                        serializer.emit_i64(key, n)?;
                    } else if let Some(n) = n.as_u64() {
                        serializer.emit_u64(key, n)?;
                    } else if let Some(n) = n.as_f64() {
                        serializer.emit_f64(key, n)?;
                    } else if let Some(n) = n.as_i128() {
                        serializer.emit_i128(key, n)?;
                    } else if let Some(n) = n.as_u128() {
                        serializer.emit_u128(key, n)?;
                    } else {
                        serializer.emit_arguments(key, &format_args!("{n}"))?;
                    }
                }
                // Everything else gets fmt::Displayed
                v => {
                    serializer.emit_arguments(key, &format_args!("{v}"))?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogLevel {
    #[default]
    Info,
    Warn,
}

impl iddqd::IdOrdItem for CaseReport {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

impl AnalysisReport {
    pub fn display_multiline(
        &self,
        indent: usize,
    ) -> AnalysisReportDisplayer<'_> {
        AnalysisReportDisplayer { report: self, indent, colored: false }
    }
}

/// Displays an [`AnalysisReport`], as returned by
/// [`AnalysisReport::display_multiline`].
#[must_use = "this struct does nothing unless displayed"]
pub struct AnalysisReportDisplayer<'a> {
    report: &'a AnalysisReport,
    indent: usize,
    colored: bool,
}

impl AnalysisReportDisplayer<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for AnalysisReportDisplayer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self {
            report: AnalysisReport { cases, sitrep_id, comment, log },
            indent,
            colored,
        } = self;
        let styles = display::Styles::new(colored);

        display::Comment::from(comment)
            .indent(indent)
            .colored(colored)
            .fmt(f)?;
        writeln!(
            f,
            "{:indent$}{}: {sitrep_id}",
            "",
            styles.heading().style("sitrep ID")
        )?;
        log.display_multiline(indent)
            .colored(colored)
            .titled("analysis log")
            .fmt(f)?;
        if cases.is_empty() {
            writeln!(
                f,
                "{:indent$}{}",
                "",
                styles
                    .heading()
                    .style("no cases changed in this analysis step")
            )?;
        } else {
            writeln!(
                f,
                "{:indent$}{} ({} with activity):",
                "",
                styles.heading().style("cases"),
                cases.len(),
            )?;
            for case in cases {
                case.display_multiline(indent + 2, Some(*sitrep_id))
                    .colored(colored)
                    .fmt(f)?;
            }
        }
        Ok(())
    }
}

impl CaseReport {
    pub fn display_multiline(
        &self,
        indent: usize,
        this_sitrep: Option<SitrepUuid>,
    ) -> CaseReportDisplayer<'_> {
        CaseReportDisplayer {
            report: self,
            indent,
            this_sitrep,
            colored: false,
        }
    }
}

/// Displays a [`CaseReport`], as returned by
/// [`CaseReport::display_multiline`].
#[must_use = "this struct does nothing unless displayed"]
pub struct CaseReportDisplayer<'a> {
    report: &'a CaseReport,
    indent: usize,
    this_sitrep: Option<SitrepUuid>,
    colored: bool,
}

impl CaseReportDisplayer<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for CaseReportDisplayer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self {
            report: CaseReport { id, metadata, log },
            indent,
            this_sitrep,
            colored,
        } = self;
        let styles = display::Styles::new(colored);
        let bullet = if indent > 0 { "* " } else { "" };
        writeln!(
            f,
            "{:indent$}{bullet}{}",
            "",
            styles.heading().style(format_args!("case {id}"))
        )?;
        let indent = indent + 2;
        metadata
            .display_multiline(indent, this_sitrep)
            .colored(colored)
            .fmt(f)?;
        log.display_multiline(indent)
            .colored(colored)
            .titled("activity in this analysis")
            .fmt(f)?;
        Ok(())
    }
}

pub struct LogEntryBuilder<'log> {
    slog_log: &'log slog::Logger,
    analysis_log: &'log mut DebugLog,
    entry: LogEntry,
}

impl DebugLog {
    pub fn entry<'a>(
        &'a mut self,
        log: &'a slog::Logger,
        event: impl ToString,
    ) -> LogEntryBuilder<'a> {
        LogEntryBuilder {
            slog_log: log,
            analysis_log: self,
            entry: LogEntry::new(event, LogLevel::Info),
        }
    }

    pub fn warning<'a>(
        &'a mut self,
        log: &'a slog::Logger,
        event: impl ToString,
    ) -> LogEntryBuilder<'a> {
        LogEntryBuilder {
            slog_log: log,
            analysis_log: self,
            entry: LogEntry::new(event, LogLevel::Warn),
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn display_multiline(&self, indent: usize) -> DebugLogDisplayer<'_> {
        DebugLogDisplayer { log: self, indent, title: None, colored: false }
    }
}

struct DebugLogDisplayer<'a> {
    log: &'a DebugLog,
    indent: usize,
    title: Option<&'a str>,
    colored: bool,
}

impl<'a> DebugLogDisplayer<'a> {
    fn titled(self, title: &'a str) -> Self {
        Self { title: Some(title), ..self }
    }

    /// If `colored` is true, style the output with ANSI terminal colors.
    fn colored(self, colored: bool) -> Self {
        Self { colored, ..self }
    }
}

impl fmt::Display for DebugLogDisplayer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self { log, indent, title, colored } = self;
        let styles = display::Styles::new(colored);
        let mut indent = indent;
        if let Some(title) = title {
            if log.is_empty() {
                return writeln!(
                    f,
                    "{:<indent$}{}",
                    "",
                    styles.heading().style(format_args!("no {title}"))
                );
            } else {
                writeln!(
                    f,
                    "{:<indent$}{}:",
                    "",
                    styles.heading().style(format_args!("{title}"))
                )?;
                // log entries will be indented under the title
                indent += 2;
            }
        }
        if log.is_empty() {
            return writeln!(
                f,
                "{:<indent$}{}",
                "",
                styles.heading().style("(no log entries)")
            );
        }

        for entry in &log.0 {
            entry.display_indented(indent).colored(colored).fmt(f)?;
        }

        Ok(())
    }
}

impl LogEntryBuilder<'_> {
    pub fn comment(mut self, comment: impl ToString) -> Self {
        self.entry.comment(comment);
        self
    }

    pub fn kv(self, key: impl ToString, value: impl Serialize) -> Self {
        self.kvs(std::iter::once((key, value)))
    }

    pub fn kvs(
        mut self,
        kvs: impl IntoIterator<Item = (impl ToString, impl Serialize)>,
    ) -> Self {
        self.entry.kvs(kvs);
        self
    }

    pub fn finish(self) {
        drop(self)
    }
}

impl Drop for LogEntryBuilder<'_> {
    fn drop(&mut self) {
        // The comment will be formatted as part of the slog `msg` field, rather
        // than as a separate field, because it is likely to contain newlines,
        // and that's the only part of the log record that `looker` will
        // un-escape newlines in...
        let comment =
            display::Comment::from(&self.entry.comment).with_leading_newline();
        match self.entry.level {
            LogLevel::Info => {
                slog::info!(
                    &self.slog_log,
                    "{}{comment}",
                    self.entry.event;
                    &self.entry
                )
            }
            LogLevel::Warn => {
                slog::warn!(
                    &self.slog_log,
                    "{}{comment}",
                    self.entry.event;
                    &self.entry
                )
            }
        }
        self.analysis_log.0.push(std::mem::replace(
            &mut self.entry,
            LogEntry::new(String::new(), LogLevel::Info),
        ));
    }
}

impl LogEntry {
    fn new(event: impl ToString, level: LogLevel) -> Self {
        Self {
            event: event.to_string(),
            comment: None,
            level,
            kvs: BTreeMap::new(),
        }
    }

    fn comment(&mut self, comment: impl ToString) -> &mut Self {
        self.comment = Some(comment.to_string());
        self
    }

    fn kvs(
        &mut self,
        kvs: impl IntoIterator<Item = (impl ToString, impl Serialize)>,
    ) -> &mut Self {
        self.kvs.extend(kvs.into_iter().map(|(k, v)| {
            let v = serde_json::to_value(v)
                .unwrap_or_else(|e| serde_json::Value::String(e.to_string()));
            (k.to_string(), v)
        }));
        self
    }

    pub fn display_indented(&self, indent: usize) -> LogEntryDisplayer<'_> {
        LogEntryDisplayer { entry: self, indent, colored: false }
    }
}

/// Displays a [`LogEntry`], as returned by [`LogEntry::display_indented`].
#[must_use = "this struct does nothing unless displayed"]
pub struct LogEntryDisplayer<'a> {
    entry: &'a LogEntry,
    indent: usize,
    colored: bool,
}

impl LogEntryDisplayer<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for LogEntryDisplayer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self {
            entry: LogEntry { event, comment, kvs, level },
            indent,
            colored,
        } = self;
        let styles = display::Styles::new(colored);
        let bullet = if indent > 0 { "* " } else { "" };
        let colon = if kvs.is_empty() { "" } else { ":" };

        match level {
            LogLevel::Info => {
                writeln!(f, "{:indent$}{bullet}{event}{colon}", "")?;
            }
            LogLevel::Warn => {
                writeln!(
                    f,
                    "{:indent$}{bullet}{}{}{colon}",
                    "",
                    styles.warning().style("/!\\ WARNING: "),
                    styles.warning_text().style(event),
                )?;
            }
        }
        display::Comment::from(comment)
            .indent(indent + 2)
            .colored(colored)
            .fmt(f)?;
        for (k, v) in kvs {
            display::fmt_json_value(f, k, v, indent + 2, styles)?;
        }
        Ok(())
    }
}

/// Summarizes the inputs to sitrep analysis.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputReport {
    pub parent_sitrep_id: Option<SitrepUuid>,
    pub parent_inv_id: Option<CollectionUuid>,
    pub inv_id: CollectionUuid,
    pub new_ereport_ids: BTreeSet<EreportId>,
    /// Cases which were open in the parent sitrep.
    pub open_cases: BTreeMap<CaseUuid, case::Metadata>,
    /// Cases which have closed, but which have been copied forwards as they
    /// contain ereports which have not yet been marked seen.
    pub closed_cases_copied_forward: BTreeMap<CaseUuid, ClosedCaseReport>,
    /// Number of entries in the ereporter restart table.
    pub num_ereporter_restarts: usize,
    /// All control-plane-managed physical disks visible to the diagnosis
    /// engines for this analysis pass.
    pub in_service_disks: BTreeSet<PhysicalDiskUuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosedCaseReport {
    pub metadata: case::Metadata,
    pub unmarked_ereports: BTreeSet<EreportId>,
    pub unmarked_alert_requests: BTreeSet<AlertUuid>,
    pub unmarked_support_bundle_requests: BTreeSet<SupportBundleUuid>,
}

impl InputReport {
    pub fn display_multiline(
        &self,
        indent: usize,
    ) -> InputReportMultilineDisplay<'_> {
        InputReportMultilineDisplay { report: self, indent, colored: false }
    }
}

/// Displays an [`InputReport`], as returned by
/// [`InputReport::display_multiline`].
#[must_use = "this struct does nothing unless displayed"]
pub struct InputReportMultilineDisplay<'report> {
    report: &'report InputReport,
    indent: usize,
    colored: bool,
}

impl InputReportMultilineDisplay<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for InputReportMultilineDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            report:
                InputReport {
                    parent_sitrep_id,
                    parent_inv_id,
                    inv_id,
                    new_ereport_ids,
                    open_cases,
                    closed_cases_copied_forward,
                    num_ereporter_restarts,
                    in_service_disks,
                },
            indent,
            colored,
        } = self;
        let styles = display::Styles::new(*colored);
        let heading = styles.heading();

        if let Some(id) = parent_sitrep_id {
            writeln!(
                f,
                "{:indent$}{}:        {id}",
                "",
                heading.style("parent sitrep")
            )?;
        } else {
            writeln!(
                f,
                "{:indent$}{}:        {}",
                "",
                heading.style("parent sitrep"),
                styles.missing().style("<none>")
            )?;
        }

        writeln!(
            f,
            "{:indent$}{}: {inv_id}",
            "",
            heading.style("inventory collection")
        )?;
        if Some(inv_id) == parent_inv_id.as_ref() {
            writeln!(
                f,
                "{:indent$} {}",
                "",
                styles
                    .annotation()
                    .style("--> same collection as parent sitrep"),
            )?;
        } else if let Some(parent_inv_id) = parent_inv_id {
            writeln!(
                f,
                "{:indent$} {}",
                "",
                styles.annotation().style(format_args!(
                    "--> different from parent sitrep \
                     (collection {parent_inv_id})"
                )),
            )?;
        }

        writeln!(
            f,
            "{:indent$}{}: {num_ereporter_restarts}",
            "",
            heading.style("total known ereport restart IDs"),
        )?;

        if !new_ereport_ids.is_empty() {
            writeln!(
                f,
                "\n{:indent$}{} ({} total):",
                "",
                heading.style("new ereports"),
                new_ereport_ids.len()
            )?;
            let indent = indent + 2;
            for ereport_id in new_ereport_ids {
                writeln!(f, "{:indent$}* ereport {ereport_id}", "")?;
            }
        } else {
            writeln!(
                f,
                "{:indent$}{}",
                "",
                styles
                    .heading()
                    .style("no new ereports since the parent sitrep"),
            )?;
        }

        let total_cases = open_cases.len() + closed_cases_copied_forward.len();
        if total_cases > 0 {
            writeln!(
                f,
                "\n{:indent$}{} ({total_cases} total):",
                "",
                heading.style("cases")
            )?;
            let indent = indent + 2;
            if open_cases.is_empty() {
                writeln!(
                    f,
                    "{:indent$}{}",
                    "",
                    styles.heading().style("no open cases"),
                )?;
            } else {
                writeln!(
                    f,
                    "{:indent$}{} ({} total):",
                    "",
                    heading.style("open cases"),
                    open_cases.len()
                )?;
                let indent = indent + 2;
                for (case_id, metadata) in open_cases {
                    writeln!(
                        f,
                        "{:indent$}* {}",
                        "",
                        heading.style(format_args!("case {case_id}"))
                    )?;
                    metadata
                        .display_multiline(indent + 2, None)
                        .colored(*colored)
                        .fmt(f)?;
                }
            }

            if closed_cases_copied_forward.is_empty() {
                writeln!(
                    f,
                    "{:indent$}{}",
                    "",
                    styles
                        .heading()
                        .style("no closed cases must be copied forwards"),
                )?;
            } else {
                writeln!(
                    f,
                    "{:indent$}{} ({} total):",
                    "",
                    heading.style("closed cases copied forwards"),
                    closed_cases_copied_forward.len()
                )?;
                let indent = indent + 2;
                for (
                    case_id,
                    ClosedCaseReport {
                        metadata,
                        unmarked_ereports,
                        unmarked_alert_requests,
                        unmarked_support_bundle_requests,
                    },
                ) in closed_cases_copied_forward
                {
                    writeln!(
                        f,
                        "{:indent$}* {}",
                        "",
                        heading.style(format_args!("case {case_id}"))
                    )?;
                    let indent = indent + 2;
                    metadata
                        .display_multiline(indent, None)
                        .colored(*colored)
                        .fmt(f)?;
                    // A closed case is only carried forward when it has
                    // outstanding work, so spell out why. If we don't seem to
                    // have any outstanding work but the closed case was carried
                    // forward anyway, that's weird and worth a warning.
                    if unmarked_ereports.is_empty()
                        && unmarked_alert_requests.is_empty()
                        && unmarked_support_bundle_requests.is_empty()
                    {
                        writeln!(
                            f,
                            "{:indent$}{} {}",
                            "",
                            styles.warning().style("/!\\ WEIRD:"),
                            styles.warning_text().style(
                                "this case has no recorded \
                                 reason for being copied forwards!"
                            )
                        )?;
                        continue;
                    }
                    writeln!(
                        f,
                        "{:indent$}{}:",
                        "",
                        heading.style("copied forwards due to")
                    )?;
                    let indent = indent + 2;
                    if !unmarked_ereports.is_empty() {
                        writeln!(
                            f,
                            "{:indent$}{}:",
                            "",
                            heading.style("ereports not yet marked seen")
                        )?;
                        let indent = indent + 2;
                        for ereport_id in unmarked_ereports {
                            writeln!(
                                f,
                                "{:indent$}* ereport {ereport_id}",
                                ""
                            )?;
                        }
                    }
                    if !unmarked_alert_requests.is_empty() {
                        writeln!(
                            f,
                            "{:indent$}{}:",
                            "",
                            heading.style("alert requests not yet satisfied")
                        )?;
                        let indent = indent + 2;
                        for alert_id in unmarked_alert_requests {
                            writeln!(f, "{:indent$}* alert {alert_id}", "")?;
                        }
                    }
                    if !unmarked_support_bundle_requests.is_empty() {
                        writeln!(
                            f,
                            "{:indent$}{}:",
                            "",
                            heading.style(
                                "support bundle requests not yet satisfied"
                            )
                        )?;
                        let indent = indent + 2;
                        for bundle_id in unmarked_support_bundle_requests {
                            writeln!(
                                f,
                                "{:indent$}* support bundle {bundle_id}",
                                ""
                            )?;
                        }
                    }
                }
            }
        } else {
            writeln!(
                f,
                "{:indent$}{}",
                "",
                styles.heading().style("no cases copied forward"),
            )?;
        }

        if in_service_disks.is_empty() {
            writeln!(
                f,
                "\n{:indent$}{}",
                "",
                styles.heading().style("no in-service control plane disks"),
            )?;
        } else {
            writeln!(
                f,
                "\n{:indent$}{} ({} total):",
                "",
                heading.style("in-service control plane disks"),
                in_service_disks.len()
            )?;
            let indent = indent + 2;
            for disk_id in in_service_disks {
                writeln!(f, "{:indent$}* disk {disk_id}", "")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::DiagnosisEngineKind;
    use super::super::case;
    use super::*;
    use ereport_types::{Ena, EreportId};
    use omicron_uuid_kinds::{
        CaseUuid, CollectionUuid, EreporterRestartUuid, PhysicalDiskUuid,
        SitrepUuid,
    };
    use std::str::FromStr;

    fn example_report_with_cases() -> InputReport {
        let parent_sitrep_id =
            SitrepUuid::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                .unwrap();
        let inv_id =
            CollectionUuid::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                .unwrap();
        let parent_inv_id =
            CollectionUuid::from_str("cccccccc-cccc-cccc-cccc-cccccccccccc")
                .unwrap();
        let restart_id = EreporterRestartUuid::from_str(
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
        )
        .unwrap();

        let case1_id =
            CaseUuid::from_str("11111111-1111-1111-1111-111111111111").unwrap();
        let case1_created_sitrep =
            SitrepUuid::from_str("22222222-2222-2222-2222-222222222222")
                .unwrap();

        let case2_id =
            CaseUuid::from_str("33333333-3333-3333-3333-333333333333").unwrap();
        let case2_created_sitrep =
            SitrepUuid::from_str("44444444-4444-4444-4444-444444444444")
                .unwrap();
        let case2_closed_sitrep =
            SitrepUuid::from_str("55555555-5555-5555-5555-555555555555")
                .unwrap();

        let case3_id =
            CaseUuid::from_str("77777777-7777-7777-7777-777777777777").unwrap();

        let mut open_cases = BTreeMap::new();
        open_cases.insert(
            case1_id,
            case::Metadata {
                created_sitrep_id: case1_created_sitrep,
                closed_sitrep_id: None,
                de: DiagnosisEngineKind::PowerShelf,
                comment: "PSU 0 faulted".to_string(),
            },
        );

        let mut new_ereport_ids = BTreeSet::new();
        new_ereport_ids.insert(EreportId { restart_id, ena: Ena::from(3) });
        new_ereport_ids.insert(EreportId { restart_id, ena: Ena::from(4) });

        let mut closed_cases_copied_forward = BTreeMap::new();
        let mut unmarked_ereports = BTreeSet::new();
        unmarked_ereports
            .insert(EreportId { restart_id, ena: Ena::from(2u64) });
        let mut unmarked_alert_requests = BTreeSet::new();
        unmarked_alert_requests.insert(
            AlertUuid::from_str("66666666-6666-6666-6666-666666666666")
                .unwrap(),
        );
        let mut unmarked_support_bundle_requests = BTreeSet::new();
        unmarked_support_bundle_requests.insert(
            SupportBundleUuid::from_str("88888888-8888-8888-8888-888888888888")
                .unwrap(),
        );
        closed_cases_copied_forward.insert(
            case2_id,
            ClosedCaseReport {
                metadata: case::Metadata {
                    created_sitrep_id: case2_created_sitrep,
                    closed_sitrep_id: Some(case2_closed_sitrep),
                    de: DiagnosisEngineKind::PowerShelf,
                    comment: "PSU 1 replaced".to_string(),
                },
                unmarked_ereports,
                unmarked_alert_requests,
                unmarked_support_bundle_requests,
            },
        );
        // A closed case with no recorded reason for being copied forwards. The
        // real input builder never produces this; the display code prints a
        // warning instead of an empty reason list.
        closed_cases_copied_forward.insert(
            case3_id,
            ClosedCaseReport {
                metadata: case::Metadata {
                    created_sitrep_id: case2_created_sitrep,
                    closed_sitrep_id: Some(case2_closed_sitrep),
                    de: DiagnosisEngineKind::PowerShelf,
                    comment: "PSU 2 replaced".to_string(),
                },
                unmarked_ereports: BTreeSet::new(),
                unmarked_alert_requests: BTreeSet::new(),
                unmarked_support_bundle_requests: BTreeSet::new(),
            },
        );

        let mut in_service_disks = BTreeSet::new();
        in_service_disks.insert(
            PhysicalDiskUuid::from_str("11111111-1111-1111-1111-111111111111")
                .unwrap(),
        );
        in_service_disks.insert(
            PhysicalDiskUuid::from_str("22222222-2222-2222-2222-222222222222")
                .unwrap(),
        );

        InputReport {
            parent_sitrep_id: Some(parent_sitrep_id),
            parent_inv_id: Some(parent_inv_id),
            inv_id,
            num_ereporter_restarts: 420,
            new_ereport_ids,
            open_cases,
            closed_cases_copied_forward,
            in_service_disks,
        }
    }

    fn example_report_empty() -> InputReport {
        let inv_id =
            CollectionUuid::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                .unwrap();

        InputReport {
            parent_sitrep_id: None,
            parent_inv_id: None,
            inv_id,
            num_ereporter_restarts: 0,
            new_ereport_ids: BTreeSet::new(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
            in_service_disks: BTreeSet::new(),
        }
    }

    fn example_report_same_inv() -> InputReport {
        let parent_sitrep_id =
            SitrepUuid::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                .unwrap();
        let inv_id =
            CollectionUuid::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                .unwrap();

        InputReport {
            parent_sitrep_id: Some(parent_sitrep_id),
            parent_inv_id: Some(inv_id),
            inv_id,
            num_ereporter_restarts: 420,
            new_ereport_ids: BTreeSet::new(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
            in_service_disks: BTreeSet::new(),
        }
    }

    // Note: the example report includes the `/!\ WEIRD` warning, so
    // the colored-display check here also covers warning styling.
    #[test]
    fn test_analysis_input_report_display_with_cases() {
        let report = example_report_with_cases();
        let output = display::test_utils::check_colored_display(|colored| {
            report.display_multiline(0).colored(colored)
        });
        expectorate::assert_contents(
            "output/analysis_input_report_with_cases.out",
            &output,
        );
    }

    #[test]
    fn test_analysis_input_report_display_empty() {
        let report = example_report_empty();
        let output = display::test_utils::check_colored_display(|colored| {
            report.display_multiline(0).colored(colored)
        });
        expectorate::assert_contents(
            "output/analysis_input_report_empty.out",
            &output,
        );
    }

    #[test]
    fn test_analysis_input_report_display_same_inv() {
        let report = example_report_same_inv();
        let output = display::test_utils::check_colored_display(|colored| {
            report.display_multiline(0).colored(colored)
        });
        expectorate::assert_contents(
            "output/analysis_input_report_same_inv.out",
            &output,
        );
    }

    /// Golden-file test for the full `AnalysisReport` displayer tree, which
    /// composes the case report, debug log, and case metadata displayers.
    /// The example includes a warning-level log entry, so the colored
    /// check also covers `/!\ WARNING:` styling.
    #[test]
    fn test_analysis_report_display_full() {
        let report: AnalysisReport =
            serde_json::from_value(serde_json::json!({
                "sitrep_id": "ea7affb0-36eb-4a9a-b9bd-22e00f1bcc04",
                "comment": "an example analysis report",
                "cases": [{
                    "id": "b0d36461-e3e7-4a53-9162-82414fb30088",
                    "created_sitrep_id": "ea7affb0-36eb-4a9a-b9bd-22e00f1bcc04",
                    "closed_sitrep_id": null,
                    "de": "power_shelf",
                    "comment": "PSU 0 faulted",
                    "log": [
                        { "event": "opened case" },
                        {
                            "event": "something suspicious",
                            "level": "Warn",
                            "comment": "hmmm...",
                            "key": "value",
                        },
                    ],
                }],
                "log": [{ "event": "began analysis" }],
            }))
            .expect("example analysis report should deserialize");

        let output = display::test_utils::check_colored_display(|colored| {
            report.display_multiline(0).colored(colored)
        });
        expectorate::assert_contents(
            "output/analysis_report_full.out",
            &output,
        );
    }

    /// As above, for a warning-level `LogEntry`, covering the
    /// `/!\ WARNING:` styling.
    #[test]
    fn test_log_entry_display_colored() {
        let json = serde_json::json!({
            "event": "something suspicious happened",
            "comment": "hmmm...",
            "level": "Warn",
            "key": "value",
            "missing": null,
        });
        let entry: LogEntry = serde_json::from_value(json).unwrap();
        display::test_utils::check_colored_display(|colored| {
            entry.display_indented(2).colored(colored)
        });
    }

    /// A JSON object containing only the required `event` field should
    /// always deserialize as a valid `LogEntry`.
    #[test]
    fn test_log_entry_backwards_compat_event_only() {
        let json = serde_json::json!({ "event": "something happened" });
        let entry: LogEntry = serde_json::from_value(json).unwrap();
        assert_eq!(entry.event, "something happened");
        assert_eq!(entry.comment, None);
        assert!(entry.kvs.is_empty());
    }

    /// A JSON object with `event`, `comment`, and arbitrary additional
    /// fields should deserialize as a valid `LogEntry` — the extra
    /// fields land in the flattened `kvs` map.
    #[test]
    fn test_log_entry_forwards_compat_extra_fields() {
        let json = serde_json::json!({
            "event": "something happened",
            "comment": "this is a comment",
            "extra_string": "extra_value",
            "extra_number": 42,
            "nested": { "a": 1, "b": "two" }
        });
        let entry: LogEntry = serde_json::from_value(json).unwrap();
        assert_eq!(entry.event, "something happened");
        assert_eq!(entry.comment.as_deref(), Some("this is a comment"));
        assert_eq!(
            entry.kvs.get("extra_string"),
            Some(&serde_json::Value::String("extra_value".to_string()))
        );
        assert_eq!(entry.kvs.get("extra_number"), Some(&serde_json::json!(42)));
        assert_eq!(
            entry.kvs.get("nested"),
            Some(&serde_json::json!({ "a": 1, "b": "two" }))
        );
    }

    /// A JSON object with `event`, `comment`, and extra fields round-trips
    /// through serialization: serialize → deserialize produces an
    /// equivalent `LogEntry`.
    #[test]
    fn test_log_entry_roundtrip() {
        let mut entry = LogEntry::new("test event", LogLevel::Info);
        entry
            .comment("a comment")
            .kvs([("simple", "value")])
            .kvs([("number", 123)])
            .kvs([("flag", true)]);

        let serialized = serde_json::to_value(&entry).unwrap();
        let deserialized: LogEntry =
            serde_json::from_value(serialized).unwrap();
        assert_eq!(entry, deserialized);
    }

    /// Extra fields of various JSON types (string, number, bool, null,
    /// array, nested object) are all preserved through deserialization.
    #[test]
    fn test_log_entry_forwards_compat_varied_types() {
        let json = serde_json::json!({
            "event": "varied types",
            "a_bool": true,
            "a_null": null,
            "an_array": [1, "two", false],
            "deep": { "level1": { "level2": "hi" } }
        });
        let entry: LogEntry = serde_json::from_value(json).unwrap();
        assert_eq!(entry.event, "varied types");
        assert_eq!(entry.kvs.get("a_bool"), Some(&serde_json::json!(true)));
        assert_eq!(entry.kvs.get("a_null"), Some(&serde_json::Value::Null));
        assert_eq!(
            entry.kvs.get("an_array"),
            Some(&serde_json::json!([1, "two", false]))
        );
        assert_eq!(
            entry.kvs.get("deep"),
            Some(&serde_json::json!({ "level1": { "level2": "hi" } }))
        );
    }

    /// The pretty-printer handles nested objects and arrays by indenting
    /// sub-entries under their parent bullet point.
    #[test]
    fn test_log_entry_display_nested_values() {
        let json = serde_json::json!({
            "event": "nested example",
            "comment": "shows nesting",
            "flat_key": "flat_value",
            "obj": { "inner_a": "va", "inner_b": 2 },
            "arr": [10, 20]
        });
        let entry: LogEntry = serde_json::from_value(json).unwrap();
        let output = display::test_utils::check_colored_display(|colored| {
            entry.display_indented(0).colored(colored)
        });
        expectorate::assert_contents(
            "output/log_entry_display_nested_values.out",
            &output,
        );
    }

    /// The pretty-printer handles multi-line strings in comments by outputting
    /// them at the correct indentation and prefixing each line with a `//`.
    #[test]
    fn test_log_entry_display_multiline_comment() {
        let json = serde_json::json!({
            "event": "multi-line comment",
            "comment": "this comment\nspans multiple lines\nisn't that cool?",
            "flat_key": "flat_value",
        });
        let entry: LogEntry = serde_json::from_value(json).unwrap();
        let output = display::test_utils::check_colored_display(|colored| {
            entry.display_indented(0).colored(colored)
        });
        expectorate::assert_contents(
            "output/log_entry_display_multiline_comment.out",
            &output,
        );
    }
}
