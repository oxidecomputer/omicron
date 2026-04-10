// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Human-readable reports summarizing what occurred during fault management
//! analysis.

use super::case;
use super::ereport::EreportId;
use iddqd::IdOrdMap;
use omicron_uuid_kinds::{CaseUuid, CollectionUuid, SitrepUuid};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnalysisReport {
    pub sitrep_id: SitrepUuid,
    pub comment: String,
    pub cases: IdOrdMap<CaseReport>,
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
    #[serde(flatten)]
    kvs: BTreeMap<String, serde_json::Value>,
}

impl iddqd::IdOrdItem for CaseReport {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

impl AnalysisReport {
    pub fn display_multiline(&self, indent: usize) -> impl fmt::Display + '_ {
        struct AnalysisReportDisplayer<'a> {
            report: &'a AnalysisReport,
            indent: usize,
        }

        impl<'a> fmt::Display for AnalysisReportDisplayer<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let &Self {
                    report: AnalysisReport { cases, sitrep_id, comment },
                    indent,
                } = self;
                writeln!(f, "{:indent$}fault management analysis report", "")?;
                writeln!(f, "{:indent$}--------------------------------", "")?;
                if !comment.is_empty() {
                    writeln!(f, "{:indent$}// {comment}", "")?;
                }
                writeln!(f, "{:indent$}sitrep ID: {sitrep_id}", "")?;
                writeln!(f, "{:indent$}cases:", "")?;
                for case in cases {
                    case.display_multiline(indent + 2, Some(*sitrep_id))
                        .fmt(f)?;
                }
                Ok(())
            }
        }

        AnalysisReportDisplayer { report: self, indent }
    }
}

impl CaseReport {
    pub fn display_multiline(
        &self,
        indent: usize,
        this_sitrep: Option<SitrepUuid>,
    ) -> impl fmt::Display + '_ {
        struct CaseReportDisplayer<'a> {
            report: &'a CaseReport,
            indent: usize,
            this_sitrep: Option<SitrepUuid>,
        }

        impl<'a> fmt::Display for CaseReportDisplayer<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let &Self {
                    report: CaseReport { id, metadata, log: DebugLog(log) },
                    indent,
                    this_sitrep,
                } = self;
                let bullet = if indent > 0 { "* " } else { "" };
                writeln!(f, "{:indent$}{bullet}case {id}", "")?;
                let indent = indent + 2;
                metadata.display_multiline(indent, this_sitrep).fmt(f)?;
                if !log.is_empty() {
                    writeln!(f, "{:indent$}activity in this analysis:", "")?;
                    let indent = indent + 2;
                    for entry in &log[..] {
                        entry.display_indented(indent).fmt(f)?;
                    }
                } else {
                    writeln!(f, "{:indent$}no activity in this analysis", "")?;
                }
                Ok(())
            }
        }

        CaseReportDisplayer { report: self, indent, this_sitrep }
    }
}

impl DebugLog {
    pub fn entry(&mut self, event: impl ToString) -> &mut LogEntry {
        self.0.push(LogEntry::new(event));
        self.0.last_mut().expect("we just pushed it")
    }
}

/// Recursively format a JSON value as a bulleted list entry, nesting any
/// object or array children as indented sub-bullets.
fn fmt_json_value(
    f: &mut fmt::Formatter<'_>,
    key: &str,
    value: &serde_json::Value,
    indent: usize,
) -> fmt::Result {
    match value {
        serde_json::Value::Object(map) => {
            writeln!(f, "{:indent$}* {key}:", "")?;
            for (k, v) in map {
                fmt_json_value(f, k, v, indent + 2)?;
            }
            Ok(())
        }
        serde_json::Value::Array(arr) => {
            writeln!(f, "{:indent$}* {key}:", "")?;
            let indent = indent + 2;
            for (i, v) in arr.iter().enumerate() {
                fmt_json_array_item(f, i + 1, v, indent)?;
            }
            Ok(())
        }
        serde_json::Value::String(s) => {
            writeln!(f, "{:indent$}* {key}: {s}", "")
        }
        serde_json::Value::Null => {
            writeln!(f, "{:indent$}* {key}: <none>", "")
        }
        serde_json::Value::Bool(b) => {
            writeln!(f, "{:indent$}* {key}: {b}", "")
        }
        serde_json::Value::Number(n) => {
            writeln!(f, "{:indent$}* {key}: {n}", "")
        }
    }
}

/// Format a single element of a JSON array as a numbered list item,
/// e.g. `1. value` for scalars or `1.` followed by indented children for
/// objects and nested arrays.
fn fmt_json_array_item(
    f: &mut fmt::Formatter<'_>,
    n: usize,
    value: &serde_json::Value,
    indent: usize,
) -> fmt::Result {
    match value {
        serde_json::Value::Object(map) => {
            writeln!(f, "{:indent$}{n}.", "")?;
            for (k, v) in map {
                fmt_json_value(f, k, v, indent + 2)?;
            }
            Ok(())
        }
        serde_json::Value::Array(arr) => {
            writeln!(f, "{:indent$}{n}.", "")?;
            let indent = indent + 2;
            for (i, v) in arr.iter().enumerate() {
                fmt_json_array_item(f, i + 1, v, indent)?;
            }
            Ok(())
        }
        serde_json::Value::String(s) => {
            writeln!(f, "{:indent$}{n}. {s}", "")
        }
        serde_json::Value::Null => {
            writeln!(f, "{:indent$}{n}. <none>", "")
        }
        serde_json::Value::Bool(b) => {
            writeln!(f, "{:indent$}{n}. {b}", "")
        }
        serde_json::Value::Number(num) => {
            writeln!(f, "{:indent$}{n}. {num}", "")
        }
    }
}

impl LogEntry {
    pub fn new(event: impl ToString) -> Self {
        Self { event: event.to_string(), comment: None, kvs: BTreeMap::new() }
    }

    pub fn comment(&mut self, comment: impl ToString) -> &mut Self {
        self.comment = Some(comment.to_string());
        self
    }

    pub fn kv(
        &mut self,
        key: impl ToString,
        value: impl Serialize,
    ) -> &mut Self {
        self.kvs(std::iter::once((key, value)))
    }

    pub fn kvs(
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

    pub fn display_indented(&self, indent: usize) -> impl fmt::Display + '_ {
        struct LogEntryDisplayer<'a> {
            entry: &'a LogEntry,
            indent: usize,
        }

        impl<'a> fmt::Display for LogEntryDisplayer<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let &Self { entry: LogEntry { event, comment, kvs }, indent } =
                    self;
                let bullet = if indent > 0 { "* " } else { "" };
                let colon = if kvs.is_empty() { "" } else { ":" };
                writeln!(f, "{:indent$}{bullet}{event}{colon}", "")?;
                if let Some(comment) = comment {
                    writeln!(f, "{:indent$}  // {comment}", "")?;
                }
                for (k, v) in kvs {
                    fmt_json_value(f, k, v, indent + 2)?;
                }
                Ok(())
            }
        }
        LogEntryDisplayer { entry: self, indent }
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosedCaseReport {
    pub metadata: case::Metadata,
    pub unmarked_ereports: BTreeSet<EreportId>,
}

impl InputReport {
    pub fn display_multiline(&self, indent: usize) -> impl fmt::Display + '_ {
        InputReportMultilineDisplay { report: self, indent }
    }
}

struct InputReportMultilineDisplay<'report> {
    report: &'report InputReport,
    indent: usize,
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
                },
            indent,
        } = self;

        writeln!(f, "{:indent$}fault management analysis inputs", "")?;
        writeln!(f, "{:indent$}--------------------------------", "")?;
        if let Some(id) = parent_sitrep_id {
            writeln!(f, "{:indent$}parent sitrep:        {id}", "",)?;
        } else {
            writeln!(f, "{:indent$}parent sitrep:        <none>", "")?;
        }

        writeln!(f, "{:indent$}inventory collection: {inv_id}", "",)?;
        if Some(inv_id) == parent_inv_id.as_ref() {
            writeln!(f, "{:indent$} --> same collection as parent sitrep", "",)?;
        } else if let Some(parent_inv_id) = parent_inv_id {
            writeln!(
                f,
                "{:indent$} --> different from parent sitrep \
                 (collection {parent_inv_id})",
                "",
            )?;
        }

        if !new_ereport_ids.is_empty() {
            writeln!(
                f,
                "\n{:indent$}new ereports ({} total):",
                "",
                new_ereport_ids.len()
            )?;
            let indent = indent + 2;
            for ereport_id in new_ereport_ids {
                writeln!(f, "{:indent$}* ereport {ereport_id}", "")?;
            }
        } else {
            writeln!(
                f,
                "{:indent$}no new ereports since the parent sitrep",
                "",
            )?;
        }

        let total_cases = open_cases.len() + closed_cases_copied_forward.len();
        if total_cases > 0 {
            writeln!(f, "\n{:indent$}cases ({} total):", "", total_cases)?;
            let indent = indent + 2;
            if open_cases.is_empty() {
                writeln!(f, "{:indent$}no open cases", "",)?;
            } else {
                writeln!(
                    f,
                    "{:indent$}open cases ({} total):",
                    "",
                    open_cases.len()
                )?;
                let indent = indent + 2;
                for (case_id, metadata) in open_cases {
                    writeln!(f, "{:indent$}* case {case_id}", "")?;
                    metadata.display_multiline(indent + 2, None).fmt(f)?;
                }
            }

            if closed_cases_copied_forward.is_empty() {
                writeln!(
                    f,
                    "{:indent$}no closed cases must be copied forwards",
                    "",
                )?;
            } else {
                writeln!(
                    f,
                    "{:indent$}closed cases copied forwards ({} total):",
                    "",
                    closed_cases_copied_forward.len()
                )?;
                let indent = indent + 2;
                for (
                    case_id,
                    ClosedCaseReport { metadata, unmarked_ereports },
                ) in closed_cases_copied_forward
                {
                    writeln!(f, "{:indent$}* case {case_id}", "")?;
                    let indent = indent + 2;
                    metadata.display_multiline(indent, None).fmt(f)?;
                    writeln!(
                        f,
                        "{:indent$}copied forwards because these ereports \
                         haven't been marked seen yet:",
                        ""
                    )?;
                    for ereport_id in unmarked_ereports {
                        writeln!(f, "{:indent$}* ereport {ereport_id}", "")?;
                    }
                }
            }
        } else {
            writeln!(f, "{:indent$}no cases copied forward", "")?;
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
        CaseUuid, CollectionUuid, EreporterRestartUuid, SitrepUuid,
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
            },
        );

        InputReport {
            parent_sitrep_id: Some(parent_sitrep_id),
            parent_inv_id: Some(parent_inv_id),
            inv_id,
            new_ereport_ids,
            open_cases,
            closed_cases_copied_forward,
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
            new_ereport_ids: BTreeSet::new(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
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
            new_ereport_ids: BTreeSet::new(),
            open_cases: BTreeMap::new(),
            closed_cases_copied_forward: BTreeMap::new(),
        }
    }

    #[test]
    fn test_analysis_input_report_display_with_cases() {
        let report = example_report_with_cases();
        let output = format!("{}", report.display_multiline(0));
        expectorate::assert_contents(
            "output/analysis_input_report_with_cases.out",
            &output,
        );
    }

    #[test]
    fn test_analysis_input_report_display_empty() {
        let report = example_report_empty();
        let output = format!("{}", report.display_multiline(0));
        expectorate::assert_contents(
            "output/analysis_input_report_empty.out",
            &output,
        );
    }

    #[test]
    fn test_analysis_input_report_display_same_inv() {
        let report = example_report_same_inv();
        let output = format!("{}", report.display_multiline(0));
        expectorate::assert_contents(
            "output/analysis_input_report_same_inv.out",
            &output,
        );
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
        let mut entry = LogEntry::new("test event");
        entry
            .comment("a comment")
            .kv("simple", "value")
            .kv("number", 123)
            .kv("flag", true);

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
        let output = format!("{}", entry.display_indented(0));
        expectorate::assert_contents(
            "output/log_entry_display_nested_values.out",
            &output,
        );
    }
}
