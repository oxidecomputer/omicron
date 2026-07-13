// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Human-readable reports summarizing what occurred during fault management
//! analysis.

use super::case;
use super::ereport::EreportId;
use super::json_display::fmt_json_value;
use crate::observed_saga::{ObservedSagaState, SagaOwnerState};
use chrono::{DateTime, Utc};
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

                for line in comment.lines() {
                    writeln!(f, "{:indent$}// {line}", "")?;
                }
                writeln!(f, "{:indent$}sitrep ID: {sitrep_id}", "")?;
                if cases.is_empty() {
                    writeln!(
                        f,
                        "{:indent$}no cases changed in this analysis step",
                        ""
                    )?;
                } else {
                    writeln!(
                        f,
                        "{:indent$}cases ({} with activity):",
                        "",
                        cases.len()
                    )?;
                    for case in cases {
                        case.display_multiline(indent + 2, Some(*sitrep_id))
                            .fmt(f)?;
                    }
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
                    for line in comment.lines() {
                        writeln!(f, "{:indent$}  // {line}", "")?;
                    }
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
    /// Number of entries in the ereporter restart table.
    pub num_ereporter_restarts: usize,
    /// All control-plane-managed physical disks visible to the diagnosis
    /// engines for this analysis pass.
    pub in_service_disks: BTreeSet<PhysicalDiskUuid>,
    /// All non-terminal sagas visible to the diagnosis engines for this
    /// analysis pass.
    pub observed_sagas: BTreeMap<steno::SagaId, ObservedSagaReport>,
}

/// Summary of one non-terminal saga in an [`InputReport`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservedSagaReport {
    pub saga_name: String,
    pub saga_state: ObservedSagaState,
    /// The latest node event recorded for this saga, or `None` if it has
    /// recorded none.
    pub last_event_time: Option<DateTime<Utc>>,
    /// The classified state of the owning Nexus, or `None` if the saga has
    /// no current SEC.
    pub owner_state: Option<SagaOwnerState>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosedCaseReport {
    pub metadata: case::Metadata,
    pub unmarked_ereports: BTreeSet<EreportId>,
    pub unmarked_alert_requests: BTreeSet<AlertUuid>,
    pub unmarked_support_bundle_requests: BTreeSet<SupportBundleUuid>,
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
                    num_ereporter_restarts,
                    in_service_disks,
                    observed_sagas,
                },
            indent,
        } = self;

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

        writeln!(
            f,
            "{:indent$}total known ereport restart IDs: \
                {num_ereporter_restarts}",
            "",
        )?;

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
                    ClosedCaseReport {
                        metadata,
                        unmarked_ereports,
                        unmarked_alert_requests,
                        unmarked_support_bundle_requests,
                    },
                ) in closed_cases_copied_forward
                {
                    writeln!(f, "{:indent$}* case {case_id}", "")?;
                    let indent = indent + 2;
                    metadata.display_multiline(indent, None).fmt(f)?;
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
                            "{:indent$}/!\\ WEIRD: this case has no recorded \
                             reason for being copied forwards!",
                            ""
                        )?;
                        continue;
                    }
                    writeln!(f, "{:indent$}copied forwards due to:", "")?;
                    let indent = indent + 2;
                    if !unmarked_ereports.is_empty() {
                        writeln!(
                            f,
                            "{:indent$}ereports not yet marked seen:",
                            ""
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
                            "{:indent$}alert requests not yet satisfied:",
                            ""
                        )?;
                        let indent = indent + 2;
                        for alert_id in unmarked_alert_requests {
                            writeln!(f, "{:indent$}* alert {alert_id}", "")?;
                        }
                    }
                    if !unmarked_support_bundle_requests.is_empty() {
                        writeln!(
                            f,
                            "{:indent$}support bundle requests not yet \
                             satisfied:",
                            ""
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
            writeln!(f, "{:indent$}no cases copied forward", "")?;
        }

        if in_service_disks.is_empty() {
            writeln!(f, "\n{:indent$}no in-service control plane disks", "")?;
        } else {
            writeln!(
                f,
                "\n{:indent$}in-service control plane disks ({} total):",
                "",
                in_service_disks.len()
            )?;
            let indent = indent + 2;
            for disk_id in in_service_disks {
                writeln!(f, "{:indent$}* disk {disk_id}", "")?;
            }
        }

        if observed_sagas.is_empty() {
            writeln!(f, "\n{:indent$}no non-terminal sagas observed", "")?;
        } else {
            writeln!(
                f,
                "\n{:indent$}non-terminal sagas observed ({} total):",
                "",
                observed_sagas.len()
            )?;
            let indent = indent + 2;
            for (saga_id, saga) in observed_sagas {
                let ObservedSagaReport {
                    saga_name,
                    saga_state,
                    last_event_time,
                    owner_state,
                } = saga;
                write!(
                    f,
                    "{:indent$}* saga {saga_id} ({saga_name}): \
                     {saga_state:?}, last event: ",
                    ""
                )?;
                match last_event_time {
                    Some(t) => write!(f, "{t}")?,
                    None => write!(f, "<none recorded>")?,
                }
                match owner_state {
                    Some(s) => writeln!(f, ", owner: {s:?}")?,
                    None => writeln!(f, ", owner: <no current SEC>")?,
                }
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

        let observed_sagas = example_observed_sagas();

        InputReport {
            parent_sitrep_id: Some(parent_sitrep_id),
            parent_inv_id: Some(parent_inv_id),
            inv_id,
            num_ereporter_restarts: 420,
            new_ereport_ids,
            open_cases,
            closed_cases_copied_forward,
            in_service_disks,
            observed_sagas,
        }
    }

    fn example_observed_sagas() -> BTreeMap<steno::SagaId, ObservedSagaReport> {
        let mut observed_sagas = BTreeMap::new();
        observed_sagas.insert(
            steno::SagaId(
                uuid::Uuid::from_str("5a9a0001-5a9a-45a9-85a9-5a9a5a9a5a9a")
                    .unwrap(),
            ),
            ObservedSagaReport {
                saga_name: "fake-saga".to_string(),
                saga_state: ObservedSagaState::Unwinding,
                last_event_time: DateTime::from_timestamp(0, 0),
                owner_state: Some(SagaOwnerState::Quiesced),
            },
        );
        observed_sagas.insert(
            steno::SagaId(
                uuid::Uuid::from_str("5a9a0002-5a9a-45a9-85a9-5a9a5a9a5a9a")
                    .unwrap(),
            ),
            ObservedSagaReport {
                saga_name: "another-fake-saga".to_string(),
                saga_state: ObservedSagaState::Abandoned,
                last_event_time: None,
                owner_state: None,
            },
        );
        observed_sagas
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
            observed_sagas: BTreeMap::new(),
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
            observed_sagas: BTreeMap::new(),
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
        let output = format!("{}", entry.display_indented(0));
        expectorate::assert_contents(
            "output/log_entry_display_multiline_comment.out",
            &output,
        );
    }
}
