// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::alert::AlertClass;
use crate::fm::DiagnosisEngineKind;
use crate::fm::Ereport;
use iddqd::{IdOrdItem, IdOrdMap};
use omicron_uuid_kinds::{AlertUuid, CaseEreportUuid, CaseUuid, SitrepUuid};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Case {
    pub id: CaseUuid,
    pub created_sitrep_id: SitrepUuid,
    pub closed_sitrep_id: Option<SitrepUuid>,

    pub de: DiagnosisEngineKind,

    pub ereports: IdOrdMap<CaseEreport>,
    pub alerts_requested: IdOrdMap<AlertRequest>,

    pub comment: String,
}

impl Case {
    pub fn is_open(&self) -> bool {
        self.closed_sitrep_id.is_none()
    }

    pub fn display_indented(
        &self,
        indent: usize,
        sitrep_id: Option<SitrepUuid>,
    ) -> impl fmt::Display + '_ {
        DisplayCase { case: self, indent, sitrep_id }
    }
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_indented(0, None).fmt(f)
    }
}

impl IdOrdItem for Case {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct CaseEreport {
    pub id: CaseEreportUuid,
    pub ereport: Arc<Ereport>,
    pub assigned_sitrep_id: SitrepUuid,
    pub comment: String,
}

impl IdOrdItem for CaseEreport {
    type Key<'a> = <Arc<Ereport> as IdOrdItem>::Key<'a>;
    fn key(&self) -> Self::Key<'_> {
        self.ereport.key()
    }

    iddqd::id_upcast!();
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertRequest {
    pub id: AlertUuid,
    pub class: AlertClass,
    pub payload: serde_json::Value,
    pub requested_sitrep_id: SitrepUuid,
}

impl iddqd::IdOrdItem for AlertRequest {
    type Key<'a> = &'a AlertUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

struct DisplayCase<'a> {
    case: &'a Case,
    indent: usize,
    sitrep_id: Option<SitrepUuid>,
}

impl fmt::Display for DisplayCase<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const BULLET: &str = "* ";
        const fn const_max_len(strs: &[&str]) -> usize {
            let mut max = 0;
            let mut i = 0;
            while i < strs.len() {
                let len = strs[i].len();
                if len > max {
                    max = len;
                }
                i += 1;
            }
            max
        }

        let &Self {
            case:
                Case {
                    id,
                    created_sitrep_id,
                    closed_sitrep_id,
                    de,
                    ereports,
                    comment,
                    alerts_requested,
                },
            indent,
            sitrep_id,
        } = self;

        let this_sitrep = move |s| {
            if Some(s) == sitrep_id { " <-- this sitrep" } else { "" }
        };

        writeln!(
            f,
            "{:>indent$}case {id}",
            if indent > 0 { BULLET } else { "" }
        )?;
        writeln!(
            f,
            "{:>indent$}=========================================",
            ""
        )?;

        const DE: &str = "diagnosis engine:";
        const OPENED_IN: &str = "opened in sitrep:";
        const CLOSED_IN: &str = "closed in sitrep:";
        const WIDTH: usize = const_max_len(&[DE, OPENED_IN, CLOSED_IN]);
        writeln!(f, "{:>indent$}{DE:<WIDTH$} {de}", "")?;
        writeln!(
            f,
            "{:>indent$}{OPENED_IN:<WIDTH$} {created_sitrep_id}{}",
            "",
            this_sitrep(*created_sitrep_id)
        )?;
        if let Some(closed_id) = closed_sitrep_id {
            writeln!(
                f,
                "{:>indent$}{CLOSED_IN:<WIDTH$} {closed_id}{}",
                "",
                this_sitrep(*closed_id)
            )?;
        }

        writeln!(f, "\n{:>indent$}comment: {comment}", "")?;

        if !ereports.is_empty() {
            writeln!(f, "\n{:>indent$}ereports:", "")?;
            writeln!(f, "{:>indent$}---------", "")?;

            let indent = indent + 2;
            for CaseEreport { id, ereport, assigned_sitrep_id, comment } in
                ereports
            {
                const CLASS: &str = "class:";
                const REPORTED_BY: &str = "reported by:";
                const ADDED_IN: &str = "added in:";
                const ASSIGNMENT_ID: &str = "assignment ID:";
                const COMMENT: &str = "comment:";

                const WIDTH: usize = const_max_len(&[
                    CLASS,
                    REPORTED_BY,
                    ADDED_IN,
                    ASSIGNMENT_ID,
                    COMMENT,
                ]);

                let pn = ereport.part_number.as_deref().unwrap_or("<UNKNOWN>");
                let sn =
                    ereport.serial_number.as_deref().unwrap_or("<UNKNOWN>");
                writeln!(f, "{BULLET:>indent$}ereport {}", ereport.id())?;
                writeln!(
                    f,
                    "{:>indent$}{CLASS:<WIDTH$} {}",
                    "",
                    ereport.class.as_deref().unwrap_or("<NONE>")
                )?;
                writeln!(
                    f,
                    "{:>indent$}{REPORTED_BY:<WIDTH$} {pn:>11}:{sn:<11} ({})",
                    "", ereport.reporter
                )?;
                writeln!(
                    f,
                    "{:>indent$}{ADDED_IN:<WIDTH$} {assigned_sitrep_id}{}",
                    "",
                    this_sitrep(*assigned_sitrep_id)
                )?;
                writeln!(f, "{:>indent$}{ASSIGNMENT_ID:<WIDTH$} {id}", "")?;
                writeln!(f, "{:>indent$}{COMMENT:<WIDTH$} {comment}\n", "",)?;
            }
        }

        if !alerts_requested.is_empty() {
            writeln!(f, "\n{:>indent$}alerts requested:", "")?;
            writeln!(f, "{:>indent$}-----------------", "")?;

            let indent = indent + 2;
            for AlertRequest { id, class, payload: _, requested_sitrep_id } in
                alerts_requested.iter()
            {
                const CLASS: &str = "class:";
                const REQUESTED_IN: &str = "requested in:";

                const WIDTH: usize = const_max_len(&[CLASS, REQUESTED_IN]);

                writeln!(f, "{BULLET:>indent$}alert {id}",)?;
                writeln!(f, "{:>indent$}{CLASS:<WIDTH$} {class}", "",)?;
                writeln!(
                    f,
                    "{:>indent$}{REQUESTED_IN:<WIDTH$} {requested_sitrep_id}{}\n",
                    "",
                    this_sitrep(*requested_sitrep_id)
                )?;
            }
        }

        writeln!(f)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fm::DiagnosisEngineKind;
    use crate::inventory::SpType;
    use ereport_types::{Ena, EreportId};
    use omicron_uuid_kinds::{
        AlertUuid, CaseUuid, EreporterRestartUuid, OmicronZoneUuid, SitrepUuid,
    };
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_case_display() {
        // Create UUIDs for the case
        let case_id =
            CaseUuid::from_str("b0d36461-e3e7-4a53-9162-82414fb30088").unwrap();
        let created_sitrep_id =
            SitrepUuid::from_str("ea7affb0-36eb-4a9a-b9bd-22e00f1bcc04")
                .unwrap();
        let closed_sitrep_id =
            SitrepUuid::from_str("bba63ba7-bf6b-45f4-b241-d13ccd07fe1c")
                .unwrap();
        let restart_id = EreporterRestartUuid::from_str(
            "b633f40b-38c7-41f6-813b-61fc76381696",
        )
        .unwrap();
        let collector_id =
            OmicronZoneUuid::from_str("34f3b79e-168a-46f8-be49-4fe8eaff539a")
                .unwrap();
        let alert1_id =
            AlertUuid::from_str("7fe8c664-5b91-490f-b71b-b19e6e07ff3a")
                .unwrap();
        let alert2_id =
            AlertUuid::from_str("8a6f88ef-c436-44a9-b4cb-cae91d7306c9")
                .unwrap();

        // Create some ereports
        let mut ereports = IdOrdMap::new();
        let time_collected = chrono::DateTime::<chrono::Utc>::MIN_UTC;

        let ereport1 = CaseEreport {
            id: CaseEreportUuid::from_str(
                "89f650fd-c67c-4dcc-9acc-0ce02d43a62b",
            )
            .unwrap(),
            ereport: Arc::new(Ereport {
                data: crate::fm::ereport::EreportData {
                    id: EreportId { restart_id, ena: Ena::from(2u64) },
                    time_collected,
                    collector_id,
                    serial_number: Some("BRM6900420".to_string()),
                    part_number: Some("913-0000037".to_string()),
                    class: Some("hw.pwr.remove.psu".to_string()),
                    report: serde_json::json!({}),
                },
                reporter: crate::fm::ereport::Reporter::Sp {
                    sp_type: SpType::Power,
                    slot: 0,
                },
            }),
            assigned_sitrep_id: created_sitrep_id,
            comment: "PSU removed".to_string(),
        };
        ereports.insert_unique(ereport1).unwrap();

        let ereport2 = CaseEreport {
            id: CaseEreportUuid::from_str(
                "7b923ffc-f5fc-4001-acf4-1224dad7d3ef",
            )
            .unwrap(),
            ereport: Arc::new(Ereport {
                data: crate::fm::ereport::EreportData {
                    id: EreportId { restart_id, ena: Ena::from(3u64) },
                    time_collected,
                    collector_id,
                    serial_number: Some("BRM6900420".to_string()),
                    part_number: Some("913-0000037".to_string()),
                    class: Some("hw.pwr.insert.psu".to_string()),
                    report: serde_json::json!({}),
                },
                reporter: crate::fm::ereport::Reporter::Sp {
                    sp_type: SpType::Power,
                    slot: 0,
                },
            }),
            assigned_sitrep_id: closed_sitrep_id,
            comment: "PSU inserted, closing this case".to_string(),
        };
        ereports.insert_unique(ereport2).unwrap();

        let mut alerts_requested = IdOrdMap::new();
        alerts_requested
            .insert_unique(AlertRequest {
                id: alert1_id,
                class: AlertClass::TestFoo,
                payload: serde_json::json!({}),
                requested_sitrep_id: created_sitrep_id,
            })
            .unwrap();
        alerts_requested
            .insert_unique(AlertRequest {
                id: alert2_id,
                class: AlertClass::TestFooBar,
                payload: serde_json::json!({}),
                requested_sitrep_id: closed_sitrep_id,
            })
            .unwrap();

        // Create the case
        let case = Case {
            id: case_id,
            created_sitrep_id,
            closed_sitrep_id: Some(closed_sitrep_id),
            de: DiagnosisEngineKind::PowerShelf,
            ereports,
            alerts_requested,
            comment: "Power shelf rectifier added and removed here :-)"
                .to_string(),
        };

        eprintln!("example case display:");
        eprintln!("=====================\n");
        eprintln!("{case}");

        eprintln!("example case display (indented by 4):");
        eprintln!("=====================================\n");
        eprintln!("{}", case.display_indented(4, Some(closed_sitrep_id)));
    }
}
