// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::fm::AlertRequest;
use crate::fm::DiagnosisEngine;
use crate::fm::Ereport;
use crate::inventory::SpType;
use chrono::{DateTime, Utc};
use iddqd::{IdOrdItem, IdOrdMap};
use omicron_uuid_kinds::{CaseUuid, SitrepUuid};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Case {
    pub id: CaseUuid,
    pub created_sitrep_id: SitrepUuid,
    pub time_created: DateTime<Utc>,

    pub closed_sitrep_id: Option<SitrepUuid>,
    pub time_closed: Option<DateTime<Utc>>,

    pub de: DiagnosisEngine,

    pub ereports: IdOrdMap<CaseEreport>,
    pub alerts_requested: IdOrdMap<AlertRequest>,
    pub impacted_sp_slots: IdOrdMap<ImpactedSpSlot>,

    pub comment: String,
}

impl Case {
    pub fn is_open(&self) -> bool {
        self.time_closed.is_none()
    }

    pub fn display_indented(&self, indent: usize) -> impl fmt::Display + '_ {
        DisplayCase { case: self, indent }
    }
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_indented(0).fmt(f)
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

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ImpactedSpSlot {
    pub sp_type: SpType,
    pub slot: u8,
    pub created_sitrep_id: SitrepUuid,
    pub comment: String,
}

impl IdOrdItem for ImpactedSpSlot {
    type Key<'a> = (&'a SpType, &'a u8);
    fn key(&self) -> Self::Key<'_> {
        (&self.sp_type, &self.slot)
    }

    iddqd::id_upcast!();
}

struct DisplayCase<'a> {
    case: &'a Case,
    indent: usize,
}

impl fmt::Display for DisplayCase<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const BULLET: &str = "* ";
        const LIST_INDENT: usize = 4;

        let &Self {
            case:
                Case {
                    ref id,
                    ref created_sitrep_id,
                    ref time_created,
                    ref closed_sitrep_id,
                    ref time_closed,
                    ref de,
                    ref ereports,
                    ref alerts_requested,
                    ref impacted_sp_slots,
                    ref comment,
                },
            indent,
        } = self;
        writeln!(
            f,
            "{:>indent$}case: {id:?}",
            if indent > 0 { BULLET } else { "" }
        )?;
        writeln!(f, "{:>indent$}comment: {comment}", "")?;
        writeln!(f, "{:>indent$}diagnosis engine: {de}", "")?;
        writeln!(f, "{:>indent$}created in sitrep: {created_sitrep_id}", "")?;
        writeln!(f, "{:>indent$}  at: {time_created}", "")?;
        if let Some(closed_id) = closed_sitrep_id {
            writeln!(f, "{:>indent$}closed in sitrep: {closed_id}", "")?;
            if let Some(time_closed) = time_closed {
                writeln!(f, "{:>indent$}  at: {time_closed}", "")?;
            } else {
                writeln!(f, "{:>indent$}  at: <MISSING TIME CLOSED>", "")?;
            }
        }

        if !ereports.is_empty() {
            writeln!(f, "\n{:>indent$}ereports:", "")?;
            let indent = indent + LIST_INDENT;
            for CaseEreport { ereport, assigned_sitrep_id, comment } in ereports
            {
                writeln!(f, "{BULLET:>indent$}{}", ereport.id())?;
                writeln!(f, "{:>indent$}class: {:?}", "", ereport.class)?;
                writeln!(f, "{:>indent$}reporter: {}", "", ereport.reporter)?;
                writeln!(
                    f,
                    "{:>indent$}added in sitrep: {assigned_sitrep_id}",
                    ""
                )?;
                writeln!(f, "{:>indent$}comment: {comment}", "")?;
            }
        }

        if !impacted_sp_slots.is_empty() {
            writeln!(f, "\n{:>indent$}SP slots impacted:", "")?;
            let indent = indent + LIST_INDENT;
            for ImpactedSpSlot { sp_type, slot, created_sitrep_id, comment } in
                impacted_sp_slots
            {
                writeln!(f, "{BULLET:>indent$}{sp_type:<6} {slot:02}")?;
                writeln!(
                    f,
                    "{:>indent$}added in sitrep: {created_sitrep_id}",
                    ""
                )?;
                writeln!(f, "{:>indent$}comment: {comment}", "")?;
            }
        }

        if !alerts_requested.is_empty() {
            writeln!(f, "\n{:>indent$}alerts requested:", "")?;
            let indent = indent + LIST_INDENT;
            for AlertRequest { id, class, requested_sitrep_id, .. } in
                alerts_requested
            {
                writeln!(f, "{BULLET:>indent$}{id:?}")?;
                writeln!(f, "{:>indent$}class: {class:?}", "")?;
                writeln!(
                    f,
                    "{:>indent$}requested in sitrep: {requested_sitrep_id}",
                    ""
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
    use crate::fm::{AlertClass, AlertRequest, DiagnosisEngine};
    use chrono::Utc;
    use ereport_types::{Ena, EreportId};
    use omicron_uuid_kinds::{
        AlertUuid, CaseUuid, EreporterRestartUuid, OmicronZoneUuid, SitrepUuid,
    };
    use std::sync::Arc;

    #[test]
    fn test_case_display() {
        // Create UUIDs for the case
        let case_id = CaseUuid::new_v4();
        let created_sitrep_id = SitrepUuid::new_v4();
        let closed_sitrep_id = SitrepUuid::new_v4();
        let time_created = Utc::now();
        let time_closed = Utc::now();

        // Create some ereports
        let mut ereports = IdOrdMap::new();

        let ereport1 = CaseEreport {
            ereport: Arc::new(Ereport {
                data: crate::fm::ereport::EreportData {
                    id: EreportId {
                        restart_id: EreporterRestartUuid::new_v4(),
                        ena: Ena::from(2u64),
                    },
                    time_collected: time_created,
                    collector_id: OmicronZoneUuid::new_v4(),
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
            ereport: Arc::new(Ereport {
                data: crate::fm::ereport::EreportData {
                    id: EreportId {
                        restart_id: EreporterRestartUuid::new_v4(),
                        ena: Ena::from(3u64),
                    },
                    time_collected: time_created,
                    collector_id: OmicronZoneUuid::new_v4(),
                    serial_number: Some("BRM6900420".to_string()),
                    part_number: Some("913-0000037".to_string()),
                    class: Some("hw.pwr.insert.psu".to_string()),
                    report: serde_json::json!({"link": "eth0", "status": "down"}),
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

        // Create some alerts
        let mut alerts_requested = IdOrdMap::new();

        let alert1 = AlertRequest {
            id: AlertUuid::new_v4(),
            class: AlertClass::PsuRemoved,
            payload: serde_json::json!({}),
            requested_sitrep_id: created_sitrep_id,
        };
        alerts_requested.insert_unique(alert1).unwrap();

        let alert2 = AlertRequest {
            id: AlertUuid::new_v4(),
            class: AlertClass::PsuInserted,
            payload: serde_json::json!({}),
            requested_sitrep_id: closed_sitrep_id,
        };
        alerts_requested.insert_unique(alert2).unwrap();

        let mut impacted_sp_slots = IdOrdMap::new();
        let slot2 = ImpactedSpSlot {
            sp_type: SpType::Power,
            slot: 0,
            created_sitrep_id,
            comment: "Power shelf 0 reduced redundancy".to_string(),
        };
        impacted_sp_slots.insert_unique(slot2).unwrap();

        // Create the case
        let case = Case {
            id: case_id,
            created_sitrep_id,
            time_created,
            closed_sitrep_id: Some(closed_sitrep_id),
            time_closed: Some(time_closed),
            de: DiagnosisEngine::PowerShelf,
            ereports,
            alerts_requested,
            impacted_sp_slots,
            comment: "Power shelf rectifier added and removed here :-)"
                .to_string(),
        };

        eprintln!("example case display:");
        eprintln!("=====================");
        eprintln!("{case}");

        eprintln!("example case display (indented by 4):");
        eprintln!("======================================");
        eprintln!("{}", case.display_indented(4));
    }
}
