// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SitrepBuilder;
use crate::analysis_input::Input;
use crate::ereport;
use crate::ereport::Ereport;
use anyhow::Context;
use iddqd::{BiHashItem, BiHashMap};
use nexus_types::fm;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::inventory;
use omicron_uuid_kinds::CaseUuid;
use serde::Deserialize;
use std::sync::Arc;

pub const PSU_REMOVE_EREPORT: &str = "hw.remove.psu";
pub const PSU_INSERT_EREPORT: &str = "hw.insert.psu";

pub fn analyze(
    builder: &mut SitrepBuilder<'_>,
) -> anyhow::Result<()> {
    let input = builder.input();

    // Okay so basically, here's what we do:
    // 1. index existing cases
    // 2. look at ereports and open/close/assign to case
    //
    // There's two kinds of cases, which are:
    // - has an ereport which indicates the rectifier was removed,
    // - has only a rectifier inserted ereport

    let parent_cases = input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::PowerShelf);
    for case in parent_cases {
        // Reconstruct the case by looking at its ereports:
        // - the ereports should all be associated with a single PSC at this
        //   point.
        // - put them in a map by PSC location
    }

    for ereport in input.new_ereports().iter() {
        // for each ereport that we haven't already seen before:
        // if it is a PSU insert or PSU remove event:
        // - see if there is an open case for the PSC and PSU slot named in that
        //   ereport
        //  - if so, assign the ereport to the case
        //  - if not, create a new case for the PSC and PSU slot named in that
        // ereport
    }

    // for each case:
    // - looking at the sequence of ereports based on their ENAs and timestamps,
    //   determine if the PSU is present or not.
    //   - if the PSU is present (i.e., the most recent ereport is an insert),
    //     close the case
    //   - if the PSU is not present (i.e., the most recent ereport is a
    //     remove), leave the case open
    //
    // - generate an alert for each PSU insert/remove ereport in the case that
    //   does not already have an alert for that event.
    //
    // We don't actually *need* to leave the case open, and *could* get away
    // with just making a case for every ereport and requesting an alert, but
    // doing it like this sets us up for being able to produce a "rectifier
    // missing" active problem for the open cases later.
    Ok(())
}

struct PsuCase {
    case_id: CaseUuid,
    location: PsuLocation,
}

#[derive(Debug)]
struct PsuEreport {
    location: PsuLocation,
    ereport: Arc<Ereport>,
    kind: PsuEreportKind,
    data: PsuEreportData,
}

impl PsuEreport {
    fn parse(ereport: &Arc<ereport::Ereport>) -> anyhow::Result<Self> {
        let kind = match ereport.data.class.as_deref() {
            Some(k) if k == PSU_INSERT_EREPORT => PsuEreportKind::Insert,
            Some(k) if k == PSU_REMOVE_EREPORT => PsuEreportKind::Remove,
            k => anyhow::bail!("unknown ereport class: {k:?}"),
        };
        let shelf = match ereport.reporter {
            ereport::Reporter::Sp {
                sp_type: inventory::SpType::Power,
                slot,
            } => u8::try_from(slot).with_context(|| {
                format!("power shelf slot number {slot} is way too big")
            })?,
            reporter => anyhow::bail!(
                "invalid reporter type for what seems to be a PSC ereport: \
                 {reporter:?}"
            ),
        };
        let data: PsuEreportData =
            serde_json::from_value(ereport.data.report.clone())
                .context("invalid data for a PSC ereport")?;
        let location = PsuLocation { shelf, slot: data.slot };
        Ok(Self { kind, data, location, ereport: ereport.clone() })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PsuEreportKind {
    Insert,
    Remove,
}

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
struct PsuEreportData {
    fruid: Option<PsuFruid>,
    rail: String,
    slot: u8,
    refdes: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct PsuLocation {
    shelf: u8,
    slot: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct PsuFruid {
    fw_rev: String,
    mfr: String,
    mpn: String,
    serial: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::FmTest;
    use chrono::Utc;
    use nexus_types::inventory::SpType;

    // These are real life ereports I copied from the dogfood rack.
    mod ereports {
        use super::*;

        pub(super) const PSU_REMOVE_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197337481,
            "k": "hw.remove.psu",
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;

        pub(super) const PSU_INSERT_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197337481,
            "k": "hw.insert.psu",
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;

        pub(super) const PSU_PWR_BAD_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197408566,
            "k": "hw.pwr.pwr_good.bad",
            "pmbus_status": {
                "cml": 0,
                "input": 48,
                "iout": 0,
                "mfr": 0,
                "temp": 0,
                "vout": 0,
                "word": 10312
            },
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;

        pub(super) const PSU_PWR_GOOD_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197408580,
            "k": "hw.pwr.pwr_good.good",
            "pmbus_status": {
                "cml": 0,
                "input": 0,
                "iout": 0,
                "mfr": 0,
                "temp": 0,
                "vout": 0,
                "word": 0
            },
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;
    }

    fn test_dogfood_ereport_parses(
        test_name: &str,
        expected_class: PsuEreportKind,
        json: &str,
    ) {
        const SHELF: u16 = 0;
        let (mut fmtest, logctx) = FmTest::new_with_logctx(test_name);
        let mut reporter = fmtest.reporters.reporter(ereport::Reporter::Sp {
            sp_type: SpType::Power,
            slot: SHELF,
        });
        let ereport = dbg!(Arc::new(reporter.parse_ereport(Utc::now(), json)));
        let parsed = dbg!(PsuEreport::parse(&ereport))
            .expect("dogfood ereport should parse as a PsuEreport");

        // The payload fields shared by every dogfood ereport above; all of them
        // describe PSU4 on rail V54_PSU4.
        let expected_data = PsuEreportData {
            fruid: Some(PsuFruid {
                fw_rev: "0701".to_string(),
                mfr: "Murata-PS".to_string(),
                mpn: "MWOCP68-3600-D-RM".to_string(),
                serial: "LL2216RB003Z".to_string(),
            }),
            rail: "V54_PSU4".to_string(),
            slot: 4,
            refdes: "PSU4".to_string(),
        };

        assert_eq!(parsed.kind, expected_class);
        assert_eq!(
            parsed.location,
            PsuLocation { shelf: SHELF as u8, slot: 4 }
        );
        assert_eq!(parsed.data, expected_data);
        logctx.cleanup_successful();
    }

    #[test]
    fn test_psu_remove_json_parses() {
        test_dogfood_ereport_parses(
            "test_psu_remove_json_parses",
            PsuEreportKind::Remove,
            ereports::PSU_REMOVE_JSON,
        );
    }

    #[test]
    fn test_psu_insert_json_parses() {
        test_dogfood_ereport_parses(
            "test_psu_insert_json_parses",
            PsuEreportKind::Insert,
            ereports::PSU_INSERT_JSON,
        );
    }
}
