// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ereport analysis tools.

use nexus_types::fm as types;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// Metadata that should be present in *all* hubris ereports.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize)]
pub(crate) struct HubrisMetadata {
    pub hubris_archive_id: String,
    pub hubris_task_gen: u16,
    pub hubris_task_name: String,
    pub hubris_uptime_ms: u64,
    // Added by MGS
    pub ereport_message_version: u8,

    #[serde(rename = "v")]
    pub version: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize)]
pub(crate) struct Baseboard {
    #[serde(rename = "baseboard_part_number")]
    pub part_number: String,
    #[serde(rename = "baseboard_rev")]
    pub rev: u32,
    #[serde(rename = "serial_number")]
    pub serial_number: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedEreport<D> {
    pub ereport: Arc<types::Ereport>,
    pub hubris_metadata: Option<HubrisMetadata>,
    pub baseboard: Option<Baseboard>,
    pub report: D,
}

impl<D: DeserializeOwned> ParsedEreport<D> {
    pub(crate) fn from_raw(
        ereport: &Arc<types::Ereport>,
    ) -> Result<Self, serde_json::Error> {
        let fields: EreportFields<D> =
            serde_json::from_value(ereport.data.report.clone())?;
        let EreportFields { hubris_metadata, baseboard, report } = fields;
        Ok(Self {
            ereport: ereport.clone(),
            hubris_metadata,
            baseboard,
            report,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
struct EreportFields<D> {
    #[serde(flatten)]
    hubris_metadata: Option<HubrisMetadata>,
    #[serde(flatten)]
    baseboard: Option<Baseboard>,
    #[serde(flatten)]
    report: D,
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    // These are real life ereports I copied from the dogfood rack.
    pub(crate) const PSU_REMOVE_JSON: &str = r#"{
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

    pub(crate) const PSU_INSERT_JSON: &str = r#"{
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

    pub(crate) const PSU_PWR_BAD_JSON: &str = r#"{
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

    pub(crate) const PSU_PWR_GOOD_JSON: &str = r#"{
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

    #[test]
    fn test_hubris_metadata() {
        let expected_metadata = HubrisMetadata {
            hubris_archive_id: "qSm4IUtvQe0".to_string(),
            hubris_task_gen: 0,
            hubris_task_name: "sequencer".to_string(),
            hubris_uptime_ms: 0,
            ereport_message_version: 0,
            version: Some(0),
        };
        let ereports = [
            (PSU_REMOVE_JSON, 1197337481),
            (PSU_INSERT_JSON, 1197337481),
            (PSU_PWR_BAD_JSON, 1197408566),
            (PSU_PWR_GOOD_JSON, 1197408580),
        ];

        for (json, hubris_uptime_ms) in ereports {
            let json_value: serde_json::Value =
                serde_json::from_str(json).expect("JSON should parse");
            let metadata: HubrisMetadata =
                serde_json::from_value(dbg!(json_value))
                    .expect("value should contain a HubrisMetadata");
            assert_eq!(
                metadata,
                HubrisMetadata {
                    hubris_uptime_ms,
                    ..expected_metadata.clone()
                }
            );
        }
    }
}
