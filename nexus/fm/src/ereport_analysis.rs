// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ereport analysis tools.

/// Metadata that should be present in *all* hubris ereports.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Deserialize)]
pub(crate) struct Baseboard {
    #[serde(rename = "baseboard_part_number")]
    pub part_number: String,
    #[serde(rename = "baseboard_rev")]
    pub rev: u32,
    #[serde(rename = "serial_number")]
    pub serial_number: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Deserialize)]
pub(crate) struct ParsedEreport<D> {
    #[serde(flatten)]
    pub hubris_metadata: Option<HubrisMetadata>,
    #[serde(flatten)]
    pub baseboard: Option<Baseboard>,
    #[serde(flatten)]
    pub report: D,
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use nexus_types::fm::ereport::{
        Ena, Ereport, EreportData, EreportId, Reporter,
    };
    use omicron_uuid_kinds::{EreporterRestartUuid, OmicronZoneUuid};

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
        "k": "hw.remove.psu",
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

    pub(crate) struct SimReporter {
        reporter: Reporter,
        restart_id: EreporterRestartUuid,
        ena: Ena,
    }

    impl SimReporter {
        pub(crate) fn new(
            reporter: Reporter,
            restart_id: EreporterRestartUuid,
        ) -> Self {
            Self { reporter, restart_id, ena: Ena(0x1) }
        }

        #[track_caller]
        pub(crate) fn parse_ereport(&mut self, json: &str) -> Ereport {
            self.mk_ereport(
                json.parse().expect("must be called with valid ereport JSON"),
            )
        }

        pub(crate) fn mk_ereport(
            &mut self,
            json: serde_json::Value,
        ) -> Ereport {
            self.ena.0 += 1;
            mk_ereport(
                self.reporter,
                EreportId { ena: self.ena, restart_id: self.restart_id },
                json,
            )
        }

        pub(crate) fn restart(&mut self, restart_id: EreporterRestartUuid) {
            self.ena = Ena(0x1);
            self.restart_id = restart_id;
        }
    }

    pub(crate) fn mk_ereport(
        reporter: Reporter,
        id: EreportId,
        json: serde_json::Value,
    ) -> Ereport {
        Ereport {
            reporter,
            data: EreportData {
                id,
                collector_id: OmicronZoneUuid::new_v4(), // just make something up...
                time_collected: chrono::Utc::now(),
                class: json["class"]
                    .as_str()
                    .or_else(|| json["k"].as_str())
                    .map(ToOwned::to_owned),
                serial_number: json["serial_number"]
                    .as_str()
                    .map(ToOwned::to_owned),
                part_number: json["part_number"]
                    .as_str()
                    .map(ToOwned::to_owned),
                report: json,
            },
        }
    }

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
