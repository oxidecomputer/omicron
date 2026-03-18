// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ereports

use crate::inventory::SpType;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

pub use ereport_types::{Ena, EreportId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ereport {
    #[serde(flatten)]
    pub data: EreportData,
    #[serde(flatten)]
    pub reporter: Reporter,
}

impl Ereport {
    pub fn id(&self) -> &EreportId {
        &self.data.id
    }
}

impl core::ops::Deref for Ereport {
    type Target = EreportData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl iddqd::IdOrdItem for Ereport {
    type Key<'a> = &'a EreportId;
    fn key(&self) -> Self::Key<'_> {
        self.id()
    }

    iddqd::id_upcast!();
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EreportData {
    #[serde(flatten)]
    pub id: EreportId,
    pub time_collected: DateTime<Utc>,
    pub collector_id: OmicronZoneUuid,
    pub serial_number: Option<String>,
    pub part_number: Option<String>,
    pub class: Option<String>,
    #[serde(flatten)]
    pub report: serde_json::Value,
}

impl EreportData {
    /// Interpret a service processor ereport from a raw JSON blobule, plus the
    /// restart ID and collection metadata.
    ///
    /// This conversion is lossy; if some information is not present in the raw
    /// ereport JSON, such as the SP's VPD identity, we log a warning, rather
    /// than returning an error, and leave those fields empty in the returned
    /// `EreportData`. This is because, if we receive an ereport that is
    /// incomplete, we would still like to preserve whatever information *is*
    /// present, rather than throwing the whole thing away. Thus, this function
    /// also takes a `slog::Logger` for logging warnings if some expected fields
    /// are not present or malformed.
    pub fn from_sp_ereport(
        log: &slog::Logger,
        restart_id: EreporterRestartUuid,
        ereport: ereport_types::Ereport,
        time_collected: DateTime<Utc>,
        collector_id: OmicronZoneUuid,
    ) -> Self {
        const MISSING_VPD: &str = " (perhaps the SP doesn't know its own VPD?)";
        let part_number = get_sp_metadata_string(
            "baseboard_part_number",
            &ereport,
            &restart_id,
            &log,
            MISSING_VPD,
        );
        let serial_number = get_sp_metadata_string(
            "baseboard_serial_number",
            &ereport,
            &restart_id,
            &log,
            MISSING_VPD,
        );
        let ena = ereport.ena;
        let class = ereport
            .data
            // "k" (for "kind") is used as an abbreviation of
            // "class" to save 4 bytes of ereport.
            .get("k")
            .or_else(|| ereport.data.get("class"));
        let class = match (class, ereport.data.get("lost")) {
            (Some(serde_json::Value::String(class)), _) => {
                Some(class.to_string())
            }
            (Some(v), _) => {
                slog::warn!(
                    &log,
                    "malformed ereport: value for 'k'/'class' \
                     should be a string, but found: {v:?}";
                    "ena" => ?ena,
                    "restart_id" => ?restart_id,
                );
                None
            }
            // This is a loss record! I know this!
            (None, Some(serde_json::Value::Null)) => {
                Some("ereport.data_loss.possible".to_string())
            }
            (None, Some(serde_json::Value::Number(_))) => {
                Some("ereport.data_loss.certain".to_string())
            }
            (None, _) => {
                slog::warn!(
                    &log,
                    "ereport missing 'k'/'class' key";
                    "ena" => ?ena,
                    "restart_id" => ?restart_id,
                );
                None
            }
        };

        EreportData {
            id: EreportId { restart_id, ena },
            time_collected,
            collector_id,
            part_number,
            serial_number,
            class,
            report: serde_json::Value::Object(ereport.data),
        }
    }
}

/// Describes the source of an ereport.
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Hash,
)]
pub struct Reporter {
    /// The type of slot occupied by the reporter.
    pub slot_type: SpType,
    /// The slot number of the reporter.
    pub slot: u16,
    /// Whether this reporter is a service processor or the host OS.
    pub kind: ReporterKind,
}

/// Whether an ereport reporter is a service processor or the host OS.
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Hash,
)]
pub enum ReporterKind {
    /// The reporter is a service processor.
    Sp,
    /// The reporter is the host OS on a sled.
    HostOs { sled: SledUuid },
}

impl fmt::Display for Reporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display format based on:
        // https://rfd.shared.oxide.computer/rfd/200#_labeling
        let Self { slot_type, slot, kind } = self;
        match kind {
            ReporterKind::Sp if *slot_type == SpType::Sled => {
                write!(f, "{slot_type} {slot:<2} (SP)")
            }
            ReporterKind::HostOs { sled } => {
                write!(f, "{slot_type} {slot:<2} (OS, sled {sled})")
            }
            ReporterKind::Sp => {
                write!(f, "{slot_type} {slot}")
            }
        }
    }
}

/// Attempt to extract a VPD metadata from an SP ereport, logging a warning if
/// it's missing. We still want to keep such ereports, as the error condition
/// could be that the SP couldn't determine the metadata field, but it's
/// uncomfortable, so we ought to complain about it.
fn get_sp_metadata_string(
    key: &str,
    ereport: &ereport_types::Ereport,
    restart_id: &EreporterRestartUuid,
    log: &slog::Logger,
    extra_context: &'static str,
) -> Option<String> {
    match ereport.data.get(key) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(v) => {
            slog::warn!(
                &log,
                "malformed ereport: value for '{key}' should be a string, \
                 but found: {v:?}";
                "ena" => ?ereport.ena,
                "restart_id" => ?restart_id,
            );
            None
        }
        None => {
            slog::warn!(
                &log,
                "ereport missing '{key}'{extra_context}";
                "ena" => ?ereport.ena,
                "restart_id" => ?restart_id,
            );
            None
        }
    }
}
