// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ereports

use crate::inventory::SpType;
use chrono::{DateTime, Utc};
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

/// Describes the source of an ereport.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(tag = "reporter")]
pub enum Reporter {
    Sp { sp_type: SpType, slot: u16 },
    HostOs { sled: SledUuid },
}

impl fmt::Display for Reporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sp { sp_type: SpType::Sled, slot } => {
                write!(f, "Sled (SP) {slot:02}")
            }
            Self::Sp { sp_type: SpType::Switch, slot } => {
                write!(f, "Switch {slot}")
            }
            Self::Sp { sp_type: SpType::Power, slot } => {
                write!(f, "PSC {slot}")
            }
            Self::HostOs { sled } => {
                write!(f, "Sled (OS) {sled:?}")
            }
        }
    }
}
