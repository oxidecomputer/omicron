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
        // Display format based on:
        // https://rfd.shared.oxide.computer/rfd/200#_labeling
        match self {
            Self::Sp { sp_type: sp_type @ SpType::Sled, slot } => {
                write!(f, "{sp_type} {slot:<2} (SP)")
            }
            Self::HostOs { sled } => {
                write!(f, "{} {sled:?} (OS)", SpType::Sled)
            }
            Self::Sp { sp_type, slot } => {
                write!(f, "{sp_type} {slot}")
            }
        }
    }
}
