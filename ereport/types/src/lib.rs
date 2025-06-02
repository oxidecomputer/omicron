// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Core types for representing ereports.

use core::fmt;
pub use omicron_uuid_kinds::EreporterRestartUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU32;

/// An ereport message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Ereport {
    /// The ENA of the ereport.
    pub ena: Ena,
    #[serde(flatten)]
    pub data: serde_json::Map<String, serde_json::Value>,
}

/// An Error Numeric Association (ENA)
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[repr(transparent)]
pub struct Ena(pub u64);

impl fmt::Display for Ena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

impl fmt::Debug for Ena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ena({:#x})", self.0)
    }
}

impl fmt::UpperHex for Ena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for Ena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

/// Query parameters to request a tranche of ereports from a reporter.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EreportQuery {
    /// The restart ID of the reporter at which all other query parameters are
    /// valid.
    ///
    /// If this value does not match the reporter's restart ID, the
    /// reporter's response will include the current generation, and will start
    /// at the earliest known ENA, rather than the provided `start_at` ENA.
    pub restart_id: EreporterRestartUuid,

    /// If present, the reporter should not include ENAs earlier than this one
    /// in its response, provided that the query's requested restart ID matches
    /// the current restart ID.
    pub start_at: Option<Ena>,

    /// The ENA of the last ereport committed to persistent storage from the
    /// requested reporter restart generation.
    ///
    /// If the restart ID parameter matches the reporter's current restart ID,
    /// it is permitted to discard any ereports with ENAs up to and including
    /// this value. If the restart ID has changed from the provided one, the
    /// reporter will not discard data.
    pub committed: Option<Ena>,

    /// Maximum number of ereports to return in this tranche.
    pub limit: NonZeroU32,
}

/// A tranche of ereports received from a reporter.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Ereports {
    /// The reporter's current restart ID.
    ///
    /// If this is not equal to the current known restart ID, then the reporter
    /// has restarted.
    pub restart_id: EreporterRestartUuid,
    /// The ereports in this tranche, and the ENA of the next page of ereports
    /// (if one exists).)
    #[serde(flatten)]
    pub reports: dropshot::ResultsPage<Ereport>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arb_json_deserializes() {
        let ereport = serde_json::json!({
            "ena": Ena(0x3cae76440c100001),
            "version": 0x0,
            "class": "list.suspect",
            "uuid": "0348743e-0600-4c77-b7ea-6eda191536e4",
            "code": "FMD-8000-0W",
            "diag-time": "1705014884 472900",
            "de": {
                "version": 0x0,
                "scheme": "fmd",
                "authority": {
                    "version": 0x0,
                    "product-id": "oxide",
                    "server-id": "BRM42220016",
                },
                "mod-name": "fmd-self-diagnosis",
                "mod-version": "1.0",
            },
            "fault-list": [
                {
                    "version": 0x0,
                    "class": "defect.sunos.fmd.nosub",
                    "certainty": 0x64,
                    "nosub_class": "ereport.cpu.generic-x86.cache",
                }
            ],
            "fault-status": 0x1,
            "severity": "minor",
        });
        let ereport_string = serde_json::to_string_pretty(&ereport)
            .expect("ereport should serialize");
        eprintln!("JSON: {ereport_string}");
        let deserialized =
            dbg!(serde_json::from_str::<Ereport>(&ereport_string))
                .expect("ereport should deserialize");
        eprintln!("EREPORT: {deserialized:#?}");
    }
}
