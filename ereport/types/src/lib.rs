// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Core types for representing ereports.

use core::fmt;
pub use omicron_uuid_kinds::EreporterGenerationUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU32;
use uuid::Uuid;

/// An ereport message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Ereport {
    /// The ENA of the ereport.
    pub ena: Ena,
    /// The body of the ereport.
    pub report: ReportKind,
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
/// Uniquely identifies the entity that generated an ereport.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
pub struct Reporter {
    pub reporter_id: Uuid,
    pub gen_id: EreporterGenerationUuid,
}

/// The body of an ereport: either an event is reported, or a loss report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportKind {
    /// An ereport.
    Event(Event),
    /// Ereports were lost, or may have been lost.
    Loss(LossReport),
}

/// The number of ereports that were discarded, if it is known.
///
/// If ereports are dropped because a buffer has reached its capacity,
/// the reporter is strongly encouraged to attempt to count the number
/// of ereports lost. In other cases, such as a reporter crashing and
/// restarting, the reporter may not be capable of determining the
/// number of ereports that were lost, or even *if* data loss actually
/// occurred.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "lost")]
pub enum LossReport {
    /// An unknown number of ereports MAY have been lost.
    Unknown,
    /// The provided number of ereports are known to have been lost.
    Exact(u32),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Event {
    pub class: String,
    pub data: serde_json::Value,
}

/// Query parameters to request a tranche of ereports from a reporter.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EreportQuery {
    /// The generation (restart nonce) of the reporter at which all other query
    /// parameters are valid.
    ///
    /// If this value does not match the reporter's current generation, the
    /// reporter's response will include the current generation, and will start
    /// at the earliest known ENA, rather than the provided `last_seen` ENA.`
    pub generation: EreporterGenerationUuid,

    /// If present, the reporter should not include ENAs earlier than this one
    /// in its response, provided that the query's requested generation matches
    /// the current generation.
    pub start_at: Option<Ena>,

    /// The ENA of the last ereport committed to persistent storage from the
    /// requested reporter generation.
    ///
    /// If the generation parameter matches the reporter's current generation,
    /// it is permitted to discard any ereports with ENAs up to and including
    /// this value. If the generation has changed from the provided generation,
    /// the reporter will not discard data.
    pub committed: Option<Ena>,

    /// Maximum number of ereports to return in this tranche.
    pub limit: NonZeroU32,
}

/// A tranche of ereports received from a reporter.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Ereports {
    /// The reporter's current generation ID.
    ///
    /// If this is not equal to the current known generation, then the reporter
    /// has restarted.
    pub generation: EreporterGenerationUuid,
    /// The ereports in this tranche, and the ENA of the next page of ereports
    /// (if one exists).)
    #[serde(flatten)]
    pub reports: dropshot::ResultsPage<Ereport>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let ereport = Ereport {
            ena: Ena(0x3cae76440c100001),
            report: ReportKind::Event(Event {
                // Example ereport taken from https://rfd.shared.oxide.computer/rfd/0520#_ereports_in_the_fma
                class: "ereport.cpu.generic-x86.cache".to_string(),
                data: serde_json::json!({
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
                }),
            }),
        };
        let ereport_string = serde_json::to_string_pretty(&ereport)
            .expect("ereport should serialize");
        eprintln!("JSON: {ereport_string}");
        let deserialized = dbg!(serde_json::from_str(&ereport_string))
            .expect("ereport should deserialize");
        assert_eq!(ereport, deserialized)
    }
}
