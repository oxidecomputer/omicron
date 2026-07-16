// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to uplinkd in the switch zone.

use indent_write::fmt::IndentWriter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum UplinkdReconcilerStatus {
    Failed(String),
    SkippedConfigUpToDate,
    Reconciled { ports: BTreeMap<String, Vec<String>> },
}

impl slog::KV for UplinkdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            UplinkdReconcilerStatus::Failed(reason) => {
                serializer.emit_str("uplinkd".into(), &reason)
            }
            UplinkdReconcilerStatus::SkippedConfigUpToDate => serializer
                .emit_str("uplinkd".into(), "skipped: config up-to-date"),
            UplinkdReconcilerStatus::Reconciled { ports } => serializer
                .emit_usize("uplinkd-reconciled-ports".into(), ports.len()),
        }
    }
}

impl super::DisplayableStatus for UplinkdReconcilerStatus {
    type DisplayAdapter<'a>
        = UplinkdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    /// Get a `fmt::Display`-able version of this status (e.g., for `omdb`).
    fn display(&self) -> UplinkdReconcilerStatusDisplay<'_> {
        UplinkdReconcilerStatusDisplay(self)
    }
}

pub struct UplinkdReconcilerStatusDisplay<'a>(&'a UplinkdReconcilerStatus);

impl fmt::Display for UplinkdReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            UplinkdReconcilerStatus::Failed(reason) => {
                writeln!(f, "reconciliation failed: {reason}")
            }
            UplinkdReconcilerStatus::SkippedConfigUpToDate => {
                writeln!(f, "reconciliation skipped: config is up to date")
            }
            UplinkdReconcilerStatus::Reconciled { ports } => {
                if ports.is_empty() {
                    writeln!(f, "reconciliation skipped: no ports")
                } else {
                    writeln!(
                        f,
                        "successfully reconciled {} ports:",
                        ports.len()
                    )?;
                    let mut w = IndentWriter::new("    ", f);
                    for (port, values) in ports.iter() {
                        writeln!(w, "* {port}: {}", values.join(", "))?;
                    }
                    Ok(())
                }
            }
        }
    }
}
