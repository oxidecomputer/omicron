// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to lldpd in the switch zone.

use indent_write::fmt::IndentWriter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::LldpAdminStatus;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum LldpdReconcilerStatus {
    Failed(String),
    SkippedConfigUpToDate,
    Reconciled { ports: BTreeMap<String, LldpAdminStatus> },
}

impl slog::KV for LldpdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            LldpdReconcilerStatus::Failed(reason) => {
                serializer.emit_str("lldpd".into(), reason)
            }
            LldpdReconcilerStatus::SkippedConfigUpToDate => serializer
                .emit_str("lldpd".into(), "skipped: config up-to-date"),
            LldpdReconcilerStatus::Reconciled { ports } => serializer
                .emit_usize("lldpd-reconciled-ports".into(), ports.len()),
        }
    }
}

impl super::DisplayableStatus for LldpdReconcilerStatus {
    type DisplayAdapter<'a>
        = LldpdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    /// Get a `fmt::Display`-able version of this status (e.g., for `omdb`).
    fn display(&self) -> LldpdReconcilerStatusDisplay<'_> {
        LldpdReconcilerStatusDisplay(self)
    }
}

pub struct LldpdReconcilerStatusDisplay<'a>(&'a LldpdReconcilerStatus);

impl fmt::Display for LldpdReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            LldpdReconcilerStatus::Failed(reason) => {
                writeln!(f, "reconciliation failed: {reason}")
            }
            LldpdReconcilerStatus::SkippedConfigUpToDate => {
                writeln!(f, "reconciliation skipped: config is up to date")
            }
            LldpdReconcilerStatus::Reconciled { ports } => {
                if ports.is_empty() {
                    writeln!(f, "reconciliation skipped: no ports")
                } else {
                    writeln!(
                        f,
                        "successfully reconciled {} ports:",
                        ports.len()
                    )?;
                    let mut w = IndentWriter::new("    ", f);
                    for (port, status) in ports.iter() {
                        writeln!(w, "* {port}: {status:?}")?;
                    }
                    Ok(())
                }
            }
        }
    }
}
