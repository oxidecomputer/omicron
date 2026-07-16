// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to lldpd in the switch zone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::LldpAdminStatus;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
