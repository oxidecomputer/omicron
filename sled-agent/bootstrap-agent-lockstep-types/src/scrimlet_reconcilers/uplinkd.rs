// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to uplinkd in the switch zone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

