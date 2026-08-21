// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconciler responsible for
//! syncing configuration from the bootstore to ddmd in the switch zone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Status of the `ddmd` scrimlet reconciler.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum DdmdReconcilerStatus {
    Failed(String),
    Reconciled { interfaces: BTreeSet<String> },
}

impl slog::KV for DdmdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            DdmdReconcilerStatus::Failed(reason) => {
                serializer.emit_str("ddmd".into(), reason)
            }
            DdmdReconcilerStatus::Reconciled { interfaces } => serializer
                .emit_usize(
                    "ddmd-reconciled-interfaces".into(),
                    interfaces.len(),
                ),
        }
    }
}
