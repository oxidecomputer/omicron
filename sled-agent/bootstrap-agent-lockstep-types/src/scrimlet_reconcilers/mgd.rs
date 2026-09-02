// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconciler responsible for
//! syncing configuration from the bootstore to mgd in the switch zone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Status of the `mgd` scrimlet reconciler.
///
/// mgd is configured declaratively: each reconciliation sends one
/// `multi_router_apply` request carrying the complete desired router list
/// for this switch, so the status reflects that single request.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdReconcilerStatus {
    /// Reconciliation was skipped because we couldn't render a desired
    /// router set from the bootstore contents.
    ///
    /// This should never happen - it indicates there's faulty data in the
    /// persisted config.
    FailedGeneratingDesiredConfig(String),

    /// The apply request to mgd failed.
    FailedApplying { routers: usize, error: String },

    /// The complete desired router list was applied.
    Success { routers: usize },
}

impl slog::KV for MgdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            Self::FailedGeneratingDesiredConfig(reason) => {
                serializer.emit_str("mgd-apply-skipped".into(), reason)
            }
            Self::FailedApplying { routers, error } => {
                serializer.emit_usize("mgd-routers-desired".into(), *routers)?;
                serializer.emit_str("mgd-apply-error".into(), error)
            }
            Self::Success { routers } => {
                serializer.emit_usize("mgd-routers-applied".into(), *routers)
            }
        }
    }
}
