// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics types for Sled Agent API v1.

use schemars::JsonSchema;
use serde::Deserialize;

/// Path parameters for sled-diagnostics log requests used by support bundles.
// NOTE: The original type had a typo (Parm vs Param). We keep both for compat.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadPathParam {
    /// The zone for which one would like to collect logs for
    pub zone: String,
}

/// Type alias for backward compatibility with the original typo.
pub type SledDiagnosticsLogsDownloadPathParm =
    SledDiagnosticsLogsDownloadPathParam;

/// Query parameters for sled-diagnostics log download requests.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadQueryParam {
    /// The max number of rotated logs to include in the final support bundle
    pub max_rotated: usize,
}
