// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics types for Sled Agent API `ADD_LOG_TIME_RANGE`.

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::v1;

/// Query parameters for sled-diagnostics log download requests.
///
/// `max_rotated` becomes optional in this version: callers using a
/// time-range bound typically don't want a count cap on top.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadQueryParam {
    /// The max number of rotated logs to include in the final support
    /// bundle. If absent, no count cap is applied.
    #[serde(default)]
    pub max_rotated: Option<usize>,

    /// Lower bound (inclusive) on log file `mtime`. If absent, no
    /// lower bound is applied.
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,

    /// Upper bound (inclusive) on log file `mtime`. If absent, no
    /// upper bound is applied.
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
}

impl From<v1::diagnostics::SledDiagnosticsLogsDownloadQueryParam>
    for SledDiagnosticsLogsDownloadQueryParam
{
    fn from(
        old: v1::diagnostics::SledDiagnosticsLogsDownloadQueryParam,
    ) -> Self {
        Self {
            max_rotated: Some(old.max_rotated),
            start_time: None,
            end_time: None,
        }
    }
}
