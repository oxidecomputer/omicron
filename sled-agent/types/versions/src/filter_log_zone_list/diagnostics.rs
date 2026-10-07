// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics types for Sled Agent API `FILTER_LOG_ZONE_LIST`.

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;

/// Query parameters for sled-diagnostics zone-listing requests.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogZonesQueryParam {
    /// Lower bound (inclusive) on log content. A zone is omitted when the
    /// last modification (`mtime`) of every one of its log files predates
    /// this; a file's `mtime` is the timestamp of its newest content, so an
    /// omitted zone has no log data from the window. If absent, no lower
    /// bound is applied.
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,

    /// Upper bound (inclusive) on log content. A zone is omitted when every
    /// one of its log files has content beginning after this. If absent, no
    /// upper bound is applied.
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
}
