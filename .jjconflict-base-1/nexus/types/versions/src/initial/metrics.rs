// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics types for the Nexus external API.

use chrono::{DateTime, Utc};
use omicron_common::api::external::PaginationOrder;
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Display, Deserialize, JsonSchema)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum SystemMetricName {
    VirtualDiskSpaceProvisioned,
    CpusProvisioned,
    RamProvisioned,
}

#[derive(Deserialize, JsonSchema)]
pub struct SystemMetricsPathParam {
    pub metric_name: SystemMetricName,
}

/// Query parameters common to resource metrics endpoints.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ResourceMetrics {
    /// An inclusive start time of metrics.
    pub start_time: DateTime<Utc>,
    /// An exclusive end time of metrics.
    pub end_time: DateTime<Utc>,
    /// Query result order
    pub order: Option<PaginationOrder>,
}
