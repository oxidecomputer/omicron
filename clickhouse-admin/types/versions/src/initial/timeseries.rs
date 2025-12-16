// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Timeseries types shared by ClickHouse Admin Server and Single APIs.

use chrono::{DateTime, Utc};
use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[inline]
fn default_interval() -> u64 {
    60
}

#[inline]
fn default_time_range() -> u64 {
    86400
}

#[inline]
fn default_timestamp_format() -> TimestampFormat {
    TimestampFormat::Utc
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Available metrics tables in the `system` database
pub enum SystemTable {
    AsynchronousMetricLog,
    MetricLog,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Which format should the timestamp be in.
pub enum TimestampFormat {
    Utc,
    UnixEpoch,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct MetricInfoPath {
    /// Table to query in the `system` database
    pub table: SystemTable,
    /// Name of the metric to retrieve.
    pub metric: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TimeSeriesSettingsQuery {
    /// The interval to collect monitoring metrics in seconds.
    /// Default is 60 seconds.
    #[serde(default = "default_interval")]
    pub interval: u64,
    /// Range of time to collect monitoring metrics in seconds.
    /// Default is 86400 seconds (24 hrs).
    #[serde(default = "default_time_range")]
    pub time_range: u64,
    /// Format in which each timeseries timestamp will be in.
    /// Default is UTC
    #[serde(default = "default_timestamp_format")]
    pub timestamp_format: TimestampFormat,
}

/// Settings to specify which time series to retrieve.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SystemTimeSeriesSettings {
    /// Time series retrieval settings (time range and interval)
    pub retrieval_settings: TimeSeriesSettingsQuery,
    /// Database table and name of the metric to retrieve
    pub metric_info: MetricInfoPath,
}

// Our OpenAPI generator does not allow for enums to be of different
// primitive types. Because Utc is a "string" in json, Unix cannot be an int.
// This is why we set it as a `String`.
#[derive(Debug, Display, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum Timestamp {
    Utc(DateTime<Utc>),
    Unix(String),
}

/// Retrieved time series from the internal `system` database.
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SystemTimeSeries {
    pub time: String,
    pub value: f64,
    // TODO: Would be really nice to have an enum with possible units (s, ms, bytes)
    // Not sure if I can even add this, the system tables don't mention units at all.
}
