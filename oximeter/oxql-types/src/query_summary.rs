// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing summaries of queries against the timeseries database.

// Copyright 2024 Oxide Computer Company

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// A count of bytes / rows accessed during a query.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct IoCount {
    /// The number of bytes accessed.
    pub bytes: u64,
    /// The number of rows accessed.
    pub rows: u64,
}

impl std::fmt::Display for IoCount {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} rows ({} bytes)", self.rows, self.bytes)
    }
}

/// Summary of the I/O resources used by a query.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct IoSummary {
    /// The bytes and rows read by the query.
    pub read: IoCount,
    /// The bytes and rows written by the query.
    pub written: IoCount,
}

/// Basic metadata about the resource usage of a single SQL query.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct QuerySummary {
    /// The database-assigned query ID.
    pub id: Uuid,
    /// The raw SQL query.
    pub query: String,
    /// The total duration of the query (network plus execution).
    pub elapsed: Duration,
    /// Summary of the data read and written.
    pub io_summary: IoSummary,
}
