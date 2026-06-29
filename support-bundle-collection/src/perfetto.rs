// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Perfetto Trace Event format support for visualizing support bundle collection

use serde::Deserialize;
use serde::Serialize;

/// Represents a Perfetto Trace Event format JSON file for visualization.
///
/// This format is used by the Perfetto trace viewer (<https://ui.perfetto.dev/>)
/// to visualize timing information for operations.
#[derive(Serialize, Deserialize)]
pub struct Trace {
    #[serde(rename = "traceEvents")]
    pub trace_events: Vec<TraceEvent>,
    /// Display unit for time values in the UI (e.g., "ms" for milliseconds)
    #[serde(rename = "displayTimeUnit")]
    pub display_time_unit: String,
}

/// A single event in the Perfetto Trace Event format.
///
/// This represents a complete event (duration event) showing when an operation
/// started and how long it took.
#[derive(Serialize, Deserialize)]
pub struct TraceEvent {
    /// Human-readable name of the event
    pub name: String,
    /// Category name (abbreviated as "cat" in Perfetto format).
    /// Used to group related events together in the trace viewer.
    pub cat: String,
    /// Phase type (abbreviated as "ph" in Perfetto format).
    /// "X" means a "Complete" event with both timestamp and duration.
    pub ph: String,
    /// Timestamp in microseconds (abbreviated as "ts" in Perfetto format).
    /// Represents when the event started, as microseconds since the epoch.
    pub ts: i64,
    /// Duration in microseconds (abbreviated as "dur" in Perfetto format).
    /// How long the event took to complete.
    pub dur: i64,
    /// Process ID. Used to separate events into different process lanes
    /// in the trace viewer.
    pub pid: u32,
    /// Thread ID. Used to separate events into different thread lanes
    /// within a process in the trace viewer.
    pub tid: usize,
    /// Arbitrary key-value pairs with additional event metadata
    pub args: serde_json::Value,
}
