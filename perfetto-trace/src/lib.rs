// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Chrome Trace Event format support for visualizing operation timing
//!
//! Traces produced with this crate can be loaded into the Perfetto trace
//! viewer (<https://ui.perfetto.dev/>) or `chrome://tracing`.

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

/// Represents a Perfetto Trace Event format JSON file for visualization.
///
/// This format is used by the Perfetto trace viewer (<https://ui.perfetto.dev/>)
/// to visualize timing information for operations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

/// A completed timed operation, recorded with wall-clock timestamps.
///
/// Spans are collected while work runs and later converted into a [`Trace`]
/// with [`assemble`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TraceSpan {
    /// Name identifying the specific operation (e.g., a target URL)
    pub name: String,
    /// The kind of operation, used for grouping and filtering
    pub category: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Arbitrary key-value pairs with additional metadata
    pub args: serde_json::Value,
}

impl TraceSpan {
    /// Creates a span lasting from `start` until now.
    pub fn since(
        name: impl Into<String>,
        category: impl Into<String>,
        start: DateTime<Utc>,
    ) -> Self {
        TraceSpan {
            name: name.into(),
            category: category.into(),
            start,
            end: Utc::now(),
            args: serde_json::Value::Null,
        }
    }
}

/// Runs `fut` to completion, returning its output along with a [`TraceSpan`]
/// covering its execution.
pub async fn timed<T>(
    name: impl Into<String>,
    category: impl Into<String>,
    fut: impl Future<Output = T>,
) -> (TraceSpan, T) {
    let start = Utc::now();
    let output = fut.await;
    (TraceSpan::since(name, category, start), output)
}

/// Assembles spans into a [`Trace`] in the Chrome Trace Event format.
///
/// Spans are packed into the minimum number of `tid` lanes: each span is
/// assigned the lowest-numbered lane that is free at its start time.  The
/// trace format renders overlapping events in a single lane poorly, so
/// concurrent spans must land in separate lanes; packing (rather than giving
/// every event its own lane) keeps the lane count equal to the maximum
/// observed concurrency.  Spans are sorted by start time with ties broken by
/// later end time, so a span that fully contains others (e.g., a phase
/// containing the operations it ran) is placed in a lower lane than its
/// contents.
pub fn assemble(mut spans: Vec<TraceSpan>) -> Trace {
    spans.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| b.end.cmp(&a.end)));

    // The end time of the last span assigned to each lane.
    let mut lane_ends: Vec<DateTime<Utc>> = Vec::new();
    let trace_events = spans
        .into_iter()
        .map(|span| {
            // Guard against clock adjustments making a span end before it
            // starts.
            let end = span.end.max(span.start);
            let tid = match lane_ends
                .iter()
                .position(|lane_end| *lane_end <= span.start)
            {
                Some(lane) => {
                    lane_ends[lane] = end;
                    lane
                }
                None => {
                    lane_ends.push(end);
                    lane_ends.len() - 1
                }
            };
            TraceEvent {
                name: span.name,
                cat: span.category,
                ph: "X".to_string(),
                ts: span.start.timestamp_micros(),
                dur: (end - span.start).num_microseconds().unwrap_or(0),
                pid: 1,
                tid,
                args: span.args,
            }
        })
        .collect();

    Trace { trace_events, display_time_unit: "ms".to_string() }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;

    fn span(name: &str, start_us: i64, end_us: i64) -> TraceSpan {
        TraceSpan {
            name: name.to_string(),
            category: "test".to_string(),
            start: Utc.timestamp_micros(start_us).unwrap(),
            end: Utc.timestamp_micros(end_us).unwrap(),
            args: serde_json::Value::Null,
        }
    }

    fn lanes_by_name(trace: &Trace) -> Vec<(String, usize)> {
        trace.trace_events.iter().map(|e| (e.name.clone(), e.tid)).collect()
    }

    #[test]
    fn test_overlapping_spans_get_distinct_lanes() {
        let trace = assemble(vec![
            span("a", 0, 100),
            span("b", 50, 150),
            span("c", 75, 200),
        ]);
        assert_eq!(
            lanes_by_name(&trace),
            vec![
                ("a".to_string(), 0),
                ("b".to_string(), 1),
                ("c".to_string(), 2),
            ]
        );
    }

    #[test]
    fn test_lane_reuse_after_span_ends() {
        let trace = assemble(vec![
            span("a", 0, 100),
            span("b", 50, 150),
            span("c", 100, 200),
        ]);
        // "c" starts exactly when "a" ends, so it reuses lane 0.
        assert_eq!(
            lanes_by_name(&trace),
            vec![
                ("a".to_string(), 0),
                ("b".to_string(), 1),
                ("c".to_string(), 0),
            ]
        );
    }

    #[test]
    fn test_containing_span_gets_lower_lane() {
        // A "phase" span fully contains the operations it ran, including one
        // starting at the same instant.  The tie is broken by later end time,
        // so the phase lands in lane 0.
        let trace = assemble(vec![
            span("op1", 0, 50),
            span("phase", 0, 200),
            span("op2", 50, 150),
        ]);
        assert_eq!(
            lanes_by_name(&trace),
            vec![
                ("phase".to_string(), 0),
                ("op1".to_string(), 1),
                ("op2".to_string(), 1),
            ]
        );
    }

    #[test]
    fn test_event_field_mapping() {
        let mut trace = assemble(vec![span("a", 25, 100)]);
        let event = trace.trace_events.pop().unwrap();
        assert_eq!(event.ts, 25);
        assert_eq!(event.dur, 75);
        assert_eq!(event.ph, "X");
        assert_eq!(event.cat, "test");
        assert_eq!(event.pid, 1);

        // A span ending before it starts (clock adjustment) clamps to zero
        // duration rather than going negative.
        let mut trace = assemble(vec![span("b", 100, 25)]);
        assert_eq!(trace.trace_events.pop().unwrap().dur, 0);
    }

    #[test]
    fn test_serialized_key_names() {
        let trace = assemble(vec![span("a", 0, 100)]);
        let json = serde_json::to_value(&trace).unwrap();
        assert!(json.get("traceEvents").is_some());
        assert_eq!(
            json.get("displayTimeUnit").unwrap(),
            &serde_json::Value::String("ms".to_string())
        );
    }
}
