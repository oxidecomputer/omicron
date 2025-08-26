// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing timeseries alignment operations.

// Copyright 2024 Oxide Computer Company

use anyhow::Context;
use anyhow::Error;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use oxql_types::Alignment;
use oxql_types::Table;
use oxql_types::Timeseries;
use oxql_types::point::DataType;
use oxql_types::point::MetricType;
use oxql_types::point::Points;
use oxql_types::point::ValueArray;
use oxql_types::point::Values;
use std::fmt;
use std::time::Duration;

// The maximum factor by which an alignment operation may upsample data.
//
// This is a crude way to limit the size of a query result. We do not currently
// paginate the results of OxQL queries, so we need to find other ways to avoid
// DOS attacks due to large query results.
//
// While we also apply limits on the total number of samples fetched from the
// ClickHouse database, this alone is insufficient. For example, suppose we have
// two samples, spaced 1 second apart, which are then passed to an alignment
// table operation with a period of 1 nanosecond. Now you have a billion points!
//
// To prevent this, we restrict the total amount by which any alignment
// operation can upsample the data. Another way to think of it is that this
// limits the ratio between the requested period and the largest interval
// between timestamps in the data.
const MAX_UPSAMPLING_RATIO: u128 = 100;

fn verify_max_upsampling_ratio(
    timestamps: &[DateTime<Utc>],
    period: &Duration,
) -> Result<(), Error> {
    let period = period.as_nanos();
    let max = MAX_UPSAMPLING_RATIO * period;
    for (t1, t0) in timestamps.iter().skip(1).zip(timestamps.iter()) {
        let Some(nanos) = t1.signed_duration_since(t0).num_nanoseconds() else {
            anyhow::bail!("Overflow computing timestamp delta");
        };
        assert!(nanos > 0, "Timestamps should be sorted");
        let nanos = nanos as u128;
        anyhow::ensure!(
            nanos <= max,
            "A table alignment operation may not upsample data by \
            more than a factor of {MAX_UPSAMPLING_RATIO}"
        );
    }
    Ok(())
}

/// An `align` table operation, used to produce data at well-defined periods.
///
/// Alignment is important for any kind of aggregation. Data is actually
/// produced at variable intervals, under the control of the producer itself.
/// This means that in general, two timeseries that are related (say, the same
/// schema) may have data samples at slightly different timestamps.
///
/// Alignment is used to produce data at the defined timestamps, so that samples
/// from multiple timeseries may be combined or correlated in meaningful ways.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Align {
    /// The alignment method, used to describe how data over the input period
    /// is used to generate an output sample.
    pub method: AlignmentMethod,
    // TODO-completeness. We'd like to separate the concept of the period, the
    // interval on which data is produced by this alignment, and the input
    // window, the range of time in the past over which data is considered to
    // produce the output values.
    //
    // For example, we might want to produce a moving average, by considering
    // the last 1h of data, and produce an output value every 10m. Each of those
    // output values would share 50m of data with the points on either side.
    //
    // For now, we'll enforce that the output period and input window are the
    // same.
    pub period: Duration,
}

impl std::fmt::Display for Align {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}({:?})", self.method, self.period)
    }
}

impl Align {
    // Apply the alignment function to the set of tables.
    pub(crate) fn apply(
        &self,
        tables: &[Table],
        query_end: &DateTime<Utc>,
    ) -> Result<Vec<Table>, Error> {
        match self.method {
            AlignmentMethod::Interpolate => tables
                .iter()
                .map(|table| align_interpolate(table, query_end, &self.period))
                .collect(),
            AlignmentMethod::MeanWithin => tables
                .iter()
                .map(|table| align_mean_within(table, query_end, &self.period))
                .collect(),
            AlignmentMethod::Rate => tables
                .iter()
                .map(|table| align_rate(table, query_end, &self.period))
                .collect(),
        }
    }
}

/// An alignment method.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AlignmentMethod {
    /// Alignment is done by interpolating the output data at the specified
    /// period.
    Interpolate,
    /// Alignment is done by computing the mean of the output data within the
    /// specified period.
    MeanWithin,

    /// Alignment is done by computing the per second rate of the output data
    /// within the specified period.
    Rate
}

impl fmt::Display for AlignmentMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlignmentMethod::Interpolate => write!(f, "interpolate"),
            AlignmentMethod::MeanWithin => write!(f, "mean_within"),
            AlignmentMethod::Rate => write!(f, "rate"),
        }
    }
}

// Align the timeseries in a table by computing the average within each output
// period.
fn align_mean_within(
    table: &Table,
    query_end: &DateTime<Utc>,
    period: &Duration,
) -> Result<Table, Error> {
    let mut output_table = Table::new(table.name());
    for timeseries in table.iter() {
        let points = &timeseries.points;
        anyhow::ensure!(
            points.dimensionality() == 1,
            "Aligning multidimensional timeseries is not yet supported"
        );
        let data_type = points.data_types().next().unwrap();
        anyhow::ensure!(
            data_type.is_numeric(),
            "Alignment by mean requires numeric data type, not {}",
            data_type
        );
        let metric_type = points.metric_type().unwrap();
        anyhow::ensure!(
            matches!(metric_type, MetricType::Gauge | MetricType::Delta),
            "Alignment by mean requires a gauge or delta metric, not {}",
            metric_type,
        );
        verify_max_upsampling_ratio(points.timestamps(), &period)?;

        // Always convert the output to doubles, when computing the mean. The
        // output is always a gauge, so we do not need the start times of the
        // input either.
        //
        // IMPORTANT: We compute the mean in the loop below from the back of the
        // array (latest timestamp) to the front (earliest timestamp). They are
        // appended to these arrays here in that _reversed_ order. These arrays
        // are flipped before pushing them onto the timeseries at the end of the
        // loop below.
        let mut output_values = Vec::with_capacity(points.len());
        let mut output_timestamps = Vec::with_capacity(points.len());

        // Convert the input to doubles now, so the tight loop below does less
        // conversion / matching inside.
        let input_points = match points.values(0).unwrap() {
            ValueArray::Integer(values) => values
                .iter()
                .map(|maybe_int| maybe_int.map(|int| int as f64))
                .collect(),
            ValueArray::Double(values) => values.clone(),
            _ => unreachable!(),
        };

        // Alignment works as follows:
        //
        // - Start at the end of the timestamp array, working our way backwards
        // in time.
        // - Create the output timestamp from the current step.
        // - Find all points in the input array that are within the alignment
        // period.
        // - Compute the mean of those.
        let period_ =
            TimeDelta::from_std(*period).context("time delta out of range")?;
        let first_timestamp = points.timestamps()[0];
        let mut ix: u32 = 0;
        loop {
            // Compute the next output timestamp, by shifting the query end time
            // by the period and the index.
            let time_offset = TimeDelta::from_std(ix * *period)
                .context("time delta out of range")?;
            let output_time = query_end
                .checked_sub_signed(time_offset)
                .context("overflow computing next output timestamp")?;
            let window_start = output_time
                .checked_sub_signed(period_)
                .context("overflow computing next output window start")?;

            // The output time is before any of the data in the input array,
            // we're done. It's OK for the _start time_ to be before any input
            // timestamps.
            if output_time < first_timestamp {
                break;
            }

            // Aggregate all values within this time window.
            //
            // This works a bit differently for gauge timeseries and deltas.
            // Gauges are simpler, so let's consider them first. A point is
            // "within" the window if the timestamp is within the window. Every
            // point is either completely within or completely without the
            // window, so we just add the values.
            //
            // Deltas have a start time, which makes things a bit more
            // complicated. In that case, a point can overlap _partially_ with
            // the output time window, and we'd like to take that partial
            // overlap into account. To do that, we find relevant values which
            // have either a start time or timestamp within the output window.
            // We compute the fraction of overlap with the window, which is in
            // [0.0, 1.0], and multiply the value by that fraction. One can
            // think of this as a dot-product between the interval-overlap array
            // and the value array, divided by the 1-norm, or number of nonzero
            // entries.
            let output_value = if matches!(metric_type, MetricType::Gauge) {
                mean_gauge_value_in_window(
                    points.timestamps(),
                    &input_points,
                    window_start,
                    output_time,
                )
            } else {
                mean_delta_value_in_window(
                    points.start_times().unwrap(),
                    points.timestamps(),
                    &input_points,
                    window_start,
                    output_time,
                )
            };
            output_values.push(output_value);

            // In any case, we push the window's end time and increment to the
            // next period.
            output_timestamps.push(output_time);
            ix += 1;
        }

        // We've accumulated our input values into the output arrays, but in
        // reverse order. Flip them and push onto the existing table, as a gauge
        // timeseries.
        let mut new_timeseries = Timeseries::new(
            timeseries.fields.clone().into_iter(),
            DataType::Double,
            MetricType::Gauge,
        )
        .unwrap();
        let values =
            ValueArray::Double(output_values.into_iter().rev().collect());
        let timestamps = output_timestamps.into_iter().rev().collect();
        let values = Values { values, metric_type: MetricType::Gauge };
        new_timeseries.points = Points::new(None, timestamps, vec![values]);
        new_timeseries
            .set_alignment(Alignment { end_time: *query_end, period: *period });
        output_table.insert(new_timeseries).unwrap();
    }
    Ok(output_table)
}

// Align the timeseries in a table by computing the per second rate within each output
// period.

fn align_rate(
    table: &Table,
    query_end: &DateTime<Utc>,
    period: &Duration,
) -> Result<Table, Error> {
    let mut output_table = Table::new(table.name());
    for timeseries in table.iter() {
        let points = &timeseries.points;
        anyhow::ensure!(
            points.dimensionality() == 1,
            "Aligning multidimensional timeseries is not yet supported"
        );
        let data_type = points.data_types().next().unwrap();
        anyhow::ensure!(
            data_type.is_numeric(),
            "Alignment by mean requires numeric data type, not {}",
            data_type
        );
        let metric_type = points.metric_type().unwrap();
        anyhow::ensure!(
            metric_type == MetricType::Delta,
            "Alignment by rate requires a delta metric, not {}",
            metric_type,
        );
        verify_max_upsampling_ratio(points.timestamps(), &period)?;

        // Always convert the output to doubles, when computing the mean. The
        // output is always a gauge, so we do not need the start times of the
        // input either.
        //
        // IMPORTANT: We compute the mean in the loop below from the back of the
        // array (latest timestamp) to the front (earliest timestamp). They are
        // appended to these arrays here in that _reversed_ order. These arrays
        // are flipped before pushing them onto the timeseries at the end of the
        // loop below.
        let mut output_values = Vec::with_capacity(points.len());
        let mut output_timestamps = Vec::with_capacity(points.len());

        // Convert the input to doubles now, so the tight loop below does less
        // conversion / matching inside.
        let input_points = match points.values(0).unwrap() {
            ValueArray::Integer(values) => values
                .iter()
                .map(|maybe_int| maybe_int.map(|int| int as f64))
                .collect(),
            ValueArray::Double(values) => values.clone(),
            _ => unreachable!(),
        };

        // Alignment works as follows:
        //
        // - Start at the end of the timestamp array, working our way backwards
        // in time.
        // - Create the output timestamp from the current step.
        // - Find all points in the input array that are within the alignment
        // period.
        // - Compute the mean of those.
        let period_ =
            TimeDelta::from_std(*period).context("time delta out of range")?;
        let first_timestamp = points.timestamps()[0];
        let mut ix: u32 = 0;
        loop {
            // Compute the next output timestamp, by shifting the query end time
            // by the period and the index.
            let time_offset = TimeDelta::from_std(ix * *period)
                .context("time delta out of range")?;
            let output_time = query_end
                .checked_sub_signed(time_offset)
                .context("overflow computing next output timestamp")?;
            let window_start = output_time
                .checked_sub_signed(period_)
                .context("overflow computing next output window start")?;

            // The output time is before any of the data in the input array,
            // we're done. It's OK for the _start time_ to be before any input
            // timestamps.
            if output_time < first_timestamp {
                break;
            }

            // Aggregate all values within this time window.
            //
            // Deltas have a start time, which makes things a bit more
            // complicated. In that case, a point can overlap _partially_ with
            // the output time window, and we'd like to take that partial
            // overlap into account. To do that, we find relevant values which
            // have either a start time or timestamp within the output window.
            // We compute the fraction of overlap with the window, which is in
            // [0.0, 1.0], and multiply the value by that fraction. One can
            // think of this as a dot-product between the interval-overlap array
            // and the value array, divided by the 1-norm, or number of nonzero
            // entries.
            let output_value = rate_value_in_window(
                points.start_times().unwrap(),
                points.timestamps(),
                &input_points,
                window_start,
                output_time,
            );
            output_values.push(output_value);

            // In any case, we push the window's end time and increment to the
            // next period.
            output_timestamps.push(output_time);
            ix += 1;
        }

        // We've accumulated our input values into the output arrays, but in
        // reverse order. Flip them and push onto the existing table, as a gauge
        // timeseries.
        let mut new_timeseries = Timeseries::new(
            timeseries.fields.clone().into_iter(),
            DataType::Double,
            MetricType::Gauge,
        )
        .unwrap();
        let values =
            ValueArray::Double(output_values.into_iter().rev().collect());
        let timestamps = output_timestamps.into_iter().rev().collect();
        let values = Values { values, metric_type: MetricType::Gauge };
        new_timeseries.points = Points::new(None, timestamps, vec![values]);
        new_timeseries
            .set_alignment(Alignment { end_time: *query_end, period: *period });
        output_table.insert(new_timeseries).unwrap();
    }
    Ok(output_table)
}

// Given an interval start and end, and a window start and end, compute the
// fraction of the _interval_ that the time window represents.
fn fraction_overlap_with_window(
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> f64 {
    assert!(interval_start < interval_end);
    assert!(window_start < window_end);
    let end = window_end.min(interval_end);
    let start = window_start.max(interval_start);
    let contained_size = (end - start).num_nanoseconds().unwrap() as f64;
    if contained_size < 0.0 {
        return 0.0;
    }
    let interval_size =
        (interval_end - interval_start).num_nanoseconds().unwrap() as f64;
    let fraction = contained_size / interval_size;
    assert!(fraction >= 0.0);
    assert!(fraction <= 1.0);
    fraction
}

// For a delta metric, compute the mean of points falling within the provided
// window.
//
// This uses both the start and end times when considering each point. Each
// point's value is weighted by the faction of overlap with the window.
fn mean_delta_value_in_window(
    start_times: &[DateTime<Utc>],
    timestamps: &[DateTime<Utc>],
    input_points: &[Option<f64>],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Option<f64> {
    // We can find the indices where the timestamp and start times separately
    // overlap the window of interest. Then any interval is potentially of
    // interest if _either_ its start time or timestamp is within the window.
    //
    // Since the start times are <= the timestamps, we can take the min of those
    // two to get the first point that overlaps at all, and the max to get the
    // last.
    let first_timestamp = timestamps.partition_point(|t| t <= &window_start);
    let last_timestamp = timestamps.partition_point(|t| t <= &window_end);
    let first_start_time = start_times.partition_point(|t| t <= &window_start);
    let last_start_time = start_times.partition_point(|t| t <= &window_end);
    let first_index = first_timestamp.min(first_start_time);
    let last_index = last_timestamp.max(last_start_time);

    // Detect the possible case where the interval is entirely before or
    // entirely after the window.
    if first_index == last_index {
        let t = *timestamps.get(first_timestamp)?;
        let s = *start_times.get(first_timestamp)?;
        if t < window_start || s > window_end {
            return None;
        }
        let Some(val) = input_points[first_timestamp] else {
            return None;
        };
        let fraction = fraction_overlap_with_window(
            start_times[first_start_time],
            timestamps[first_timestamp],
            window_start,
            window_end,
        );
        return Some(fraction * val);
    }

    // Compute the overlap for all points which have some overlap.
    let starts = &start_times[first_index..last_index];
    let times = &timestamps[first_index..last_index];
    let vals = &input_points[first_index..last_index];
    let iter = starts
        .into_iter()
        .copied()
        .zip(times.into_iter().copied())
        .zip(vals.into_iter().copied());
    let count = (last_timestamp - first_timestamp).max(1) as f64;
    let mut maybe_sum = None;
    for it in iter.filter_map(|((start, time), maybe_val)| {
        let Some(val) = maybe_val else {
            return None;
        };
        let fraction =
            fraction_overlap_with_window(start, time, window_start, window_end);
        Some(fraction * val)
    }) {
        *maybe_sum.get_or_insert(0.0) += it;
    }
    maybe_sum.map(|sum| sum / count)
}

// For a gauge metric, compute the mean of points falling within the provided
// window.
fn mean_gauge_value_in_window(
    timestamps: &[DateTime<Utc>],
    input_points: &[Option<f64>],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Option<f64> {
    // Find the position of the window start and end in the sorted
    // array of input timestamps. The `partition_point()` method accepts
    // a closure, which partitions the input into a prefix where the
    // closure evaluates to true, and a suffix where it's false. It
    // returns the first element in the suffix.
    //
    // So the first closure returns true for all timestamps we want to
    // exclude, which are those up to and including the window start time.
    // So we get the index of the first point strictly later than the
    // window start.
    //
    // The second closure returns true for all points up to and
    // including the output time as well.
    let start_index = timestamps.partition_point(|t| t <= &window_start);
    let output_index = timestamps.partition_point(|t| t <= &window_end);
    assert!(output_index >= start_index);

    // Accumulate the values over this set of indices.
    //
    // If there are really zero points in this time interval, we add
    // a missing value.
    if start_index != output_index {
        let mut maybe_sum = None;
        for it in input_points[start_index..output_index]
            .iter()
            .filter_map(|x| x.as_ref().copied())
        {
            *maybe_sum.get_or_insert(0.0) += it;
        }
        maybe_sum.map(|output_value| {
            output_value / (output_index - start_index) as f64
        })
    } else {
        None
    }
}
//
// For a delta metric, compute the per second rate of points falling within the
// provided window.
//
// This uses both the start and end times when considering each point. Each
// point's value is weighted by the faction of overlap with the window.
fn rate_value_in_window(
    start_times: &[DateTime<Utc>],
    timestamps: &[DateTime<Utc>],
    input_points: &[Option<f64>],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Option<f64> {
    // We can find the indices where the timestamp and start times separately
    // overlap the window of interest. Then any interval is potentially of
    // interest if _either_ its start time or timestamp is within the window.
    //
    // Since the start times are <= the timestamps, we can take the min of those
    // two to get the first point that overlaps at all, and the max to get the
    // last.
    let first_timestamp = timestamps.partition_point(|t| t <= &window_start);
    let last_timestamp = timestamps.partition_point(|t| t <= &window_end);
    let first_start_time = start_times.partition_point(|t| t <= &window_start);
    let last_start_time = start_times.partition_point(|t| t <= &window_end);
    let first_index = first_timestamp.min(first_start_time);
    let last_index = last_timestamp.max(last_start_time);

    let window_secs = (window_end - window_start)
        .to_std()
        .ok()
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    if window_secs <= 0.0 {
        return None;
    }

    // Detect the possible case where the interval is entirely before or
    // entirely after the window.
    if first_index == last_index {
        let t = *timestamps.get(first_timestamp)?;
        let s = *start_times.get(first_timestamp)?;
        if t < window_start || s > window_end {
            return None;
        }
        let Some(val) = input_points[first_timestamp] else {
            return None;
        };
        let fraction = fraction_overlap_with_window(
            start_times[first_start_time],
            timestamps[first_timestamp],
            window_start,
            window_end,
        );
        return Some((fraction * val) / window_secs);
    }

    // Compute the overlap for all points which have some overlap.
    let starts = &start_times[first_index..last_index];
    let times = &timestamps[first_index..last_index];
    let vals = &input_points[first_index..last_index];
    let iter = starts
        .into_iter()
        .copied()
        .zip(times.into_iter().copied())
        .zip(vals.into_iter().copied());

    let mut maybe_sum = None;
    for it in iter.filter_map(|((start, time), maybe_val)| {
        let Some(val) = maybe_val else {
            return None;
        };
        let fraction =
            fraction_overlap_with_window(start, time, window_start, window_end);
        Some(fraction * val)
    }) {
        *maybe_sum.get_or_insert(0.0) += it;
    }
    maybe_sum.map(|sum| sum / window_secs)
}

fn align_interpolate(
    _table: &Table,
    _query_end: &DateTime<Utc>,
    _period: &Duration,
) -> Result<Table, Error> {
    anyhow::bail!("Alignment with interpolation not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fraction_overlap_with_window() {
        let now = Utc::now();
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let interval_start = window_start;
        let interval_end = window_end;
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            1.0
        );

        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let interval_start = window_start;
        let interval_end = now - Duration::from_secs_f64(0.5);
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            1.0,
            "This interval is aligned with the start time \
            of the window, and contained entirely within it, \
            so the fraction should be 1.0",
        );

        // If we reverse the window and interval, then the interval entirely
        // contains the window, which is 50% of the interval.
        let (window_start, window_end, interval_start, interval_end) =
            (interval_start, interval_end, window_start, window_end);
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            0.5,
            "The window is entirely contained within the interval, \
            and covers 50% of it",
        );

        // If the interval is entirely contained in the window, we should have
        // the entire interval as our fraction.
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let interval_start = window_start + Duration::from_secs_f64(0.25);
        let interval_end = window_start + Duration::from_secs_f64(0.5);
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            1.0,
            "The interval is entirely contained within the window",
        );

        // This is aligned at the right with the window end.
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let interval_start = window_start + Duration::from_secs_f64(0.25);
        let interval_end = window_end;
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            1.0,
            "The interval is aligned at right with the window, and \
            entirely contained within it, so the fraction should still \
            be 1.0",
        );

        // But if we reverse it again, the fraction should reveal itself.
        let (window_start, window_end, interval_start, interval_end) =
            (interval_start, interval_end, window_start, window_end);
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            0.75,
            "The window represents 75% of the interval",
        );

        // This interval does not overlap at all, to the left.
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let interval_start = window_start - Duration::from_secs(2);
        let interval_end = window_start - Duration::from_secs(1);
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            0.0,
        );

        // This interval does not overlap at all, to the right.
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let interval_start = window_start + Duration::from_secs(1);
        let interval_end = window_start + Duration::from_secs(2);
        assert_eq!(
            fraction_overlap_with_window(
                interval_start,
                interval_end,
                window_start,
                window_end,
            ),
            0.0,
        );
    }

    #[test]
    fn test_mean_delta_value_in_window() {
        let now = Utc::now();
        let start_times = &[
            now - Duration::from_secs(4),
            now - Duration::from_secs(3),
            now - Duration::from_secs(2),
            now - Duration::from_secs(1),
        ];
        let timestamps = &[
            now - Duration::from_secs(3),
            now - Duration::from_secs(2),
            now - Duration::from_secs(1),
            now,
        ];
        let input_points = &[Some(0.0), Some(1.0), Some(2.0), Some(3.0)];

        let window_start = now - Duration::from_secs_f64(0.5);
        let window_end = now;
        let mean = mean_delta_value_in_window(
            start_times,
            timestamps,
            input_points,
            window_start,
            window_end,
        )
        .expect("This should overlap the last interval");
        assert_eq!(
            mean,
            input_points.last().unwrap().unwrap() / 2.0,
            "This overlaps the last interval by half",
        );
    }

    #[test]
    fn test_mean_gauge_value_in_window() {
        let now = Utc::now();
        let timestamps = &[
            now - Duration::from_secs(3),
            now - Duration::from_secs(2),
            now - Duration::from_secs(1),
            now,
        ];
        let input_points = &[Some(0.0), Some(1.0), Some(2.0), Some(3.0)];

        let window_start = now - Duration::from_secs(4);
        let window_end = now - Duration::from_secs(3);
        let mean = mean_gauge_value_in_window(
            timestamps,
            input_points,
            window_start,
            window_end,
        )
        .expect("This window should overlap the first timestamp");
        assert_eq!(
            mean, 0.0,
            "This window should overlap the first timestamp, so the \
            mean value should be the mean of the first point only"
        );

        let window_start = now - Duration::from_secs(4);
        let window_end = now - Duration::from_secs(2);
        let mean = mean_gauge_value_in_window(
            timestamps,
            input_points,
            window_start,
            window_end,
        )
        .expect("This window should overlap the first two timestamps");
        assert_eq!(
            mean, 0.5,
            "This window should overlap the first two timestamps, so the \
            mean value should be the mean of the first two points"
        );

        let window_start = now - Duration::from_secs(3);
        let window_end = now - Duration::from_secs(2);
        let mean = mean_gauge_value_in_window(
            timestamps,
            input_points,
            window_start,
            window_end,
        )
        .expect("This window should overlap the second timestamps");
        assert_eq!(
            mean, 1.0,
            "This window should overlap the second timestamp, so the \
            mean value should be the mean of the second point only."
        );

        let window_start = now - Duration::from_secs(4);
        let window_end = *timestamps.last().unwrap();
        let mean = mean_gauge_value_in_window(
            timestamps,
            input_points,
            window_start,
            window_end,
        )
        .expect("This window should overlap the all timestamps");
        assert_eq!(
            mean,
            input_points.iter().map(|x| x.unwrap()).sum::<f64>()
                / input_points.len() as f64,
            "This window should overlap the all timestamps, so the \
            mean value should be the mean of all points",
        );

        let window_start = now - Duration::from_secs(3);
        let window_end = now - Duration::from_secs_f64(2.5);
        assert!(
            mean_gauge_value_in_window(
                timestamps,
                input_points,
                window_start,
                window_end,
            )
            .is_none(),
            "This window should overlap none of the points"
        );
    }

    #[test]
    fn test_rate_value_in_window_steady_state() {
        // 1000 bytes every 10 seconds -> 100 bytes per second
        let raw_data = &[
            ("2025-08-12T19:17:00.0000Z", "2025-08-12T19:17:10.0000Z", 1000f64),
            ("2025-08-12T19:17:10.0000Z", "2025-08-12T19:17:20.0000Z", 1000f64),
            ("2025-08-12T19:17:20.0000Z", "2025-08-12T19:17:30.0000Z", 1000f64),
            ("2025-08-12T19:17:30.0000Z", "2025-08-12T19:17:40.0000Z", 1000f64),
            ("2025-08-12T19:17:40.0000Z", "2025-08-12T19:17:50.0000Z", 1000f64),
            ("2025-08-12T19:17:50.0000Z", "2025-08-12T19:18:00.0000Z", 1000f64),
        ];

        let start_times: Vec<DateTime<Utc>> =
            raw_data.into_iter().map(|r| r.0.parse().unwrap()).collect();
        let timestamps: Vec<DateTime<Utc>> =
            raw_data.into_iter().map(|r| r.1.parse().unwrap()).collect();

        let input_points: Vec<_> =
            raw_data.into_iter().map(|r| Some(r.2)).collect();

        let window_start = start_times[0];
        let window_end = timestamps[timestamps.len() - 1];

        let mean = rate_value_in_window(
            &start_times,
            &timestamps,
            &input_points,
            window_start,
            window_end,
        )
        .unwrap();
        let expected = 100.0;
        assert!((mean - expected).abs() < 1e-6, "mean={mean}, expected={expected}");
    }

    #[test]
    fn test_verify_max_upsampling_ratio() {
        // We'll use a 10 second period, and ensure that we allow downsampling,
        // and upsampling up to the max factor. That's 1/10th of a second,
        // currently.
        let now = Utc::now();
        let timestamps = &[now - Duration::from_secs(10), now];

        // All values within the threshold.
        for period in [
            Duration::from_secs_f64(0.5),
            Duration::from_secs(10),
            Duration::from_millis(100),
        ] {
            assert!(verify_max_upsampling_ratio(timestamps, &period).is_ok());
        }

        // Just below the threshold.
        assert!(verify_max_upsampling_ratio(
            timestamps,
            &Duration::from_millis(99),
        )
        .is_err());

        // Sanity check for way below the threshold.
        assert!(
            verify_max_upsampling_ratio(timestamps, &Duration::from_nanos(1),)
                .is_err()
        );

        // Arrays where we can't compute an interval are fine.
        assert!(
            verify_max_upsampling_ratio(
                &timestamps[..1],
                &Duration::from_nanos(1),
            )
            .is_ok()
        );
        assert!(
            verify_max_upsampling_ratio(&[], &Duration::from_nanos(1),).is_ok()
        );
    }

    #[test]
    fn test_mean_delta_does_not_modify_missing_values() {
        let now = Utc::now();
        let start_times =
            &[now - Duration::from_secs(2), now - Duration::from_secs(1)];
        let timestamps = &[now - Duration::from_secs(1), now];
        let input_points = &[Some(1.0), None];
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let mean = mean_delta_value_in_window(
            start_times,
            timestamps,
            input_points,
            window_start,
            window_end,
        );
        assert!(
            mean.is_none(),
            "This time window contains only a None value, which should not be \
            included in the sum"
        );
    }

    #[test]
    fn test_mean_gauge_does_not_modify_missing_values() {
        let now = Utc::now();
        let timestamps = &[now - Duration::from_secs(1), now];
        let input_points = &[Some(1.0), None];
        let window_start = now - Duration::from_secs(1);
        let window_end = now;
        let mean = mean_gauge_value_in_window(
            timestamps,
            input_points,
            window_start,
            window_end,
        );
        assert!(
            mean.is_none(),
            "This time window contains only a None value, which should not be \
            included in the sum"
        );
    }
}
