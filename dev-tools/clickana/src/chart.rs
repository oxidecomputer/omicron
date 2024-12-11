// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use clickhouse_admin_server_client::types::{SystemTable, SystemTimeSeries};
use ratatui::{
    layout::{Constraint, Rect},
    style::{Color, Style, Stylize},
    symbols::Marker,
    text::Line,
    widgets::{Axis, Block, Chart, Dataset, GraphType, LegendPosition},
    Frame,
};
use std::fmt::Display;

const GIBIBYTE_F64: f64 = 1073741824.0;
const MEBIBYTE_F64: f64 = 1048576.0;

#[derive(Clone, Copy, Debug)]
pub enum Unit {
    Count,
    Gibibyte,
    Mebibyte,
}

impl Display for Unit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Unit::Count => "",
            Unit::Gibibyte => "GiB",
            &Unit::Mebibyte => "MiB",
        };
        write!(f, "{s}")
    }
}

impl Unit {
    /// Returns the value of the unit represented in bytes.
    fn as_bytes_f64(&self) -> Result<f64> {
        let bytes = match self {
            Unit::Gibibyte => GIBIBYTE_F64,
            Unit::Mebibyte => MEBIBYTE_F64,
            Unit::Count => bail!("Count cannot be converted into bytes"),
        };
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum MetricName {
    DiskUsage,
    MemoryTracking,
    QueryCount,
    RunningQueries,
}

impl Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MetricName::DiskUsage => "DiskUsed_default",
            MetricName::MemoryTracking => "CurrentMetric_MemoryTracking",
            MetricName::QueryCount => "ProfileEvent_Query",
            MetricName::RunningQueries => "CurrentMetric_Query",
        };
        write!(f, "{s}")
    }
}

impl MetricName {
    pub fn table(&self) -> SystemTable {
        match self {
            MetricName::DiskUsage => SystemTable::AsynchronousMetricLog,
            MetricName::MemoryTracking
            | MetricName::QueryCount
            | MetricName::RunningQueries => SystemTable::MetricLog,
        }
    }

    fn unit(&self) -> Unit {
        match self {
            MetricName::DiskUsage => Unit::Gibibyte,
            MetricName::MemoryTracking => Unit::Mebibyte,
            MetricName::QueryCount | MetricName::RunningQueries => Unit::Count,
        }
    }
}

#[derive(Debug)]
pub struct ChartMetadata {
    pub _metric: MetricName,
    pub title: String,
    pub unit: Unit,
}

impl ChartMetadata {
    pub fn new(metric: MetricName, title: String) -> Self {
        let unit = metric.unit();
        Self { _metric: metric, title, unit }
    }
}

#[derive(Debug)]
struct TimeSeriesValues {
    values: Vec<f64>,
}

impl TimeSeriesValues {
    fn new(raw_data: &Vec<SystemTimeSeries>) -> Self {
        let values: Vec<f64> = raw_data.iter().map(|ts| ts.value).collect();
        Self { values }
    }

    fn min(&self) -> Result<&f64> {
        let Some(min_value) =
            self.values.iter().min_by(|a, b| a.partial_cmp(b).unwrap())
        else {
            bail!("no values have been retrieved")
        };

        Ok(min_value)
    }

    fn max(&self) -> Result<&f64> {
        let Some(max_value) =
            self.values.iter().max_by(|a, b| a.partial_cmp(b).unwrap())
        else {
            bail!("no values have been retrieved")
        };

        Ok(max_value)
    }
}

// The result of the following functions will not be precise, but it doesn't
// matter since we just want an estimate for the chart's labels and bounds.
// all we need are values that are larger than the maximum value in the
// timeseries or smaller than the minimum value in the timeseries.

/// Returns the sum of the maximum raw value and 1 or the equivalent of 1
/// MiB or GiB in bytes.
fn padded_max_value_raw(unit: Unit, max_value_raw: &f64) -> Result<f64> {
    let ceil_value = max_value_raw.ceil();
    let padded_value = match unit {
        Unit::Count => ceil_value + 1.0,
        Unit::Gibibyte | Unit::Mebibyte => ceil_value + unit.as_bytes_f64()?,
    };
    Ok(padded_value)
}

/// Returns the sum of the max raw value and 1 or the equivalent of 1
/// Mib or Gib.
fn padded_max_value_as_unit(unit: Unit, max_value_raw: &f64) -> Result<f64> {
    let label_value = match unit {
        Unit::Count => max_value_raw + 1.0,
        Unit::Gibibyte | Unit::Mebibyte => {
            (max_value_raw / unit.as_bytes_f64()?) + 1.0
        }
    };
    Ok(label_value.ceil())
}

/// Returns the difference of the minimum raw value and 1 or the equivalent
/// of 1 in MiB or GiB in bytes. If the minimum is equal to or less than 1.0,
/// or the equivalent of 1 once converted from bytes to the expected unit
/// (e.g. less than or equal to 1048576 if we're using MiB) we'll use 0.0 as
/// the minimum value as we don't expect any of our charts
/// to require negative numbers for now.
fn padded_min_value_raw(unit: Unit, min_value_raw: &f64) -> Result<f64> {
    let padded_value = match unit {
        Unit::Count => {
            if *min_value_raw <= 1.0 {
                0.0
            } else {
                min_value_raw - 1.0
            }
        }
        Unit::Gibibyte | Unit::Mebibyte => {
            let bytes = unit.as_bytes_f64()?;
            if *min_value_raw <= bytes {
                0.0
            } else {
                min_value_raw - bytes
            }
        }
    };
    Ok(padded_value.floor())
}

/// Returns the difference of the minimum raw value and 1 or the equivalent
/// of 1 in MiB or GiB in bytes. If the minimum is less than 1, we'll use
/// 0 as the minimum value as we don't expect any of our charts to require
/// negative numbers for now.
fn padded_min_value_as_unit(unit: Unit, min_value_raw: &f64) -> Result<f64> {
    let padded_value = match unit {
        Unit::Count => {
            if *min_value_raw < 1.0 {
                0.0
            } else {
                min_value_raw - 1.0
            }
        }
        Unit::Gibibyte | Unit::Mebibyte => {
            let value_as_unit = min_value_raw / unit.as_bytes_f64()?;
            if value_as_unit < 1.0 {
                0.0
            } else {
                value_as_unit - 1.0
            }
        }
    };
    Ok(padded_value.floor())
}

/// Returns the average value of the max and min values.
fn avg(min_value_label: &f64, max_value_label: &f64) -> f64 {
    (min_value_label + max_value_label) / 2.0
}

#[derive(Debug)]
struct YAxisValues {
    lower_label: f64,
    mid_label: f64,
    upper_label: f64,
    lower_bound: f64,
    upper_bound: f64,
}

impl YAxisValues {
    fn new(unit: Unit, raw_data: &Vec<SystemTimeSeries>) -> Result<Self> {
        // Retrieve values only to create Y axis bounds and labels
        let values = TimeSeriesValues::new(&raw_data);
        let max_value = values.max()?;
        let min_value = values.min()?;

        // In case there is very little variance in the y axis, we will be adding some
        // padding to the bounds and labels so we don't end up with repeated labels or
        // straight lines too close to the upper bounds.
        let upper_bound = padded_max_value_raw(unit, max_value)?;
        let upper_label = padded_max_value_as_unit(unit, max_value)?;
        let lower_bound = padded_min_value_raw(unit, min_value)?;
        let lower_label = padded_min_value_as_unit(unit, min_value)?;
        let mid_label = avg(&lower_label, &upper_label);

        Ok(Self {
            lower_label,
            mid_label,
            upper_label,
            lower_bound,
            upper_bound,
        })
    }
}

#[derive(Debug)]
struct TimeSeriesTimestamps {
    timestamps: Vec<i64>,
}

impl TimeSeriesTimestamps {
    fn new(raw_data: &Vec<SystemTimeSeries>) -> Self {
        let timestamps: Vec<i64> = raw_data
            .iter()
            .map(|ts| {
                ts.time.trim_matches('"').parse::<i64>().unwrap_or_else(|_| {
                    panic!("could not parse timestamp {} into i64", ts.time)
                })
            })
            .collect();
        Self { timestamps }
    }

    fn min(&self) -> Result<&i64> {
        let Some(start_time) = self.timestamps.iter().min() else {
            bail!("failed to retrieve start time, timestamp list is empty")
        };
        Ok(start_time)
    }

    fn max(&self) -> Result<&i64> {
        let Some(end_time) = self.timestamps.iter().max() else {
            bail!("failed to retrieve end time, timestamp list is empty")
        };
        Ok(end_time)
    }

    fn avg(&self, start_time: &i64, end_time: &i64) -> i64 {
        (start_time + end_time) / 2
    }
}

#[derive(Debug)]
pub struct XAxisTimestamps {
    mid_time_label: DateTime<Utc>,
    pub start_time_label: DateTime<Utc>,
    pub end_time_label: DateTime<Utc>,
    start_time_bound: f64,
    end_time_bound: f64,
}

impl XAxisTimestamps {
    fn new(raw_data: &Vec<SystemTimeSeries>) -> Result<Self> {
        // Retrieve timestamps only to create chart bounds and labels
        let timestamps = TimeSeriesTimestamps::new(&raw_data);
        // These timestamps will be used to calculate start and end timestamps in order
        // to create labels and set bounds for the X axis. As above, some of these conversions
        // may lose precision, but it's OK as these values are only used to make sure the
        // datapoints fit within the graph nicely.
        let start_time = timestamps.min()?;
        let end_time = timestamps.max()?;
        let avg_time = timestamps.avg(start_time, end_time);

        let Some(start_time_label) = DateTime::from_timestamp(*start_time, 0)
        else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                start_time
            )
        };
        let Some(end_time_label) = DateTime::from_timestamp(*end_time, 0)
        else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                end_time
            )
        };
        let Some(mid_time_label) = DateTime::from_timestamp(avg_time, 0) else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                avg_time
            )
        };

        let start_time_bound = *start_time as f64;
        let end_time_bound = *end_time as f64;

        Ok(Self {
            mid_time_label,
            start_time_label,
            end_time_label,
            start_time_bound,
            end_time_bound,
        })
    }
}

#[derive(Debug)]
struct DataPoints {
    data: Vec<(f64, f64)>,
}

impl DataPoints {
    fn new(timeseries: &Vec<SystemTimeSeries>) -> Self {
        // These values will be used to render the graph and ratatui
        // requires them to be f64
        let data: Vec<(f64, f64)> = timeseries
            .iter()
            .map(|ts| {
                (
                    ts.time.trim_matches('"').parse::<f64>().unwrap_or_else(
                        |_| {
                            panic!(
                                "could not parse timestamp {} into f64",
                                ts.time
                            )
                        },
                    ),
                    ts.value,
                )
            })
            .collect();
        Self { data }
    }
}

#[derive(Debug)]
pub struct ChartData {
    metadata: ChartMetadata,
    data_points: DataPoints,
    pub x_axis_timestamps: XAxisTimestamps,
    y_axis_values: YAxisValues,
}

impl ChartData {
    pub fn new(
        raw_data: Vec<SystemTimeSeries>,
        metadata: ChartMetadata,
    ) -> Result<Self> {
        // Retrieve datapoints that will be charted
        let data_points = DataPoints::new(&raw_data);

        // Retrieve X axis bounds and labels
        let x_axis_timestamps = XAxisTimestamps::new(&raw_data)?;

        // Retrieve X axis bounds and labels
        let y_axis_values = YAxisValues::new(metadata.unit, &raw_data)?;

        Ok(Self { metadata, data_points, x_axis_timestamps, y_axis_values })
    }

    pub fn render_line_chart(&self, frame: &mut Frame, area: Rect) {
        let datasets = vec![Dataset::default()
            .marker(Marker::Braille)
            .style(Style::default().fg(Color::LightGreen))
            .graph_type(GraphType::Line)
            .data(&self.data_points.data)];

        // TODO: Put this somewhere else?
        //
        // To nicely display the mid value label for the Y axis, we do the following:
        // - It is not displayed it if it is a 0.0.
        // - If it does not have a fractional number we display it as an integer.
        // - Else, we display the number as is up to the first fractional number.
        let mid_value = self.y_axis_values.mid_label;
        let fractional_of_mid_value = mid_value - mid_value.floor();
        let mid_value_formatted = format!("{:.1}", mid_value);
        let y_axis_mid_label = if mid_value_formatted == *"0.0" {
            "".to_string()
        } else if fractional_of_mid_value == 0.0 {
            mid_value_formatted.split('.').next().unwrap().to_string()
        } else {
            mid_value_formatted
        };

        let chart = Chart::new(datasets)
            .block(
                Block::bordered().title(
                    Line::from(self.metadata.title.clone())
                        .cyan()
                        .bold()
                        .centered(),
                ),
            )
            .x_axis(
                Axis::default()
                    .style(Style::default().gray())
                    .bounds([
                        self.x_axis_timestamps.start_time_bound,
                        self.x_axis_timestamps.end_time_bound,
                    ])
                    .labels([
                        format!(
                            "{}",
                            self.x_axis_timestamps.start_time_label.time()
                        )
                        .bold(),
                        format!(
                            "{}",
                            self.x_axis_timestamps.mid_time_label.time()
                        )
                        .bold(),
                        format!(
                            "{}",
                            self.x_axis_timestamps.end_time_label.time()
                        )
                        .bold(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().gray())
                    .bounds([
                        self.y_axis_values.lower_bound,
                        self.y_axis_values.upper_bound,
                    ])
                    .labels([
                        format!(
                            "{} {}",
                            self.y_axis_values.lower_label, self.metadata.unit
                        )
                        .bold(),
                        // TODO: Only show fractional number if not .0 ?
                        format!("{} {}", y_axis_mid_label, self.metadata.unit)
                            .bold(),
                        format!(
                            "{} {}",
                            self.y_axis_values.upper_label, self.metadata.unit
                        )
                        .bold(),
                    ]),
            )
            .legend_position(Some(LegendPosition::TopLeft))
            .hidden_legend_constraints((
                Constraint::Ratio(1, 2),
                Constraint::Ratio(1, 2),
            ));

        frame.render_widget(chart, area);
    }
}
