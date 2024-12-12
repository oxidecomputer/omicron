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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    /// Returns the associated table to query for each metric.
    pub fn table(&self) -> SystemTable {
        match self {
            MetricName::DiskUsage => SystemTable::AsynchronousMetricLog,
            MetricName::MemoryTracking
            | MetricName::QueryCount
            | MetricName::RunningQueries => SystemTable::MetricLog,
        }
    }

    /// Returns the unit the data values will be represented as.
    fn unit(&self) -> Unit {
        match self {
            MetricName::DiskUsage => Unit::Gibibyte,
            MetricName::MemoryTracking => Unit::Mebibyte,
            MetricName::QueryCount | MetricName::RunningQueries => Unit::Count,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChartMetadata {
    pub title: String,
    pub unit: Unit,
}

impl ChartMetadata {
    pub fn new(metric: MetricName, title: String) -> Self {
        let unit = metric.unit();
        Self { title, unit }
    }
}

#[derive(Debug)]
struct TimeSeriesValues {
    /// A collection of all the values from the timeseries
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

    /// Returns the average value of the max and min values.
    fn avg(&self, min_value_label: f64, max_value_label: f64) -> f64 {
        (min_value_label + max_value_label) / 2.0
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

#[derive(Debug, PartialEq)]
struct YAxisValues {
    lower_label: String,
    mid_label: String,
    upper_label: String,
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
        let upper_label_as_unit = padded_max_value_as_unit(unit, max_value)?;
        let lower_bound = padded_min_value_raw(unit, min_value)?;
        let lower_label_as_unit = padded_min_value_as_unit(unit, min_value)?;
        let mid_label_as_unit =
            values.avg(lower_label_as_unit, upper_label_as_unit);

        // To nicely display the mid value label for the Y axis, we do the following:
        // - It is not displayed it if it is a 0.0.
        // - If it does not have a fractional number we display it as an integer.
        // - Else, we display the number as is up to the first fractional number.
        //let mid_value = mid_label;
        let fractional_of_mid_value =
            mid_label_as_unit - mid_label_as_unit.floor();
        let mid_value_formatted = format!("{:.1}", mid_label_as_unit);
        let mid_label = if mid_value_formatted == *"0.0" {
            "".to_string()
        } else if fractional_of_mid_value == 0.0 {
            format!("{} {}", mid_value_formatted.split('.').next().unwrap(), unit)
        } else {
            format!("{} {}", mid_value_formatted, unit)
        };

        let upper_label = format!("{} {}", upper_label_as_unit, unit);
        let lower_label = format!("{} {}", lower_label_as_unit, unit);

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
    /// A collection of all the timestamps from the timeseries
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

        let chart = Chart::new(datasets)
            .block(
                Block::bordered()
                    .title(Line::from(self.title()).cyan().bold().centered()),
            )
            .x_axis(
                Axis::default()
                    .style(Style::default().gray())
                    .bounds([self.start_time_bound(), self.end_time_bound()])
                    .labels([
                        self.start_time_label().bold(),
                        self.mid_time_label().bold(),
                        self.end_time_label().bold(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().gray())
                    .bounds([
                        self.lower_value_bound(),
                        self.upper_value_bound(),
                    ])
                    .labels([
                        self.lower_value_label().bold(),
                        self.mid_value_label().bold(),
                        self.upper_value_label().bold(),
                    ]),
            )
            .legend_position(Some(LegendPosition::TopLeft))
            .hidden_legend_constraints((
                Constraint::Ratio(1, 2),
                Constraint::Ratio(1, 2),
            ));

        frame.render_widget(chart, area);
    }

    fn title(&self) -> String {
        self.metadata.title.clone()
    }

    pub fn start_date_time(&self) -> DateTime<Utc> {
        self.x_axis_timestamps.start_time_label
    }

    pub fn end_date_time(&self) -> DateTime<Utc> {
        self.x_axis_timestamps.end_time_label
    }
    
    fn start_time_label(&self) -> String {
        self.x_axis_timestamps.start_time_label.time().to_string()
    }

    fn mid_time_label(&self) -> String {
        self.x_axis_timestamps.mid_time_label.time().to_string()
    }

    fn end_time_label(&self) -> String {
        self.x_axis_timestamps.end_time_label.time().to_string()
    }

    fn start_time_bound(&self) -> f64 {
        self.x_axis_timestamps.start_time_bound
    }

    fn end_time_bound(&self) -> f64 {
        self.x_axis_timestamps.end_time_bound
    }

    fn lower_value_label(&self) -> String {
        self.y_axis_values.lower_label.clone()
    }

    fn mid_value_label(&self) -> String {
        self.y_axis_values.mid_label.clone()
    }

    fn upper_value_label(&self) -> String {
        self.y_axis_values.upper_label.clone()
    }

    fn lower_value_bound(&self) -> f64 {
        self.y_axis_values.lower_bound
    }

    fn upper_value_bound(&self) -> f64 {
        self.y_axis_values.upper_bound
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        chart::{Unit, YAxisValues},
        ChartData, ChartMetadata, MetricName,
    };
    use chrono::DateTime;
    use clickhouse_admin_server_client::types::SystemTimeSeries;

    use super::{DataPoints, XAxisTimestamps};

    #[test]
    fn gather_chart_data_for_disk_usage_success() {
        let metadata =
            ChartMetadata::new(MetricName::DiskUsage, "Test Chart".to_string());
        let raw_data = vec![
            SystemTimeSeries {
                time: "1732223400".to_string(),
                value: 479551511587.3104,
            },
            SystemTimeSeries {
                time: "1732223520".to_string(),
                value: 479555459822.93335,
            },
            SystemTimeSeries {
                time: "1732223640".to_string(),
                value: 479560290201.6,
            },
        ];

        let expected_result = ChartData {
            metadata: ChartMetadata {
                title: "Test Chart".to_string(),
                unit: Unit::Gibibyte,
            },
            data_points: DataPoints {
                data: vec![
                    (1732223400.0, 479551511587.3104),
                    (1732223520.0, 479555459822.93335),
                    (1732223640.0, 479560290201.6),
                ],
            },
            x_axis_timestamps: XAxisTimestamps {
                start_time_label: DateTime::from_timestamp(1732223400, 0)
                    .unwrap(),
                mid_time_label: DateTime::from_timestamp(1732223520, 0)
                    .unwrap(),
                end_time_label: DateTime::from_timestamp(1732223640, 0)
                    .unwrap(),
                start_time_bound: 1732223400.0,
                end_time_bound: 1732223640.0,
            },
            y_axis_values: YAxisValues {
                lower_label: "445 GiB".to_string(),
                mid_label: "446.5 GiB".to_string(),
                upper_label: "448 GiB".to_string(),
                lower_bound: 478477769763.0,
                upper_bound: 480634032026.0,
            },
        };
        let result = ChartData::new(raw_data, metadata).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn gather_chart_data_for_memory_tracking_success() {
        let metadata =
            ChartMetadata::new(MetricName::MemoryTracking, "Test Chart".to_string());
        let raw_data = vec![
            SystemTimeSeries {
                time: "1732223400".to_string(),
                value: 479551511587.3104,
            },
            SystemTimeSeries {
                time: "1732223520".to_string(),
                value: 479555459822.93335,
            },
            SystemTimeSeries {
                time: "1732223640".to_string(),
                value: 479560290201.6,
            },
        ];

        let expected_result = ChartData {
            metadata: ChartMetadata {
                title: "Test Chart".to_string(),
                unit: Unit::Mebibyte,
            },
            data_points: DataPoints {
                data: vec![
                    (1732223400.0, 479551511587.3104),
                    (1732223520.0, 479555459822.93335),
                    (1732223640.0, 479560290201.6),
                ],
            },
            x_axis_timestamps: XAxisTimestamps {
                start_time_label: DateTime::from_timestamp(1732223400, 0)
                    .unwrap(),
                mid_time_label: DateTime::from_timestamp(1732223520, 0)
                    .unwrap(),
                end_time_label: DateTime::from_timestamp(1732223640, 0)
                    .unwrap(),
                start_time_bound: 1732223400.0,
                end_time_bound: 1732223640.0,
            },
            y_axis_values: YAxisValues {
                lower_label: "457334 MiB".to_string(),
                mid_label: "457340 MiB".to_string(),
                upper_label: "457346 MiB".to_string(),
                lower_bound: 479550463011.0,
                upper_bound: 479561338778.0,
            },
        };
        let result = ChartData::new(raw_data, metadata).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn gather_chart_data_for_query_count_success() {
        let metadata =
            ChartMetadata::new(MetricName::QueryCount, "Test Chart".to_string());
        let raw_data = vec![
            SystemTimeSeries {
                time: "1732223400".to_string(),
                value: 0.0,
            },
            SystemTimeSeries {
                time: "1732223520".to_string(),
                value: 0.004,
            },
            SystemTimeSeries {
                time: "1732223640".to_string(),
                value: 0.0,
            },
        ];

        let expected_result = ChartData {
            metadata: ChartMetadata {
                title: "Test Chart".to_string(),
                unit: Unit::Count,
            },
            data_points: DataPoints {
                data: vec![
                    (1732223400.0, 0.0),
                    (1732223520.0, 0.004),
                    (1732223640.0, 0.0),
                ],
            },
            x_axis_timestamps: XAxisTimestamps {
                start_time_label: DateTime::from_timestamp(1732223400, 0)
                    .unwrap(),
                mid_time_label: DateTime::from_timestamp(1732223520, 0)
                    .unwrap(),
                end_time_label: DateTime::from_timestamp(1732223640, 0)
                    .unwrap(),
                start_time_bound: 1732223400.0,
                end_time_bound: 1732223640.0,
            },
            y_axis_values: YAxisValues {
                lower_label: "0 ".to_string(),
                mid_label: "1 ".to_string(),
                upper_label: "2 ".to_string(),
                lower_bound: 0.0,
                upper_bound: 2.0,
            },
        };
        let result = ChartData::new(raw_data, metadata).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn gather_chart_data_for_running_queries_success() {
        let metadata =
            ChartMetadata::new(MetricName::RunningQueries, "Test Chart".to_string());
        let raw_data = vec![
            SystemTimeSeries {
                time: "1732223400".to_string(),
                value: 1.554,
            },
            SystemTimeSeries {
                time: "1732223520".to_string(),
                value: 1.877,
            },
            SystemTimeSeries {
                time: "1732223640".to_string(),
                value: 1.3456,
            },
        ];

        let expected_result = ChartData {
            metadata: ChartMetadata {
                title: "Test Chart".to_string(),
                unit: Unit::Count,
            },
            data_points: DataPoints {
                data: vec![
                    (1732223400.0, 1.554),
                    (1732223520.0, 1.877),
                    (1732223640.0, 1.3456),
                ],
            },
            x_axis_timestamps: XAxisTimestamps {
                start_time_label: DateTime::from_timestamp(1732223400, 0)
                    .unwrap(),
                mid_time_label: DateTime::from_timestamp(1732223520, 0)
                    .unwrap(),
                end_time_label: DateTime::from_timestamp(1732223640, 0)
                    .unwrap(),
                start_time_bound: 1732223400.0,
                end_time_bound: 1732223640.0,
            },
            y_axis_values: YAxisValues {
                lower_label: "0 ".to_string(),
                mid_label: "1.5 ".to_string(),
                upper_label: "3 ".to_string(),
                lower_bound: 0.0,
                upper_bound: 3.0,
            },
        };
        let result = ChartData::new(raw_data, metadata).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    #[should_panic(
        expected = "could not parse timestamp Some nonsense string into f64"
    )]
    fn gather_chart_data_failure() {
        let metadata =
            ChartMetadata::new(MetricName::DiskUsage, "Test Chart".to_string());
        let raw_data = vec![
            SystemTimeSeries {
                time: "Some nonsense string".to_string(),
                value: 479551511587.3104,
            },
            SystemTimeSeries {
                time: "1732223520".to_string(),
                value: 479555459822.93335,
            },
            SystemTimeSeries {
                time: "1732223640".to_string(),
                value: 479560290201.6,
            },
        ];

        let _ = ChartData::new(raw_data, metadata);
    }
}
