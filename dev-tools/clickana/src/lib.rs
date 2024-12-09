// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, bail, Context, Result};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clickhouse_admin_server_client::types::{
    SystemTable, SystemTimeSeries, TimestampFormat,
};
use clickhouse_admin_server_client::Client as ClickhouseServerClient;
use omicron_common::FileKv;
use ratatui::{
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::Marker,
    text::Line,
    widgets::{Axis, Block, Chart, Dataset, GraphType, LegendPosition},
    DefaultTerminal, Frame,
};
use slog::{o, Drain, Logger};
use slog_async::Async;
use slog_term::{FullFormat, PlainDecorator};
use std::fmt::Display;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const GIBIBYTE_F64: f64 = 1073741824.0;
const MEBIBYTE_F64: f64 = 1048576.0;

#[derive(Debug)]
enum Unit {
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
    
    // TODO: Should all of these be f64?

    // The result of the following functions will not be precise, but it doesn't
    // matter since we just want an estimate for the chart's labels and bounds.
    // all we need are values that are larger than the maximum value in the
    // timeseries or smaller than the minimum value in the timeseries.

    /// Returns the sum of the maximum raw value and 1 or the equivalent of 1
    /// MiB or GiB in bytes.
    fn padded_max_value_raw(&self, max_value_raw: &f64) -> Result<f64> {
        let ceil_value = max_value_raw.ceil();
        let padded_value = match self {
            Unit::Count => ceil_value + 1.0,
            Unit::Gibibyte | Unit::Mebibyte => {
                ceil_value + self.as_bytes_f64()?
            }
        };
        Ok(padded_value)
    }

    /// Returns the sum of the max raw value and 1 or the equivalent of 1
    /// Mib or Gib.
    fn padded_max_value_unit(&self, max_value_raw: &f64) -> Result<f64> {
        let label_value = match self {
            Unit::Count => max_value_raw + 1.0,
            Unit::Gibibyte | Unit::Mebibyte => {
                (max_value_raw / self.as_bytes_f64()?) + 1.0
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
    fn padded_min_value_raw(&self, min_value_raw: &f64) -> Result<f64> {
        let padded_value = match self {
            Unit::Count => {
                if *min_value_raw <= 1.0 {
                    0.0
                } else {
                    min_value_raw - 1.0
                }
            }
            Unit::Gibibyte | Unit::Mebibyte => {
                let bytes = self.as_bytes_f64()?;
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
    fn padded_min_value_unit(&self, min_value_raw: &f64) -> Result<f64> {
        let padded_value = match self {
            Unit::Count => {
                if *min_value_raw <= 1.0 {
                    0.0
                } else {
                    min_value_raw - 1.0
                }
            }
            Unit::Gibibyte | Unit::Mebibyte => {
                let value_as_unit = min_value_raw / self.as_bytes_f64()?;
                if value_as_unit <= 1.0 {
                    0.0
                } else {
                    value_as_unit - 1.0
                }
            }
        };
        Ok(padded_value.floor())
    }

    fn avg_value_unit(
        &self,
        min_value_raw: &f64,
        max_value_raw: &f64,
    ) -> Result<f64> {
        let avg = ((min_value_raw + max_value_raw) / 2.0).round();
        let avg_value = match self {
            Unit::Count => avg,
            Unit::Gibibyte | Unit::Mebibyte => avg / self.as_bytes_f64()?,
        };
        Ok(avg_value)
    }
}

#[derive(Clone, Copy, Debug)]
enum MetricName {
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
    fn table(&self) -> SystemTable {
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
struct ChartMetadata {
    metric: MetricName,
    title: String,
    unit: Unit,
}

impl ChartMetadata {
    fn new(metric: MetricName, title: String) -> Self {
        let unit = metric.unit();
        Self { metric, title, unit }
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
struct ChartData {
    metadata: ChartMetadata,
    data_points: DataPoints,
    mid_time_utc: DateTime<Utc>,
    start_time_utc: DateTime<Utc>,
    end_time_utc: DateTime<Utc>,
    start_time_unix: f64,
    end_time_unix: f64,
    lower_label_value: f64,
    mid_label_value: f64,
    upper_label_value: f64,
    lower_bound_value: f64,
    upper_bound_value: f64,
}

impl ChartData {
    fn new(
        raw_data: Vec<SystemTimeSeries>,
        metadata: ChartMetadata,
    ) -> Result<Self> {
        // Retrieve datapoints that will be charted
        let data_points = DataPoints::new(&raw_data);

        // Retrieve values only to create chart bouds and labels
        let values = TimeSeriesValues::new(&raw_data);
        let max_value = values.max()?;
        let min_value = values.min()?;

        // In case there is very little variance in the y axis, we will be adding some
        // padding to the bounds and labels so we don't end up with repeated labels or
        // straight lines too close to the upper bounds.
        let upper_bound_value =
            metadata.unit.padded_max_value_raw(max_value)?;
        let upper_label_value =
            metadata.unit.padded_max_value_unit(max_value)?;
        let lower_bound_value =
            metadata.unit.padded_min_value_raw(min_value)?;
        let lower_label_value =
            metadata.unit.padded_min_value_unit(min_value)?;
        let mid_label_value =
            metadata.unit.avg_value_unit(min_value, max_value)?;

        // These timestamps will be used to calculate maximum and minimum values in order
        // to create labels and set bounds for the X axis. As above, some of these conversions
        // may lose precision, but it's OK as these values are only used to make sure the
        // datapoints fit within the graph nicely.
        let timestamps: Vec<i64> = raw_data
            .iter()
            .map(|ts| {
                ts.time.trim_matches('"').parse::<i64>().unwrap_or_else(|_| {
                    panic!("could not parse timestamp {} into i64", ts.time)
                })
            })
            .collect();

        let Some(start_time) = timestamps.iter().min() else {
            bail!("failed to retrieve start time, timestamp list is empty")
        };
        let Some(end_time) = timestamps.iter().max() else {
            bail!("failed to retrieve end time, timestamp list is empty")
        };
        let avg_time = (*start_time + *end_time) / 2;
        let Some(start_time_utc) = DateTime::from_timestamp(*start_time, 0)
        else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                start_time
            )
        };
        let Some(end_time_utc) = DateTime::from_timestamp(*end_time, 0) else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                end_time
            )
        };
        let Some(mid_time_utc) = DateTime::from_timestamp(avg_time, 0) else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                avg_time
            )
        };

        let start_time_unix = *start_time as f64;
        let end_time_unix = *end_time as f64;

        Ok(Self {
            metadata,
            data_points,
            mid_time_utc,
            start_time_utc,
            end_time_utc,
            start_time_unix,
            end_time_unix,
            lower_label_value,
            mid_label_value,
            upper_label_value,
            lower_bound_value,
            upper_bound_value,
        })
    }

    fn render_line_chart(&self, frame: &mut Frame, area: Rect) {
        let datasets = vec![Dataset::default()
            .marker(Marker::Braille)
            .style(Style::default().fg(Color::LightGreen))
            .graph_type(GraphType::Line)
            .data(&self.data_points.data)];

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
                    .bounds([self.start_time_unix, self.end_time_unix])
                    .labels([
                        // TODO: Remove start time and print the interval at the top of the
                        // dashboard
                        format!("{}", self.start_time_utc).bold(),
                        format!("{}", self.mid_time_utc).bold(),
                        format!("{}", self.end_time_utc).bold(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().gray())
                    .bounds([self.lower_bound_value, self.upper_bound_value])
                    .labels([
                        format!(
                            "{} {}",
                            self.lower_label_value, self.metadata.unit
                        )
                        .bold(),
                        // TODO: Only show fractional number if not .0 ?
                        format!(
                            "{:.1} {}",
                            self.mid_label_value, self.metadata.unit
                        )
                        .bold(),
                        format!(
                            "{} {}",
                            self.upper_label_value, self.metadata.unit
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

pub struct Clickana {
    clickhouse_addr: SocketAddr,
    log_path: Utf8PathBuf,
    sampling_interval: u64,
    time_range: u64,
    refresh_interval: u64,
}

impl Clickana {
    pub fn new(
        clickhouse_addr: SocketAddr,
        log_path: Utf8PathBuf,
        sampling_interval: u64,
        time_range: u64,
        refresh_interval: u64,
    ) -> Self {
        Self {
            clickhouse_addr,
            log_path,
            sampling_interval,
            time_range,
            refresh_interval,
        }
    }

    pub fn run(self, mut terminal: DefaultTerminal) -> Result<()> {
        let tick_rate = Duration::from_secs(self.refresh_interval);
        let mut last_tick = Instant::now();
        loop {
            let top_left_frame_metadata = ChartMetadata::new(
                MetricName::DiskUsage,
                "Disk Usage".to_string(),
            );
            let top_left_frame = ChartData::new(
                self.get_api_data(top_left_frame_metadata.metric)?,
                top_left_frame_metadata,
            )?;
            let dashboard = Dashboard {
                top_left_frame,
                _top_right_frame: None,
                _bottom_left_frame: None,
                _bottom_right_frame: None,
            };
            terminal.draw(|frame| self.draw(frame, dashboard))?;

            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') {
                        return Ok(());
                    }
                }
            }

            if last_tick.elapsed() >= tick_rate {
                last_tick = Instant::now();
            }
        }
    }

    fn draw(&self, frame: &mut Frame, dashboard: Dashboard) {
        let [all] =
            Layout::vertical([Constraint::Fill(1); 1]).areas(frame.area());
        let [line_chart] =
            Layout::horizontal([Constraint::Fill(1); 1]).areas(all);

        dashboard.top_left_frame.render_line_chart(frame, line_chart);
    }

    fn get_api_data(
        &self,
        metric: MetricName,
    ) -> Result<Vec<SystemTimeSeries>> {
        let admin_url = format!("http://{}", self.clickhouse_addr);
        let log = self.new_logger()?;
        let client = ClickhouseServerClient::new(&admin_url, log.clone());

        let rt = Runtime::new()?;
        let result = rt.block_on(async {
            let timeseries = client
                .system_timeseries_avg(
                    metric.table(),
                    &format!("{metric}"),
                    Some(self.sampling_interval),
                    Some(self.time_range),
                    Some(TimestampFormat::UnixEpoch),
                )
                .await
                .map(|t| t.into_inner())
                .map_err(|e| {
                    anyhow!(
                        concat!(
                "failed to retrieve timeseries from clickhouse server; ",
                "admin_url = {} error = {}",
            ),
                        admin_url,
                        e
                    )
                });

            timeseries
        })?;

        Ok(result)
    }

    fn new_logger(&self) -> Result<Logger> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.log_path.clone())
            .with_context(|| {
                format!("error opening log file {}", self.log_path)
            })?;

        let decorator = PlainDecorator::new(file);
        let drain = FullFormat::new(decorator).build().fuse();
        let drain = Async::new(drain).build().fuse();

        Ok(slog::Logger::root(drain, o!(FileKv)))
    }
}

#[derive(Debug)]
struct Dashboard {
    top_left_frame: ChartData,
    _top_right_frame: Option<ChartData>,
    _bottom_left_frame: Option<ChartData>,
    _bottom_right_frame: Option<ChartData>,
    // Add more charts
}

#[allow(dead_code)]
fn test_data() -> Result<Vec<SystemTimeSeries>, serde_json::Error> {
    let data = r#"
[
{
  "time": "1732223400",
  "value": 479551511587.3104
},
{
  "time": "1732223520",
  "value": 479555459822.93335
},
{
  "time": "1732223640",
  "value": 479560290201.6
},
{
  "time": "1732223760",
  "value": 469566801510.4
},
{
  "time": "1732223880",
  "value": 479587460778.6667
},
{
  "time": "1732224000",
  "value": 479618897442.13336
},
{
  "time": "1732224120",
  "value": 479649160567.4667
},
{
  "time": "1732224240",
  "value": 479677700300.8
},
{
  "time": "1732224360",
  "value": 479700512324.26666
},
{
  "time": "1732224480",
  "value": 479707099818.6667
},
{
  "time": "1732224600",
  "value": 479884974080.0
},
{
  "time": "1732224720",
  "value": 479975529779.2
},
{
  "time": "1732224840",
  "value": 479975824896.0
},
{
  "time": "1732224960",
  "value": 479976462062.93335
},
{
  "time": "1732225080",
  "value": 479986014242.13336
},
{
  "time": "1732225200",
  "value": 480041533235.2
},
{
  "time": "1732225320",
  "value": 480072114790.4
},
{
  "time": "1732225440",
  "value": 480097851050.6667
},
{
  "time": "1732225560",
  "value": 480138863854.93335
},
{
  "time": "1732225680",
  "value": 480178496648.5333
},
{
  "time": "1732225800",
  "value": 480196185941.3333
},
{
  "time": "1732225920",
  "value": 480208033792.0
},
{
  "time": "1732226040",
  "value": 480215815953.06665
},
{
  "time": "1732226160",
  "value": 480228655308.8
},
{
  "time": "1732226280",
  "value": 480237302749.86664
},
{
  "time": "1732226400",
  "value": 480251067016.5333
},
{
  "time": "1732226520",
  "value": 480239292381.86664
},
{
  "time": "1732226640",
  "value": 480886515029.3333
},
{
  "time": "1732226760",
  "value": 480663042423.4667
},
{
  "time": "1732226880",
  "value": 480213984085.3333
},
{
  "time": "1732227000",
  "value": 480265637816.1404
}
]
"#;

    serde_json::from_str(data)
}
