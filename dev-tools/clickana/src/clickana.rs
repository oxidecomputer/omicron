// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, NaiveTime, Utc};
use clickhouse_admin_server_client::types::SystemTimeSeries;
use clickhouse_admin_server_client::{types, Client as ClickhouseServerClient};
use omicron_common::FileKv;
use ratatui::{
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    symbols::{self, Marker},
    text::{Line, Span},
    widgets::{Axis, Block, Chart, Dataset, GraphType, LegendPosition},
    DefaultTerminal, Frame,
};
use serde::Deserialize;
use slog::{info, o, Drain};
use slog_async::Async;
use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
use tokio::runtime::Runtime;

const GIBIBYTE: f64 = 1073741824.0;

pub struct Clickana {}

impl Clickana {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run(self, mut terminal: DefaultTerminal) -> Result<()> {
        // Refresh after one minute
        let tick_rate = Duration::from_secs(60);
        let mut last_tick = Instant::now();
        loop {
            let dashboard = DashboardData::new(get_api_data()?)?;
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

    fn draw(&self, frame: &mut Frame, dashboard: DashboardData) {
        // let [_top, bottom] = Layout::vertical([Constraint::Fill(1); 2]).areas(frame.area());
        // let [animated_chart, bar_chart] =
        //     Layout::horizontal([Constraint::Fill(1), Constraint::Length(29)]).areas(top);
        let [all] =
            Layout::vertical([Constraint::Fill(1); 1]).areas(frame.area());
        // let [line_chart, scatter] = Layout::horizontal([Constraint::Fill(1); 2]).areas(all);

        let [line_chart] =
            Layout::horizontal([Constraint::Fill(1); 1]).areas(all);

        dashboard.render_line_chart(frame, line_chart);
    }
}

fn log_path() -> Result<Utf8PathBuf> {
    // TODO: Add a log path via env vars? or maybe a flag
    // Maybe just find the temp directory
    match std::env::var("CLICKANA_LOG_PATH") {
        Ok(path) => Ok(path.into()),
        Err(std::env::VarError::NotPresent) => Ok("/tmp/clickana.log".into()),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("CLICKANA_LOG_PATH is not valid unicode");
        }
    }
}

fn new_logger(path: &Utf8Path) -> anyhow::Result<slog::Logger> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("error opening log file {path}"))?;

    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let drain = slog_async::Async::new(drain).build().fuse();

    Ok(slog::Logger::root(drain, slog::o!(FileKv)))
}

fn get_api_data() -> Result<Vec<SystemTimeSeries>> {
    let rt = Runtime::new()?;
    // TODO: Take address from a flag
    let admin_url = format!("http://[::1]:8888");
    let log = new_logger(&log_path()?)?;

    let client = ClickhouseServerClient::new(&admin_url, log.clone());
    let result = rt.block_on(async {
        let timeseries = client
            .system_timeseries_avg(
                types::SystemTable::AsynchronousMetricLog,
                "DiskUsed_default",
                // TODO: Take interval and time_range from flag
                Some(120),
                Some(3600),
                Some(types::TimestampFormat::UnixEpoch),
            )
            .await
            .map(|t| t.into_inner()) //;
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

#[derive(Debug)]
struct DashboardData {
    data_points: Vec<(f64, f64)>,
    avg_time_utc: DateTime<Utc>,
    start_time_utc: DateTime<Utc>,
    end_time_utc: DateTime<Utc>,
    start_time_unix: f64,
    end_time_unix: f64,
    avg_value_gib: u64,
    min_value_gib: u64,
    max_value_gib: u64,
    min_value_bytes: f64,
    max_value_bytes: f64,
}

impl DashboardData {
    fn new(raw_data: Vec<SystemTimeSeries>) -> Result<Self> {
        // These values will be used to render the graph and ratatui
        // requires them to be f64
        let data_points: Vec<(f64, f64)> = raw_data
            .iter()
            .map(|ts| {
                (
                    ts.time.trim_matches('"').parse::<f64>().expect(&format!(
                        "could not parse timestamp {} into f64",
                        ts.time
                    )),
                    ts.value,
                )
            })
            .collect();

        // These values will be used to calculate maximum and minimum values in order
        // to create labels and set bounds for the Y axis.
        let values: Vec<f64> = raw_data.iter().map(|ts| ts.value).collect();
        let min_value_bytes = values
            .iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap()
            .floor();
        let max_value_bytes = values
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap()
            .ceil();

        // The result of these calculations will not be precise, but it doesn't matter since we just
        // want an estimate for the labels
        let min_value_gib = (min_value_bytes / GIBIBYTE).floor() as u64;
        let max_value_gib = (max_value_bytes / GIBIBYTE).ceil() as u64;
        let avg_value_gib = (((min_value_bytes + max_value_bytes) / 2.0)
            / GIBIBYTE)
            .round() as u64;

        // These timestamps will be used to calculate maximum and minimum values in order
        // to create labels and set bounds for the X axis.
        let timestamps: Vec<i64> = raw_data
            .iter()
            .map(|ts| {
                ts.time.trim_matches('"').parse::<i64>().expect(&format!(
                    "could not parse timestamp {} into f64",
                    ts.time
                ))
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

        let Some(avg_time_utc) = DateTime::from_timestamp(avg_time, 0) else {
            bail!(
                "failed to convert timestamp to UTC date and time;
        timestamp = {}",
                avg_time
            )
        };

        let start_time_unix = *start_time as f64;
        let end_time_unix = *end_time as f64;

        Ok(Self {
            data_points,
            avg_time_utc,
            start_time_utc,
            end_time_utc,
            start_time_unix,
            end_time_unix,
            avg_value_gib,
            min_value_gib,
            max_value_gib,
            min_value_bytes,
            max_value_bytes,
        })
    }

    fn render_line_chart(&self, frame: &mut Frame, area: Rect) {
        let datasets = vec![Dataset::default()
            .name("DiskUsage per minute".italic())
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::Yellow))
            .graph_type(GraphType::Line)
            .data(&self.data_points)];

        let chart = Chart::new(datasets)
            .block(
                Block::bordered().title(
                    Line::from(format!("Disk max {}", self.max_value_bytes))
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
                        format!("{}", self.start_time_utc).bold(),
                        format!("{}", self.avg_time_utc).bold(),
                        format!("{}", self.end_time_utc).bold(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().gray())
                    .bounds([self.min_value_bytes, self.max_value_bytes])
                    .labels([
                        format!("{} GiB", self.min_value_gib).bold(),
                        format!("{} GiB", self.avg_value_gib).bold(),
                        format!("{} GiB", self.max_value_gib).bold(),
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
