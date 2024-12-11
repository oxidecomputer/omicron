// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, Context, Result};
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use clickhouse_admin_server_client::types::{
    SystemTimeSeries, TimestampFormat,
};
use clickhouse_admin_server_client::Client as ClickhouseServerClient;
use omicron_common::FileKv;
use ratatui::crossterm::event::{self, Event, KeyCode};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::Paragraph;
use ratatui::{DefaultTerminal, Frame};
use slog::{o, Drain, Logger};
use slog_async::Async;
use slog_term::{FullFormat, PlainDecorator};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::chart::{ChartData, ChartMetadata, MetricName};

mod chart;

#[derive(Debug)]
struct Dashboard {
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    top_left_frame: ChartData,
    top_right_frame: ChartData,
    bottom_left_frame: ChartData,
    bottom_right_frame: ChartData,
    // TODO: Add more charts?
}

#[derive(Clone, Debug)]
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

    pub async fn run(self, mut terminal: DefaultTerminal) -> Result<()> {
        let admin_url = format!("http://{}", self.clickhouse_addr);
        let log = self.new_logger()?;
        let client = ClickhouseServerClient::new(&admin_url, log.clone());

        let tick_rate = Duration::from_secs(self.refresh_interval);
        let mut last_tick = Instant::now();
        loop {
            let charts = BTreeMap::from([
                (MetricName::DiskUsage, "Disk Usage".to_string()),
                (
                    MetricName::MemoryTracking,
                    "Memory Allocated by the Server".to_string(),
                ),
                (
                    MetricName::QueryCount,
                    "Queries Started per Second".to_string(),
                ),
                (MetricName::RunningQueries, "Queries Running".to_string()),
            ]);

            let mut tasks = Vec::new();

            for (metric_name, title) in charts {
                let s = self.clone();
                let c = client.clone();

                let task = tokio::spawn(async move {
                    s.populate_chart_data(c, metric_name, title).await
                });

                tasks.push(task);
            }

            let mut results = futures::future::join_all(tasks)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            // TODO: Eventually we may want to not have a set amount of charts and make the
            // dashboard a bit more dynamic. Perhaps taking a toml configuration file or
            // something like that. We can then create a vector of "ChartData"s for Dashboard
            // to take and create the layout dynamically.
            let top_left_frame: ChartData = results.remove(0)?;
            let top_right_frame: ChartData = results.remove(0)?;
            let bottom_left_frame: ChartData = results.remove(0)?;
            let bottom_right_frame: ChartData = results.remove(0)?;

            // We only need to retrieve from one chart as they will all be relatively the same.
            // Rarely, the charts may have a variance of a second or so depending on when
            // the API calls were made, but for the header block we don't need exact precision.
            let start_time = top_left_frame.x_axis_timestamps.start_time_label;
            let end_time = top_left_frame.x_axis_timestamps.end_time_label;

            let dashboard = Dashboard {
                start_time,
                end_time,
                top_left_frame,
                top_right_frame,
                bottom_left_frame,
                bottom_right_frame,
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

    async fn populate_chart_data(
        self,
        client: ClickhouseServerClient,
        metric_name: MetricName,
        title: String,
    ) -> Result<ChartData> {
        let metadata = ChartMetadata::new(metric_name, title);
        let data = self.get_api_data(&client, metric_name).await?;
        ChartData::new(data, metadata)
    }

    fn draw(&self, frame: &mut Frame, dashboard: Dashboard) {
        let [heading, top, bottom] = Layout::vertical([
            Constraint::Length(4),
            // TODO: If we make the dashboard with too many charts
            // we may want to reconsider setting sizes instead of filling
            // the space
            Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .areas(frame.area());
        let [title] =
            Layout::horizontal([Constraint::Fill(1); 1]).areas(heading);
        let [top_left_frame, top_right_frame] =
            Layout::horizontal([Constraint::Fill(1); 2]).areas(top);
        let [bottom_left_frame, bottom_right_frame] =
            Layout::horizontal([Constraint::Fill(1); 2]).areas(bottom);

        self.render_title_bar(frame, title, &dashboard);

        dashboard.top_left_frame.render_line_chart(frame, top_left_frame);
        dashboard.top_right_frame.render_line_chart(frame, top_right_frame);
        dashboard.bottom_left_frame.render_line_chart(frame, bottom_left_frame);
        dashboard
            .bottom_right_frame
            .render_line_chart(frame, bottom_right_frame);
    }

    fn render_title_bar(
        &self,
        frame: &mut Frame,
        area: Rect,
        dashboard: &Dashboard,
    ) {
        let style = Style::new().fg(Color::Green).bold();
        let title = vec![
            Span::styled("CLICKANA", style).into_centered_line(),
            Span::styled(
                format!("Sampling Interval: {}s", self.sampling_interval),
                style,
            )
            .into_left_aligned_line(),
            Span::styled(
                format!(
                    "Time Range: {} - {} ({}s)",
                    dashboard.start_time, dashboard.end_time, self.time_range
                ),
                style,
            )
            .into_left_aligned_line(),
            Span::styled(
                format!("Refresh Interval {}s", self.refresh_interval),
                style,
            )
            .into_left_aligned_line(),
        ];
        let p = Paragraph::new(title);

        frame.render_widget(p, area);
    }

    async fn get_api_data(
        &self,
        client: &ClickhouseServerClient,
        metric: MetricName,
    ) -> Result<Vec<SystemTimeSeries>> {
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
                "error = {}",
            ),
                    e
                )
            });

        timeseries
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
