// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, Context, Result};
use camino::Utf8PathBuf;
use clickhouse_admin_server_client::types::{
    SystemTimeSeries, TimestampFormat,
};
use clickhouse_admin_server_client::Client as ClickhouseServerClient;
use omicron_common::FileKv;
use ratatui::{
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Layout},
    DefaultTerminal, Frame,
};
use slog::{o, Drain, Logger};
use slog_async::Async;
use slog_term::{FullFormat, PlainDecorator};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

use crate::chart::{ChartData, ChartMetadata, MetricName};

mod chart;

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

            let top_right_frame_metadata = ChartMetadata::new(
                MetricName::MemoryTracking,
                "Memory Allocated by the Server".to_string(),
            );
            let top_right_frame = ChartData::new(
                self.get_api_data(top_right_frame_metadata.metric)?,
                top_right_frame_metadata,
            )?;

            let bottom_left_frame_metadata = ChartMetadata::new(
                MetricName::QueryCount,
                "Queries Started per Second".to_string(),
            );
            let bottom_left_frame = ChartData::new(
                self.get_api_data(bottom_left_frame_metadata.metric)?,
                bottom_left_frame_metadata,
            )?;

            let bottom_right_frame_metadata = ChartMetadata::new(
                MetricName::RunningQueries,
                "Queries Running".to_string(),
            );
            let bottom_right_frame = ChartData::new(
                self.get_api_data(bottom_right_frame_metadata.metric)?,
                bottom_right_frame_metadata,
            )?;

            let dashboard = Dashboard {
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

    fn draw(&self, frame: &mut Frame, dashboard: Dashboard) {
        let [top, bottom] =
            Layout::vertical([Constraint::Fill(1); 2]).areas(frame.area());
        let [top_left_frame, top_right_frame] =
            Layout::horizontal([Constraint::Fill(1); 2]).areas(top);
        let [bottom_left_frame, bottom_right_frame] =
            Layout::horizontal([Constraint::Fill(1); 2]).areas(bottom);

        dashboard.top_left_frame.render_line_chart(frame, top_left_frame);
        dashboard.top_right_frame.render_line_chart(frame, top_right_frame);
        dashboard.bottom_left_frame.render_line_chart(frame, bottom_left_frame);
        dashboard
            .bottom_right_frame
            .render_line_chart(frame, bottom_right_frame);
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
    top_right_frame: ChartData,
    bottom_left_frame: ChartData,
    bottom_right_frame: ChartData,
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
