// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::{Duration, Instant};

use clickhouse_admin_server_client::{types, Client as ClickhouseServerClient};
use anyhow::{anyhow,Result};
use ratatui::{
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    symbols::{self, Marker},
    text::{Line, Span},
    widgets::{Axis, Block, Chart, Dataset, GraphType, LegendPosition},
    DefaultTerminal, Frame,
};
use chrono::DateTime;
use serde::Deserialize;
use slog::{info, o, Drain};
use slog_async::Async;
use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
use tokio::runtime::Runtime;

const GIBIBYTE: u64 = 1073741824;

#[derive(Deserialize, Debug)]
struct Timeseries {
    time: String,
    value: f64,
}

//#[derive(Debug)]
//pub struct Clickana {}
//
//impl Clickana {
//    pub fn new() -> Self {
//        Self {}
//    }
//
//    pub fn view_dashboards(&self) -> Result<()> {
//        let terminal = ratatui::init();
//        let app_result = App::new().run(terminal);
//        ratatui::restore();
//        app_result
//    }
//}

fn main() -> Result<()> {
    let terminal = ratatui::init();
    let app_result = App::new().run(terminal);
    ratatui::restore();
    app_result
}

struct App {
    signal1: SinSignal,
    data1: Vec<(f64, f64)>,
    signal2: SinSignal,
    data2: Vec<(f64, f64)>,
    window: [f64; 2],
}

#[derive(Clone)]
struct SinSignal {
    x: f64,
    interval: f64,
    period: f64,
    scale: f64,
}

impl SinSignal {
    const fn new(interval: f64, period: f64, scale: f64) -> Self {
        Self {
            x: 0.0,
            interval,
            period,
            scale,
        }
    }
}

impl Iterator for SinSignal {
    type Item = (f64, f64);
    fn next(&mut self) -> Option<Self::Item> {
        let point = (self.x, (self.x * 1.0 / self.period).sin() * self.scale);
        self.x += self.interval;
        Some(point)
    }
}

impl App {
    fn new() -> Self {
        let mut signal1 = SinSignal::new(0.2, 3.0, 18.0);
        let mut signal2 = SinSignal::new(0.1, 2.0, 10.0);
        let data1 = signal1.by_ref().take(200).collect::<Vec<(f64, f64)>>();
        let data2 = signal2.by_ref().take(200).collect::<Vec<(f64, f64)>>();
        Self {
            signal1,
            data1,
            signal2,
            data2,
            window: [0.0, 20.0],
        }
    }

    fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
      // TODO: Actually fix this
        let tick_rate = Duration::from_secs(250);
        let mut last_tick = Instant::now();
        loop {
            terminal.draw(|frame| self.draw(frame))?;

            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') {
                        return Ok(());
                    }
                }
            }
            if last_tick.elapsed() >= tick_rate {
                self.on_tick();
                last_tick = Instant::now();
            }
        }
    }

    fn on_tick(&mut self) {
        self.data1.drain(0..5);
        self.data1.extend(self.signal1.by_ref().take(5));

        self.data2.drain(0..10);
        self.data2.extend(self.signal2.by_ref().take(10));

        self.window[0] += 1.0;
        self.window[1] += 1.0;
    }

    fn draw(&self, frame: &mut Frame) {
       // let [_top, bottom] = Layout::vertical([Constraint::Fill(1); 2]).areas(frame.area());
       // let [animated_chart, bar_chart] =
       //     Layout::horizontal([Constraint::Fill(1), Constraint::Length(29)]).areas(top);
        let [all] = Layout::vertical([Constraint::Fill(1); 1]).areas(frame.area());
        // let [line_chart, scatter] = Layout::horizontal([Constraint::Fill(1); 2]).areas(all);

        let [line_chart] = Layout::horizontal([Constraint::Fill(1); 1]).areas(all);

       // self.render_animated_chart(frame, animated_chart);
       // render_barchart(frame, bar_chart);
        render_line_chart(frame, line_chart);
       // render_scatter(frame, scatter);
    }

    fn _render_animated_chart(&self, frame: &mut Frame, area: Rect) {
        let x_labels = vec![
            Span::styled(
                format!("{}", self.window[0]),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("{}", (self.window[0] + self.window[1]) / 2.0)),
            Span::styled(
                format!("{}", self.window[1]),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ];
        let datasets = vec![
            Dataset::default()
                .name("data2")
                .marker(symbols::Marker::Dot)
                .style(Style::default().fg(Color::Cyan))
                .data(&self.data1),
            Dataset::default()
                .name("data3")
                .marker(symbols::Marker::Braille)
                .style(Style::default().fg(Color::Yellow))
                .data(&self.data2),
        ];

        let chart = Chart::new(datasets)
            .block(Block::bordered())
            .x_axis(
                Axis::default()
                    .title("X Axis")
                    .style(Style::default().fg(Color::Gray))
                    .labels(x_labels)
                    .bounds(self.window),
            )
            .y_axis(
                Axis::default()
                    .title("Y Axis")
                    .style(Style::default().fg(Color::Gray))
                    .labels(["-20".bold(), "0".into(), "20".bold()])
                    .bounds([-20.0, 20.0]),
            );

        frame.render_widget(chart, area);
    }
}

fn get_data() -> Result<Vec<Timeseries>, serde_json::Error> {
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

// TODO: Build better logger
fn log() -> slog::Logger {
  let decorator = PlainDecorator::new(TestStdoutWriter);
  let drain = FullFormat::new(decorator).build().fuse();
  let drain = slog_async::Async::new(drain).build().fuse();
  slog::Logger::root(drain, o!())
}

fn get_api_data() -> Result<Vec<Timeseries>> {
  // TODO: Clean this up
  let rt = Runtime::new().unwrap();

  let admin_url = format!("http://[::1]:8888");
  let log = log();
  let client = ClickhouseServerClient::new(&admin_url, log.clone());
  let result = rt.block_on( async {
    // TODO: Do something with the logs! It's messing up my chart :(
    // I think I need to call drawe after this
  let timeseries = client
      .system_timeseries_avg(types::SystemTable::AsynchronousMetricLog, "DiskUsed_default", Some(120), Some(3600), Some(types::TimestampFormat::UnixEpoch) )
      .await
      .map(|t| t.into_inner()).unwrap();
      //.map_err(|e| {
      //    anyhow!(
      //        concat!(
      //        "failed to send config for clickhouse server ",
      //        "with id {} to clickhouse-admin-server; admin_url = {}",
      //        "error = {}"
      //    ),
      //        config.settings.id,
      //        admin_url,
      //        e
      //    )
      //});

   timeseries
  });
  // TODO: actually format time properly
  let timeseries: Vec<Timeseries> = result.into_iter().map(|s| Timeseries{time: format!("{:?}", s.time), value: s.value} ).collect();

  Ok(timeseries)

}

fn render_line_chart(frame: &mut Frame, area: Rect) {
    //let raw_data = get_data().unwrap();
    let raw_data = get_api_data().unwrap();
    // TODO: Also retreive time and value separately for the human readable labels?
    let times: Vec<i64> = raw_data.iter()
    .map(|ts| (ts.time.trim_matches('"').parse::<i64>().expect(&format!("WHAT? time:{} struct:{:?}", ts.time, ts))))
    .collect();

    let values: Vec<f64> = raw_data.iter()
    .map(|ts| (ts.value))
    .collect();

    let data: Vec<(f64, f64)> = raw_data.iter()
    .map(|ts| (ts.time.trim_matches('"').parse::<f64>().unwrap(), ts.value))
    .collect();

    let min_value = values.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap().floor();
    let max_value = values.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap().ceil();

    let min_time = times.iter().min().unwrap();
    let max_time = times.iter().max().unwrap();
    // Are these timestamps correct? is the i64 conversion working?
    let min_time_utc = DateTime::from_timestamp(*min_time, 0).expect("invalid timestamp").time();
    let max_time_utc = DateTime::from_timestamp(*max_time, 0).expect("invalid timestamp").time();
    let avg_time_utc = DateTime::from_timestamp((*min_time + *max_time)/2, 0).expect("invalid timestamp").time();

    let min_value_gib =  min_value as u64 / GIBIBYTE;
    let max_value_gib = max_value as u64 / GIBIBYTE;
    let avg_value_gib = (min_value_gib + max_value_gib) / 2;

     // let data = [(1., 1.), (4., 4.), (4.5, 1.0)];

    let datasets = vec![Dataset::default()
        .name("DiskUsage per minute".italic())
        .marker(symbols::Marker::Braille)
        .style(Style::default().fg(Color::Yellow))
        .graph_type(GraphType::Line)
        .data(&data)];

    let chart = Chart::new(datasets)
        .block(Block::bordered().title(Line::from(format!("Disk max {}", max_value)).cyan().bold().centered()))
        .x_axis(
            Axis::default()
             //   .title("Time")
                .style(Style::default().gray())
                .bounds([*min_time as f64, *max_time as f64])
                .labels([format!("{}", min_time_utc).bold(), format!("{}", avg_time_utc).bold(), format!("{}", max_time_utc).bold()]),
        )
        .y_axis(
            Axis::default()
              //  .title("Bytes")
                .style(Style::default().gray())
                .bounds([min_value, max_value])
                .labels([format!("{} GiB", min_value_gib).bold(), format!("{} GiB", avg_value_gib).bold(), format!("{} GiB", max_value_gib).bold()]),
        )
        .legend_position(Some(LegendPosition::TopLeft))
        .hidden_legend_constraints((Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)));

    frame.render_widget(chart, area);
}
