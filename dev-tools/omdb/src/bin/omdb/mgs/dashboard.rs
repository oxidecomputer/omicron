// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code for the MGS dashboard subcommand

use anyhow::{Context, Result};
use chrono::{Local, Offset, TimeZone};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode,
        KeyModifiers,
    },
    execute,
    terminal::{
        disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
        LeaveAlternateScreen,
    },
};
use dyn_clone::DynClone;
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, Borders, Chart, Dataset, List, ListItem, ListState,
        Paragraph,
    },
    Frame, Terminal,
};

use crate::mgs::sensors::{
    sensor_data, sensor_metadata, SensorId, SensorInput, SensorMetadata,
    SensorValues, SensorsArgs,
};
use crate::mgs::sp_to_string;
use clap::Args;
use gateway_client::types::MeasurementKind;
use gateway_client::types::SpIdentifier;
use multimap::MultiMap;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Args)]
pub(crate) struct DashboardArgs {
    #[clap(flatten)]
    sensors_args: SensorsArgs,

    /// simulate real-time with input
    #[clap(long)]
    simulate_realtime: bool,
}

struct StatefulList {
    state: ListState,
    n: usize,
}

impl StatefulList {
    fn next(&mut self) {
        self.state.select(match self.state.selected() {
            Some(ndx) => Some((ndx + 1) % self.n),
            None => Some(0),
        });
    }

    fn previous(&mut self) {
        self.state.select(match self.state.selected() {
            Some(0) => Some(self.n - 1),
            Some(ndx) => Some(ndx - 1),
            None => Some(0),
        });
    }

    fn unselect(&mut self) {
        self.state.select(None);
    }

    fn selected(&self) -> Option<usize> {
        self.state.selected()
    }
}

struct Series {
    name: String,
    color: Color,
    data: Vec<(f64, f64)>,
    raw: Vec<Option<f32>>,
}

trait Attributes: DynClone {
    fn label(&self) -> String;
    fn legend_label(&self) -> String;
    fn x_axis_label(&self) -> String {
        "Time".to_string()
    }
    fn y_axis_label(&self) -> String;
    fn axis_value(&self, val: f64) -> String;
    fn legend_value(&self, val: f64) -> String;
}

dyn_clone::clone_trait_object!(Attributes);

#[derive(Clone)]
struct TempGraph;

impl Attributes for TempGraph {
    fn label(&self) -> String {
        "Temperature".to_string()
    }
    fn legend_label(&self) -> String {
        "Sensors".to_string()
    }

    fn y_axis_label(&self) -> String {
        "Degrees Celsius".to_string()
    }

    fn axis_value(&self, val: f64) -> String {
        format!("{:2.0}°", val)
    }

    fn legend_value(&self, val: f64) -> String {
        format!("{:4.2}°", val)
    }
}

#[derive(Clone)]
struct FanGraph;

impl Attributes for FanGraph {
    fn label(&self) -> String {
        "Fan speed".to_string()
    }
    fn legend_label(&self) -> String {
        "Fans".to_string()
    }

    fn y_axis_label(&self) -> String {
        "RPM".to_string()
    }

    fn axis_value(&self, val: f64) -> String {
        format!("{:3.1}K", val / 1000.0)
    }

    fn legend_value(&self, val: f64) -> String {
        format!("{:.0}", val)
    }
}

#[derive(Clone)]
struct CurrentGraph;

impl Attributes for CurrentGraph {
    fn label(&self) -> String {
        "Output current".to_string()
    }

    fn legend_label(&self) -> String {
        "Regulators".to_string()
    }

    fn y_axis_label(&self) -> String {
        "Rails".to_string()
    }

    fn axis_value(&self, val: f64) -> String {
        format!("{:2.2}A", val)
    }

    fn legend_value(&self, val: f64) -> String {
        format!("{:3.2}A", val)
    }
}

#[derive(Clone)]
struct VoltageGraph;

impl Attributes for VoltageGraph {
    fn label(&self) -> String {
        "Voltage".to_string()
    }

    fn legend_label(&self) -> String {
        "Rails".to_string()
    }

    fn y_axis_label(&self) -> String {
        "Volts".to_string()
    }

    fn axis_value(&self, val: f64) -> String {
        format!("{:2.2}V", val)
    }

    fn legend_value(&self, val: f64) -> String {
        format!("{:3.2}V", val)
    }
}

#[derive(Clone)]
struct SensorGraph;

impl Attributes for SensorGraph {
    fn label(&self) -> String {
        "Sensor output".to_string()
    }

    fn legend_label(&self) -> String {
        "Sensors".to_string()
    }

    fn y_axis_label(&self) -> String {
        "Units".to_string()
    }

    fn axis_value(&self, val: f64) -> String {
        format!("{:2.2}", val)
    }

    fn legend_value(&self, val: f64) -> String {
        format!("{:3.2}", val)
    }
}

struct Graph {
    series: Vec<Series>,
    legend: StatefulList,
    time: usize,
    width: usize,
    offs: usize,
    interpolate: usize,
    bounds: [f64; 2],
    attributes: Box<dyn Attributes>,
}

impl Graph {
    fn new(all: &[String], attr: Box<dyn Attributes>) -> Result<Self> {
        let mut series = vec![];

        let colors = [
            Color::Yellow,
            Color::Green,
            Color::Magenta,
            Color::White,
            Color::Red,
            Color::LightRed,
            Color::Blue,
            Color::LightMagenta,
            Color::LightYellow,
            Color::LightCyan,
            Color::LightGreen,
            Color::LightBlue,
            Color::LightRed,
        ];

        for (ndx, s) in all.iter().enumerate() {
            series.push(Series {
                name: s.to_string(),
                color: colors[ndx % colors.len()],
                data: Vec::new(),
                raw: Vec::new(),
            })
        }

        Ok(Graph {
            series,
            legend: StatefulList { state: ListState::default(), n: all.len() },
            time: 0,
            width: 600,
            offs: 0,
            interpolate: 0,
            bounds: [20.0, 120.0],
            attributes: attr,
        })
    }

    fn flip(from: &[(&Self, String)], series_ndx: usize) -> Self {
        let rep = from[0].0;
        let mut series = vec![];

        let colors = [
            Color::Yellow,
            Color::Green,
            Color::Magenta,
            Color::White,
            Color::Red,
            Color::LightRed,
            Color::Blue,
            Color::LightMagenta,
            Color::LightYellow,
            Color::LightCyan,
            Color::LightGreen,
            Color::LightBlue,
            Color::LightRed,
        ];

        for (ndx, (graph, name)) in from.iter().enumerate() {
            series.push(Series {
                name: name.clone(),
                color: colors[ndx % colors.len()],
                data: graph.series[series_ndx].data.clone(),
                raw: graph.series[series_ndx].raw.clone(),
            });
        }

        Graph {
            series,
            legend: StatefulList { state: ListState::default(), n: from.len() },
            time: rep.time,
            width: rep.width,
            offs: rep.offs,
            interpolate: rep.interpolate,
            bounds: rep.bounds,
            attributes: rep.attributes.clone(),
        }
    }

    fn data(&mut self, data: &[Option<f32>]) {
        for (ndx, s) in self.series.iter_mut().enumerate() {
            s.raw.push(data[ndx]);
        }

        self.time += 1;

        if self.offs > 0 {
            self.offs += 1;
        }
    }

    fn update_data(&mut self) {
        for s in &mut self.series {
            s.data = Vec::new();
        }

        for i in 0..self.width {
            if self.time < (self.width - i) + self.offs {
                continue;
            }

            let offs = self.time - (self.width - i) - self.offs;

            for (_ndx, s) in &mut self.series.iter_mut().enumerate() {
                if let Some(datum) = s.raw[offs] {
                    let point = (i as f64, f64::from(datum));

                    if self.interpolate != 0 {
                        if let Some(last) = s.data.last() {
                            let x_delta = point.0 - last.0;
                            let slope = (point.1 - last.1) / x_delta;
                            let x_inc = x_delta / self.interpolate as f64;

                            for x in 0..self.interpolate {
                                s.data.push((
                                    point.0 + x as f64 * x_inc,
                                    point.1 + (slope * x_inc),
                                ));
                            }
                        }
                    }

                    s.data.push((i as f64, f64::from(datum)));
                }
            }
        }

        self.update_bounds();
    }

    fn update_bounds(&mut self) {
        let selected = self.legend.state.selected();
        let mut min = None;
        let mut max = None;

        for (ndx, s) in self.series.iter().enumerate() {
            if let Some(selected) = selected {
                if ndx != selected {
                    continue;
                }
            }

            for (_, datum) in &s.data {
                min = match min {
                    Some(min) if datum < min => Some(datum),
                    None => Some(datum),
                    _ => min,
                };

                max = match max {
                    Some(max) if datum > max => Some(datum),
                    None => Some(datum),
                    _ => max,
                };
            }
        }

        if let Some(min) = min {
            self.bounds[0] = ((min * 0.85) / 2.0) * 2.0;
        }

        if self.bounds[0] < 0.0 {
            self.bounds[0] = 0.0;
        }

        if let Some(max) = max {
            self.bounds[1] = ((max * 1.15) / 2.0) * 2.0;
        }
    }

    fn previous(&mut self) {
        self.legend.previous();
    }

    fn next(&mut self) {
        self.legend.next();
    }

    fn unselect(&mut self) {
        self.legend.unselect();
    }

    fn selected(&self) -> Option<usize> {
        self.legend.selected()
    }

    fn set_interpolate(&mut self) {
        let interpolate = (1000.0 - self.width as f64) / self.width as f64;

        if interpolate >= 1.0 {
            self.interpolate = interpolate as usize;
        } else {
            self.interpolate = 0;
        }
    }

    fn zoom_in(&mut self) {
        self.width = (self.width as f64 * 0.8) as usize;
        self.set_interpolate();
    }

    fn zoom_out(&mut self) {
        self.width = (self.width as f64 * 1.25) as usize;
        self.set_interpolate();
    }

    fn time_right(&mut self) {
        let delta = (self.width as f64 * 0.25) as usize;

        if delta > self.offs {
            self.offs = 0;
        } else {
            self.offs -= delta;
        }
    }

    fn time_left(&mut self) {
        self.offs += (self.width as f64 * 0.25) as usize;
    }
}

struct Dashboard {
    graphs: HashMap<(SpIdentifier, MeasurementKind), Graph>,
    flipped: HashMap<MeasurementKind, Graph>,
    sids: HashMap<(SpIdentifier, MeasurementKind), Vec<SensorId>>,
    kinds: Vec<MeasurementKind>,
    selected_kind: usize,
    sps: Vec<SpIdentifier>,
    selected_sp: usize,
    status: String,
    time: u64,
}

impl Dashboard {
    fn new(metadata: &SensorMetadata) -> Result<Dashboard> {
        let mut sps =
            metadata.sensors_by_sp.keys().copied().collect::<Vec<_>>();
        let mut graphs = HashMap::new();
        let mut sids = HashMap::new();
        sps.sort();

        let kinds = vec![
            MeasurementKind::Temperature,
            MeasurementKind::Speed,
            MeasurementKind::Current,
        ];

        for &sp in sps.iter() {
            let sensors = metadata.sensors_by_sp.get_vec(&sp).unwrap();
            let mut by_kind = MultiMap::new();

            for sid in sensors {
                let (_, s, _) = metadata.sensors_by_id.get(sid).unwrap();
                by_kind.insert(s.kind, (s.name.clone(), *sid));
            }

            let keys = by_kind.keys().copied().collect::<Vec<_>>();

            for k in keys {
                let mut v = by_kind.remove(&k).unwrap();
                v.sort();

                let labels =
                    v.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();

                graphs.insert(
                    (sp, k),
                    Graph::new(
                        labels.as_slice(),
                        match k {
                            MeasurementKind::Temperature => Box::new(TempGraph),
                            MeasurementKind::Current => Box::new(CurrentGraph),
                            MeasurementKind::Speed => Box::new(FanGraph),
                            MeasurementKind::Voltage => Box::new(VoltageGraph),
                            _ => Box::new(SensorGraph),
                        },
                    )?,
                );

                sids.insert(
                    (sp, k),
                    v.iter().map(|(_, sid)| *sid).collect::<Vec<_>>(),
                );
            }
        }

        let status = sp_to_string(&sps[0]);

        Ok(Dashboard {
            graphs,
            flipped: HashMap::new(),
            sids,
            kinds,
            selected_kind: 0,
            sps,
            selected_sp: 0,
            status,
            time: secs()?,
        })
    }

    fn status(&self) -> Vec<(&str, &str)> {
        vec![("Status", &self.status)]
    }

    fn update_data(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.update_data();
        }

        for graph in self.flipped.values_mut() {
            graph.update_data();
        }
    }

    fn up(&mut self) {
        let selected_kind = self.kinds[self.selected_kind];
        let type_ = self.sps[self.selected_sp].type_;

        if let Some(flipped) = self.flipped.get_mut(&selected_kind) {
            flipped.previous();
            return;
        }

        for sp in self.sps.iter().filter(|&s| s.type_ == type_) {
            self.graphs.get_mut(&(*sp, selected_kind)).unwrap().previous();
        }
    }

    fn down(&mut self) {
        let selected_kind = self.kinds[self.selected_kind];
        let type_ = self.sps[self.selected_sp].type_;

        if let Some(flipped) = self.flipped.get_mut(&selected_kind) {
            flipped.next();
            return;
        }

        for sp in self.sps.iter().filter(|&s| s.type_ == type_) {
            self.graphs.get_mut(&(*sp, selected_kind)).unwrap().next();
        }
    }

    fn esc(&mut self) {
        let selected_kind = self.kinds[self.selected_kind];
        let type_ = self.sps[self.selected_sp].type_;

        if let Some(flipped) = self.flipped.get_mut(&selected_kind) {
            flipped.unselect();
            return;
        }

        for sp in self.sps.iter().filter(|&s| s.type_ == type_) {
            self.graphs.get_mut(&(*sp, selected_kind)).unwrap().unselect();
        }
    }

    fn left(&mut self) {
        if self.selected_sp == 0 {
            self.selected_sp = self.sps.len() - 1;
        } else {
            self.selected_sp -= 1;
        }

        self.status = sp_to_string(&self.sps[self.selected_sp]);
    }

    fn right(&mut self) {
        self.selected_sp = (self.selected_sp + 1) % self.sps.len();
        self.status = sp_to_string(&self.sps[self.selected_sp]);
    }

    fn time_left(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.time_left();
        }

        for graph in self.flipped.values_mut() {
            graph.time_left();
        }
    }

    fn time_right(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.time_right();
        }

        for graph in self.flipped.values_mut() {
            graph.time_right();
        }
    }

    fn flip(&mut self) {
        let selected_kind = self.kinds[self.selected_kind];
        let type_ = self.sps[self.selected_sp].type_;

        if self.flipped.remove(&selected_kind).is_some() {
            return;
        }

        let sp = self.sps[self.selected_sp];

        let graph = self.graphs.get(&(sp, selected_kind)).unwrap();

        if let Some(ndx) = graph.selected() {
            let mut from = vec![];

            for sp in self.sps.iter().filter(|&s| s.type_ == type_) {
                from.push((
                    self.graphs.get(&(*sp, selected_kind)).unwrap(),
                    sp_to_string(sp),
                ));
            }

            self.flipped
                .insert(selected_kind, Graph::flip(from.as_slice(), ndx));
        }
    }

    fn tab(&mut self) {
        self.selected_kind = (self.selected_kind + 1) % self.kinds.len();
    }

    fn zoom_in(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.zoom_in();
        }

        for graph in self.flipped.values_mut() {
            graph.zoom_in();
        }
    }

    fn zoom_out(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.zoom_out();
        }

        for graph in self.flipped.values_mut() {
            graph.zoom_out();
        }
    }

    fn gap(&mut self, length: u64) {
        let mut gap: Vec<Option<f32>> = vec![];

        for (graph, sids) in &self.sids {
            while gap.len() < sids.len() {
                gap.push(None);
            }

            let graph = self.graphs.get_mut(graph).unwrap();

            for _ in 0..length {
                graph.data(&gap[0..sids.len()]);
            }
        }
    }

    fn values(&mut self, values: &SensorValues) {
        for (graph, sids) in &self.sids {
            let mut data = vec![];

            for sid in sids {
                if let Some(value) = values.values.get(sid) {
                    data.push(*value);
                } else {
                    data.push(None);
                }
            }

            let graph = self.graphs.get_mut(graph).unwrap();
            graph.data(data.as_slice());
        }

        self.time = values.time;
    }
}

fn run_dashboard<B: Backend>(
    terminal: &mut Terminal<B>,
    dashboard: &mut Dashboard,
    force_update: bool,
) -> Result<bool> {
    let update = if crossterm::event::poll(Duration::from_secs(0))? {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Char('q') => return Ok(true),
                KeyCode::Char('+') => dashboard.zoom_in(),
                KeyCode::Char('-') => dashboard.zoom_out(),
                KeyCode::Char('<') => dashboard.time_left(),
                KeyCode::Char('>') => dashboard.time_right(),
                KeyCode::Char('!') => dashboard.flip(),
                KeyCode::Char('l') => {
                    //
                    // ^L -- form feed -- is historically used to clear and
                    // redraw the screen.  And, notably, it is what dtach(1)
                    // will send when attaching to a dashboard.  If we
                    // see ^L, clear the terminal to force a total redraw.
                    //
                    if key.modifiers == KeyModifiers::CONTROL {
                        terminal.clear()?;
                    }
                }
                KeyCode::Up => dashboard.up(),
                KeyCode::Down => dashboard.down(),
                KeyCode::Right => dashboard.right(),
                KeyCode::Left => dashboard.left(),
                KeyCode::Esc => dashboard.esc(),
                KeyCode::Tab => dashboard.tab(),
                _ => {}
            }
        }
        true
    } else {
        force_update
    };

    if update {
        dashboard.update_data();
        terminal.draw(|f| draw(f, dashboard))?;
    }

    Ok(false)
}

fn secs() -> Result<u64> {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(now.as_secs())
}

///
/// Runs `omdb mgs dashboard`
///
pub(crate) async fn cmd_mgs_dashboard(
    omdb: &crate::Omdb,
    log: &slog::Logger,
    mgs_args: &crate::mgs::MgsArgs,
    args: &DashboardArgs,
) -> Result<(), anyhow::Error> {
    let mut input = if let Some(ref input) = args.sensors_args.input {
        let file = File::open(input)
            .with_context(|| format!("failed to open {input}"))?;
        SensorInput::CsvReader(
            csv::Reader::from_reader(file),
            csv::Position::new(),
        )
    } else {
        SensorInput::MgsClient(mgs_args.mgs_client(omdb, log).await?)
    };

    let (metadata, values) =
        sensor_metadata(&mut input, &args.sensors_args).await?;

    let mut dashboard = Dashboard::new(&metadata)?;
    let mut last = values.time;
    let mut force = true;
    let mut update = true;

    dashboard.values(&values);

    if args.sensors_args.input.is_some() && !args.simulate_realtime {
        loop {
            let values = sensor_data(&mut input, &metadata).await?;

            if values.time == 0 {
                break;
            }

            if values.time != last + 1 {
                dashboard.gap(values.time - last - 1);
            }

            last = values.time;
            dashboard.values(&values);
        }

        update = false;
    }

    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = 'outer: loop {
        match run_dashboard(&mut terminal, &mut dashboard, force) {
            Err(err) => break Err(err),
            Ok(true) => break Ok(()),
            _ => {}
        }

        force = false;

        let now = match secs() {
            Err(err) => break Err(err),
            Ok(now) => now,
        };

        if update && now != last {
            let kicked = Instant::now();
            let f = sensor_data(&mut input, &metadata);
            last = now;

            while Instant::now().duration_since(kicked).as_millis() < 800 {
                tokio::time::sleep(Duration::from_millis(10)).await;

                match run_dashboard(&mut terminal, &mut dashboard, force) {
                    Err(err) => break 'outer Err(err),
                    Ok(true) => break 'outer Ok(()),
                    _ => {}
                }
            }

            let values = match f.await {
                Err(err) => break Err(err),
                Ok(v) => v,
            };

            dashboard.values(&values);
            force = true;
            continue;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{err:?}");
    }

    Ok(())
}

fn draw_graph(f: &mut Frame, parent: Rect, graph: &mut Graph, now: u64) {
    //
    // We want the right panel to be 31 characters wide (a left-justified 20
    // and a right justified 8 + margins), but we don't want it to consume
    // more than 80%; calculate accordingly.
    //
    let r = std::cmp::min((31 * 100) / parent.width, 80);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [Constraint::Percentage(100 - r), Constraint::Percentage(r)]
                .as_ref(),
        )
        .split(parent);

    let latest = now as i64 - graph.offs as i64;
    let earliest = Local.timestamp_opt(latest - graph.width as i64, 0).unwrap();
    let latest = Local.timestamp_opt(latest, 0).unwrap();

    //
    // We want a format that preserves horizontal real estate just a tad more
    // than .to_rfc3339_opts()...
    //
    let fmt = "%Y-%m-%d %H:%M:%S";

    let tz_offset = earliest.offset().fix().local_minus_utc();
    let tz = if tz_offset != 0 {
        let hours = tz_offset / 3600;
        let minutes = (tz_offset % 3600) / 60;

        if minutes != 0 {
            format!("Z{:+}:{:02}", hours, minutes.abs())
        } else {
            format!("Z{:+}", hours)
        }
    } else {
        "Z".to_string()
    };

    let x_labels = vec![
        Span::styled(
            format!("{}{}", earliest.format(fmt), tz),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{}{}", latest.format(fmt), tz),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    let mut datasets = vec![];
    let selected = graph.legend.state.selected();

    for (ndx, s) in graph.series.iter().enumerate() {
        if let Some(selected) = selected {
            if ndx != selected {
                continue;
            }
        }

        datasets.push(
            Dataset::default()
                .name(&*s.name)
                .marker(symbols::Marker::Braille)
                .style(Style::default().fg(s.color))
                .data(&s.data),
        );
    }

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(Span::styled(
                    graph.attributes.label(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL),
        )
        .x_axis(
            Axis::default()
                .title(graph.attributes.x_axis_label())
                .style(Style::default().fg(Color::Gray))
                .labels(x_labels)
                .bounds([0.0, graph.width as f64])
                .labels_alignment(Alignment::Right),
        )
        .y_axis(
            Axis::default()
                .title(graph.attributes.y_axis_label())
                .style(Style::default().fg(Color::Gray))
                .labels(vec![
                    Span::styled(
                        graph.attributes.axis_value(graph.bounds[0]),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        graph.attributes.axis_value(graph.bounds[1]),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                ])
                .bounds(graph.bounds),
        );

    f.render_widget(chart, chunks[0]);

    let mut rows = vec![];

    for s in &graph.series {
        let val = match s.raw.last() {
            None | Some(None) => "-".to_string(),
            Some(Some(val)) => graph.attributes.legend_value((*val).into()),
        };

        rows.push(ListItem::new(Line::from(vec![
            Span::styled(
                format!("{:<20}", s.name),
                Style::default().fg(s.color),
            ),
            Span::styled(format!("{:>8}", val), Style::default().fg(s.color)),
        ])));
    }

    let list = List::new(rows)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(graph.attributes.legend_label()),
        )
        .highlight_style(
            Style::default()
                .bg(Color::LightGreen)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        );

    // We can now render the item list
    f.render_stateful_widget(list, chunks[1], &mut graph.legend.state);
}

fn draw_graphs(f: &mut Frame, parent: Rect, dashboard: &mut Dashboard) {
    let screen = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Ratio(1, 2),
                Constraint::Ratio(1, 4),
                Constraint::Ratio(1, 4),
            ]
            .as_ref(),
        )
        .split(parent);

    let sp = dashboard.sps[dashboard.selected_sp];

    for (i, k) in dashboard.kinds.iter().enumerate() {
        if let Some(graph) = dashboard.flipped.get_mut(k) {
            draw_graph(f, screen[i], graph, dashboard.time);
        } else {
            draw_graph(
                f,
                screen[i],
                dashboard.graphs.get_mut(&(sp, *k)).unwrap(),
                dashboard.time,
            );
        }
    }
}

fn draw_status(f: &mut Frame, parent: Rect, status: &[(&str, &str)]) {
    let mut bar = vec![];

    for i in 0..status.len() {
        let s = &status[i];

        bar.push(Span::styled(
            s.0,
            Style::default().add_modifier(Modifier::BOLD),
        ));

        bar.push(Span::styled(
            ": ",
            Style::default().add_modifier(Modifier::BOLD),
        ));

        bar.push(Span::raw(s.1));

        if i < status.len() - 1 {
            bar.push(Span::raw(" | "));
        }
    }

    let text = vec![Line::from(bar)];

    let para = Paragraph::new(text)
        .alignment(Alignment::Right)
        .style(Style::default().fg(Color::White).bg(Color::Black));

    f.render_widget(para, parent);
}

fn draw(f: &mut Frame, dashboard: &mut Dashboard) {
    let size = f.size();

    let screen = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)].as_ref())
        .split(size);

    draw_graphs(f, screen[0], dashboard);
    draw_status(f, screen[1], &dashboard.status());
}
