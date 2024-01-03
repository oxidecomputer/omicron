// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code for the MGS dashboard subcommand

use anyhow::{anyhow, bail, Result};
use clap::{CommandFactory, Parser};
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
    sensor_metadata, Sensor, SensorId, SensorInput, SensorMetadata,
    SensorValues, SensorsArgs,
};
use clap::Args;
use gateway_client::types::MeasurementKind;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use multimap::MultiMap;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::time::{Duration, Instant};

#[derive(Debug, Args)]
pub(crate) struct DashboardArgs {
    #[clap(flatten)]
    sensors_args: SensorsArgs,
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
            Some(ndx) if ndx == 0 => Some(self.n - 1),
            Some(ndx) => Some(ndx - 1),
            None => Some(0),
        });
    }

    fn unselect(&mut self) {
        self.state.select(None);
    }
}

struct Series {
    name: String,
    color: Color,
    data: Vec<(f64, f64)>,
    raw: Vec<Option<f32>>,
}

trait Attributes {
    fn label(&self) -> String;
    fn legend_label(&self) -> String;
    fn x_axis_label(&self) -> String {
        "Time".to_string()
    }
    fn y_axis_label(&self) -> String;
    fn axis_value(&self, val: f64) -> String;
    fn legend_value(&self, val: f64) -> String;

    fn increase(&mut self, _ndx: usize) -> Option<u8> {
        None
    }

    fn decrease(&mut self, _ndx: usize) -> Option<u8> {
        None
    }

    fn clear(&mut self) {}
}

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

struct CurrentGraph;

impl Attributes for CurrentGraph {
    fn label(&self) -> String {
        "Output current".to_string()
    }
    fn legend_label(&self) -> String {
        "Regulators".to_string()
    }

    fn y_axis_label(&self) -> String {
        "Amperes".to_string()
    }

    fn axis_value(&self, val: f64) -> String {
        format!("{:2.2}A", val)
    }

    fn legend_value(&self, val: f64) -> String {
        format!("{:3.2}A", val)
    }
}

struct Graph {
    series: Vec<Series>,
    legend: StatefulList,
    time: usize,
    width: usize,
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
            interpolate: 0,
            bounds: [20.0, 120.0],
            attributes: attr,
        })
    }

    fn data(&mut self, data: &[Option<f32>]) {
        for (ndx, s) in self.series.iter_mut().enumerate() {
            s.raw.push(data[ndx]);
        }

        self.time += 1;
    }

    fn update_data(&mut self) {
        for s in &mut self.series {
            s.data = Vec::new();
        }

        for i in 0..self.width {
            if self.time < self.width - i {
                continue;
            }

            let offs = self.time - (self.width - i);

            for (_ndx, s) in &mut self.series.iter_mut().enumerate() {
                if let Some(datum) = s.raw[offs] {
                    let point = (i as f64, datum as f64);

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

                    s.data.push((i as f64, datum as f64));
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
}

struct Dashboard {
    graphs: HashMap<(SpIdentifier, MeasurementKind), Graph>,
    current: usize,
    sps: Vec<SpIdentifier>,
    sensor_to_graph: HashMap<SensorId, (SpIdentifier, MeasurementKind, usize)>,
    current_sp: usize,
    last: Instant,
    interval: u32,
    outstanding: bool,
}

impl Dashboard {
    fn new(
        args: &DashboardArgs,
        metadata: std::sync::Arc<&SensorMetadata>,
    ) -> Result<Dashboard> {
        let mut sensor_to_graph = HashMap::new();
        let mut sps =
            metadata.sensors_by_sp.keys().map(|m| *m).collect::<Vec<_>>();
        let mut graphs = HashMap::new();
        sps.sort();

        for sp in sps.iter() {
            let sp = *sp;
            let sensors = metadata.sensors_by_sp.get_vec(&sp).unwrap();
            let mut by_kind = MultiMap::new();

            for sid in sensors {
                let (_, s, _) = metadata.sensors_by_id.get(sid).unwrap();
                by_kind.insert(s.kind, (s.name.clone(), *sid));
            }

            let keys = by_kind.keys().map(|k| *k).collect::<Vec<_>>();

            for k in keys {
                let mut v = by_kind.remove(&k).unwrap();
                v.sort();

                for (ndx, (_, sid)) in v.iter().enumerate() {
                    sensor_to_graph.insert(*sid, (sp, k, ndx));
                }

                let labels =
                    v.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();

                graphs.insert(
                    (sp, k),
                    Graph::new(labels.as_slice(), Box::new(TempGraph))?,
                );
            }
        }

        Ok(Dashboard {
            graphs,
            current: 0,
            sps,
            sensor_to_graph,
            current_sp: 0,
            outstanding: true,
            last: Instant::now(),
            interval: 1000,
        })
    }

    fn status(&self) -> Vec<(&str, &str)> {
        vec![("Foo", "bar")]
    }

    fn need_update(&mut self) -> Result<bool> {
        Ok(true)
    }

    fn update_data(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.update_data();
        }
    }

    fn up(&mut self) {
        // self.graphs[self.current].previous();
    }

    fn down(&mut self) {
        // self.graphs[self.current].next();
    }

    fn esc(&mut self) {
        // self.graphs[self.current].unselect();
    }

    fn tab(&mut self) {
        // self.current = (self.current + 1) % self.graphs.len();
    }

    fn zoom_in(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.zoom_in();
        }
    }

    fn zoom_out(&mut self) {
        for graph in self.graphs.values_mut() {
            graph.zoom_out();
        }
    }
}

fn run_dashboard<B: Backend>(
    terminal: &mut Terminal<B>,
    mut dashboard: Dashboard,
) -> Result<()> {
    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(100);

    loop {
        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        let update = if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => return Ok(()),
                    KeyCode::Char('+') => dashboard.zoom_in(),
                    KeyCode::Char('-') => dashboard.zoom_out(),
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
                    KeyCode::Esc => dashboard.esc(),
                    KeyCode::Tab => dashboard.tab(),
                    _ => {}
                }
            }
            true
        } else {
            dashboard.need_update()?
        };

        if update {
            dashboard.update_data();
            terminal.draw(|f| draw(f, &mut dashboard))?;
        }

        last_tick = Instant::now();
    }
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
        let file = File::open(input)?;
        SensorInput::CsvReader(
            csv::Reader::from_reader(file),
            csv::Position::new(),
        )
    } else {
        SensorInput::MgsClient(mgs_args.mgs_client(omdb, log).await?)
    };

    let (metadata, mut values) =
        sensor_metadata(&mut input, &args.sensors_args).await?;

    //
    // A bit of shenangians to force metadata to be 'static -- which allows
    // us to share it with tasks.
    //
    let metadata = Box::leak(Box::new(metadata));
    let metadata: &_ = metadata;
    let metadata = std::sync::Arc::new(metadata);

    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let dashboard = Dashboard::new(&args, metadata)?;

    let res = run_dashboard(&mut terminal, dashboard);

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

fn draw_graph<B: Backend>(f: &mut Frame<B>, parent: Rect, graph: &mut Graph) {
    //
    // We want the right panel to be 30 characters wide (a left-justified 20
    // and a right justified 8 + margins), but we don't want it to consume
    // more than 80%; calculate accordingly.
    //
    let r = std::cmp::min((30 * 100) / parent.width, 80);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [Constraint::Percentage(100 - r), Constraint::Percentage(r)]
                .as_ref(),
        )
        .split(parent);

    let x_labels = vec![
        Span::styled(
            format!("t-{}", graph.width),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("t-{}", 1),
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
                .name(&s.name)
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
                .bounds([0.0, graph.width as f64]),
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

fn draw_graphs<B: Backend>(
    f: &mut Frame<B>,
    parent: Rect,
    dashboard: &mut Dashboard,
) {
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

    let sp = dashboard.sps[dashboard.current_sp];

    draw_graph(
        f,
        screen[0],
        dashboard.graphs.get_mut(&(sp, MeasurementKind::Temperature)).unwrap(),
    );
    draw_graph(
        f,
        screen[1],
        dashboard.graphs.get_mut(&(sp, MeasurementKind::Speed)).unwrap(),
    );
    draw_graph(
        f,
        screen[2],
        dashboard.graphs.get_mut(&(sp, MeasurementKind::Current)).unwrap(),
    );
}

fn draw_status<B: Backend>(
    f: &mut Frame<B>,
    parent: Rect,
    status: &[(&str, &str)],
) {
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

fn draw<B: Backend>(f: &mut Frame<B>, dashboard: &mut Dashboard) {
    let size = f.size();

    let screen = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)].as_ref())
        .split(size);

    draw_graphs(f, screen[0], dashboard);
    draw_status(f, screen[1], &dashboard.status());
}
