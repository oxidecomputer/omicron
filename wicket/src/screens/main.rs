// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::{get_control_id, Control, ControlId, NullPane, OverviewPane, Pane};
use crate::{Action, Event, Frame, State, Term};
use crossterm::event::Event as TermEvent;
use crossterm::event::{KeyCode, KeyEvent};
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{
    Block, BorderType, Borders, List, ListItem, ListState, Paragraph, Row,
    Table, Tabs,
};

/// The [`MainScreen`] is the primary UI element of the terminal, covers the
/// entire terminal window/buffer and is visible for all interactions except
/// the initial splash screen animation.
///
/// This structure allows us to maintain similar styling and navigation
/// throughout wicket with a minimum of code.
///
/// Specific functionality is put inside [`Pane`]s, which can be customized
/// as needed.
pub struct MainScreen {
    selected: ControlId,
    sidebar: Sidebar,
    panes: BTreeMap<&'static str, Box<dyn Pane>>,
}

impl MainScreen {
    pub fn new() -> MainScreen {
        let sidebar = Sidebar::new();
        MainScreen {
            selected: sidebar.control_id(),
            sidebar,
            panes: BTreeMap::from([
                ("overview", Box::new(OverviewPane::new()) as Box<dyn Pane>),
                ("update", Box::new(NullPane::new()) as Box<dyn Pane>),
                ("help", Box::new(NullPane::new()) as Box<dyn Pane>),
            ]),
        }
    }

    /// Draw the [`MainScreen`]
    pub fn draw(
        &mut self,
        state: &State,
        terminal: &mut Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|frame| {
            let mut rect = frame.size();
            rect.height -= 1;

            // Size the individual components of the screen
            let horizontal_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .margin(1)
                .constraints(
                    [Constraint::Length(20), Constraint::Max(1000)].as_ref(),
                )
                .split(rect);

            let vertical_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Max(1000),
                    Constraint::Length(3),
                ])
                .split(horizontal_chunks[1]);

            self.sidebar.draw(state, frame, horizontal_chunks[0]);

            self.draw_pane(
                state,
                frame,
                vertical_chunks[0],
                vertical_chunks[1],
            );
        })?;
        Ok(())
    }

    /// Handle an [`Event`] to update state and output any necessary actions for the
    /// system to take.
    pub fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        // TODO: Do we need ControlIds at all? Can we keep all the Controls in an array tha allows
        // us to tab through them?
        if self.selected == self.sidebar.control_id() {
            self.sidebar.on(state, event)
        } else {
            None
        }
    }

    fn draw_pane(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        tabs_rect: Rect,
        npane_rect: Rect,
    ) {
        let pane = self.panes.get_mut(self.sidebar.selected()).unwrap();
        let titles = pane.tabs().iter().cloned().map(Spans::from).collect();
        let tabs = Tabs::new(titles)
            .block(Block::default().borders(Borders::ALL))
            .style(Style::default().fg(Color::White))
            .highlight_style(Style::default().fg(Color::Yellow))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded),
            )
            .style(Style::default().fg(Color::Cyan));
        frame.render_widget(tabs, tabs_rect);
    }
}

/// The mechanism for selecting panes
pub struct Sidebar {
    control_id: ControlId,
    panes: StatefulList<&'static str>,
}

impl Sidebar {
    pub fn new() -> Sidebar {
        let mut sidebar = Sidebar {
            control_id: get_control_id(),
            // TODO: The panes here must match the keys in `MainScreen::panes`
            // We should probably make this a touch less error prone
            panes: StatefulList::new(vec!["overview", "update", "help"]),
        };
        // Select the first pane
        sidebar.panes.next();
        sidebar
    }

    /// Return the name of the selected Pane
    ///
    /// TODO: Is an `&'static str` good enough? Should we define a `PaneId`?
    pub fn selected(&self) -> &'static str {
        self.panes.items[self.panes.state.selected().unwrap()]
    }
}

impl Control for Sidebar {
    fn control_id(&self) -> ControlId {
        self.control_id
    }

    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Up => {
                    self.panes.previous();
                    Some(Action::Redraw)
                }
                KeyCode::Down => {
                    self.panes.next();
                    Some(Action::Redraw)
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn draw(&mut self, _state: &State, frame: &mut Frame<'_>, area: Rect) {
        let items: Vec<ListItem> = self
            .panes
            .items
            .iter()
            .map(|t| {
                let text = *t;
                return ListItem::new(text.to_ascii_uppercase())
                    .style(Style::default().fg(Color::Cyan));
            })
            .collect();

        let tabs = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded),
            )
            .highlight_style(
                Style::default()
                    .add_modifier(Modifier::BOLD)
                    .fg(Color::Black)
                    .bg(Color::White),
            );

        frame.render_stateful_widget(tabs, area, &mut self.panes.state);
    }
}

pub struct StatefulList<T> {
    pub state: ListState,
    pub items: Vec<T>,
}

impl<T> StatefulList<T> {
    pub fn new(items: Vec<T>) -> StatefulList<T> {
        StatefulList { state: ListState::default(), items }
    }

    pub fn next(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }
}
