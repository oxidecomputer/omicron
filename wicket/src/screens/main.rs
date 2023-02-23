// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{get_control_id, Control, ControlId};
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
}

impl MainScreen {
    pub fn new() -> MainScreen {
        let sidebar = Sidebar::new();
        MainScreen { selected: sidebar.control_id(), sidebar }
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
}

/// The mechanism for selecting panes
pub struct Sidebar {
    control_id: ControlId,
    panes: StatefulList<&'static str>,
}

impl Sidebar {
    fn new() -> Sidebar {
        let mut sidebar = Sidebar {
            control_id: get_control_id(),
            panes: StatefulList::new(vec!["Overview", "Update", "Help"]),
        };
        // Select the first pane
        sidebar.panes.next();
        sidebar
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

    fn draw(
        &mut self,
        _state: &State,
        frame: &mut Frame<'_>,
        area: Rect,
    ) -> anyhow::Result<()> {
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
                    .title("TABS <CTRL>")
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
        Ok(())
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
