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

use crate::defaults::colors::*;
use crate::defaults::style;

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
    sidebar_selected: bool,
    sidebar: Sidebar,
    panes: BTreeMap<&'static str, Box<dyn Pane>>,
}

impl MainScreen {
    pub fn new() -> MainScreen {
        MainScreen {
            sidebar_selected: true,
            sidebar: Sidebar::new(),
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
            let statusbar_rect = Rect {
                y: rect.height - 1,
                height: 1,
                x: 2,
                width: rect.width - 3,
            };

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
                .constraints([Constraint::Length(3), Constraint::Max(1000)])
                .split(horizontal_chunks[1]);

            self.sidebar.draw(state, frame, horizontal_chunks[0]);

            self.draw_pane(
                state,
                frame,
                vertical_chunks[0],
                vertical_chunks[1],
            );

            self.draw_statusbar(state, frame, statusbar_rect);
        })?;
        Ok(())
    }

    /// Handle an [`Event`] to update state and output any necessary actions for the
    /// system to take.
    pub fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Esc => {
                    if self.sidebar_selected {
                        None
                    } else {
                        self.sidebar_selected = true;
                        Some(Action::Redraw)
                    }
                }
                KeyCode::Enter | KeyCode::Tab | KeyCode::Right => {
                    if self.sidebar_selected {
                        self.sidebar_selected = false;
                        Some(Action::Redraw)
                    } else {
                        self.current_pane()
                            .on(state, Event::Term(TermEvent::Key(e)))
                    }
                }
                _ => {
                    let event = Event::Term(TermEvent::Key(e));
                    if self.sidebar_selected {
                        self.sidebar.on(state, event)
                    } else {
                        self.current_pane()
                            .on(state, Event::Term(TermEvent::Key(e)))
                    }
                }
            },
            e => {
                if self.sidebar_selected {
                    self.sidebar.on(state, e)
                } else {
                    self.current_pane().on(state, e)
                }
            }
        }
    }

    fn current_pane(&mut self) -> &mut Box<dyn Pane> {
        self.panes.get_mut(self.sidebar.selected()).unwrap()
    }

    fn draw_pane(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        tabs_rect: Rect,
        pane_rect: Rect,
    ) {
        let pane = self.current_pane();

        // Draw the Top bar (tabs)
        let titles = pane.tabs().iter().cloned().map(Spans::from).collect();
        let tabs = Tabs::new(titles)
            .style(Style::default().fg(Color::White))
            .highlight_style(Style::default().fg(Color::Yellow))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded),
            )
            .style(Style::default().fg(Color::Cyan));
        frame.render_widget(tabs, tabs_rect);

        // Draw the pane border
        let border = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = border.inner(pane_rect);
        frame.render_widget(border, pane_rect);

        // Draw the pane
        pane.draw(state, frame, inner);
    }

    // TODO: Use the real status and version
    fn draw_statusbar(
        &mut self,
        _state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
    ) {
        let main = Paragraph::new(Spans::from(vec![
            Span::styled("WICKETD: ", Style::default().fg(Color::Cyan)),
            Span::styled("CONNECTED", Style::default().fg(Color::Blue)),
            Span::styled(" | ", Style::default().fg(Color::Cyan)),
            Span::styled("MGS: ", Style::default().fg(Color::Cyan)),
            Span::styled("NO RESPONSE", Style::default().fg(Color::Blue)),
        ]))
        .style(Style::default().fg(Color::Cyan));
        frame.render_widget(main, rect);

        let test = Paragraph::new(Spans::from(vec![
            Span::styled("VERSION: ", Style::default().fg(Color::Cyan)),
            Span::styled("v0.0.1", Style::default().fg(Color::Blue)),
        ]))
        .alignment(Alignment::Right);
        frame.render_widget(test, rect);
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
                    .title("<ESC>")
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
