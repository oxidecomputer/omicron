// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::{Control, OverviewPane, StatefulList};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::{Action, Event, Frame, State, Term};
use crossterm::event::Event as TermEvent;
use crossterm::event::KeyCode;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, BorderType, Borders, List, ListItem, Paragraph};

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
    sidebar: Sidebar,
    panes: BTreeMap<&'static str, Box<dyn Control>>,
}

impl MainScreen {
    pub fn new() -> MainScreen {
        // We want the sidebar ordered in this specific manner
        let sidebar_ordered_panes = vec![(
            "overview",
            Box::new(OverviewPane::new()) as Box<dyn Control>,
        )];
        let sidebar_keys: Vec<_> =
            sidebar_ordered_panes.iter().map(|&(title, _)| title).collect();
        MainScreen {
            sidebar: Sidebar::new(sidebar_keys),
            panes: BTreeMap::from_iter(sidebar_ordered_panes),
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
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .margin(1)
                .constraints(
                    [Constraint::Length(20), Constraint::Max(1000)].as_ref(),
                )
                .split(rect);

            // Draw all the components, starting with the background
            let background = Block::default().style(style::background());
            frame.render_widget(background, frame.size());
            self.sidebar.draw(state, frame, chunks[0], self.sidebar.selected);
            self.draw_pane(state, frame, chunks[1]);
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
                    if self.sidebar.selected {
                        None
                    } else {
                        self.sidebar.selected = true;
                        Some(Action::Redraw)
                    }
                }
                KeyCode::Enter => {
                    if self.sidebar.selected {
                        self.sidebar.selected = false;
                        Some(Action::Redraw)
                    } else {
                        self.current_pane()
                            .on(state, Event::Term(TermEvent::Key(e)))
                    }
                }
                _ => {
                    let event = Event::Term(TermEvent::Key(e));
                    if self.sidebar.selected {
                        self.sidebar.on(state, event)
                    } else {
                        self.current_pane()
                            .on(state, Event::Term(TermEvent::Key(e)))
                    }
                }
            },
            e => {
                if self.sidebar.selected {
                    self.sidebar.on(state, e)
                } else {
                    self.current_pane().on(state, e)
                }
            }
        }
    }

    fn current_pane(&mut self) -> &mut Box<dyn Control> {
        self.panes.get_mut(self.sidebar.selected()).unwrap()
    }

    fn draw_pane(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        pane_rect: Rect,
    ) {
        let active = !self.sidebar.selected;
        let pane = self.current_pane();
        pane.draw(state, frame, pane_rect, active)
    }

    fn draw_statusbar(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
    ) {
        let wicketd_spans =
            state.service_status.wicketd_liveness.compute().to_spans();
        let mgs_spans = state.service_status.mgs_liveness.compute().to_spans();
        let mut spans = vec![Span::styled("WICKETD: ", style::service())];
        spans.extend_from_slice(&wicketd_spans);
        spans.push(Span::styled(" | ", style::divider()));
        spans.push(Span::styled("MGS: ", style::service()));
        spans.extend_from_slice(&mgs_spans);
        let main = Paragraph::new(Spans::from(spans));
        frame.render_widget(main, rect);

        let test = Paragraph::new(Spans::from(vec![
            Span::styled("VERSION: ", Style::default().fg(TUI_GREEN_DARK)),
            Span::styled("v0.0.1", Style::default().fg(TUI_GREEN)),
        ]))
        .alignment(Alignment::Right);
        frame.render_widget(test, rect);
    }
}

/// The mechanism for selecting panes
pub struct Sidebar {
    panes: StatefulList<&'static str>,
    // Whether the sidebar is selected currently.
    selected: bool,
}

impl Sidebar {
    pub fn new(panes: Vec<&'static str>) -> Sidebar {
        let mut sidebar =
            Sidebar { panes: StatefulList::new(panes), selected: true };

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
    fn on(&mut self, _: &mut State, event: Event) -> Option<Action> {
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
        _active: bool,
    ) {
        let items: Vec<ListItem> = self
            .panes
            .items
            .iter()
            .map(|t| {
                let text = *t;
                return ListItem::new(text.to_ascii_uppercase())
                    .style(style::deselected());
            })
            .collect();

        let border_style = if self.selected {
            style::selected_line()
        } else {
            style::deselected()
        };

        let panes = List::new(items)
            .block(
                Block::default()
                    .title("<ESC>")
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .style(border_style),
            )
            .highlight_style(style::selected().add_modifier(Modifier::BOLD));

        frame.render_stateful_widget(panes, area, &mut self.panes.state);
    }
}
