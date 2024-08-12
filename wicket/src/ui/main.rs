// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::{Control, OverviewPane, RackSetupPane, StatefulList, UpdatePane};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::widgets::Fade;
use crate::{Action, Cmd, State, Term};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, List, ListItem, Paragraph};
use ratatui::Frame;
use slog::{o, Logger};
use wicketd_client::types::GetLocationResponse;

/// The [`MainScreen`] is the primary UI element of the terminal, covers the
/// entire terminal window/buffer and is visible for all interactions except
/// the initial splash screen animation.
///
/// This structure allows us to maintain similar styling and navigation
/// throughout wicket with a minimum of code.
///
/// Specific functionality is put inside Panes, which can be customized
/// as needed.
pub struct MainScreen {
    #[allow(unused)]
    log: Logger,
    sidebar: Sidebar,
    panes: BTreeMap<&'static str, Box<dyn Control>>,
    rect: Rect,
    sidebar_rect: Rect,
    pane_rect: Rect,
}

impl MainScreen {
    pub fn new(log: &Logger) -> MainScreen {
        // We want the sidebar ordered in this specific manner
        let sidebar_ordered_panes = vec![
            ("overview", Box::new(OverviewPane::new()) as Box<dyn Control>),
            ("update", Box::new(UpdatePane::new(log))),
            ("rack setup", Box::<RackSetupPane>::default()),
        ];
        let sidebar_keys: Vec<_> =
            sidebar_ordered_panes.iter().map(|&(title, _)| title).collect();
        let log = log.new(o!("component" => "MainScreen"));
        MainScreen {
            log,
            sidebar: Sidebar::new(sidebar_keys),
            panes: BTreeMap::from_iter(sidebar_ordered_panes),
            rect: Rect::default(),
            sidebar_rect: Rect::default(),
            pane_rect: Rect::default(),
        }
    }

    /// Draw the [`MainScreen`]
    pub fn draw(
        &mut self,
        state: &State,
        terminal: &mut Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|frame| {
            let mut rect = frame.area();

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
            frame.render_widget(background, frame.area());
            self.sidebar.draw(state, frame, chunks[0], self.sidebar.active);
            self.draw_pane(state, frame, chunks[1]);
            self.draw_statusbar(state, frame, statusbar_rect);
        })?;
        Ok(())
    }

    /// Compute the layout of the [`Sidebar`] and pane
    ///
    // A draw is issued after every resize, so no need to return an Action
    pub fn resize(&mut self, state: &mut State, width: u16, height: u16) {
        self.rect = Rect { x: 0, y: 0, width, height };

        // We have a 1 row status bar at the bottom when we draw. Subtract it
        // from the height;
        let mut layout_rect = self.rect;
        layout_rect.height -= 1;

        // Size the individual components of the screen
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .margin(1)
            .constraints(
                [Constraint::Length(20), Constraint::Max(1000)].as_ref(),
            )
            .split(layout_rect);

        self.sidebar_rect = chunks[0];
        self.pane_rect = chunks[1];

        // Avoid borrow checker complaints
        let pane_rect = self.pane_rect;
        self.current_pane().resize(state, pane_rect);
    }

    /// Handle a [`Cmd`] to update state and output any necessary actions for the
    /// system to take.
    pub fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            // There's just two panes, so next and previous do the same thing
            // for now.
            Cmd::NextPane | Cmd::PrevPane => {
                if self.sidebar.active {
                    self.sidebar.active = false;
                    Some(Action::Redraw)
                } else {
                    if self.current_pane().is_modal_active() {
                        self.current_pane().on(state, cmd)
                    } else {
                        self.sidebar.active = true;
                        Some(Action::Redraw)
                    }
                }
            }
            Cmd::Enter => {
                if self.sidebar.active {
                    self.sidebar.active = false;
                    Some(Action::Redraw)
                } else {
                    self.current_pane().on(state, cmd)
                }
            }
            _ => self.dispatch_cmd(state, cmd),
        }
    }

    fn dispatch_cmd(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        let current_pane = self.sidebar.selected_pane();
        if self.sidebar.active {
            let _ = self.sidebar.on(state, cmd);
            if self.sidebar.selected_pane() != current_pane {
                // We need to inform the new pane, which may not have
                // ever been drawn what its Rect is.
                self.resize(state, self.rect.width, self.rect.height);
                Some(Action::Redraw)
            } else {
                None
            }
        } else {
            self.current_pane().on(state, cmd)
        }
    }

    fn current_pane(&mut self) -> &mut Box<dyn Control> {
        self.panes.get_mut(self.sidebar.selected_pane()).unwrap()
    }

    fn draw_pane(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        pane_rect: Rect,
    ) {
        let active = !self.sidebar.active;
        let pane = self.current_pane();
        pane.draw(state, frame, pane_rect, active);
        if !active {
            let fade = Fade::default();
            frame.render_widget(fade, pane_rect);
        }
    }

    fn draw_statusbar(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
    ) {
        let location_spans = location_spans(&state.wicketd_location);
        let wicketd_spans = state.service_status.wicketd_liveness().to_spans();
        let mgs_spans = state.service_status.mgs_liveness().to_spans();
        let mut spans = vec![Span::styled("You are here: ", style::service())];
        spans.extend_from_slice(&location_spans);
        spans.push(Span::styled(" | ", style::divider()));
        spans.push(Span::styled("WICKETD: ", style::service()));
        spans.extend_from_slice(&wicketd_spans);
        spans.push(Span::styled(" | ", style::divider()));
        spans.push(Span::styled("MGS: ", style::service()));
        spans.extend_from_slice(&mgs_spans);
        let main = Paragraph::new(Line::from(spans));
        frame.render_widget(main, rect);

        let system_version = state
            .update_state
            .system_version
            .as_ref()
            .map_or_else(|| "UNKNOWN".to_string(), |v| v.to_string());

        let test = Paragraph::new(Line::from(vec![
            Span::styled(
                "UPDATE VERSION: ",
                Style::default().fg(TUI_GREEN_DARK),
            ),
            Span::styled(system_version, style::plain_text()),
        ]))
        .alignment(Alignment::Right);
        frame.render_widget(test, rect);
    }
}

fn location_spans(location: &GetLocationResponse) -> Vec<Span<'static>> {
    // We reuse `style::connected()` and `style::delayed()` in these spans to
    // match the wicketd/mgs connection statuses that follow us in the status
    // bar.
    let mut spans = Vec::new();
    if let Some(id) = location.sled_id.as_ref() {
        spans.push(Span::styled(
            format!("Sled {}", id.slot),
            style::connected(),
        ));
    } else if let Some(baseboard) = location.sled_baseboard.as_ref() {
        spans.push(Span::styled(
            format!("Sled {}", baseboard.identifier()),
            style::connected(),
        ));
    } else {
        spans.push(Span::styled("Sled UNKNOWN", style::delayed()));
    };
    spans.push(Span::styled("/", style::divider()));
    if let Some(id) = location.switch_id.as_ref() {
        spans.push(Span::styled(
            format!("Switch {}", id.slot),
            style::connected(),
        ));
    } else if let Some(baseboard) = location.switch_baseboard.as_ref() {
        spans.push(Span::styled(
            format!("Switch {}", baseboard.identifier()),
            style::connected(),
        ));
    } else {
        spans.push(Span::styled("Switch UNKNOWN", style::delayed()));
    };
    spans
}

/// The mechanism for selecting panes
pub struct Sidebar {
    panes: StatefulList<&'static str>,
    // Whether the sidebar is selected currently.
    active: bool,
}

impl Sidebar {
    pub fn new(panes: Vec<&'static str>) -> Sidebar {
        let mut sidebar =
            Sidebar { panes: StatefulList::new(panes), active: true };

        // Select the first pane
        sidebar.panes.next();
        sidebar
    }

    /// Return the name of the selected Pane
    ///
    /// TODO: Is an `&'static str` good enough? Should we define a `PaneId`?
    pub fn selected_pane(&self) -> &'static str {
        self.panes.items[self.panes.state.selected().unwrap()]
    }
}

impl Control for Sidebar {
    fn on(&mut self, _: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            Cmd::Up => {
                self.panes.previous();
                Some(Action::Redraw)
            }
            Cmd::Down => {
                self.panes.next();
                Some(Action::Redraw)
            }
            _ => None,
        }
    }

    fn draw(
        &mut self,
        _state: &State,
        frame: &mut Frame<'_>,
        area: Rect,
        active: bool,
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

        let border_style = style::selected_line();

        let panes = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .style(border_style),
            )
            .highlight_style(style::selected().add_modifier(Modifier::BOLD));

        frame.render_stateful_widget(panes, area, &mut self.panes.state);

        if !active {
            let fade = Fade::default();
            frame.render_widget(fade, area);
        }
    }
}
