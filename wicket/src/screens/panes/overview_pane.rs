// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Control, Pane, Tab};
use crate::defaults::colors::*;
use crate::defaults::style;
use crate::widgets::{Rack, RackState};
use crate::{Action, Event, Frame, State};
use crossterm::event::Event as TermEvent;
use crossterm::event::{KeyCode, KeyEvent};
use tui::style::{Color, Modifier, Style};

/// The OverviewPane shows a rendering of the rack.
///
/// This is useful for getting a quick view of the state of the rack.
pub struct OverviewPane {
    // We want the controls in a specific order accessible by index
    tabs: Vec<Tab>,
    selected: usize,
}

impl OverviewPane {
    pub fn new() -> OverviewPane {
        OverviewPane {
            tabs: vec![
                Tab { title: "OXIDE RACK", control: Box::new(RackTab {}) },
                Tab { title: "INVENTORY", control: Box::new(InventoryTab {}) },
            ],
            selected: 0,
        }
    }
}

impl Pane for OverviewPane {
    fn tab_titles(&self) -> Vec<&'static str> {
        self.tabs.iter().map(|t| t.title).collect()
    }

    fn selected_tab(&self) -> usize {
        self.selected
    }
}

impl Control for OverviewPane {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Tab => {
                    self.selected = (self.selected + 1) % self.tabs.len();
                    Some(Action::Redraw)
                }
                _ => self.tabs[self.selected].control.on(state, event),
            },
            _ => self.tabs[self.selected].control.on(state, event),
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: tui::layout::Rect,
    ) {
        self.tabs[self.selected].control.draw(state, frame, rect)
    }
}

pub struct RackTab {}

impl Control for RackTab {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Up => {
                    state.rack_state.up();
                    Some(Action::Redraw)
                }
                KeyCode::Down => {
                    state.rack_state.down();
                    Some(Action::Redraw)
                }
                KeyCode::Char('k') => {
                    state.rack_state.toggle_knight_rider_mode();
                    Some(Action::Redraw)
                }
                KeyCode::Left | KeyCode::Right => {
                    state.rack_state.left_or_right();
                    Some(Action::Redraw)
                }
                _ => None,
            },
            Event::Tick => {
                // TODO: This only animates when the pane is active. Should we move the
                // tick into the wizard instead?
                if let Some(k) = state.rack_state.knight_rider_mode.as_mut() {
                    k.step();
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: tui::layout::Rect,
    ) {
        let rack = Rack {
            state: &state.rack_state,
            switch_style: Style::default().bg(OX_GRAY_DARK).fg(OX_WHITE),
            power_shelf_style: Style::default().bg(OX_GRAY).fg(OX_OFF_WHITE),
            sled_style: Style::default().bg(OX_GREEN_LIGHT).fg(Color::Black),
            sled_selected_style: Style::default()
                .fg(Color::Black)
                .bg(OX_GRAY_DARK),

            border_style: Style::default().fg(OX_GRAY).bg(Color::Black),
            border_selected_style: Style::default()
                .fg(OX_YELLOW)
                .bg(OX_GRAY_DARK),

            switch_selected_style: Style::default().bg(OX_GRAY_DARK),
            power_shelf_selected_style: Style::default().bg(OX_GRAY),
        };

        frame.render_widget(rack, rect);
    }
}

pub struct InventoryTab {}

impl Control for InventoryTab {
    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: tui::layout::Rect,
    ) {
    }

    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        None
    }
}
