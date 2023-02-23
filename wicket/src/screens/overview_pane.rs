// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{get_control_id, Control, ControlId, Pane};
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
    control_id: ControlId,
}

impl OverviewPane {
    pub fn new() -> OverviewPane {
        OverviewPane { control_id: get_control_id() }
    }
}

impl Pane for OverviewPane {
    fn tabs(&self) -> &[&'static str] {
        &["Oxide Rack"]
    }

    fn selected_tab(&self) -> usize {
        // There's only one tab
        0
    }
}

impl Control for OverviewPane {
    fn control_id(&self) -> ControlId {
        self.control_id
    }

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
                _ => None,
            },
            Event::Tick => {
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
