// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A screen for playing terminal based games

use crate::state::game::{SpecialDelivery, Truck};
use crate::ui::defaults::colors::*;
use crate::{Action, Cmd, Control, Frame, State, TICK_INTERVAL};
use ratatui::prelude::*;
use ratatui::widgets::Widget;

/// A screen for games
///
/// This screen is separate from the [`crate::ui::main::MainScreen`] because we
/// want games to be full screen and have as much real estate as possible.
pub struct GameScreen {
    // Whether the screen has been resized initially.
    initialized: bool,
}

impl GameScreen {
    pub fn new() -> GameScreen {
        GameScreen { initialized: false }
    }

    pub fn update(&mut self, state: &mut SpecialDelivery) {
        state.now_ms += u64::try_from(TICK_INTERVAL.as_millis()).unwrap();
        if state.trucks.is_empty() {
            state.trucks.push(Truck::new(5, 3.0));
        }
        self.compute_truck_positions(state);
    }

    fn compute_truck_positions(&mut self, state: &mut SpecialDelivery) {
        state.trucks.retain_mut(|truck| {
            truck.position =
                (truck.travel_time_ms as f32 * truck.speed).ceil() as u16;
            if truck.position + truck.bed_width > state.rect.width {
                false
            } else {
                true
            }
        });
    }
}

impl Control for GameScreen {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        // Just one truck for now
        if Cmd::Tick == cmd {
            self.update(&mut state.game_state.delivery);
        }
        Some(Action::Redraw)
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        _: Rect,
        _active: bool,
    ) {
        for truck in &state.game_state.delivery.trucks {
            let truck_widget = TruckWidget {
                position: truck.position,
                bed_width: truck.bed_width,
            };
            let mut road_rect = frame.size();
            road_rect.y = road_rect.height - 2;
            frame.render_widget(truck_widget, road_rect);
        }
    }

    fn resize(&mut self, state: &mut State, rect: Rect) {
        let game_state = &mut state.game_state.delivery;
        game_state.rect = rect;
        if !self.initialized {
            self.initialized = true;
            game_state.dropper_pos = rect.width / 2;
        }
    }
}

pub struct TruckWidget {
    position: u16,
    bed_width: u16,
}

impl Widget for TruckWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        // Draw the middle of the truck
        /*buf.get_mut(2, 0)
            .set_symbol(RACK_TOP)
            .set_fg(TUI_GREEN)
            .set_bg(TUI_BLACK);
        buf.get_mut(2, 1)
            .set_symbol(RACK_BOTTOM)
            .set_fg(TUI_GREEN)
            .set_bg(TUI_BLACK);
            buf.get_mut(3, 0).set_symbol(" ").set_bg(TUI_BLACK);
            buf.get_mut(3, 1).set_symbol(" ").set_bg(TUI_BLACK);
            buf.get_mut(1, 0).set_symbol(" ").set_bg(TUI_BLACK);
            buf.get_mut(1, 1).set_symbol(" ").set_bg(TUI_BLACK);
        */
        // Draw the bed
        for i in 0..self.bed_width {
            let x = rect.x + i;
            buf.get_mut(x, rect.y).set_symbol(" ").set_bg(Color::White);
        }
        // Draw the front bumper
        buf.get_mut(self.bed_width, rect.y).set_symbol("⬤");

        // Draw the wheels
        for i in 0..self.bed_width {
            buf.get_mut(i, rect.y + 1).set_symbol("◎");
        }
    }
}

const RACK_TOP: &str = "☳";
const RACK_BOTTOM: &str = "☶";
