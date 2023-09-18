// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A screen for playing terminal based games

use crate::state::game::{Rack, SpecialDelivery, Truck};
use crate::ui::defaults::colors::*;
use crate::{Action, Cmd, Control, Frame, State, TICK_INTERVAL};
use ratatui::prelude::*;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Widget};
use slog::{debug, o, Logger};

// How much a rack accelerates while falling in cells/ms^2
const GRAVITY: f32 = 0.000_02;

// The minimum distance that a truck can ever become to the truck in front of
// it. We calculate the closest a truck can come to the truck in front of it
// before we  spawn a truck, so that we can ensure they never crash into each
// other.
const MIN_DIST_BETWEEN_TRUCKS: u16 = 20;

const MIN_TRUCK_SPEED: f32 = 10.0;
const MAX_TRUCK_SPEED: f32 = 30.0;

/// A screen for games
///
/// This screen is separate from the [`crate::ui::main::MainScreen`] because we
/// want games to be full screen and have as much real estate as possible.
pub struct GameScreen {
    #[allow(unused)]
    log: Logger,
    // Whether the screen has been resized initially.
    initialized: bool,
}

impl GameScreen {
    pub fn new(log: &Logger) -> GameScreen {
        let log = log.new(o!("component" => "GameScreen"));
        GameScreen { log, initialized: false }
    }

    pub fn update(&mut self, state: &mut SpecialDelivery) {
        let travel_time_ms = u64::try_from(TICK_INTERVAL.as_millis()).unwrap();
        state.now_ms += travel_time_ms;
        // TODO: Should we inject randomness in the event loop? This makes
        // the games non-deterministic and inhibits the replay debugger.
        let rand_speed = rand::random::<f32>()
            * (MAX_TRUCK_SPEED - MIN_TRUCK_SPEED)
            + MIN_TRUCK_SPEED;
        if self.can_spawn_truck(state, rand_speed) {
            state.trucks.push(Truck::new(5, rand_speed, state.now_ms));
        } else {
        }
        self.compute_truck_positions(state);
        self.computer_rack_positions(state, travel_time_ms);
    }

    // Calculate when the tail of the last truck reaches `state.rect.width
    // - MIN_DIST_BETWEEN_TRUCKS` and when the front bumper of a new truck
    // driving at `speed` reaches `state.rect.width`. If the new truck reaches
    // first, we can't spawn it.
    fn can_spawn_truck(&self, state: &SpecialDelivery, speed: f32) -> bool {
        let time_for_new_truck = (state.rect.width as f32 / speed) * 1000.0;
        state.trucks.last().map_or(true, |truck| {
            let dist_to_end = state.rect.width + MIN_DIST_BETWEEN_TRUCKS
                - truck.position
                + truck.bed_width;
            // Stored speed is in cells/ms, but we pass in cells/s as that makes
            // more sense to a human.
            let time_for_last_truck = dist_to_end as f32 / (truck.speed);
            debug!(
                self.log,
                "dist_to_end = {}, rect width = {}",
                dist_to_end,
                state.rect.width
            );
            debug!(self.log, "{} {}", time_for_last_truck, time_for_new_truck);
            let maintains_distance = time_for_last_truck < time_for_new_truck
                && truck.position > MIN_DIST_BETWEEN_TRUCKS + truck.bed_width;

            // Only spawn 10% of the attempts
            maintains_distance && rand::random::<f32>() < 0.1
        })
    }

    fn compute_truck_positions(&self, state: &mut SpecialDelivery) {
        state.trucks.retain_mut(|truck| {
            let travel_time_ms = state.now_ms - truck.creation_time_ms;
            truck.position =
                (travel_time_ms as f32 * truck.speed).round() as u16;
            if truck.position + truck.bed_width > state.rect.width {
                false
            } else {
                true
            }
        });
    }

    fn computer_rack_positions(
        &self,
        state: &mut SpecialDelivery,
        travel_time_ms: u64,
    ) {
        state.racks.retain_mut(|rack| {
            // Perform some lightweight numerical integration
            // First we find our velocity change over the given travel time If
            // you want to pretend this is calculus, just pretend the interval
            // is infinitely small. Then we calculate the change in position
            // during the travel time using the new velocity. Then, we see if
            // the position advanced enough make it to the next cell.
            let delta = travel_time_ms as f32 * GRAVITY;
            rack.velocity += delta;
            rack.vertical_pos += rack.velocity * travel_time_ms as f32;
            rack.rect.y = rack.vertical_pos.round() as u16;
            if rack.rect.y >= state.rect.height {
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
        let state = &mut state.game_state.delivery;
        match cmd {
            Cmd::Tick => self.update(state),
            Cmd::Left => {
                state.dropper_pos = u16::max(state.dropper_pos - 1, 1);
            }
            Cmd::Right => {
                state.dropper_pos =
                    u16::min(state.dropper_pos + 1, state.rect.width - 2);
            }
            Cmd::GotoLeft => state.dropper_pos = 1,
            Cmd::GotoRight => state.dropper_pos = state.rect.width - 2,
            Cmd::GotoCenter => state.dropper_pos = state.rect.width / 2,
            Cmd::Toggle => {
                if state.racks_remaining > 0 {
                    let mut rect = state.rect;
                    rect.y = 1;
                    rect.x = state.dropper_pos - 1;
                    rect.height = 2;
                    rect.width = 3;
                    state.racks.push(Rack::new(rect));
                    state.racks_remaining =
                        state.racks_remaining.saturating_sub(1);
                }
            }
            _ => return None,
        }

        Some(Action::Redraw)
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        _active: bool,
    ) {
        let state = &state.game_state.delivery;

        // Draw the trucks
        for truck in &state.trucks {
            let truck_widget = TruckWidget {
                position: truck.position,
                bed_width: truck.bed_width,
            };
            let mut road_rect = rect;
            road_rect.y = road_rect.height - 2;
            frame.render_widget(truck_widget, road_rect);
        }

        // Draw the dropper
        let mut dropper_rect = rect;
        dropper_rect.y = 0;
        dropper_rect.x = state.dropper_pos;
        frame.render_widget(DropperWidget {}, dropper_rect);

        // Draw the rack attached to the dropper if there are racks remaining
        if state.racks_remaining > 0 {
            let mut rack_rect = dropper_rect;
            rack_rect.y += 1;
            rack_rect.x -= 1;
            frame.render_widget(RackWidget {}, rack_rect);
        }

        // Draw the falling racks
        for rack in &state.racks {
            if rack.rect.y < rect.height - 1 {
                frame.render_widget(RackWidget {}, rack.rect);
            }
        }

        // Draw the scoreboard
        let line = Line::styled(
            format!("Racks Remaining: {}", state.racks_remaining),
            Style::default().fg(OX_RED),
        );
        let mut remaining_rect = rect;
        remaining_rect.x = 2;
        remaining_rect.width = line.width() as u16;
        let remaining = Paragraph::new(line);
        frame.render_widget(remaining, remaining_rect);

        let line = Line::styled(
            format!("Racks Delivered: {}", state.racks_delivered),
            Style::default().fg(TUI_GREEN),
        );
        let mut delivered_rect = rect;
        delivered_rect.x = rect.width - line.width() as u16 - 2;
        delivered_rect.width = line.width() as u16;
        let delivered = Paragraph::new(line);
        frame.render_widget(delivered, delivered_rect);
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

struct TruckWidget {
    position: u16,
    bed_width: u16,
}

impl Widget for TruckWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        // Draw the bed and wheels
        for i in 0..self.bed_width {
            if self.position > i && (self.position + i) < rect.width {
                let x = self.position - i;
                buf.get_mut(x, rect.y).set_symbol(" ").set_bg(Color::White);
                buf.get_mut(x, rect.y + 1).set_symbol("◎");
            }
        }
        // Draw the front bumper
        if self.position < rect.width {
            buf.get_mut(self.position + 1, rect.y).set_symbol("⬤");
        }
    }
}

const RACK_TOP: &str = "☳";
const RACK_BOTTOM: &str = "☶";
struct RackWidget {}

impl Widget for RackWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        buf.get_mut(rect.x + 1, rect.y)
            .set_symbol(RACK_TOP)
            .set_fg(TUI_GREEN)
            .set_bg(TUI_BLACK);
        buf.get_mut(rect.x + 1, rect.y + 1)
            .set_symbol(RACK_BOTTOM)
            .set_fg(TUI_GREEN)
            .set_bg(TUI_BLACK);
        buf.get_mut(rect.x + 2, rect.y).set_symbol(" ").set_bg(TUI_BLACK);
        buf.get_mut(rect.x + 2, rect.y + 1).set_symbol(" ").set_bg(TUI_BLACK);
        buf.get_mut(rect.x, rect.y).set_symbol(" ").set_bg(TUI_BLACK);
        buf.get_mut(rect.x, rect.y + 1).set_symbol(" ").set_bg(TUI_BLACK);
    }
}

struct DropperWidget {}

impl Widget for DropperWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        buf.get_mut(rect.x, rect.y).set_symbol("⬇").set_fg(TUI_PURPLE);
    }
}
