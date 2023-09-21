// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A screen for playing terminal based games

use crate::state::game::{Rack, SpecialDelivery, Truck};
use crate::ui::defaults::colors::*;
use crate::{Action, Cmd, Control, Frame, State, TICK_INTERVAL};
use ratatui::prelude::*;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};
use slog::{debug, o, Logger};

use super::defaults::dimensions::RectExt;

// How much a rack accelerates while falling in cells/ms^2
const GRAVITY: f32 = 0.000_01;

// The minimum distance that a truck can ever become to the truck in front of
// it. We calculate the closest a truck can come to the truck in front of it
// before we  spawn a truck, so that we can ensure they never crash into each
// other.
const MIN_DIST_BETWEEN_TRUCKS: u16 = 20;

const MIN_TRUCK_SPEED: f32 = 10.0;
const MAX_TRUCK_SPEED: f32 = 30.0;
const ROAD_HEIGHT: u16 = 2;

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
            state.trucks.push(Truck::new(10, rand_speed, state.now_ms));
        }
        self.compute_truck_positions(state);
        self.compute_rack_positions(state, travel_time_ms);
        self.detect_collisions(state);
        Self::compute_game_over(state);
    }

    fn compute_game_over(state: &mut SpecialDelivery) {
        if !state.game_over
            && state.racks_remaining == 0
            && state.racks.is_empty()
            && state.trucks.iter().all(|truck| truck.landed_racks.is_empty())
        {
            state.game_over = true;
        }
    }

    // Calculate when the tail of the last truck reaches `state.rect.width
    // - MIN_DIST_BETWEEN_TRUCKS` and when the front bumper of a new truck
    // driving at `speed` reaches `state.rect.width`. If the new truck reaches
    // first, we can't spawn it.
    fn can_spawn_truck(&self, state: &SpecialDelivery, speed: f32) -> bool {
        let time_for_new_truck = (state.rect.width as f32 / speed) * 1000.0;
        state.trucks.last().map_or(true, |truck| {
            let dist_to_end = state.rect.width + MIN_DIST_BETWEEN_TRUCKS
                - (truck.position + truck.width());
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
                && truck.position > MIN_DIST_BETWEEN_TRUCKS;

            // Only spawn 10% of the attempts
            maintains_distance && rand::random::<f32>() < 0.1
        })
    }

    fn compute_truck_positions(&self, state: &mut SpecialDelivery) {
        state.trucks.retain_mut(|truck| {
            let travel_time_ms = state.now_ms - truck.creation_time_ms;
            truck.position =
                (travel_time_ms as f32 * truck.speed).round() as u16;

            if truck.position >= state.rect.width {
                state.racks_remaining += truck.landed_racks.len() as u32;
                state.racks_delivered += truck.landed_racks.len() as u32;
                false
            } else {
                true
            }
        });
    }

    fn compute_rack_positions(
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

    fn detect_collisions(&self, state: &mut SpecialDelivery) {
        state.racks.retain(|rack| {
            // If the rack can possibly be on a truck, then scan the trucks
            //
            // We account for acceleration jumping more than one cell by allowing
            // the bottom of the ract to not be exactly on top of the truck
            if rack.rect.y + 1 >= state.rect.height - ROAD_HEIGHT {
                for truck in &mut state.trucks {
                    if Self::rack_on_truck(rack, truck) {
                        truck
                            .landed_racks
                            .push(truck.position.saturating_sub(rack.rect.x));
                        // We remove the rack from the "falling" racks set
                        // We end up just drawing it on top of the truck at
                        // the recorded position.
                        return false;
                    }
                }
            }
            true
        });
    }

    // Return true if the rack is positioned on a truck
    fn rack_on_truck(rack: &Rack, truck: &Truck) -> bool {
        let rack_right_pos = rack.rect.x + rack.rect.width;
        rack.rect.x > truck.position.saturating_sub(truck.bed_width)
            && rack_right_pos < truck.position
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
            Cmd::Enter => {
                if state.game_over {
                    state.new_game();
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

        // Is the game over?
        if state.game_over {
            let rect = rect.center_horizontally(66).center_vertically(45);
            let text = Text::from(GRUMPY_CAT);
            let paragraph =
                Paragraph::new(text).style(Style::default().fg(OX_PINK));
            frame.render_widget(paragraph, rect);
            return;
        }

        // Draw the trucks
        for truck in &state.trucks {
            let truck_widget = TruckWidget {
                position: truck.position,
                bed_width: truck.bed_width,
            };
            let mut road_rect = rect;
            road_rect.y = road_rect.height - ROAD_HEIGHT;
            frame.render_widget(truck_widget, road_rect);

            // Draw any racks on top of trucks
            // TODO: Allow more than one rack to land and detect rack collisions
            if let Some(x) = truck.landed_racks.get(0) {
                let x = truck.position.saturating_sub(*x);
                let rack_rect =
                    Rect { x, y: road_rect.y - 2, width: 3, height: 2 };
                frame.render_widget(RackWidget {}, rack_rect);
            }
        }

        // Draw the dropper
        let mut dropper_rect = rect;
        dropper_rect.y = 0;
        dropper_rect.x = state.dropper_pos;
        frame.render_widget(DropperWidget {}, dropper_rect);

        // Only draw the rack attache to the dropper if there isn't one falling in
        // that space currently.
        let mut draw_dropper_rack = true;

        // Draw the falling racks
        for rack in &state.racks {
            if rack.rect.y < rect.height - 1 {
                frame.render_widget(RackWidget {}, rack.rect);
            }
            if rack.rect.y < rect.height / 2 {
                draw_dropper_rack = false;
            }
        }

        // Draw the rack attached to the dropper if there are racks remaining
        if state.racks_remaining > 0 && draw_dropper_rack {
            let mut rack_rect = dropper_rect;
            rack_rect.y += 1;
            rack_rect.x -= 1;
            frame.render_widget(RackWidget {}, rack_rect);
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

        // Draw the entry and exit tunnels
        frame.render_widget(TunnelWidget {}, rect);
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

struct TunnelWidget {}

impl Widget for TunnelWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        // Draw the entry tunnel
        for x in 0..3 {
            for y in rect.height - ROAD_HEIGHT - 3..rect.height {
                buf.get_mut(x, y).set_symbol(" ").set_bg(Color::Black);
            }
        }

        // Draw the exit tunnel
        for x in rect.width - 3..rect.width {
            for y in rect.height - ROAD_HEIGHT - 3..rect.height {
                buf.get_mut(x, y).set_symbol(" ").set_bg(Color::Black);
            }
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
                let x = self.position.saturating_sub(i);
                buf.get_mut(x, rect.y).set_symbol(" ").set_bg(Color::White);
                if (i + 1) % 3 != 0 {
                    buf.get_mut(x, rect.y + 1).set_symbol("◎");
                }
            }
        }
        // Draw the front bumper
        if self.position + 1 < rect.width {
            buf.get_mut(self.position + 1, rect.y).set_symbol("⬤");
        }
    }
}

const RACK_TOP: &str = "☳";
const RACK_BOTTOM: &str = "☶";
struct RackWidget {}

impl Widget for RackWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        // Left black border
        buf.get_mut(rect.x, rect.y).set_symbol(" ").set_bg(TUI_BLACK);
        buf.get_mut(rect.x, rect.y + 1).set_symbol(" ").set_bg(TUI_BLACK);
        // Rack Itself
        buf.get_mut(rect.x + 1, rect.y)
            .set_symbol(RACK_TOP)
            .set_fg(TUI_GREEN)
            .set_bg(TUI_BLACK);
        buf.get_mut(rect.x + 1, rect.y + 1)
            .set_symbol(RACK_BOTTOM)
            .set_fg(TUI_GREEN)
            .set_bg(TUI_BLACK);
        // Right black border
        buf.get_mut(rect.x + 2, rect.y).set_symbol(" ").set_bg(TUI_BLACK);
        buf.get_mut(rect.x + 2, rect.y + 1).set_symbol(" ").set_bg(TUI_BLACK);
    }
}

struct DropperWidget {}

impl Widget for DropperWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        buf.get_mut(rect.x, rect.y).set_symbol("⬇").set_fg(TUI_PURPLE);
    }
}

const GRUMPY_CAT: &str =
    "⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣴⣶⣿⣿⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣿⣷⣤⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⣿⣿⣿⣿⣿⣿⣿⠿⣿⣿⡇⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⣿⣿⣟⣿⡏⠀⠸⣿⣷⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠸⣿⣿⣿⣿⣿⣷⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⠟⣿⣿⡇⠀⠀⣿⣿⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢻⣿⣷⣝⢿⣿⣿⣿⣷⣄⠀⠀⠀⣀⣠⣤⣤⣤⠴⠶⠖⠒⠛⠛⠛⠛⠛⠒⠶⠶⣤⣤⣤⣀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⣯⡽⡀⠛⣿⣧⣼⣿⣿⡏⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⢻⣿⣿⡿⣟⡿⣿⣿⣿⣷⠙⠛⠉⠉⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠻⢿⣶⣿⣿⣿⣿⣿⣿⡭⠉⠁⠀⠀⠁⠢⠈⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠈⢿⣿⣟⢿⣷⠎⠛⠈⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠙⢿⣿⣿⡒⠂⠀⠀⠀⠀⠀⣀⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠘⣿⣿⣷⣍⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠛⠻⣿⣷⣤⣄⠀⢦⡛⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⠏⠀⠀⣀⣀⣀⣴⣤⣴⣶⣿⠇⠀⠀⠀⣶⠀⠀⢰⣷⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⣽⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⣸⣿⡇⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⠁⢸⣿⠀⢰⣿⣆⣰⣿⣿⣿⣦⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⣿⣿⣿⣾⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣼⣿⣿⣷⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⢳⣿⠟⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⡷⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠠⢿⣿⣿⣿⣿⣿⣯⡇⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⣸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠘⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣦⣤⣀⣀⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠸⣿⣿⣿⣿⣽⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢀⣿⣿⣿⣿⣿⠏⢰⣿⠉⠙⢻⣿⣿⣿⡇⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⠟⠙⣿⡏⠉⠛⢿⣿⣿⣿⣿⣦⣀⠀⠀⠀⠀⠀⠀⠉⢻⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⡀⠘⠿⠀⠀⣠⣿⣿⡿⠁⠀⠀⠀⠀⠀⢹⣿⣿⣿⣿⣟⠀⠀⣿⡷⠀⠀⣸⣿⣿⣿⣿⣿⣿⣷⡄⠀⠀⠀⠀⠀⠀⠉⢿⣿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣾⣿⣿⣿⣿⣿⣿⣷⣶⣾⣿⣿⣿⡿⢁⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣤⣄⣀⣀⣠⣾⣿⣿⣿⣿⣿⣿⣿⡛⠃⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠟⣿⣾⣿⣿⣿⣶⣄⠀⠀⠀⠹⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣯⣝⡻⢿⣿⣿⡿⠛⠋⠀⠀⠈⠛⠿⣿⣿⠿⠋⠀⠀⠀⠀⠈⠻⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣷⣮⠇⠀⠀⠀⠀⠀⣀⣤⣴⣿⣿⣶⣤⣤⡀⠀⠀⠀⠀⠀⠈⠛⠿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣿⣇⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣭⣉⣛⣛⣏⠀⠀⠀⠀⣰⣾⠟⠋⠉⠀⠀⠈⠉⠙⠻⣷⡄⠀⠀⠀⠀⠀⠀⠈⠻⣿⣿⣿⣿⣿⣿⣿⡿⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⣿⡀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢻⡿⠿⢿⣟⣟⠀⠀⠀⣼⡟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣆⠀⠀⠀⠀⠀⠀⠈⠿⣭⣭⣭⣭⣭⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⡇⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢸⣶⣿⣿⣿⣿⣄⠀⠀⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠹⣇⠀⠀⠀⠀⠀⢠⣴⣶⣶⣭⣝⡋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⣷⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⠛⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢧⣀⠀⠀⢀⣾⣷⣽⡻⢿⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣿⡆⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠛⢷⣿⣿⣿⣿⣿⣿⣶⣯⣝⣻⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⣿⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣦⣤⣀⣀⠀⠀⠀⠀⠀⣀⡀⠀⠀⠀⠀⠀⠈⠙⠛⠻⠿⠿⠿⠿⠛⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⡀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡛⠛⠛⠛⠛⠛⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣿⣷⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣦⣀⡀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⣿⣆⠀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⣿⡀⠀⠀⠀
⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣭⣽⣿⠛⠛⠛⠛⠛⠛⠛⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⣿⡀⠀⠀
⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣿⣷⡀⠀
⠀⠀⠀⣸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣶⣦⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣿⣧⠀
⠀⠀⢀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠿⠿⠿⠟⠓⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠹⣿⡇
⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⣤⣄⡀⢀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⣇
⣰⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠛⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿
⠉⠉⠉⣧⣭⣍⠉⠉⠉⠉⠉⠉⠉⣭⣭⣭⣭⠉⠉⠉⠉⠀⢀⣀⣤⣴⣶⣶⣤⣄⡀⠀⠀⠀⠀⠀⢠⣤⣤⣤⣤⣤⣤⣤⣤⣄⣀⠀⠀⠀⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⠀⠈⠉
⠀⠀⠀⣿⣿⣿⣧⡀⠀⠀⠀⠀⠀⢹⣿⣿⣿⠀⠀⠀⣠⣶⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⡀⠀⠀⠘⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀
⠀⠀⠀⣿⣿⣿⣿⣿⣄⠀⠀⠀⠀⢸⣿⣿⣿⠀⢠⣾⣿⣿⣿⠿⠛⠋⠉⠉⠛⠿⢿⣿⣿⣿⣆⠀⢰⣿⣿⣿⡟⠛⠋⠉⠛⠻⣿⣿⣿⣧⣿⣿⣿⣿⠛⠛⠛⠉⠉⠉⠛⠛⠀⠀⠀
⠀⠀⠀⣿⣿⣿⣿⣿⣿⣆⠀⠀⠀⢸⣿⣿⣿⢠⣾⣿⣿⡿⠉⠀⠀⠀⠀⠀⠀⠀⠀⠻⣿⣿⣿⣇⠘⣿⣿⣿⡇⠀⠀⠀⠀⠀⢹⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣿⣿⣿⠿⣿⣿⣿⣷⡀⠀⢸⣿⣿⣿⢸⣿⣿⣿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⣿⣿⣿⡌⣿⣿⣿⡗⠀⠀⠀⠀⢀⣼⣿⣿⣿⣿⣿⣿⣿⣤⣤⣤⣤⣤⣤⣤⣤⠀⠀⠀
⠀⠀⠀⣿⣿⣿⠀⠘⢿⣿⣿⣿⣄⢸⣿⣿⣿⢸⣿⣿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣷⣿⣿⣿⣷⣶⣶⣶⣾⣿⣿⣿⣿⠇⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀
⠀⠀⠀⣿⣿⣿⠀⠀⠈⢻⣿⣿⣿⣿⣿⣿⣿⢸⣿⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣾⣿⣿⣿⢹⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠟⠁⠀⣿⣿⣿⣟⠉⠉⠉⠉⠉⠉⠉⠉⠀⠀⠀
⠀⠀⠀⣿⣿⣿⠀⠀⠀⠀⠹⣿⣿⣿⣿⣿⣿⠀⢻⣿⣿⣿⣦⡀⠀⠀⠀⠀⠀⢀⣠⣾⣿⣿⣿⠇⢸⣿⣿⣿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣿⣿⣿⠀⠀⠀⠀⠀⠈⢿⣿⣿⣿⣿⠀⠀⠹⣿⣿⣿⣿⣿⣶⣶⣶⣶⣿⣿⣿⣿⠟⠁⠀⢸⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣿⣶⣶⣶⣶⣶⣶⣶⣾⠀⠀⠀
⠀⠀⠀⣿⣿⣿⡄⠀⠀⠀⠀⠀⠀⠻⣿⣿⣿⡀⠀⠀⠈⠙⠿⢿⣿⣿⣿⣿⣿⣿⠿⠟⠁⠀⠀⠀⢸⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀";
