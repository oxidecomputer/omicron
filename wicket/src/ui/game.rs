// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A screen for playing terminal based games

use crate::state::{GameState, HorizontalDirection, HorizontalPosition};
use crate::ui::defaults::colors::*;
use crate::{Action, Cmd, Control, Frame};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Widget};

/// A screen for games
///
/// This screen is separate from the [`crate::ui::main::MainScreen`] because we
/// want games to be full screen and have as much real estate as possible.
pub struct GameScreen {}

impl GameScreen {
    pub fn new() -> GameScreen {
        GameScreen {}
    }
}

impl Control for GameScreen {
    fn on(&mut self, state: &mut crate::State, cmd: Cmd) -> Option<Action> {
        if Cmd::Tick == cmd {}
        None
    }

    fn draw(
        &mut self,
        state: &crate::State,
        frame: &mut Frame<'_>,
        rect: ratatui::prelude::Rect,
        active: bool,
    ) {
        let truck = TruckWidget {
            position: HorizontalPosition { left: 0, width: 0 },
            direction: HorizontalDirection::Right,
            bed_width: 3,
        };
        frame.render_widget(truck, frame.size());
    }
}

pub struct TruckWidget {
    position: HorizontalPosition,
    direction: HorizontalDirection,
    bed_width: u16,
}

impl Widget for TruckWidget {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        if self.direction == HorizontalDirection::Right {
            // TODO: Handle off screen drawing
            // Draw the top of the truck
            //            let mut x = rect.x + self.bed_width;
            //            buf.get_mut(x, rect.y).set_symbol("⬜");
            //          x = x + 2;
            //          buf.get_mut(x, rect.y).set_symbol("D");

            // Draw the middle of the truck
            buf.get_mut(2, 0)
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
            for i in 0..self.bed_width + 2 {
                let x = rect.x + i;
                //buf.get_mut(x, rect.y + 1).set_symbol("▀");
                //buf.get_mut(x, rect.y + 1).set_symbol("▆");
                //buf.get_mut(x, rect.y + 1).set_symbol("▇");
                buf.get_mut(x, rect.y + 2).set_symbol(" ").set_bg(Color::White);
            }
            buf.get_mut(self.bed_width + 2, rect.y + 2).set_symbol("⬤");

            // Draw the wheels
            buf.get_mut(0, rect.y + 3).set_symbol("◎");
            buf.get_mut(1, rect.y + 3).set_symbol("◎");
            buf.get_mut(3, rect.y + 3).set_symbol("◎");
            buf.get_mut(self.bed_width + 1, rect.y + 3).set_symbol("◎");
        } else {
            unimplemented!()
        }
    }
}

const RACK_TOP: &str = "☳";
const RACK_BOTTOM: &str = "☶";
