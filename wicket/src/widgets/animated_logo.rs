// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Animated Oxide logo used for the splash screen

use std::str::Lines;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;
use tui::widgets::Widget;

pub const LOGO_HEIGHT: u16 = 7;
pub const LOGO_WIDTH: u16 = 46;

pub struct LogoState {
    // The current animation frame
    pub frame: usize,

    // The text of the logo in "# banner" form
    pub text: &'static str,
}

// We don't need a `StatefulWidget`, since state is never updated during drawing.
// We just borrow the `LogoState` as part of `Logo`.
pub struct Logo<'a> {
    state: &'a LogoState,
    // The style of the not-yet-hightlighted letters
    stale_style: Style,
    // The style of the highlighted letters besides the `x`.
    style: Style,
    // The style of the highlighted `x`
    x_style: Style,
}

// Styling is mandatory!
impl<'a> Logo<'a> {
    pub fn new(state: &'a LogoState) -> Logo {
        Logo {
            state,
            stale_style: Style::default(),
            style: Style::default(),
            x_style: Style::default(),
        }
    }

    pub fn stale_style(mut self, style: Style) -> Logo<'a> {
        self.stale_style = style;
        self
    }
    pub fn style(mut self, style: Style) -> Logo<'a> {
        self.style = style;
        self
    }

    pub fn x_style(mut self, style: Style) -> Logo<'a> {
        self.x_style = style;
        self
    }
}

impl<'a> Widget for Logo<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        for (y, line) in self.state.text.lines().enumerate() {
            for (x, c) in line.chars().enumerate() {
                if c == '#' {
                    let paint_point = self.state.frame / 3 * 2;
                    let cell = buf
                        .get_mut(x as u16 + area.left(), y as u16 + area.top())
                        .set_symbol(" ");
                    if x < paint_point {
                        // The cell is highlighted
                        if x < 11 || x > 17 {
                            cell.set_bg(self.style.fg.unwrap());
                        } else {
                            // We're painting the Oxide `x`
                            cell.set_bg(self.x_style.fg.unwrap());
                        }
                    } else {
                        cell.set_bg(self.stale_style.fg.unwrap());
                    }
                }
            }
        }
    }
}
