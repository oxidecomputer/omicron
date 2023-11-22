// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::Widget;
use std::str::Lines;

/// A banner draws the text output from `banner` as stored in string.
///
/// This assumes the source string is ASCII and not Unicode. This should be
/// fine Since `banner` outputs `#` characters and whitespace only.
///
/// If the Banner is smaller than the Rect being rendered to, it won't be
/// rendered at all. Should we provide an option to print a partial
/// banner instead of nothing in this case?
pub struct Banner<'a> {
    lines: Lines<'a>,
    style: Style,
}

impl<'a> Banner<'a> {
    pub fn new(text: &'a str) -> Banner<'a> {
        Banner { lines: text.lines(), style: Style::default() }
    }

    pub fn style(mut self, style: Style) -> Banner<'a> {
        self.style = style;
        self
    }

    pub fn height(&self) -> u16 {
        self.lines.clone().count().try_into().unwrap()
    }

    pub fn width(&self) -> u16 {
        self.lines
            .clone()
            .map(|l| l.len())
            .max()
            .unwrap_or(0)
            .try_into()
            .unwrap()
    }
}

impl<'a> Widget for Banner<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height < self.height() || area.width < self.width() {
            // Don't draw anything if the banner doesn't fit in `area`
            return;
        }

        for (y, line) in self.lines.enumerate() {
            let mut len: u16 = 0;
            for (x, c) in line.chars().enumerate() {
                let cell = buf
                    .get_mut(x as u16 + area.left(), y as u16 + area.top())
                    .set_symbol(" ");
                if c == '#' {
                    if let Some(fg) = self.style.fg {
                        cell.set_bg(fg);
                    }
                } else if let Some(bg) = self.style.bg {
                    cell.set_bg(bg);
                }
                len = x as u16;
            }

            // Fill out the rest of the line with the background color
            let start = area.left() + len + 1;
            for x in start..area.right() {
                let cell =
                    buf.get_mut(x, y as u16 + area.top()).set_symbol(" ");
                if let Some(bg) = self.style.bg {
                    cell.set_bg(bg);
                }
            }
        }
    }
}
