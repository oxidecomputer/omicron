// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ui::defaults::style;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;

/// Style every character as `style::faded_background`
#[derive(Default)]
pub struct Fade {}

impl Widget for Fade {
    fn render(self, area: Rect, buf: &mut Buffer) {
        buf.set_style(area, style::faded_background());
    }
}
