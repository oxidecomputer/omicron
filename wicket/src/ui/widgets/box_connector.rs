// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::widgets::Widget;

// Connect the top and bottom borders of contents in between two bars
pub struct BoxConnector {}

impl Widget for BoxConnector {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        buf.get_mut(rect.x, rect.y - 1).set_symbol("├");
        buf.get_mut(rect.x + rect.width - 1, rect.y - 1).set_symbol("┤");
        buf.get_mut(rect.x, rect.y + rect.height).set_symbol("├");
        buf.get_mut(rect.x + rect.width - 1, rect.y + rect.height)
            .set_symbol("┤");
    }
}
