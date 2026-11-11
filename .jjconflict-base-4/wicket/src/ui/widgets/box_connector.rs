// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;

/// Do we want to connect this box (bordered block) to the one above it, below
/// it, or both?
#[derive(Debug, PartialEq, Eq)]
pub enum BoxConnectorKind {
    Top,
    Bottom,
    Both,
}

/// Connect a box (bordered block) to its adjacent boxes
pub struct BoxConnector {
    kind: BoxConnectorKind,
}

impl BoxConnector {
    pub fn new(kind: BoxConnectorKind) -> BoxConnector {
        BoxConnector { kind }
    }
}

impl Widget for BoxConnector {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        if self.kind == BoxConnectorKind::Top
            || self.kind == BoxConnectorKind::Both
        {
            buf[(rect.x, rect.y - 1)].set_symbol("├");
            buf[(rect.x + rect.width - 1, rect.y - 1)].set_symbol("┤");
        }

        if self.kind == BoxConnectorKind::Bottom
            || self.kind == BoxConnectorKind::Both
        {
            buf[(rect.x, rect.y + rect.height)].set_symbol("├");
            buf[(rect.x + rect.width - 1, rect.y + rect.height)]
                .set_symbol("┤");
        }
    }
}
