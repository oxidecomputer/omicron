// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default dimensions for various widgets common across screens

use tui::layout::Rect;

pub const MENUBAR_HEIGHT: u16 = 5;

// Extension trait for Rects that provide centering and placement capabilities
pub trait RectExt {
    /// Center a given Rect horizontally and size it to match widget_width
    fn center_x(self, widget_width: u16) -> Self;

    /// Center a given Rect vertically and size it to match widget_height
    fn center_y(self, widget_height: u16) -> Self;

    /// Place a rect at a given horizontal position and size it so it doesn't
    /// overflow its container.
    fn set_x(self, x: u16) -> Self;

    /// Place a rect at a given vertical position and size it so it doesn't
    /// overflow its container.
    fn set_y(self, y: u16) -> Self;
}

impl RectExt for Rect {
    fn center_x(mut self, widget_width: u16) -> Self {
        let center = (self.width - widget_width) / 2;
        self.x += center;
        self.width -= center;
        self
    }

    fn center_y(mut self, widget_height: u16) -> Self {
        let center = (self.height - widget_height) / 2;
        self.y += center;
        self.height -= center;
        self
    }

    fn set_x(mut self, x: u16) -> Self {
        self.x = x;
        self.width -= self.x;
        self
    }

    fn set_y(mut self, y: u16) -> Self {
        self.y = y;
        self.height -= self.y;
        self
    }
}
