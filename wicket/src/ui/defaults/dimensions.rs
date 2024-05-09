// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default dimensions for various widgets common across screens

use ratatui::layout::Rect;

// Extension trait for Rects that provide centering and placement capabilities
pub trait RectExt {
    /// Create a new `Rect` with the given width centered horizontally within
    /// `self`.
    /// Panics if `width > self.width`.
    fn center_horizontally(self, width: u16) -> Self;

    /// Create a new `Rect` with the given height centered vertically within
    /// `self`.
    ///
    /// Panics if `height > self.height`.
    fn center_vertically(self, height: u16) -> Self;
}

impl RectExt for Rect {
    fn center_horizontally(mut self, width: u16) -> Self {
        let center = (self.width.saturating_sub(width)) / 2;
        self.x += center;
        self.width = width;
        self
    }

    fn center_vertically(mut self, height: u16) -> Self {
        let center = (self.height - height) / 2;
        self.y += center;
        self.height = height;
        self
    }
}
