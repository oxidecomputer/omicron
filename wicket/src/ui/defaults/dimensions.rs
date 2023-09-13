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

    /// Create a new maximally sized `Rect` that is bounded by `self`, and
    /// shifted down by `y` columns. In order to maintain the bounding, the
    /// new `Rect` is originally sized to `self` and then shrunk by the same
    /// amount it is shifted downwards: namely `y` columns.
    ///
    /// Panics if `y > self.height`.
    fn move_down_within_bounds(self, y: u16) -> Self;
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

    fn move_down_within_bounds(mut self, y: u16) -> Self {
        self.y = self.y + y;
        self.height -= y;
        self
    }
}
