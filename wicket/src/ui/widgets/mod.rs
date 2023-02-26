// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Custom tui widgets

use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;

mod animated_logo;
mod banner;
mod rack;

pub use animated_logo::{Logo, LogoState, LOGO_HEIGHT, LOGO_WIDTH};
pub use banner::Banner;
pub use rack::Rack;

// Set the buf area to the bg color
pub fn clear_buf(area: Rect, buf: &mut Buffer, style: Style) {
    for x in area.left()..area.right() {
        for y in area.top()..area.bottom() {
            buf.get_mut(x, y).set_style(style).set_symbol(" ");
        }
    }
}

/// Animate expansion of a rec diagonally from top-left to bottom-right and
/// drawing the bg color.
///
/// Return the Rect that was drawn
pub fn animate_clear_buf(
    mut rect: Rect,
    buf: &mut Buffer,
    style: Style,
    state: AnimationState,
) -> Rect {
    rect.width = rect.width * state.frame() / state.frame_max();
    rect.height = rect.height * state.frame() / state.frame_max();
    clear_buf(rect, buf, style);
    rect
}

#[derive(Debug, Clone, Copy)]
pub enum AnimationState {
    // Count up from frame = 0 until frame = frame_max
    Opening { frame: u16, frame_max: u16 },
    // Count down from frame = frame_max until frame = 0
    Closing { frame: u16, frame_max: u16 },
}

impl AnimationState {
    pub fn is_done(&self) -> bool {
        match self {
            AnimationState::Opening { frame, frame_max } => frame == frame_max,
            AnimationState::Closing { frame, .. } => *frame == 0,
        }
    }

    // Animate one frame
    //
    /// Return true if animation is complete
    pub fn step(&mut self) -> bool {
        match self {
            AnimationState::Opening { frame, frame_max } => {
                if frame != frame_max {
                    *frame += 1;
                    false
                } else {
                    true
                }
            }
            AnimationState::Closing { frame, .. } => {
                if *frame != 0 {
                    *frame -= 1;
                    false
                } else {
                    true
                }
            }
        }
    }

    pub fn is_opening(&self) -> bool {
        matches!(self, AnimationState::Opening { .. })
    }

    pub fn is_closing(&self) -> bool {
        !self.is_opening()
    }

    pub fn frame(&self) -> u16 {
        match self {
            AnimationState::Closing { frame, .. } => *frame,
            AnimationState::Opening { frame, .. } => *frame,
        }
    }

    pub fn frame_max(&self) -> u16 {
        match self {
            AnimationState::Closing { frame_max, .. } => *frame_max,
            AnimationState::Opening { frame_max, .. } => *frame_max,
        }
    }
}
