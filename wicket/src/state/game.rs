// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The state of our game screen
//!
//! We store the state in the global state struct so that
//! we can use the replay debugger.

use ratatui::prelude::Rect;
use serde::{Deserialize, Serialize};

/// The state of our [`crate::ui::game::GameScreen`]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameState {
    delivery: SpecialDelivery,
}

impl GameState {
    pub fn new() -> GameState {
        GameState { delivery: SpecialDelivery::new() }
    }
}

/// The state for the game "Special Delivery"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecialDelivery {}

impl SpecialDelivery {
    fn new() -> SpecialDelivery {
        SpecialDelivery {}
    }
}

/// The horizontal position of an entity on the grid in characters
///
/// Since entities may take multiple characters to draw, we must represent their
/// positions to allow negatives, with a negative left position meaning shifted
/// that amount of characters off screen. However, the width of the object takes
/// up space so it can never be negative.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HorizontalPosition {
    pub left: i16,
    pub width: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HorizontalDirection {
    Left,
    Right,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Truck {
    position: HorizontalPosition,
    direction: HorizontalDirection,
    speed: u16, // meters/second
    bed_width: u16,
}

//
// Dimensions are in meters so that we can do physics pretending to be in the
// real world. We'll convert these dimensions back into characters on the grid
// for rendering.
//
const RACK_HEIGHT: f32 = 3.0;
const RACK_WIDTH: f32 = 1.0;

const CHAR_HEIGHT: f32 = RACK_HEIGHT;
const CHAR_WIDTH: f32 = RACK_WIDTH;

// The dimensions of the world in meters
pub struct World {
    width: f32,
    height: f32,
}

impl World {
    fn resize(&mut self, width: u16, height: u16) {
        self.width = width as f32 * CHAR_WIDTH;
        self.height = height as f32 * CHAR_HEIGHT;
    }
}
