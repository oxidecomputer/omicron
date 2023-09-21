// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The state of our game screen
//!
//! We store the state in the global state struct so that
//! we can use the replay debugger.

use ratatui::prelude::Rect;
use serde::{Deserialize, Serialize};

const NUM_RACKS: u32 = 10;

/// The state of our [`crate::ui::game::GameScreen`]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameState {
    pub delivery: SpecialDelivery,
}

impl GameState {
    pub fn new() -> GameState {
        GameState { delivery: SpecialDelivery::new() }
    }
}

///
/// The state for the game "Special Delivery"
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecialDelivery {
    // Time from the start of the game in ms
    pub now_ms: u64,
    pub rect: Rect,
    pub racks_remaining: u32,
    pub racks_delivered: u32,
    pub trucks: Vec<Truck>,
    pub racks: Vec<Rack>,
    // The user controlled position of the rack to be dropped
    pub dropper_pos: u16,
    pub crashes: Vec<Crash>,

    pub game_over: bool,
}

impl SpecialDelivery {
    pub fn new() -> SpecialDelivery {
        SpecialDelivery {
            now_ms: 0,
            rect: Rect::default(),
            racks_remaining: NUM_RACKS,
            racks_delivered: 0,
            trucks: Vec::new(),
            racks: Vec::new(),
            dropper_pos: 0,
            crashes: Vec::new(),
            game_over: false,
        }
    }

    pub fn new_game(&mut self) {
        self.now_ms = 0;
        self.racks_remaining = NUM_RACKS;
        self.racks_delivered = 0;
        self.trucks.clear();
        self.racks.clear();
        self.dropper_pos = self.rect.width / 2;
        self.crashes.clear();
        self.game_over = false;
    }
}

// A rack that has crashed into the ground, another rack, or the front of
// a truck.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Crash {
    pub rect: Rect,
    pub creation_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HorizontalDirection {
    Left,
    Right,
}

// Truck position = travel_time_ms * (speed / 1000)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Truck {
    // The right bumper of the truck
    //
    // We use the right bumper so that we can have the truck start off the left
    // side of the screen without having to use signed integers.
    pub position: u16,
    pub creation_time_ms: u64,
    pub bed_width: u16,
    pub speed: f32, // cells/ms

    // Bed position of racks that landed on the tucks
    // The position is where the left of the rack lands
    pub landed_racks: Vec<u16>,
}

impl Truck {
    // All trucks start with the front bumper visible from the left side of
    // the screen.
    pub fn new(
        bed_width: u16,
        cells_per_sec: f32,
        creation_time_ms: u64,
    ) -> Truck {
        let speed = cells_per_sec / 1000.0;
        Truck {
            position: 0,
            creation_time_ms,
            speed,
            bed_width,
            landed_racks: vec![],
        }
    }

    pub fn width(&self) -> u16 {
        self.bed_width + 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rack {
    pub rect: Rect,
    pub vertical_pos: f32,
    pub velocity: f32,
}

impl Rack {
    pub fn new(rect: Rect) -> Rack {
        Rack { rect, velocity: 0.0, vertical_pos: 1.0 }
    }
}
