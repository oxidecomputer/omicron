// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Global state of the rack.
//!
//! This is mostly visual selection state right now.

use super::inventory::ComponentId;
use serde::{Deserialize, Serialize};
use slog::Logger;

// Easter egg alert: Support for Knight Rider mode
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KnightRiderMode {
    pub count: usize,
}

impl KnightRiderMode {
    pub fn step(&mut self) {
        self.count += 1;
    }
}

// The visual state of the rack
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackState {
    #[serde(skip)]
    pub log: Option<Logger>,
    pub selected: ComponentId,
    pub knight_rider_mode: Option<KnightRiderMode>,

    // Useful for arrow based navigation. When we cross the switches going up
    // or down the rack we want to stay in the same column. This allows a user
    // to repeatedly hit up or down arrow and stay in the same column, as they
    // would expect.
    //
    // This is the only part of the state that is "historical", and doesn't
    // rely on the `self.selected` at all times.
    left_column: bool,
}

#[allow(clippy::new_without_default)]
impl RackState {
    pub fn new() -> RackState {
        RackState {
            log: None,
            selected: ComponentId::Sled(0),
            knight_rider_mode: None,

            // Default to the left column, where sled 0 lives
            left_column: true,
        }
    }

    pub fn toggle_knight_rider_mode(&mut self) {
        if self.knight_rider_mode.is_none() {
            self.knight_rider_mode = Some(KnightRiderMode::default());
        } else {
            self.knight_rider_mode = None;
        }
    }

    pub fn up(&mut self) {
        self.selected = match self.selected {
            ComponentId::Sled(14 | 15) => ComponentId::Switch(0),
            ComponentId::Sled(i) => ComponentId::Sled((i + 2) % 32),
            ComponentId::Switch(0) => ComponentId::Psc(0),
            ComponentId::Switch(1) => {
                if self.left_column {
                    ComponentId::Sled(16)
                } else {
                    ComponentId::Sled(17)
                }
            }
            // Skip over Psc(1) because it is always empty in currently shipping
            // racks.
            ComponentId::Psc(0) => ComponentId::Switch(1),
            _ => unreachable!(),
        };
    }

    pub fn down(&mut self) {
        self.selected = match self.selected {
            ComponentId::Sled(16 | 17) => ComponentId::Switch(1),
            ComponentId::Sled(i) => ComponentId::Sled((30 + i) % 32),
            // Skip over Psc(1) because it is always empty in currently shipping
            // racks.
            ComponentId::Switch(1) => ComponentId::Psc(0),
            ComponentId::Switch(0) => {
                if self.left_column {
                    ComponentId::Sled(14)
                } else {
                    ComponentId::Sled(15)
                }
            }
            ComponentId::Psc(0) => ComponentId::Switch(0),
            _ => unreachable!(),
        };
    }

    pub fn left_or_right(&mut self) {
        match self.selected {
            ComponentId::Sled(i) => {
                self.selected = ComponentId::Sled(i ^ 1);
                self.set_column();
            }
            _ => (),
        }
    }

    pub fn next(&mut self) {
        self.selected = match self.selected {
            ComponentId::Sled(15) => ComponentId::Switch(0),
            ComponentId::Sled(i) => ComponentId::Sled((i + 1) % 32),
            ComponentId::Switch(0) => ComponentId::Psc(0),
            // Skip over Psc(1) because it is always empty in currently shipping
            // racks.
            ComponentId::Psc(0) => ComponentId::Switch(1),
            ComponentId::Switch(1) => ComponentId::Sled(16),
            _ => unreachable!(),
        };
        self.set_column();
    }

    pub fn prev(&mut self) {
        self.selected = match self.selected {
            ComponentId::Sled(16) => ComponentId::Switch(1),
            ComponentId::Sled(0) => ComponentId::Sled(31),
            ComponentId::Sled(i) => ComponentId::Sled(i - 1),
            // Skip over Psc(1) because it is always empty in currently shipping
            // racks.
            ComponentId::Switch(1) => ComponentId::Psc(0),
            ComponentId::Psc(0) => ComponentId::Switch(0),
            ComponentId::Switch(0) => ComponentId::Sled(15),
            _ => unreachable!(),
        };
        self.set_column();
    }

    fn set_column(&mut self) {
        match self.selected {
            ComponentId::Sled(i) => self.left_column = i % 2 == 0,
            _ => (),
        }
    }

    pub fn set_logger(&mut self, log: Logger) {
        self.log = Some(log);
    }
}
