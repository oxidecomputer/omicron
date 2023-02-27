// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The global state manipulated by wicket.

mod inventory;
mod rack;
mod status;

pub use inventory::{
    Component, ComponentId, Inventory, PowerState, Sp, ALL_COMPONENT_IDS,
};
pub use rack::{KnightRiderMode, RackState};
pub use status::{ComputedLiveness, LivenessState, ServiceStatus};

/// The data state of the Wizard
///
/// Data is not tied to any specific screen and is updated upon event receipt.
#[derive(Debug)]
pub struct State {
    pub inventory: Inventory,
    pub rack_state: RackState,
    pub service_status: ServiceStatus,
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    pub fn new() -> State {
        State {
            inventory: Inventory::default(),
            rack_state: RackState::new(),
            service_status: ServiceStatus::new(),
        }
    }
}
