// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The global state manipulated by wicket.

mod inventory;
mod rack;
mod status;
mod update;

pub use inventory::{
    Component, ComponentId, Inventory, ParsableComponentId, PowerState, Sp,
    ALL_COMPONENT_IDS,
};
pub use rack::{KnightRiderMode, RackState};
pub use status::{Liveness, ServiceStatus};
pub use update::{artifact_title, RackUpdateState, UpdateState};

use serde::{Deserialize, Serialize};
use slog::Logger;

/// The global state of wicket
///
/// [`State`] is not tied to any specific screen and is updated upon event
/// receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub screen_width: u16,
    pub screen_height: u16,
    pub inventory: Inventory,
    pub rack_state: RackState,
    pub service_status: ServiceStatus,
    pub update_state: RackUpdateState,
}

impl State {
    pub fn new(log: &Logger) -> State {
        State {
            screen_height: 0,
            screen_width: 0,
            inventory: Inventory::default(),
            rack_state: RackState::new(),
            service_status: ServiceStatus::new(),
            update_state: RackUpdateState::new(log),
        }
    }
}
