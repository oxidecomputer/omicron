// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The global state manipulated by wicket.

mod force_update;
mod inventory;
mod rack;
mod status;
mod update;

pub use force_update::ForceUpdateState;
pub use inventory::{
    ALL_COMPONENT_IDS, Component, ComponentId, Inventory, ParsableComponentId,
};
pub use rack::{KnightRiderMode, RackState};
pub use status::ServiceStatus;
pub use update::{
    CreateClearUpdateStateOptions, CreateStartUpdateOptions, RackUpdateState,
    UpdateItemState, parse_event_report_map, update_component_title,
};

use serde::{Deserialize, Serialize};
use wicketd_client::types::{CurrentRssUserConfig, RackOperationStatus};
use wicketd_commission_types::inventory::{LocationInfo, SwitchSlot};

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
    pub force_update_state: ForceUpdateState,
    pub rss_config: Option<CurrentRssUserConfig>,
    pub rack_setup_state: Result<RackOperationStatus, String>,
    pub wicketd_location: Option<LocationInfo>,
}

impl State {
    pub fn new() -> State {
        State {
            screen_height: 0,
            screen_width: 0,
            inventory: Inventory::default(),
            rack_state: RackState::new(),
            service_status: ServiceStatus::new(),
            update_state: RackUpdateState::new(),
            force_update_state: ForceUpdateState::default(),
            rss_config: None,
            rack_setup_state: Err("status not yet polled from wicketd".into()),
            wicketd_location: None,
        }
    }

    pub fn selected_component_matches_wicketd_location(&self) -> bool {
        // Return false until the location is known.
        let Some(location) = self.wicketd_location.as_ref() else {
            return false;
        };
        match self.rack_state.selected {
            ComponentId::Sled(i) => {
                // Do we know the wicketd sled ID? If so, we can compare
                // directly. (We will almost always know this.)
                if let Some(wicketd_sled_id) = location.sled_id {
                    wicketd_sled_id.slot == u16::from(i)
                } else {
                    // We _could_ check and see if wicketd knows its sled's
                    // baseboard (even though it didn't know the sled) and then
                    // compare that against `self.inventory`, but it's
                    // exceedingly unlikely that we'd find anything (since we
                    // get the inventory from wicketd itself), so we'll just
                    // return false.
                    false
                }
            }
            ComponentId::Switch(i) => {
                switch_slot_number(location.switch_slot) == i
            }
            // wicketd's location is never related to a PSC.
            ComponentId::Psc(_) => false,
        }
    }
}

pub(crate) fn switch_slot_number(switch_slot: SwitchSlot) -> u8 {
    match switch_slot {
        SwitchSlot::Switch0 => 0,
        SwitchSlot::Switch1 => 1,
    }
}
