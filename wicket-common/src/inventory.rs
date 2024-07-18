// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Re-export these types from gateway_client, so that users are oblivious to
// where these types come from.
pub use gateway_client::types::{
    RotSlot, RotState, SpComponentCaboose, SpComponentInfo,
    SpComponentPresence, SpIdentifier, SpIgnition, SpIgnitionSystemType,
    SpState, SpType,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The current state of the v1 Rack as known to wicketd
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "inventory", rename_all = "snake_case")]
pub struct RackV1Inventory {
    pub sps: Vec<SpInventory>,
}

/// SP-related data
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "sp_inventory", rename_all = "snake_case")]
pub struct SpInventory {
    pub id: SpIdentifier,
    pub ignition: Option<SpIgnition>,
    pub state: Option<SpState>,
    pub components: Option<Vec<SpComponentInfo>>,
    pub caboose_active: Option<SpComponentCaboose>,
    pub caboose_inactive: Option<SpComponentCaboose>,
    pub rot: Option<RotInventory>,
}

impl SpInventory {
    /// Create an empty inventory with the given id.
    ///
    /// All remaining fields are populated later as MGS responds.
    pub fn new(id: SpIdentifier) -> SpInventory {
        SpInventory {
            id,
            ignition: None,
            state: None,
            components: None,
            caboose_active: None,
            caboose_inactive: None,
            rot: None,
        }
    }
}

/// RoT-related data that isn't already supplied in [`SpState`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "sp_inventory", rename_all = "snake_case")]
pub struct RotInventory {
    pub active: RotSlot,
    pub caboose_a: Option<SpComponentCaboose>,
    pub caboose_b: Option<SpComponentCaboose>,
    // stage0 information is not available on all RoT versions
    // `None` indicates we don't need to read
    pub caboose_stage0: Option<Option<SpComponentCaboose>>,
    pub caboose_stage0next: Option<Option<SpComponentCaboose>>,
}
