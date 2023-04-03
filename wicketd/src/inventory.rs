// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack inventory for display by wicket

use gateway_client::types::{
    SpComponentCaboose, SpComponentInfo, SpIdentifier, SpIgnition, SpState,
};
use schemars::JsonSchema;
use serde::Serialize;

/// SP-related data
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(tag = "sp_inventory", rename_all = "snake_case")]
pub struct SpInventory {
    pub id: SpIdentifier,
    pub ignition: SpIgnition,
    pub state: SpState,
    pub components: Option<Vec<SpComponentInfo>>,
    pub caboose: Option<SpComponentCaboose>,
    pub rot: RotInventory,
}
impl SpInventory {
    /// The ignition info and state of the SP are retrieved initiailly
    ///
    /// The components are filled in via a separate call
    pub fn new(
        id: SpIdentifier,
        ignition: SpIgnition,
        state: SpState,
    ) -> SpInventory {
        SpInventory {
            id,
            ignition,
            state,
            components: None,
            caboose: None,
            rot: RotInventory::default(),
        }
    }
}

/// RoT-related data that isn't already supplied in [`SpState`].
#[derive(Default, Debug, Clone, Serialize, JsonSchema)]
#[serde(tag = "sp_inventory", rename_all = "snake_case")]
pub struct RotInventory {
    pub caboose: Option<SpComponentCaboose>,
}

/// The current state of the v1 Rack as known to wicketd
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "inventory", rename_all = "snake_case")]
pub struct RackV1Inventory {
    pub sps: Vec<SpInventory>,
}
