// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack inventory for display by wicket

use gateway_client::types::{
    SpComponentInfo, SpIdentifier, SpIgnition, SpState, SpType,
};
use schemars::JsonSchema;
use serde::Serialize;
use std::fmt::Display;

/// SP related data
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(tag = "sp_inventory", rename_all = "snake_case")]
pub struct SpInventory {
    pub id: SpIdentifier,
    pub ignition: SpIgnition,
    pub state: SpState,
    pub components: Vec<SpComponentInfo>,
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
        SpInventory { id, ignition, state, components: vec![] }
    }
}

/// The current state of the v1 Rack as known to wicketd
#[derive(Default, Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "inventory", rename_all = "snake_case")]
pub struct RackV1Inventory {
    pub sps: Vec<SpInventory>,
}
