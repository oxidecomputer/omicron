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
    pub id: SpId,
    pub ignition: SpIgnition,
    pub state: SpState,
    pub components: Vec<SpComponentInfo>,
}

impl SpInventory {
    /// The ignition info and state of the SP are retrieved initiailly
    ///
    /// The components are filled in via a separate call
    pub fn new(id: SpId, ignition: SpIgnition, state: SpState) -> SpInventory {
        SpInventory { id, ignition, state, components: vec![] }
    }
}

/// The current state of the v1 Rack as known to wicketd
#[derive(Default, Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "inventory", rename_all = "snake_case")]
pub struct RackV1Inventory {
    pub sps: Vec<SpInventory>,
}

/// A local type we can use as a key to a BTreeMap.
///
/// This maps directly to `gateway_client::types::SpIdentifer`,
/// but we create a separate type because the progenitor generated type
/// doesn't derive Ord/PartialOrd.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, JsonSchema,
)]
#[serde(tag = "sp_id", rename_all = "snake_case")]
pub struct SpId {
    #[serde(rename = "type")]
    pub typ: SpType,
    pub slot: u32,
}

impl From<SpIdentifier> for SpId {
    fn from(id: SpIdentifier) -> Self {
        SpId { typ: id.type_, slot: id.slot }
    }
}

impl From<SpId> for SpIdentifier {
    fn from(id: SpId) -> Self {
        SpIdentifier { type_: id.typ, slot: id.slot }
    }
}

impl Display for SpId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {}", self.typ, self.slot)
    }
}
