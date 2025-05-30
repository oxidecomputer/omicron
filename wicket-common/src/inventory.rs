// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Re-export these types from gateway_client, so that users are oblivious to
// where these types come from.
pub use gateway_client::types::{
    RotState, SpComponentCaboose, SpComponentInfo, SpComponentPresence,
    SpIdentifier, SpIgnition, SpIgnitionSystemType, SpState, SpType,
};
pub use gateway_types::rot::RotSlot;
use omicron_common::api::external::SwitchLocation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use transceiver_controller::{
    Datapath, Monitors, PowerMode, VendorInfo, message::ExtendedStatus,
};

/// The current state of the v1 Rack as known to wicketd
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "rack_inventory", rename_all = "snake_case")]
pub struct RackV1Inventory {
    pub mgs: Option<MgsV1InventorySnapshot>,
    pub transceivers: Option<TransceiverInventorySnapshot>,
}

/// The state of the v1 Rack as known to MGS at a given time.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mgs_inventory_snapshot", rename_all = "snake_case")]
pub struct MgsV1InventorySnapshot {
    pub inventory: MgsV1Inventory,
    pub last_seen: Duration,
}

/// The current state of the v1 Rack as known to MGS
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mgs_inventory", rename_all = "snake_case")]
pub struct MgsV1Inventory {
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

/// A snapshot of the transceiver inventory at a given time.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "transceiver_inventory_snapshot", rename_all = "snake_case")]
pub struct TransceiverInventorySnapshot {
    /// The transceivers in each switch.
    pub inventory: HashMap<SwitchLocation, Vec<Transceiver>>,
    pub last_seen: Duration,
}

/// A transceiver in the switch's front ports.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "transceiver", rename_all = "snake_case")]
pub struct Transceiver {
    /// The port in which the transceiver sits.
    pub port: String,
    /// The general status of the transceiver, such as presence and faults.
    pub status: Option<ExtendedStatus>,
    /// Information about the power state of the transceiver.
    pub power: Option<PowerMode>,
    /// Details about the vendor, part number, and serial number.
    pub vendor: Option<VendorInfo>,
    /// Status of the transceiver's machinery for carrying data, the "datapath".
    pub datapath: Option<Datapath>,
    /// Environmental monitoring data, such as temperature or optical power.
    pub monitors: Option<Monitors>,
}
