// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Re-export these types from gateway_client, so that users are oblivious to
// where these types come from.
pub use gateway_client::types::{
    SpComponentCaboose, SpComponentInfo, SpComponentPresence, SpIdentifier,
};
pub use gateway_types::component::{SpState, SpType};
pub use gateway_types::ignition::{SpIgnition, SpIgnitionSystemType};
pub use gateway_types::rot::{RotSlot, RotState};
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::SwitchSlot;
use sled_hardware_types::BaseboardId;
use slog::debug;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::Ipv6Addr;
use std::time::Duration;
use transceiver_controller::{
    Datapath, Monitors, PowerMode, VendorInfo, message::ExtendedStatus,
};

use crate::rack_setup::BootstrapSledDescription;

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

impl From<BTreeSet<BootstrapSledDescription>> for SledInventory {
    fn from(sleds: BTreeSet<BootstrapSledDescription>) -> Self {
        SledInventory { sleds }
    }
}

// Inventory about sleds derived from MGS inventory and DDM
#[derive(Default, Clone)]
pub struct SledInventory {
    pub sleds: BTreeSet<BootstrapSledDescription>,
}

impl SledInventory {
    pub fn new(
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<BaseboardId, Ipv6Addr>,
        log: &slog::Logger,
    ) -> Self {
        let sleds = inventory
            .sps
            .iter()
            .filter_map(|sp| {
                if sp.id.type_ != SpType::Sled {
                    return None;
                }

                let Some(state) = sp.state.as_ref() else {
                    debug!(
                        log,
                        "sled inventory: filtering out SP with no state";
                        "sp" => ?sp,
                    );
                    return None;
                };
                let baseboard_id = BaseboardId {
                    part_number: state.model.clone(),
                    serial_number: state.serial_number.clone(),
                };

                let bootstrap_ip =
                    ddm_discovered_sleds.get(&baseboard_id).copied();
                Some(BootstrapSledDescription {
                    id: sp.id,
                    baseboard_id,
                    bootstrap_ip,
                })
            })
            .collect();
        SledInventory { sleds }
    }

    /// Ensure our baseboard is included in inventory, extract the slot for
    /// our baseboard, and then ensure that the slot is included in the user
    /// supplied `bootstrap_sled_slots`.
    pub fn verify_our_baseboard_is_in_inventory_slot(
        &self,
        bootstrap_sled_slots: &BTreeSet<u16>,
        our_baseboard: &BaseboardId,
    ) -> Result<(), String> {
        let our_slot = self
            .sleds
            .iter()
            .find_map(|sled| {
                if sled.baseboard_id == *our_baseboard {
                    Some(sled.id.slot)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                format!(
                    "Inventory is missing the scrimlet where wicketd is \
                     running ({our_baseboard:?})",
                )
            })?;
        if !bootstrap_sled_slots.contains(&our_slot) {
            return Err(format!(
                "Cannot remove the scrimlet where wicketd is running \
                (sled {our_slot}: {our_baseboard:?}) \
                from bootstrap_sleds"
            ));
        }

        Ok(())
    }

    /// Find all bootstrap sleds in inventory with slots matching
    /// `bootstrap_sled_slots` and return them. If the slot doesn't exist in
    /// inventory, then return an error.
    pub fn load_bootstrap_sleds_by_user_chosen_slots(
        &self,
        bootstrap_sled_slots: &BTreeSet<u16>,
    ) -> Result<BTreeSet<BootstrapSledDescription>, String> {
        let mut bootstrap_sleds = BTreeSet::new();
        for slot in bootstrap_sled_slots {
            let sled =
                self.sleds
                    .iter()
                    .find(|sled| sled.id.slot == *slot)
                    .ok_or_else(|| {
                        format!(
                            "cannot add unknown sled {slot} to bootstrap_sleds",
                        )
                    })?;
            bootstrap_sleds.insert(sled.clone());
        }
        Ok(bootstrap_sleds)
    }
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
    pub inventory: HashMap<SwitchSlot, Vec<Transceiver>>,
    pub last_seen: Duration,
}

/// A transceiver in the switch's front ports.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "transceiver", rename_all = "snake_case")]
pub struct Transceiver {
    /// The port in which the transceiver sits.
    pub port: String,
    /// The general status of the transceiver, such as presence and faults.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<ExtendedStatus, String>::json_schema"
    )]
    pub status: Result<ExtendedStatus, String>,
    /// Information about the power state of the transceiver.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<PowerMode, String>::json_schema"
    )]
    pub power: Result<PowerMode, String>,
    /// Details about the vendor, part number, and serial number.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<VendorInfo, String>::json_schema"
    )]
    pub vendor: Result<VendorInfo, String>,
    /// Status of the transceiver's machinery for carrying data, the "datapath".
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<Datapath, String>::json_schema"
    )]
    pub datapath: Result<Datapath, String>,
    /// Environmental monitoring data, such as temperature or optical power.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<Monitors, String>::json_schema"
    )]
    pub monitors: Result<Monitors, String>,
}
