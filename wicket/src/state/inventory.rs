// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Information about all top-level Oxide components (sleds, switches, PSCs)

use anyhow::anyhow;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::iter::Iterator;
use tui::text::Text;
use wicketd_client::types::{
    RackV1Inventory, SpComponentInfo, SpIgnition, SpState, SpType,
};

lazy_static! {
    /// All possible component ids in a rack
    pub static ref ALL_COMPONENT_IDS: Vec<ComponentId> = (0..=31u8)
        .map(|i| ComponentId::Sled(i))
        .chain((0..=1u8).map(|i| ComponentId::Switch(i)))
        .chain((0..=1u8).map(|i| ComponentId::Psc(i)))
        .collect();
}

/// Inventory is the most recent information about rack composition as
/// received from MGS.
#[derive(Debug, Default)]
pub struct Inventory {
    power: BTreeMap<ComponentId, PowerState>,
    inventory: BTreeMap<ComponentId, Component>,
}

impl Inventory {
    pub fn get_power_state(&self, id: &ComponentId) -> Option<&PowerState> {
        self.power.get(id)
    }

    pub fn get_inventory(&self, id: &ComponentId) -> Option<&Component> {
        self.inventory.get(id)
    }

    pub fn components(&self) -> impl Iterator<Item = &ComponentId> {
        self.inventory.keys()
    }

    pub fn update_inventory(
        &mut self,
        inventory: RackV1Inventory,
    ) -> anyhow::Result<()> {
        let mut new_inventory = Inventory::default();

        for sp in inventory.sps {
            let i = sp.id.slot;
            let type_ = sp.id.type_;
            let sp = Sp {
                ignition: sp.ignition,
                state: sp.state,
                components: sp.components,
            };

            // Validate and get a ComponentId
            let (id, component) = match type_ {
                SpType::Sled => {
                    if i > 31 {
                        return Err(anyhow!("Invalid sled slot: {}", i));
                    }
                    (ComponentId::Sled(i as u8), Component::Sled(sp))
                }
                SpType::Switch => {
                    if i > 1 {
                        return Err(anyhow!("Invalid switch slot: {}", i));
                    }
                    (ComponentId::Switch(i as u8), Component::Switch(sp))
                }
                SpType::Power => {
                    if i > 1 {
                        return Err(anyhow!("Invalid power shelf slot: {}", i));
                    }
                    (ComponentId::Psc(i as u8), Component::Psc(sp))
                }
            };
            new_inventory.inventory.insert(id, component);

            // TODO: Plumb through real power state
            new_inventory.power.insert(id, PowerState::A2);
        }

        self.inventory = new_inventory.inventory;
        self.power = new_inventory.power;

        Ok(())
    }
}

// We just print the debug info on the screen for now
#[allow(unused)]
#[derive(Debug)]
pub struct Sp {
    ignition: SpIgnition,
    state: SpState,
    components: Option<Vec<SpComponentInfo>>,
}

// XXX: Eventually a Sled will have a host component.
#[derive(Debug)]
pub enum Component {
    Sled(Sp),
    Switch(Sp),
    Psc(Sp),
}

// The component type and its slot.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub enum ComponentId {
    Sled(u8),
    Switch(u8),
    Psc(u8),
}

impl ComponentId {
    pub fn name(&self) -> String {
        self.to_string()
    }
}

impl Display for ComponentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentId::Sled(i) => write!(f, "SLED {}", i),
            ComponentId::Switch(i) => write!(f, "SWITCH {}", i),
            ComponentId::Psc(i) => write!(f, "PSC {}", i),
        }
    }
}

impl From<ComponentId> for Text<'_> {
    fn from(value: ComponentId) -> Self {
        value.to_string().into()
    }
}

#[derive(Debug)]
pub enum PowerState {
    /// Working
    A0,
    /// Sojourning
    A1,
    /// Quiescent
    A2,
    /// Commanded Off
    A3,
    /// Mechanical Off
    A4,
}

impl PowerState {
    pub fn description(&self) -> &'static str {
        match self {
            PowerState::A0 => "working",
            PowerState::A1 => "sojourning",
            PowerState::A2 => "quiescent",
            PowerState::A3 => "commanded off",
            PowerState::A4 => "mechanical off (unplugged)",
        }
    }
}
