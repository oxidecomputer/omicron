// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Information about all top-level Oxide components (sleds, switches, PSCs)

use anyhow::anyhow;
use std::collections::BTreeMap;

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

    pub fn update_power_state(
        &mut self,
        id: ComponentId,
        state: PowerState,
    ) -> anyhow::Result<()> {
        Self::validate_component_id(id)?;
        self.power.insert(id, state);
        Ok(())
    }

    pub fn get_inventory(&self, id: &ComponentId) -> Option<&Component> {
        self.inventory.get(id)
    }

    pub fn update_inventory(
        &mut self,
        id: ComponentId,
        component: Component,
    ) -> anyhow::Result<()> {
        Self::validate_component_id(id)?;
        self.inventory.insert(id, component);
        Ok(())
    }

    fn validate_component_id(id: ComponentId) -> anyhow::Result<()> {
        match id {
            ComponentId::Sled(i) if i > 31 => {
                Err(anyhow!("Invalid sled slot: {}", i))
            }
            ComponentId::Switch(i) if i > 1 => {
                Err(anyhow!("Invalid switch slot: {}", i))
            }
            ComponentId::Psc(i) if i > 1 => {
                Err(anyhow!("Invalid power shelf slot: {}", i))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug)]
pub struct FakeSled {
    // 0-31
    pub slot: u8,
    pub serial_number: String,
    pub part_number: String,
    pub sp_version: String,
    pub rot_version: String,
    pub host_os_version: String,
    pub control_plane_version: Option<String>,
}

#[derive(Debug)]
pub struct FakeSwitch {
    // Top is 0, bottom is 1
    pub slot: u8,
    pub serial_number: String,
    pub part_number: String,
    pub sp_version: String,
    pub rot_version: String,
}

#[derive(Debug)]
pub struct FakePsc {
    // Top is 0 power shelf, 1 is bottom
    pub slot: u8,
    pub serial_number: String,
    pub part_number: String,
    pub sp_version: String,
    pub rot_version: String,
}

/// TODO: Use real inventory received from MGS
#[derive(Debug)]
pub enum Component {
    Sled(FakeSled),
    Switch(FakeSwitch),
    Psc(FakePsc),
}

impl Component {
    pub fn name(&self) -> String {
        match self {
            Component::Sled(s) => format!("sled {}", s.slot),
            Component::Switch(s) => format!("switch {}", s.slot),
            Component::Psc(p) => format!("psc {}", p.slot),
        }
    }
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
        match self {
            ComponentId::Sled(i) => format!("sled {}", i),
            ComponentId::Switch(i) => format!("switch {}", i),
            ComponentId::Psc(i) => format!("psc {}", i),
        }
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
