// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Information about all top-level Oxide components (sleds, switches, PSCs)

use anyhow::{Context as _, Result, bail};
use omicron_common::api::external::SwitchLocation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::iter::Iterator;
use std::sync::LazyLock;
use tufaceous_artifact::KnownArtifactKind;
use wicket_common::inventory::{
    RackV1Inventory, RotInventory, RotSlot, SpComponentCaboose,
    SpComponentInfo, SpIgnition, SpState, SpType, Transceiver,
};

pub static ALL_COMPONENT_IDS: LazyLock<Vec<ComponentId>> =
    LazyLock::new(|| {
        (0..=31u8)
            .map(ComponentId::Sled)
            .chain((0..=1u8).map(ComponentId::Switch))
            .chain((0..=1u8).map(ComponentId::Psc))
            .collect()
    });

/// Inventory is the most recent information about rack composition as
/// received from MGS.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
        let mgs_inventory = inventory
            .mgs
            .map(|mgs| mgs.inventory)
            .context("Cannot update inventory without any details from MGS")?;
        let mut transceiver_inventory =
            inventory.transceivers.map(|tr| tr.inventory).unwrap_or_default();
        let mut new_inventory = Inventory::default();

        for sp in mgs_inventory.sps {
            let i = sp.id.slot;
            let type_ = sp.id.type_;
            let sp = Sp {
                ignition: sp.ignition,
                state: sp.state,
                caboose_active: sp.caboose_active,
                caboose_inactive: sp.caboose_inactive,
                components: sp.components,
                rot: sp.rot,
            };

            // Validate and get a ComponentId
            let id = ComponentId::from_sp_type_and_slot(type_, i)?;
            let component = match type_ {
                SpType::Sled => Component::Sled(sp),
                SpType::Switch => {
                    // Insert the switch's transceivers.
                    let switch_id = match i {
                        0 => SwitchLocation::Switch0,
                        1 => SwitchLocation::Switch1,
                        _ => unreachable!(),
                    };
                    let transceivers = transceiver_inventory
                        .remove(&switch_id)
                        .unwrap_or_default();
                    Component::Switch { sp, transceivers }
                }
                SpType::Power => Component::Psc(sp),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sp {
    ignition: Option<SpIgnition>,
    state: Option<SpState>,
    caboose_active: Option<SpComponentCaboose>,
    caboose_inactive: Option<SpComponentCaboose>,
    components: Option<Vec<SpComponentInfo>>,
    rot: Option<RotInventory>,
}

impl Sp {
    pub fn ignition(&self) -> Option<&SpIgnition> {
        self.ignition.as_ref()
    }

    pub fn state(&self) -> Option<&SpState> {
        self.state.as_ref()
    }

    pub fn caboose_active(&self) -> Option<&SpComponentCaboose> {
        self.caboose_active.as_ref()
    }

    pub fn caboose_inactive(&self) -> Option<&SpComponentCaboose> {
        self.caboose_inactive.as_ref()
    }

    pub fn rot(&self) -> Option<&RotInventory> {
        self.rot.as_ref()
    }

    pub fn components(&self) -> &[SpComponentInfo] {
        match self.components.as_ref() {
            Some(components) => components,
            None => &[],
        }
    }
}

// XXX: Eventually a Sled will have a host component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Component {
    Sled(Sp),
    Switch { sp: Sp, transceivers: Vec<Transceiver> },
    Psc(Sp),
}

fn version_or_unknown(caboose: Option<&SpComponentCaboose>) -> String {
    caboose.map(|c| c.version.as_str()).unwrap_or("UNKNOWN").to_string()
}

fn caboose_sign(caboose: &SpComponentCaboose) -> Option<Vec<u8>> {
    caboose.sign.as_ref().map(|s| s.as_bytes().to_vec())
}

impl Component {
    pub fn sp(&self) -> &Sp {
        match self {
            Component::Sled(sp) => sp,
            Component::Switch { sp, .. } => sp,
            Component::Psc(sp) => sp,
        }
    }

    pub fn sp_version_active(&self) -> String {
        version_or_unknown(self.sp().caboose_active.as_ref())
    }

    pub fn sp_version_inactive(&self) -> String {
        version_or_unknown(self.sp().caboose_inactive.as_ref())
    }

    pub fn rot_active_slot(&self) -> Option<RotSlot> {
        self.sp().rot.as_ref().map(|rot| rot.active)
    }

    pub fn rot_version_a(&self) -> String {
        version_or_unknown(
            self.sp().rot.as_ref().and_then(|rot| rot.caboose_a.as_ref()),
        )
    }

    pub fn rot_version_b(&self) -> String {
        version_or_unknown(
            self.sp().rot.as_ref().and_then(|rot| rot.caboose_b.as_ref()),
        )
    }

    pub fn stage0_version(&self) -> String {
        version_or_unknown(self.sp().rot.as_ref().and_then(|rot| {
            // caboose_stage0 is an Option<Option<SpComponentCaboose>>, so we
            // need to unwrap it twice, effectively. flatten would be nice but
            // it doesn't work on Option<&Option<T>>, which is what we end up
            // with.
            rot.caboose_stage0.as_ref().map_or(None, |x| x.as_ref())
        }))
    }

    pub fn stage0next_version(&self) -> String {
        version_or_unknown(self.sp().rot.as_ref().and_then(|rot| {
            // caboose_stage0next is an Option<Option<SpComponentCaboose>>, so we
            // need to unwrap it twice, effectively. flatten would be nice but
            // it doesn't work on Option<&Option<T>>, which is what we end up
            // with.
            rot.caboose_stage0next.as_ref().map_or(None, |x| x.as_ref())
        }))
    }

    // Technically the slots could have different SIGN values in the
    // caboose. An active slot implies the RoT is up and valid so
    // we should rely on that value for selection.
    // We also use this for the bootloader selection as the SIGN
    // of the bootloader is going to be identical to the RoT.
    pub fn rot_sign(&self) -> Option<Vec<u8>> {
        match self.rot_active_slot()? {
            RotSlot::A => self.sp().rot.as_ref().map_or(None, |rot| {
                rot.caboose_a.as_ref().and_then(caboose_sign)
            }),
            RotSlot::B => self.sp().rot.as_ref().map_or(None, |rot| {
                rot.caboose_b.as_ref().map_or(None, |x| caboose_sign(x))
            }),
        }
    }
}

/// The component type and its slot.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub enum ComponentId {
    Sled(u8),
    Switch(u8),
    Psc(u8),
}

impl ComponentId {
    /// The maximum possible sled ID.
    pub const MAX_SLED_ID: u8 = 31;

    /// The maximum possible switch ID.
    pub const MAX_SWITCH_ID: u8 = 1;

    /// The maximum possible power shelf ID.
    pub const MAX_PSC_ID: u8 = 1;

    pub fn new_sled(slot: u8) -> Result<Self> {
        if slot > Self::MAX_SLED_ID {
            bail!("Invalid sled slot: {}", slot);
        }
        Ok(Self::Sled(slot))
    }

    pub fn new_switch(slot: u8) -> Result<Self> {
        if slot > Self::MAX_SWITCH_ID {
            bail!("Invalid switch slot: {}", slot);
        }
        Ok(Self::Switch(slot))
    }

    pub fn new_psc(slot: u8) -> Result<Self> {
        if slot > Self::MAX_PSC_ID {
            bail!("Invalid power shelf slot: {}", slot);
        }
        Ok(Self::Psc(slot))
    }

    pub fn from_sp_type_and_slot(sp_type: SpType, slot: u16) -> Result<Self> {
        let slot = slot.try_into().map_err(|_| {
            anyhow::anyhow!("invalid slot (must fit in a u8): {}", slot)
        })?;
        match sp_type {
            SpType::Sled => Self::new_sled(slot),
            SpType::Switch => Self::new_switch(slot),
            SpType::Power => Self::new_psc(slot),
        }
    }

    pub fn name(&self) -> String {
        self.to_string()
    }

    pub fn sp_known_artifact_kind(&self) -> KnownArtifactKind {
        match self {
            ComponentId::Sled(_) => KnownArtifactKind::GimletSp,
            ComponentId::Switch(_) => KnownArtifactKind::SwitchSp,
            ComponentId::Psc(_) => KnownArtifactKind::PscSp,
        }
    }

    pub fn rot_known_artifact_kind(&self) -> KnownArtifactKind {
        match self {
            ComponentId::Sled(_) => KnownArtifactKind::GimletRot,
            ComponentId::Switch(_) => KnownArtifactKind::SwitchRot,
            ComponentId::Psc(_) => KnownArtifactKind::PscRot,
        }
    }

    pub fn rot_bootloader_known_artifact_kind(&self) -> KnownArtifactKind {
        match self {
            ComponentId::Sled(_) => KnownArtifactKind::GimletRotBootloader,
            ComponentId::Switch(_) => KnownArtifactKind::SwitchRotBootloader,
            ComponentId::Psc(_) => KnownArtifactKind::PscRotBootloader,
        }
    }

    pub fn to_string_uppercase(&self) -> String {
        let mut s = self.to_string();
        s.make_ascii_uppercase();
        s
    }
}

/// Prints the component type in standard case.
impl Display for ComponentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentId::Sled(i) => write!(f, "sled {}", i),
            ComponentId::Switch(i) => write!(f, "switch {}", i),
            ComponentId::Psc(i) => write!(f, "PSC {}", i),
        }
    }
}

pub struct ParsableComponentId<'a> {
    pub sp_type: &'a str,
    pub i: &'a str,
}

impl<'a> TryFrom<ParsableComponentId<'a>> for ComponentId {
    type Error = ();
    fn try_from(value: ParsableComponentId<'a>) -> Result<Self, Self::Error> {
        let i: u8 = value.i.parse().map_err(|_| ())?;
        match (value.sp_type, i) {
            ("sled", 0..=31) => Ok(ComponentId::Sled(i)),
            ("switch", 0..=1) => Ok(ComponentId::Switch(i)),
            ("power", 0..=1) => Ok(ComponentId::Psc(i)),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_id_display() {
        assert_eq!(ComponentId::Sled(0).to_string(), "sled 0");
        assert_eq!(ComponentId::Switch(1).to_string(), "switch 1");
        assert_eq!(ComponentId::Psc(2).to_string(), "PSC 2");
    }
}
