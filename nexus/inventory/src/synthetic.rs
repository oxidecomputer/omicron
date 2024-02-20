// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders for constructing inventory collections and blueprints for synthetic
//! systems

use crate::CollectionBuilder;
use anyhow::{anyhow, bail, Context};
use gateway_client::types::RotState;
use gateway_client::types::SpState;
use nexus_types::inventory::Collection;
use nexus_types::inventory::PowerState;
use nexus_types::inventory::RotSlot;
use nexus_types::inventory::SledRole;
use nexus_types::inventory::SpType;
use omicron_common::address::get_sled_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::ByteCount;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SyntheticSystemBuilder {
    collector: Option<String>,
    sleds: Vec<SledBuilder>,
}

impl SyntheticSystemBuilder {
    pub fn new() -> Self {
        SyntheticSystemBuilder { sleds: Vec::new(), collector: None }
    }

    /// Returns a complete system deployed on a single Sled
    pub fn single_sled() -> Self {
        let mut builder = SyntheticSystemBuilder::new();
        let sled = SledBuilder::new();
        builder.sled(sled);
        builder
    }

    /// Returns a complete system resembling a full rack
    pub fn full_rack() -> Self {
        let mut builder = SyntheticSystemBuilder::new();
        for slot_number in 1..32 {
            let mut sled = SledBuilder::new();
            if slot_number == 14 || slot_number == 16 {
                sled.sled_role(SledRole::Scrimlet);
            }
            builder.sled(sled);
        }
        builder
    }

    pub fn collector_label<S>(&mut self, collector_label: S) -> &mut Self
    where
        String: From<S>,
    {
        self.collector = Some(String::from(collector_label));
        self
    }

    pub fn sled(&mut self, sled: SledBuilder) -> &mut Self {
        self.sleds.push(sled);
        self
    }

    pub fn to_collection(&mut self) -> anyhow::Result<Collection> {
        let collector_label = self
            .collector
            .as_ref()
            .cloned()
            .unwrap_or_else(|| String::from("example"));
        let mut builder = CollectionBuilder::new(collector_label);

        // Assemble sets of available slots for use by sleds in general and for
        // Scrimlets.  Start with all possible rack slots, then remove any that
        // were explicitly assigned by the caller.  Report an error if any were
        // used twice.
        let mut available_scrimlet_slots = BTreeSet::from([14, 16]);
        let mut available_sled_slots: BTreeSet<u32> = (1..=32).collect();

        for s in &self.sleds {
            if let Some(slot) = s.hardware_slot {
                if !available_sled_slots.remove(&slot) {
                    bail!("sled slot {} was used twice", slot);
                }

                available_scrimlet_slots.remove(&slot);
            }
        }

        // Now, assign slot numbers to any Scrimlets that were not given them.
        // This is important to do up front because we will allow non-Scrimlets
        // to use the same slots later if they're still available.
        for s in &mut self.sleds {
            if s.sled_role == SledRole::Scrimlet && s.hardware_slot.is_none() {
                let slot =
                    available_scrimlet_slots.pop_first().ok_or_else(|| {
                        anyhow!("ran out of slots available for Scrimlets")
                    })?;
                assert!(available_sled_slots.remove(&slot));
                s.hardware_slot(slot);
            }
        }
        let mut available_sled_slots = available_sled_slots.into_iter();

        // Prepare an iterator to allow us to assign sled subnets.
        let rack_subnet_base: Ipv6Addr =
            "fd00:1122:3344:0100::".parse().unwrap();
        let rack_subnet =
            ipnet::Ipv6Net::new(rack_subnet_base, RACK_PREFIX).unwrap();
        // Skip the initial DNS subnet.
        // XXX-dap should this be documented somewhere with a constant?  RSS
        // seems to hardcode it in sled-agent/src/rack_setup/plan/sled.rs.
        let mut possible_sled_subnets =
            rack_subnet.subnets(SLED_PREFIX).unwrap().skip(1);

        // Now assemble inventory for each sled.
        for s in &self.sleds {
            let slot = s
                .hardware_slot
                .or_else(|| available_sled_slots.next())
                .ok_or_else(|| anyhow!("ran out of available sled slots"))?;

            let sled_id = s.id.unwrap_or_else(Uuid::new_v4);
            let unique = s.unique.clone().unwrap_or_else(|| slot.to_string());
            let model = format!("model{}", unique);
            let serial = format!("serial{}", unique);
            let sp_state = SpState {
                base_mac_address: [0; 6],
                hubris_archive_id: format!("hubris{}", unique),
                model: model.clone(),
                power_state: PowerState::A2,
                revision: 0,
                rot: RotState::Enabled {
                    active: RotSlot::A,
                    pending_persistent_boot_preference: None,
                    persistent_boot_preference: RotSlot::A,
                    slot_a_sha3_256_digest: Some(String::from("slotAdigest1")),
                    slot_b_sha3_256_digest: Some(String::from("slotBdigest1")),
                    transient_boot_preference: None,
                },
                serial_number: serial.clone(),
            };

            let baseboard = match s.hardware {
                SledHardware::Gimlet => {
                    sled_agent_client::types::Baseboard::Gimlet {
                        identifier: serial,
                        model,
                        revision: i64::from(sp_state.revision),
                    }
                }
                SledHardware::Pc => sled_agent_client::types::Baseboard::Pc {
                    identifier: serial,
                    model: model,
                },
                SledHardware::Unknown | SledHardware::Empty => {
                    sled_agent_client::types::Baseboard::Unknown
                }
            };

            if !matches!(&s.hardware, SledHardware::Empty) {
                builder
                    .found_sp_state("fake MGS 1", SpType::Sled, slot, sp_state)
                    .context("recording SP state")?;
            }

            let sled_ip_subnet = possible_sled_subnets
                .next()
                .ok_or_else(|| anyhow!("ran out of sled subnets"))?;
            let sled_subnet: Ipv6Subnet<SLED_PREFIX> =
                Ipv6Subnet::new(sled_ip_subnet.network());
            let sled_agent_address = get_sled_address(sled_subnet).to_string();
            let sled_agent = sled_agent_client::types::Inventory {
                baseboard,
                reservoir_size: ByteCount::from(1024),
                sled_role: s.sled_role,
                sled_agent_address,
                sled_id,
                usable_hardware_threads: 10,
                usable_physical_ram: ByteCount::from(1024 * 1024),
            };

            builder
                .found_sled_inventory("fake sled agent", sled_agent)
                .context("recording sled agent")?;
        }

        Ok(builder.build())
    }
}

#[derive(Clone, Debug)]
pub enum SledHardware {
    Gimlet,
    Pc,
    Unknown,
    Empty,
}

#[derive(Clone, Debug)]
pub struct SledBuilder {
    id: Option<Uuid>,
    unique: Option<String>,
    hardware: SledHardware,
    hardware_slot: Option<u32>,
    sled_role: SledRole,
    npools: u8,
}

impl SledBuilder {
    pub fn new() -> Self {
        SledBuilder {
            id: None,
            unique: None,
            hardware: SledHardware::Gimlet,
            hardware_slot: None,
            sled_role: SledRole::Gimlet,
            npools: 10,
        }
    }

    pub fn id(&mut self, id: Uuid) -> &mut Self {
        self.id = Some(id);
        self
    }

    pub fn unique<S>(&mut self, unique: S) -> &mut Self
    where
        String: From<S>,
    {
        self.unique = Some(String::from(unique));
        self
    }

    pub fn npools(&mut self, npools: u8) -> &mut Self {
        self.npools = npools;
        self
    }

    pub fn hardware(&mut self, hardware: SledHardware) -> &mut Self {
        self.hardware = hardware;
        self
    }

    pub fn hardware_slot(&mut self, hardware_slot: u32) -> &mut Self {
        self.hardware_slot = Some(hardware_slot);
        self
    }

    pub fn sled_role(&mut self, sled_role: SledRole) -> &mut Self {
        self.sled_role = sled_role;
        self
    }
}
