// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builders for constructing inventory collections and blueprints for synthetic
//! systems

use crate::blueprint_builder::BlueprintBuilder;
use anyhow::{anyhow, bail, Context};
use gateway_client::types::RotState;
use gateway_client::types::SpState;
use nexus_inventory::CollectionBuilder;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledResources;
use nexus_types::external_api::views::SledProvisionState;
use nexus_types::inventory::Collection;
use nexus_types::inventory::PowerState;
use nexus_types::inventory::RotSlot;
use nexus_types::inventory::SledRole;
use nexus_types::inventory::SpType;
use nexus_types::inventory::ZpoolName;
use omicron_common::address::get_sled_address;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::ByteCount;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use uuid::Uuid;

trait SubnetIterator: Iterator<Item = Ipv6Subnet<SLED_PREFIX>> + Debug {}
impl<T> SubnetIterator for T where
    T: Iterator<Item = Ipv6Subnet<SLED_PREFIX>> + Debug
{
}

#[derive(Debug)]
pub struct SyntheticSystemBuilder {
    collector: Option<String>,
    sleds: Vec<Sled>,
    sled_subnets: Box<dyn SubnetIterator>,
    available_non_scrimlet_slots: BTreeSet<u32>,
    available_scrimlet_slots: BTreeSet<u32>,
}

impl SyntheticSystemBuilder {
    pub fn new() -> Self {
        // Prepare sets of available slots (cubby numbers) for (1) all
        // non-Scrimlet sleds, and (2) Scrimlets in particular.  These do not
        // overlap.
        //
        // We will use these in two places:
        //
        // (1) when the caller specifies what slot a sled should go into, to
        //     validate that the slot is available and make sure we don't use it
        //     again
        //
        // (2) when the caller adds a sled but leaves the slot unspecified, so
        //     that we can assign it a slot number
        //
        // We use `BTreeSet` because it efficiently expresses what we want,
        // though the set sizes are small enough that it doesn't much matter.
        // XXX-dap are these constants defined somewhere?
        let available_scrimlet_slots: BTreeSet<u32> = BTreeSet::from([14, 16]);
        let available_non_scrimlet_slots: BTreeSet<u32> = (0..=31)
            .collect::<BTreeSet<_>>()
            .difference(&available_scrimlet_slots)
            .copied()
            .collect();

        // Prepare an iterator to allow us to assign sled subnets.
        let rack_subnet_base: Ipv6Addr =
            "fd00:1122:3344:0100::".parse().unwrap();
        let rack_subnet =
            ipnet::Ipv6Net::new(rack_subnet_base, RACK_PREFIX).unwrap();
        // Skip the initial DNS subnet.
        // XXX-dap should this be documented somewhere with a constant?  RSS
        // seems to hardcode it in sled-agent/src/rack_setup/plan/sled.rs.
        let sled_subnets = Box::new(
            rack_subnet
                .subnets(SLED_PREFIX)
                .unwrap()
                .skip(1)
                .map(|s| Ipv6Subnet::new(s.network())),
        );
        SyntheticSystemBuilder {
            sleds: Vec::new(),
            collector: None,
            sled_subnets,
            available_non_scrimlet_slots,
            available_scrimlet_slots,
        }
    }

    /// Returns a complete system deployed on a single Sled
    pub fn single_sled() -> anyhow::Result<Self> {
        let mut builder = SyntheticSystemBuilder::new();
        let sled = SledBuilder::new();
        builder.sled(sled)?;
        Ok(builder)
    }

    /// Returns a complete system resembling a full rack
    pub fn full_rack() -> anyhow::Result<Self> {
        let mut builder = SyntheticSystemBuilder::new();
        for slot_number in 1..32 {
            let mut sled = SledBuilder::new();
            if slot_number == 14 || slot_number == 16 {
                sled.sled_role(SledRole::Scrimlet);
            }
            builder.sled(sled)?;
        }
        Ok(builder)
    }

    pub fn collector_label<S>(&mut self, collector_label: S) -> &mut Self
    where
        String: From<S>,
    {
        self.collector = Some(String::from(collector_label));
        self
    }

    pub fn sled(&mut self, sled: SledBuilder) -> anyhow::Result<&mut Self> {
        let sled_subnet = self
            .sled_subnets
            .next()
            .ok_or_else(|| anyhow!("ran out of IPv6 subnets for sleds"))?;
        let hardware_slot = if let Some(slot) = sled.hardware_slot {
            // If the caller specified a slot number, use that.
            // Make sure it's still available, though.
            if !self.available_scrimlet_slots.remove(&slot)
                && !self.available_non_scrimlet_slots.remove(&slot)
            {
                bail!("sled slot {} was used twice", slot);
            }
            slot
        } else if sled.sled_role == SledRole::Scrimlet {
            // Otherwise, if this is a Scrimlet, it must be in one of the
            // allowed Scrimlet slots.
            self.available_scrimlet_slots
                .pop_first()
                .ok_or_else(|| anyhow!("ran out of slots for Scrimlets"))?
        } else {
            // Otherwise, prefer a non-Scrimlet slots, but allow a Scrimlet slot
            // to be used if we run out of non-Scrimlet slots.
            self.available_non_scrimlet_slots
                .pop_first()
                .or_else(|| self.available_scrimlet_slots.pop_first())
                .ok_or_else(|| anyhow!("ran out of slots for non-Scrimlets"))?
        };

        let sled_id = sled.id.unwrap_or_else(Uuid::new_v4);
        let sled = Sled::new(
            sled_id,
            sled_subnet,
            sled.sled_role,
            sled.unique,
            sled.hardware,
            hardware_slot,
            sled.npools,
        );
        self.sleds.push(sled);
        Ok(self)
    }

    pub fn to_collection(&self) -> anyhow::Result<Collection> {
        let collector_label = self
            .collector
            .as_ref()
            .cloned()
            .unwrap_or_else(|| String::from("example"));
        let mut builder = CollectionBuilder::new(collector_label);

        for s in &self.sleds {
            let slot = s.hardware_slot;
            if let Some(sp_state) = s.sp_state() {
                builder
                    .found_sp_state("fake MGS 1", SpType::Sled, slot, sp_state)
                    .context("recording SP state")?;
            }

            builder
                .found_sled_inventory(
                    "fake sled agent",
                    s.sled_agent_inventory(),
                )
                .context("recording sled agent")?;
        }

        Ok(builder.build())
    }

    pub fn to_policy(&self) -> anyhow::Result<Policy> {
        // XXX-dap configurable?
        let service_ip_pool_ranges = vec![IpRange::try_from((
            "192.168.1.20".parse::<Ipv4Addr>().unwrap(),
            "192.168.1.29".parse::<Ipv4Addr>().unwrap(),
        ))
        .unwrap()];
        // XXX-dap configurable?
        let target_nexus_zone_count = 3;
        let sleds = self
            .sleds
            .iter()
            .map(|sled| {
                let sled_resources = SledResources {
                    provision_state: SledProvisionState::Provisionable,
                    zpools: sled.zpools.iter().cloned().collect(),
                    subnet: sled.sled_subnet,
                };
                (sled.sled_id, sled_resources)
            })
            .collect();

        Ok(Policy {
            sleds,
            service_ip_pool_ranges: service_ip_pool_ranges,
            target_nexus_zone_count: target_nexus_zone_count,
        })
    }

    pub fn to_blueprint(&self, creator: &str) -> anyhow::Result<Blueprint> {
        BlueprintBuilder::build_initial_from_collection(
            &self.to_collection()?,
            &self.to_policy()?,
            creator,
        )
        .context("building blueprint for synthetic system")
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

#[derive(Clone, Debug)]
struct Sled {
    sled_id: Uuid,
    sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    sled_role: SledRole,
    unique: String,
    model: String,
    serial: String,
    revision: u32,
    hardware: SledHardware,
    hardware_slot: u32,
    zpools: Vec<ZpoolName>,
}

impl Sled {
    fn new(
        sled_id: Uuid,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
        sled_role: SledRole,
        unique: Option<String>,
        hardware: SledHardware,
        hardware_slot: u32,
        nzpools: u8,
    ) -> Sled {
        let unique = unique.unwrap_or_else(|| hardware_slot.to_string());
        let model = format!("model{}", unique);
        let serial = format!("serial{}", unique);
        let revision = 0;
        let zpools = (0..nzpools)
            .map(|_| format!("oxp_{}", Uuid::new_v4()).parse().unwrap())
            .collect();

        Sled {
            sled_id,
            sled_subnet,
            sled_role,
            unique,
            model,
            serial,
            revision,
            hardware,
            hardware_slot,
            zpools,
        }
    }

    fn sp_state(&self) -> Option<SpState> {
        match self.hardware {
            SledHardware::Empty => None,
            SledHardware::Gimlet | SledHardware::Pc | SledHardware::Unknown => {
                let unique = &self.unique;
                Some(SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: format!("hubris{}", unique),
                    model: self.model.clone(),
                    power_state: PowerState::A2,
                    revision: self.revision,
                    rot: RotState::Enabled {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: Some(String::from(
                            "slotAdigest1",
                        )),
                        slot_b_sha3_256_digest: Some(String::from(
                            "slotBdigest1",
                        )),
                        transient_boot_preference: None,
                    },
                    serial_number: self.serial.clone(),
                })
            }
        }
    }

    fn sled_agent_inventory(&self) -> sled_agent_client::types::Inventory {
        let baseboard = match self.hardware {
            SledHardware::Gimlet => {
                sled_agent_client::types::Baseboard::Gimlet {
                    identifier: self.serial.clone(),
                    model: self.model.clone(),
                    revision: i64::from(self.revision),
                }
            }
            SledHardware::Pc => sled_agent_client::types::Baseboard::Pc {
                identifier: self.serial.clone(),
                model: self.model.clone(),
            },
            SledHardware::Unknown | SledHardware::Empty => {
                sled_agent_client::types::Baseboard::Unknown
            }
        };
        let sled_agent_address = get_sled_address(self.sled_subnet).to_string();
        sled_agent_client::types::Inventory {
            baseboard,
            reservoir_size: ByteCount::from(1024),
            sled_role: self.sled_role,
            sled_agent_address,
            sled_id: self.sled_id,
            usable_hardware_threads: 10,
            usable_physical_ram: ByteCount::from(1024 * 1024),
        }
    }
}
