// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to convert nexus-reconfigurator-planning's `SystemDescription` into
//! queries on the sled table.

use crate::db::DataStore;
use nexus_db_model::Generation;
use nexus_db_model::Sled;
use nexus_db_model::SledBaseboard;
use nexus_db_model::SledSystemHardware;
use nexus_db_model::SledUpdate;
use nexus_reconfigurator_planning::system::SimulatedSledResources;
use nexus_reconfigurator_planning::system::Sled as SimulatedSled;
use nexus_reconfigurator_planning::system::SystemDescription;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::RackUuid;
use sled_agent_types::inventory::SledCpuFamily;
use sled_agent_types::inventory::SledRole;

/// Return a realistic `SimulatedSledResources` for use in reservation-based
/// tests.
pub fn test_sled_resources() -> SimulatedSledResources {
    SimulatedSledResources {
        usable_hardware_threads: 4,
        usable_physical_ram: ByteCount::try_from(1u64 << 40)
            .expect("1 TiB is a valid byte count"),
        reservoir_size: ByteCount::try_from(1u64 << 39)
            .expect("512 GiB is a valid byte count"),
        cpu_family: SledCpuFamily::AmdMilan,
    }
}

/// Return a `SledUpdate` upsertable row for a simulated sled.
///
/// When Sled Agent comes online, it makes an upcall to Nexus. This simulates
/// the process of Nexus receiving that information.
pub fn sled_update_from_simulated_sled(
    sled: &SimulatedSled,
    rack_id: RackUuid,
) -> SledUpdate {
    let inventory = sled.sled_agent_inventory();
    let is_scrimlet = match inventory.sled_role {
        SledRole::Scrimlet => true,
        SledRole::Gimlet => false,
    };
    let revision = sled.sp_state().map_or(0, |(_slot, sp)| sp.revision);
    SledUpdate::new(
        inventory.sled_id,
        inventory.sled_agent_address,
        // Simulated sleds don't have a repo depot port.
        0,
        SledBaseboard {
            serial_number: inventory.baseboard_id.serial_number.clone(),
            part_number: inventory.baseboard_id.part_number.clone(),
            revision,
        },
        SledSystemHardware {
            is_scrimlet,
            usable_hardware_threads: inventory.usable_hardware_threads,
            usable_physical_ram: inventory.usable_physical_ram.into(),
            reservoir_size: inventory.reservoir_size.into(),
            cpu_family: inventory.cpu_family.into(),
        },
        rack_id,
        Generation::new(),
    )
}

/// Return [`SledUpdate`] for every sled in the system, in sled ID order.
pub fn sled_updates_from_system(
    system: &SystemDescription,
    rack_id: RackUuid,
) -> Vec<SledUpdate> {
    system
        .sleds()
        .map(|sled| sled_update_from_simulated_sled(sled, rack_id))
        .collect()
}

/// Register every sled in the system, returning the sled rows in sled ID
/// order.
pub async fn upsert_sleds_from_system(
    datastore: &DataStore,
    system: &SystemDescription,
    rack_id: RackUuid,
) -> Vec<Sled> {
    let sled_updates = sled_updates_from_system(system, rack_id);
    let mut sleds = Vec::with_capacity(sled_updates.len());
    for sled_update in sled_updates {
        let sled_id = sled_update.id();
        let (sled, _) = datastore
            .sled_upsert(sled_update)
            .await
            .unwrap_or_else(|e| panic!("registered sled {sled_id}: {e}"));
        sleds.push(sled);
    }
    sleds
}
