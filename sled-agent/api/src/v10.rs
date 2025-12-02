// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-agent API types that changed from v10 to v11.

use nexus_sled_agent_shared::inventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_hardware_types::Baseboard;
use sled_hardware_types::SledCpuFamily;
use std::net::SocketAddrV6;

/// Identity and basic status information about this sled agent
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct Inventory {
    pub sled_id: SledUuid,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub baseboard: Baseboard,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,
    pub cpu_family: SledCpuFamily,
    pub reservoir_size: ByteCount,
    pub disks: Vec<InventoryDisk>,
    pub zpools: Vec<InventoryZpool>,
    pub datasets: Vec<InventoryDataset>,
    pub ledgered_sled_config: Option<OmicronSledConfig>,
    pub reconciler_status: ConfigReconcilerInventoryStatus,
    pub last_reconciliation: Option<ConfigReconcilerInventory>,
    pub zone_image_resolver: ZoneImageResolverInventory,
}

impl From<inventory::Inventory> for Inventory {
    fn from(value: inventory::Inventory) -> Self {
        let inventory::Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
            smf_services_enabled_not_running: _,
        } = value;
        Self {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
        }
    }
}
