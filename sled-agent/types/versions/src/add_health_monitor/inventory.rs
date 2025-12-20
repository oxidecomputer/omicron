// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::svcs::SvcsInMaintenanceResult;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{Baseboard, SledCpuFamily};
use std::net::SocketAddrV6;

use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::InventoryZpool;
use crate::v1::inventory::SledRole;
use crate::v1::inventory::ZoneImageResolverInventory;
use crate::v11;
pub use crate::v11::inventory::ConfigReconcilerInventory;
pub use crate::v11::inventory::ConfigReconcilerInventoryStatus;
pub use crate::v11::inventory::OmicronSledConfig;

/// Identity and basic status information about this sled agent
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
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
    pub health_monitor: HealthMonitorInventory,
}

impl From<Inventory> for v11::inventory::Inventory {
    fn from(value: Inventory) -> Self {
        let Inventory {
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
            health_monitor: _,
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

/// Fields of sled-agent inventory reported by the health monitor subsystem.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HealthMonitorInventory {
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<SvcsInMaintenanceResult, String>::json_schema"
    )]
    pub smf_services_in_maintenance: Result<SvcsInMaintenanceResult, String>,
    // TODO: Other health check results will live here as well
}

impl HealthMonitorInventory {
    pub fn new() -> Self {
        Self { smf_services_in_maintenance: Ok(SvcsInMaintenanceResult::new()) }
    }

    pub fn is_empty(&self) -> bool {
        let Self { smf_services_in_maintenance } = self;

        if let Ok(svcs) = smf_services_in_maintenance {
            svcs.is_empty()
        } else {
            false
        }
    }
}
