// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdMap;
use illumos_utils::svcs::Svc;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_option_result;
use omicron_common::snake_case_option_result::SnakeCaseOptionResult;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{Baseboard, SledCpuFamily};
use std::net::SocketAddrV6;

use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::SledRole;
use crate::v14::inventory::ConfigReconcilerInventoryStatus;
use crate::v14::inventory::OmicronFileSourceResolverInventory;
use crate::v14::inventory::OmicronSledConfig;
use crate::v16::inventory::ConfigReconcilerInventory;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24;
use crate::v24::inventory::InventoryZpool;

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
    pub file_source_resolver: OmicronFileSourceResolverInventory,
    #[serde(with = "snake_case_option_result")]
    #[schemars(
        schema_with = "SnakeCaseOptionResult::<SvcsEnabledNotOnline, String>::json_schema"
    )]
    pub smf_services_enabled_not_online:
        Option<Result<SvcsEnabledNotOnline, String>>,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
}

impl From<Inventory> for v24::inventory::Inventory {
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
            file_source_resolver,
            smf_services_enabled_not_online: _,
            reference_measurements,
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
            file_source_resolver,
            smf_services_in_maintenance: Err(
                "Unable to retrieve services in maintenance".to_string(),
            ),
            reference_measurements,
        }
    }
}

/// Lists services that are enabled but not in an online state if any, the time
/// the sample was collected, and any errors that may have ocurred during the
/// collection
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SvcsEnabledNotOnline {
    pub services: Vec<Svc>,
    pub errors: Vec<String>,
    pub time_of_status: DateTime<Utc>,
}
