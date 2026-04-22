// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{Baseboard, SledCpuFamily};
use std::net::SocketAddrV6;
use uuid::Uuid;

use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::SledRole;
use crate::v14::inventory::ConfigReconcilerInventoryStatus;
use crate::v14::inventory::OmicronFileSourceResolverInventory;
use crate::v14::inventory::OmicronSledConfig;
use crate::v16::inventory::ConfigReconcilerInventory;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v34;

/// A diagnosed fault case from the illumos Fault Management Daemon.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct FmdCase {
    /// Unique identifier for this case.
    pub uuid: Uuid,
    /// Diagnostic code (e.g. "PCIEX-8000-DJ").
    pub code: String,
    /// URL for human-readable information about this fault
    /// (e.g. `http://illumos.org/msg/PCIEX-8000-DJ`).
    pub url: String,
    /// Full fault event payload as JSON, if present. Contains the
    /// fault-list with classes, certainties, affected FMRIs, and other
    /// diagnostic detail.
    pub event: Option<serde_json::Value>,
}

/// A resource affected by a diagnosed fault.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct FmdResource {
    /// Fault Management Resource Identifier
    /// (e.g. "dev:////pci@af,0/pci1022,1483@3,5").
    pub fmri: String,
    /// Unique identifier for this resource entry.
    pub uuid: Uuid,
    /// UUID of the case that diagnosed this fault.
    pub case_id: Uuid,
    /// Whether the resource is marked faulty.
    pub faulty: bool,
    /// Whether the resource is marked unusable.
    pub unusable: bool,
    /// Whether the resource is marked invisible.
    pub invisible: bool,
}

/// Result of querying FMD for fault information.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum FmdInventoryResult {
    /// FMD data was successfully collected.
    Available(FmdInventory),
    /// FMD data collection failed or is not available on this platform.
    Error { error: String },
}

/// Successfully collected FMD fault data.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
pub struct FmdInventory {
    pub cases: Vec<FmdCase>,
    pub resources: Vec<FmdResource>,
}

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
    pub smf_services_enabled_not_online:
        v34::inventory::SvcsEnabledNotOnlineResult,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
    pub fmd: FmdInventoryResult,
}

impl From<Inventory> for v34::inventory::Inventory {
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
            smf_services_enabled_not_online,
            reference_measurements,
            fmd: _,
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
            smf_services_enabled_not_online,
            reference_measurements,
        }
    }
}
