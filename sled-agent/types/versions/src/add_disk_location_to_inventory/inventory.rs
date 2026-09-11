// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddrV6;

use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result::{self, SnakeCaseResult};
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{BaseboardId, SledCpuFamily};

use crate::v1;
use crate::v1::disk::{DiskIdentity, DiskVariant};
use crate::v1::inventory::{InventoryDataset, SledRole};
use crate::v14::inventory::OmicronFileSourceResolverInventory;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v40::inventory::{FmdInventory, FmdInventoryError};
use crate::v46::inventory::SvcsEnabledNotOnlineResult;
use crate::v51::inventory::{
    ConfigReconcilerInventory, ConfigReconcilerInventoryStatus,
    OmicronSledConfig,
};
use crate::v53;
use crate::v53::inventory::InstanceManagerStatus;

/// Identifies information about disks which may be attached to Sleds.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDisk {
    pub identity: DiskIdentity,
    pub variant: DiskVariant,
    /// PCIe physical slot number of the bridge above this disk.
    ///
    /// This is the `physical-slot#` of the parent `pcieb` device. It is
    /// internal to the board's PCIe topology and board-specific: the same
    /// U.2 bay is numbered differently on Gimlet and Cosmo. It is not the
    /// location label printed on the chassis; see `location` for that.
    pub pcie_slot: i64,
    /// Where this disk sits in the chassis, as labelled by the platform's
    /// hardware topology: "N5" for a U.2 bay, "M.2 East" for a boot device.
    ///
    /// This is the operator-facing position, matching what is printed on the
    /// sled. It is best-effort: `None` means the topology had no label for
    /// the disk or could not be read.
    pub location: Option<String>,
    // Today we only have NVMe disks so we embedded the firmware metadata here.
    // In the future we can track firmware metadata in a unique type if we
    // support more than one disk format.
    pub active_firmware_slot: u8,
    pub next_active_firmware_slot: Option<u8>,
    pub number_of_firmware_slots: u8,
    pub slot1_is_read_only: bool,
    pub slot_firmware_versions: Vec<Option<String>>,
}

impl From<InventoryDisk> for v1::inventory::InventoryDisk {
    fn from(new: InventoryDisk) -> Self {
        let InventoryDisk {
            identity,
            variant,
            pcie_slot,
            location: _,
            active_firmware_slot,
            next_active_firmware_slot,
            number_of_firmware_slots,
            slot1_is_read_only,
            slot_firmware_versions,
        } = new;
        Self {
            identity,
            variant,
            slot: pcie_slot,
            active_firmware_slot,
            next_active_firmware_slot,
            number_of_firmware_slots,
            slot1_is_read_only,
            slot_firmware_versions,
        }
    }
}

/// Identity and basic status information about this sled agent
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Inventory {
    pub sled_id: SledUuid,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub baseboard_id: BaseboardId,
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
    pub instance_manager_status: InstanceManagerStatus,
    pub file_source_resolver: OmicronFileSourceResolverInventory,
    pub smf_services_enabled_not_online: SvcsEnabledNotOnlineResult,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<FmdInventory, FmdInventoryError>::json_schema"
    )]
    pub fmd: Result<FmdInventory, FmdInventoryError>,
}

impl From<Inventory> for v53::inventory::Inventory {
    fn from(new: Inventory) -> Self {
        let Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard_id,
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
            instance_manager_status,
            file_source_resolver,
            smf_services_enabled_not_online,
            reference_measurements,
            fmd,
        } = new;
        Self {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard_id,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks: disks.into_iter().map(Into::into).collect(),
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            instance_manager_status,
            file_source_resolver,
            smf_services_enabled_not_online,
            reference_measurements,
            fmd,
        }
    }
}
