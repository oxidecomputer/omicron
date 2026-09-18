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
use crate::v53::inventory::InstanceManagerStatus;
use crate::v54;

/// Identifies information about disks which may be attached to Sleds.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDisk {
    pub identity: DiskIdentity,
    pub variant: DiskVariant,
    /// PCIe physical slot number of the bridge behind this disk's bay.
    ///
    /// This is the `binding/slot` of the bay in the hardware topology, the
    /// same number the `pcieb` device reports as `physical-slot#`. It is
    /// internal to the board's PCIe topology and board-specific: the same
    /// U.2 bay is numbered differently on Gimlet and Cosmo. It is not the
    /// location label printed on the chassis; see `location` for that.
    /// Nothing guarantees the numbering stays the same across host OS
    /// versions either, so anything that needs a stable identifier should
    /// use `location`.
    pub pcie_slot: i64,
    /// Where this disk sits in the chassis, as labelled by the platform's
    /// hardware topology: "N5" for a U.2 bay, "M.2 East" for a boot device.
    ///
    /// This is the operator-facing position, matching what is printed on the
    /// sled. The same bay appears in `Inventory::disk_bays`.
    pub location: String,
    // Today we only have NVMe disks so we embedded the firmware metadata here.
    // In the future we can track firmware metadata in a unique type if we
    // support more than one disk format.
    pub active_firmware_slot: u8,
    pub next_active_firmware_slot: Option<u8>,
    pub number_of_firmware_slots: u8,
    pub slot1_is_read_only: bool,
    pub slot_firmware_versions: Vec<Option<String>>,
}

impl From<InventoryDisk> for v54::inventory::InventoryDisk {
    fn from(new: InventoryDisk) -> Self {
        let InventoryDisk {
            identity,
            variant,
            pcie_slot,
            location,
            active_firmware_slot,
            next_active_firmware_slot,
            number_of_firmware_slots,
            slot1_is_read_only,
            slot_firmware_versions,
        } = new;
        Self {
            identity,
            variant,
            pcie_slot,
            location: Some(location),
            active_firmware_slot,
            next_active_firmware_slot,
            number_of_firmware_slots,
            slot1_is_read_only,
            slot_firmware_versions,
        }
    }
}

/// One U.2 bay or M.2 socket of the sled's chassis, and what the hardware
/// topology found behind it.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
pub struct InventoryDiskBay {
    /// The label printed on the chassis, such as "N3" or "M.2 East".
    pub location: String,
    /// Whether this is a U.2 bay or an M.2 socket.
    pub kind: DiskVariant,
    pub occupant: InventoryDiskBayOccupant,
}

/// What the hardware topology found behind a bay.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InventoryDiskBayOccupant {
    /// Nothing is behind this bay. A drive whose PCIe link is down also
    /// looks like this until the host OS gains presence detection.
    Empty,
    /// An NVMe disk that sled-agent manages. It appears in
    /// `Inventory::disks` under this identity.
    Disk { identity: DiskIdentity },
    /// A device is attached but there is no disk sled-agent can manage: an
    /// NVMe controller with no active namespace, or something that is not
    /// NVMe at all.
    Device {
        /// The driver bound to the device, if one is.
        driver: Option<String>,
        /// The device's path under `/devices`, if the topology recorded one.
        devfs_path: Option<String>,
    },
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
    /// Every U.2 bay and M.2 socket of the chassis, occupied or not. Empty
    /// on hosts that are not Oxide sleds.
    pub disk_bays: Vec<InventoryDiskBay>,
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

impl From<Inventory> for v54::inventory::Inventory {
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
            disk_bays: _,
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
