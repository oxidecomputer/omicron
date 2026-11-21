// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddrV6;

use chrono::{DateTime, Utc};
use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result::{self, SnakeCaseResult};
use omicron_uuid_kinds::{RackUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;

use crate::v1::disk::{DiskIdentity, DiskVariant};
use crate::v1::inventory::{InventoryDataset, SledCpuFamily, SledRole};
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v40::inventory::{FmdInventory, FmdInventoryError};
use crate::v46::inventory::SvcsEnabledNotOnlineResult;
use crate::v53::inventory::InstanceManagerStatus;
use crate::v55::inventory::{
    ConfigReconcilerInventory, ConfigReconcilerInventoryStatus,
    OmicronFileSourceResolverInventory, OmicronSledConfig,
};
use crate::v65;

/// The health of an NVMe disk, from the Critical Warning field of its
/// controller's SMART / Health Information log page.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct NvmeHealth {
    /// When sled-agent read the log page from the controller.
    pub time_collected: DateTime<Utc>,
    /// The controller warns that its reliability is degraded, due to media
    /// errors or an internal error.
    pub reliability_degraded: bool,
    /// The controller warns that its media has been placed in read-only
    /// mode.
    pub read_only: bool,
}

/// Why sled-agent has no [`NvmeHealth`] for a disk.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum NvmeHealthErrorKind {
    /// Sled-agent reached the disk's controller, but could not read the
    /// SMART / Health Information log page from it.
    LogPageReadFailed,
    /// Sled-agent's latest poll of its hardware did not read this disk at
    /// all. Among other reasons, this happens while the controller is locked
    /// for a format or a firmware update.
    NotPolled,
}

/// An error reported by sled-agent in place of an [`NvmeHealth`].
///
/// `message` is informational only and should not be parsed.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
    thiserror::Error,
)]
#[error("{message}")]
pub struct NvmeHealthError {
    pub kind: NvmeHealthErrorKind,
    pub message: String,
}

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
    /// Nothing guarantees the numbering stays the same across host OS
    /// versions either, so anything that needs a stable identifier should
    /// use `location`.
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
    /// The disk's health as its NVMe controller reports it, or why
    /// sled-agent has no reading of it. A reading's `time_collected` says
    /// how old it is.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<NvmeHealth, NvmeHealthError>::json_schema"
    )]
    pub nvme_health: Result<NvmeHealth, NvmeHealthError>,
}

impl From<InventoryDisk> for v65::inventory::InventoryDisk {
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
            nvme_health: _,
        } = new;
        Self {
            identity,
            variant,
            pcie_slot,
            location,
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
    pub rack_id: RackUuid,
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

impl From<Inventory> for v65::inventory::Inventory {
    fn from(new: Inventory) -> Self {
        let Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard_id,
            rack_id,
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
            rack_id,
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
