// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Changes in this version:
//!
//! * Add [`OmicronSledUpdateDisposition`] type
//! * Add `update_disposition` to [`OmicronSledConfig`]
//! * New definitions of types that directly or transitively include
//!   `OmicronSledConfig`:
//!     * [`ConfigReconcilerInventory`]
//!     * [`ConfigReconcilerInventoryStatus`]
//!     * [`Inventory`]

use crate::v1::disk::DatasetConfig;
use crate::v1::disk::OmicronPhysicalDiskConfig;
use crate::v1::inventory::BootPartitionContents;
use crate::v1::inventory::ConfigReconcilerInventoryResult;
use crate::v1::inventory::HostPhase2DesiredSlots;
use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::OrphanedDataset;
use crate::v1::inventory::RemoveMupdateOverrideInventory;
use crate::v1::inventory::SledCpuFamily;
use crate::v1::inventory::SledRole;
use crate::v11::inventory::OmicronZoneConfig;
use crate::v14::inventory::OmicronFileSourceResolverInventory;
use crate::v14::inventory::OmicronSingleMeasurement;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v40::inventory::FmdInventory;
use crate::v40::inventory::FmdInventoryError;
use crate::v46::inventory::SvcsEnabledNotOnlineResult;
use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_generation_kinds::Generation;
use omicron_ledger::Ledgerable;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::time::Duration;

/// Controls a sled's availability for VMM provisioning.
///
/// This is a simplified version of `BlueprintSledUpdateDisposition`. The
/// blueprint version of this type includes information describing _how_ the
/// sled should be evacuated as well as blueprint-specific metadata. From
/// sled-agent itself, we only need to know the high-level state; we use this to
/// determine whether or not to accept new VMM registration requests.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum OmicronSledUpdateDisposition {
    Available,
    Evacuating,
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: Generation,
    pub disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    pub datasets: IdOrdMap<DatasetConfig>,
    pub zones: IdOrdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    pub host_phase_2: HostPhase2DesiredSlots,
    pub measurements: BTreeSet<OmicronSingleMeasurement>,
    pub update_disposition: OmicronSledUpdateDisposition,
}

impl From<crate::v14::inventory::OmicronSledConfig> for OmicronSledConfig {
    fn from(value: crate::v14::inventory::OmicronSledConfig) -> Self {
        // Old versions had no `update_disposition` and were always implicitly
        // `Available`.
        let update_disposition = OmicronSledUpdateDisposition::Available;

        Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
            measurements: value.measurements,
            update_disposition,
        }
    }
}

impl From<OmicronSledConfig> for crate::v14::inventory::OmicronSledConfig {
    fn from(value: OmicronSledConfig) -> Self {
        // Converting in this direction drops the new `update_disposition` field;
        // this allows old Nexus to request our inventory after sled-agent is
        // updated.
        Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
            measurements: value.measurements,
        }
    }
}

// NOTE: Most trait impls live in the `impls` module of this crate and are only
// implemented for the `latest` version of each type. However,
// `OmicronSledConfig` is special: it's not only used in the sled-agent API
// (which would only require trait impls on `latest`); it's also ledgered to
// disk to support cold boot of the rack. In the ledgering case, we have to be
// able to handle reading older versions, which means all the old versions we
// support also need to implement `Ledgerable`. Therefore, we implement this
// trait for this specific version (and do so for every other version of
// `OmicronSledConfig` too).
impl Ledgerable for OmicronSledConfig {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.generation > other.generation
    }

    fn generation_bump(&mut self) {
        // DO NOTHING!
        //
        // Generation bumps must only ever come from nexus and will be encoded
        // in the struct itself
    }
}

/// Describes the last attempt made by the sled-agent-config-reconciler to
/// reconcile the current sled config against the actual state of the sled.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigReconcilerInventory {
    pub last_reconciled_config: OmicronSledConfig,
    pub external_disks:
        BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult>,
    pub datasets: BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult>,
    pub orphaned_datasets: IdOrdMap<OrphanedDataset>,
    pub zones: BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult>,
    pub boot_partitions: BootPartitionContents,
    /// The result of removing the mupdate override file on disk.
    ///
    /// `None` if `remove_mupdate_override` was not provided in the sled config.
    pub remove_mupdate_override: Option<RemoveMupdateOverrideInventory>,
}

impl From<ConfigReconcilerInventory>
    for crate::v16::inventory::ConfigReconcilerInventory
{
    fn from(value: ConfigReconcilerInventory) -> Self {
        let ConfigReconcilerInventory {
            last_reconciled_config,
            external_disks,
            datasets,
            orphaned_datasets,
            zones,
            boot_partitions,
            remove_mupdate_override,
        } = value;

        Self {
            last_reconciled_config: last_reconciled_config.into(),
            external_disks,
            datasets,
            orphaned_datasets,
            zones,
            boot_partitions,
            remove_mupdate_override,
        }
    }
}

/// Status of the sled-agent-config-reconciler task.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ConfigReconcilerInventoryStatus {
    /// The reconciler task has not yet run for the first time since sled-agent
    /// started.
    NotYetRun,
    /// The reconciler task is actively running.
    Running {
        config: Box<OmicronSledConfig>,
        started_at: DateTime<Utc>,
        running_for: Duration,
    },
    /// The reconciler task is currently idle, but previously did complete a
    /// reconciliation attempt.
    ///
    /// This variant does not include the `OmicronSledConfig` used in the last
    /// attempt, because that's always available via
    /// [`ConfigReconcilerInventory::last_reconciled_config`].
    Idle { completed_at: DateTime<Utc>, ran_for: Duration },
}

impl From<ConfigReconcilerInventoryStatus>
    for crate::v14::inventory::ConfigReconcilerInventoryStatus
{
    fn from(value: ConfigReconcilerInventoryStatus) -> Self {
        match value {
            ConfigReconcilerInventoryStatus::NotYetRun => Self::NotYetRun,
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Self::Running {
                config: Box::new((*config).into()),
                started_at,
                running_for,
            },
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                Self::Idle { completed_at, ran_for }
            }
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
    pub file_source_resolver: OmicronFileSourceResolverInventory,
    pub smf_services_enabled_not_online: SvcsEnabledNotOnlineResult,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<FmdInventory, FmdInventoryError>::json_schema"
    )]
    pub fmd: Result<FmdInventory, FmdInventoryError>,
}

impl From<Inventory> for crate::v46::inventory::Inventory {
    fn from(value: Inventory) -> Self {
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
            file_source_resolver,
            smf_services_enabled_not_online,
            reference_measurements,
            fmd,
        } = value;

        Self {
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
            ledgered_sled_config: ledgered_sled_config.map(From::from),
            reconciler_status: reconciler_status.into(),
            last_reconciliation: last_reconciliation.map(From::from),
            file_source_resolver,
            smf_services_enabled_not_online,
            reference_measurements,
            fmd,
        }
    }
}
