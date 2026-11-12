// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Changes in this version:
//!
//! * Introduce new types:
//!     - [`WicketSshAuth`]
//!     - [`WicketSshAuthKind`]
//!     - [`WicketSshAuthStatus`]
//! * Add [`OmicronSledConfig::wicket_ssh_auth`] field
//! * Add [`Inventory::wicket_ssh_auth_status`] field
//! * Bump all types that include [`OmicronSledConfig`]

use crate::v1::disk::DatasetConfig;
use crate::v1::disk::OmicronPhysicalDiskConfig;
use crate::v1::inventory::ConfigReconcilerInventoryResult;
use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::OrphanedDataset;
use crate::v1::inventory::RemoveMupdateOverrideInventory;
use crate::v1::inventory::SledCpuFamily;
use crate::v1::inventory::SledRole;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v40::inventory::FmdInventory;
use crate::v40::inventory::FmdInventoryError;
use crate::v46::inventory::SvcsEnabledNotOnlineResult;
use crate::v49::inventory::OmicronSledUpdateDisposition;
use crate::v53::inventory::InstanceManagerStatus;
use crate::v55::inventory::BootPartitionContents;
use crate::v55::inventory::HostPhase2DesiredSlots;
use crate::v55::inventory::OmicronFileSourceResolverInventory;
use crate::v55::inventory::OmicronSingleMeasurement;
use crate::v55::inventory::OmicronZoneConfig;
use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_generation_kinds::SledConfigGeneration;
use omicron_generation_kinds::WicketSshAuthGeneration;
use omicron_ledger::Ledgerable;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::time::Duration;

/// Describes the authentication requirements for accessing the `wicket` captive
/// shell over SSH.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct WicketSshAuth {
    pub generation: WicketSshAuthGeneration,
    pub kind: WicketSshAuthKind,
}

impl WicketSshAuth {
    pub fn unauthenticated_initial_generation() -> Self {
        Self {
            generation: WicketSshAuthGeneration::new(),
            kind: WicketSshAuthKind::Unauthenticated,
        }
    }
}

/// Particular kind of authentication requirement for accessing the `wicket`
/// captive shell over SSH.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WicketSshAuthKind {
    /// `wicket` is publicly accessible to any client.
    Unauthenticated,

    /// `wicket` is only accessible to clients supplying one of the specified
    /// SSH keys.
    PublicKeys {
        /// Set of operator-specified keys allowed to access wicket.
        ///
        /// This set may be empty. If it is empty and `allow_support_keys` is
        /// `false`, no SSH connections to `wicket` will succeed.
        ///
        /// Each item in this set is expected to be a single, valid
        /// `authorized_keys` line. Nexus is expected to perform suitable
        /// validation on incoming keys before passing them to sled-agent.
        // We explicitly _don't_ do validation on the items in this set here,
        // because its `Deserialize` impl is also used to load the ledgered
        // config from disk after a reboot. If we perform an update that changes
        // the logic for valid `authorized_keys`, we still need to be able to
        // deserialize the on-disk keys we had from before. It's awkward to know
        // that we could accept a key, upgrade to a release that would have
        // rejected that key, then keep using it, but that's much better than
        // the alternative of failing to deserialize our ledgered config.
        authorized_keys: BTreeSet<String>,

        /// Whether to allow the built-in support keys shipped with the release
        /// access to `wicket`.
        ///
        /// This setting only controls the support keys' access to `wicket`;
        /// even if this is false, the support keys can still be used for
        /// support operations (support access is strictly more powerful than
        /// access to `wicket`; we recommend leaving this set to `true`).
        allow_support_keys: bool,
    },
}

/// Status of the application of our configured [`WicketSshAuth`] to our switch
/// zone, if we have one.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WicketSshAuthStatus {
    /// We have no switch zone, and therefore have no need to apply SSH
    /// settings.
    NoSwitchZone,

    /// We have a switch zone.
    SwitchZone {
        /// The current auth generation setting in effect in our switch zone.
        ///
        /// This generation number can be compared against the ledgered
        /// generation stored in our [`OmicronSledConfig`] to check for
        /// effective application of the latest-known setting.
        effective_auth_gen: WicketSshAuthGeneration,

        /// If the most recent attempt to set a new auth generation failed,
        /// contains the error.
        last_error: Option<String>,
    },
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: SledConfigGeneration,
    pub disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    pub datasets: IdOrdMap<DatasetConfig>,
    pub zones: IdOrdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    #[serde(default = "HostPhase2DesiredSlots::current_contents")]
    pub host_phase_2: HostPhase2DesiredSlots,
    // We purposely skip a serde default here to work around some ledger
    // versioning quirks
    pub measurements: BTreeSet<OmicronSingleMeasurement>,
    pub update_disposition: OmicronSledUpdateDisposition,
    pub wicket_ssh_auth: WicketSshAuth,
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

impl From<crate::v55::inventory::OmicronSledConfig> for OmicronSledConfig {
    fn from(value: crate::v55::inventory::OmicronSledConfig) -> Self {
        let crate::v55::inventory::OmicronSledConfig {
            generation,
            disks,
            datasets,
            zones,
            remove_mupdate_override,
            host_phase_2,
            measurements,
            update_disposition,
        } = value;

        Self {
            generation,
            disks,
            datasets,
            zones,
            remove_mupdate_override,
            host_phase_2,
            measurements,
            update_disposition,
            wicket_ssh_auth: WicketSshAuth::unauthenticated_initial_generation(
            ),
        }
    }
}

impl From<OmicronSledConfig> for crate::v55::inventory::OmicronSledConfig {
    fn from(value: OmicronSledConfig) -> Self {
        let OmicronSledConfig {
            generation,
            disks,
            datasets,
            zones,
            remove_mupdate_override,
            host_phase_2,
            measurements,
            update_disposition,
            wicket_ssh_auth: _,
        } = value;

        Self {
            generation,
            disks,
            datasets,
            zones,
            remove_mupdate_override,
            host_phase_2,
            measurements,
            update_disposition,
        }
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
    for crate::v55::inventory::ConfigReconcilerInventory
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
    for crate::v55::inventory::ConfigReconcilerInventoryStatus
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
    pub wicket_ssh_auth_status: WicketSshAuthStatus,
}

impl From<Inventory> for crate::v57::inventory::Inventory {
    fn from(value: Inventory) -> Self {
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
            // Added this version; not representable to older clients.
            wicket_ssh_auth_status: _,
        } = value;
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
            disks,
            zpools,
            datasets,
            ledgered_sled_config: ledgered_sled_config.map(From::from),
            reconciler_status: reconciler_status.into(),
            last_reconciliation: last_reconciliation.map(From::from),
            instance_manager_status,
            file_source_resolver,
            smf_services_enabled_not_online,
            reference_measurements,
            fmd,
        }
    }
}
