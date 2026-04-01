// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdMap;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{Baseboard, SledCpuFamily};
use slog_error_chain::SlogInlineError;
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

/// Each service instance is always in a well-defined state based on its
/// dependencies, the results of the execution of its methods, and its potential
/// contracts events. See <https://illumos.org/man/7/smf> for more information.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SvcState {
    /// Initial state for all service instances.
    Uninitialized,
    /// The instance is enabled, but not yet running or available to run.
    Offline,
    /// The instance is enabled and running or is available to run.
    Online,
    /// The instance is enabled and running or available to run. It is, however,
    /// functioning at a limited capacity in comparison to normal operation.
    Degraded,
    /// The instance is enabled, but not able to run.
    Maintenance,
    /// The instance is disabled.
    Disabled,
    /// Represents a legacy instance that is not managed by the service
    /// management facility.
    LegacyRun,
    /// We were unable to determine the state of the service instance.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
// TODO-correctness `SvcState::Online` is one possibility; should we have a
// different enum if we're actually restricted to "enabled but not running"?
pub struct Svc {
    pub fmri: String,
    pub zone: String,
    pub state: SvcState,
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
    pub smf_services_enabled_not_online: SvcsEnabledNotOnlineResult,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SvcsEnabledNotOnlineResult {
    SvcsEnabledNotOnline(SvcsEnabledNotOnline),
    SvcsCmdError(SvcsError),
    DataUnavailable,
}

/// Error that is a one to one mapping of `illumos_utils::ExecutionError`, which
/// uses a String for any type in `ExecutionError` that is not cloneable.
#[derive(
    thiserror::Error,
    Debug,
    SlogInlineError,
    Clone,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SvcsError {
    #[error("failed to start execution of `{command}`: {err}")]
    ExecutionStart { command: String, err: String },
    #[error("command failure: {0}")]
    CommandFailure(String),
    #[error("contract error: {msg}: {err}")]
    ContractFailure { msg: String, err: String },
    #[error("failed to parse command output: {0}")]
    ParseFailure(String),
    #[error("zone is not running")]
    NotRunning,
}
