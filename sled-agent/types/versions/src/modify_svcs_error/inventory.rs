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
use std::net::SocketAddrV6;
use strum::EnumIter;

use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::SledRole;
use crate::v14::inventory::ConfigReconcilerInventoryStatus;
use crate::v14::inventory::OmicronFileSourceResolverInventory;
use crate::v14::inventory::OmicronSledConfig;
use crate::v16::inventory::ConfigReconcilerInventory;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v28;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SvcsError {
    pub error: String,
    pub time_of_status: DateTime<Utc>,
}

impl From<SvcsError> for v28::inventory::SvcsError {
    fn from(value: SvcsError) -> Self {
        let SvcsError { error, time_of_status: _ } = value;
        // We don't put too much effort into parsing this back into the proper
        // error type because this API isn't used yet. It would be adding
        // unnecessary complexity.
        Self::CommandFailure(error)
    }
}

/// Each service instance is always in a well-defined state based on its
/// dependencies, the results of the execution of its methods, and its potential
/// contracts events.
///
/// This enum contains all possible states except `online`, `disabled` and
/// `legacy_run`. We only want to represent states that represent some sort of
/// "unhealthy" or "unexpected" state.
/// See <https://illumos.org/man/7/smf> for more information.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum SvcEnabledNotOnlineState {
    /// Initial state for all service instances.
    Uninitialized,
    /// The instance is enabled, but not yet running or available to run.
    Offline,
    /// The instance is enabled and running or available to run. It is, however,
    /// functioning at a limited capacity in comparison to normal operation.
    Degraded,
    /// The instance is enabled, but not able to run.
    Maintenance,
    /// We were unable to determine the state of the service instance.
    Unknown,
}

impl From<SvcEnabledNotOnlineState> for v28::inventory::SvcState {
    fn from(value: SvcEnabledNotOnlineState) -> Self {
        match value {
            SvcEnabledNotOnlineState::Degraded => Self::Degraded,
            SvcEnabledNotOnlineState::Maintenance => Self::Maintenance,
            SvcEnabledNotOnlineState::Offline => Self::Offline,
            SvcEnabledNotOnlineState::Uninitialized => Self::Uninitialized,
            SvcEnabledNotOnlineState::Unknown => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
pub struct SvcEnabledNotOnline {
    pub fmri: String,
    pub zone: String,
    pub state: SvcEnabledNotOnlineState,
}

impl From<SvcEnabledNotOnline> for v28::inventory::Svc {
    fn from(value: SvcEnabledNotOnline) -> Self {
        let SvcEnabledNotOnline { fmri, zone, state } = value;
        Self { fmri, zone, state: state.into() }
    }
}

/// Lists services that are enabled but not in an online state if any, the time
/// the sample was collected, and any errors that may have ocurred during the
/// collection
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SvcsEnabledNotOnline {
    pub services: Vec<SvcEnabledNotOnline>,
    pub errors: Vec<String>,
    pub time_of_status: DateTime<Utc>,
}

impl From<SvcsEnabledNotOnline> for v28::inventory::SvcsEnabledNotOnline {
    fn from(value: SvcsEnabledNotOnline) -> Self {
        let SvcsEnabledNotOnline { services, errors, time_of_status } = value;
        Self {
            services: services.into_iter().map(|s| s.into()).collect(),
            errors,
            time_of_status,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SvcsEnabledNotOnlineResult {
    SvcsEnabledNotOnline(SvcsEnabledNotOnline),
    SvcsCmdError(SvcsError),
    DataUnavailable,
}

impl From<SvcsEnabledNotOnlineResult>
    for v28::inventory::SvcsEnabledNotOnlineResult
{
    fn from(value: SvcsEnabledNotOnlineResult) -> Self {
        match value {
            SvcsEnabledNotOnlineResult::DataUnavailable => {
                Self::DataUnavailable
            }
            SvcsEnabledNotOnlineResult::SvcsCmdError(e) => {
                Self::SvcsCmdError(e.into())
            }
            SvcsEnabledNotOnlineResult::SvcsEnabledNotOnline(svcs) => {
                Self::SvcsEnabledNotOnline(svcs.into())
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

impl From<Inventory> for v28::inventory::Inventory {
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
            smf_services_enabled_not_online: smf_services_enabled_not_online
                .into(),
            reference_measurements,
        }
    }
}
