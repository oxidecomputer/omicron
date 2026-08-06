// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdMap;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{BaseboardId, SledCpuFamily};
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
use crate::v34;
use crate::v34::inventory::SvcsError;
use crate::v37;
use crate::v40::inventory::{FmdInventory, FmdInventoryError};
use crate::v43;

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
    /// An instance whose state is in transition from one to another. Note: as
    /// per `man svcs`, An asterisk (*) is appended for instances in transition.
    /// So there is not an "in-transition" state per se.
    InTransition,
    /// An instance whose state is absent or unrecognized. Like `InTransition`,
    /// this state does not explicitly exist in `svcs`. Per `man svcs`: Absent
    /// or unrecognized states are denoted by a question mark (?) character.
    Unrecognized,
}

impl TryFrom<SvcState> for v34::inventory::SvcState {
    type Error = external::Error;

    fn try_from(value: SvcState) -> Result<Self, Self::Error> {
        match value {
            SvcState::Degraded => Ok(Self::Degraded),
            SvcState::Maintenance => Ok(Self::Maintenance),
            SvcState::Offline => Ok(Self::Offline),
            SvcState::Uninitialized => Ok(Self::Uninitialized),
            SvcState::Disabled => Ok(Self::Disabled),
            SvcState::LegacyRun => Ok(Self::LegacyRun),
            SvcState::Online => Ok(Self::Online),
            SvcState::InTransition | SvcState::Unrecognized => {
                Err(external::Error::InternalError {
                    internal_message: format!("unknown state {:?}", value),
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
pub struct Svc {
    pub fmri: String,
    pub zone: String,
    pub state: SvcState,
}

impl TryFrom<Svc> for v34::inventory::Svc {
    type Error = external::Error;

    fn try_from(value: Svc) -> Result<Self, Self::Error> {
        let Svc { fmri, zone, state } = value;
        let state = Self { fmri, zone, state: state.try_into()? };
        Ok(state)
    }
}

/// Each service instance is always in a well-defined state based on its
/// dependencies, the results of the execution of its methods, and its potential
/// contracts events.
///
/// This enum contains all possible states except `online`, `disabled`,
/// `uninitialized` and `legacy_run`. We only want to represent states that
/// represent some sort of "unhealthy" or "unexpected" state.
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
    /// The instance is enabled, but not yet running or available to run.
    Offline,
    /// The instance is enabled and running or available to run. It is, however,
    /// functioning at a limited capacity in comparison to normal operation.
    Degraded,
    /// The instance is enabled, but not able to run.
    Maintenance,
    /// An instance whose state is absent or unrecognized. Like `InTransition`,
    /// this state does not explicitly exist in `svcs`. Per `man svcs`: Absent
    /// or unrecognized states are denoted by a question mark (?) character.
    Unrecognized,
}

impl TryFrom<SvcEnabledNotOnlineState>
    for v37::inventory::SvcEnabledNotOnlineState
{
    type Error = external::Error;

    fn try_from(value: SvcEnabledNotOnlineState) -> Result<Self, Self::Error> {
        match value {
            SvcEnabledNotOnlineState::Offline => Ok(Self::Offline),
            SvcEnabledNotOnlineState::Degraded => Ok(Self::Degraded),
            SvcEnabledNotOnlineState::Maintenance => Ok(Self::Maintenance),
            SvcEnabledNotOnlineState::Unrecognized => {
                Err(external::Error::InternalError {
                    internal_message: format!("unknown state {:?}", value),
                })
            }
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

impl TryFrom<SvcEnabledNotOnline> for v37::inventory::SvcEnabledNotOnline {
    type Error = external::Error;

    fn try_from(value: SvcEnabledNotOnline) -> Result<Self, Self::Error> {
        let SvcEnabledNotOnline { fmri, zone, state } = value;
        Ok(Self { fmri, zone, state: state.try_into()? })
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

impl TryFrom<SvcsEnabledNotOnline> for v37::inventory::SvcsEnabledNotOnline {
    type Error = external::Error;

    fn try_from(value: SvcsEnabledNotOnline) -> Result<Self, Self::Error> {
        let SvcsEnabledNotOnline { services, errors, time_of_status } = value;
        let services = services
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { services, errors, time_of_status })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SvcsEnabledNotOnlineResult {
    SvcsEnabledNotOnline(SvcsEnabledNotOnline),
    SvcsCmdError(SvcsError),
    DataUnavailable,
}

impl TryFrom<SvcsEnabledNotOnlineResult>
    for v37::inventory::SvcsEnabledNotOnlineResult
{
    type Error = external::Error;

    fn try_from(
        value: SvcsEnabledNotOnlineResult,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            SvcsEnabledNotOnlineResult::DataUnavailable => {
                Self::DataUnavailable
            }
            SvcsEnabledNotOnlineResult::SvcsCmdError(e) => {
                Self::SvcsCmdError(e)
            }
            SvcsEnabledNotOnlineResult::SvcsEnabledNotOnline(svcs) => {
                Self::SvcsEnabledNotOnline(svcs.try_into()?)
            }
        })
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

impl TryFrom<Inventory> for v43::inventory::Inventory {
    type Error = external::Error;

    fn try_from(value: Inventory) -> Result<Self, Self::Error> {
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
        Ok(Self {
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
            smf_services_enabled_not_online: smf_services_enabled_not_online
                .try_into()?,
            reference_measurements,
            fmd,
        })
    }
}
