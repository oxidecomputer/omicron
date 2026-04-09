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
use crate::v28::inventory::SvcsEnabledNotOnline;

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
                Self::SvcsEnabledNotOnline(svcs)
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
