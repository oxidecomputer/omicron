// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_uuid_kinds::{FmdHostCaseUuid, FmdResourceUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::SledCpuFamily;
use std::net::SocketAddrV6;

use crate::v1::inventory::Baseboard;
use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::SledRole;
use crate::v14::inventory::ConfigReconcilerInventoryStatus;
use crate::v14::inventory::OmicronFileSourceResolverInventory;
use crate::v14::inventory::OmicronSledConfig;
use crate::v16::inventory::ConfigReconcilerInventory;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v37;

/// A diagnosed fault case from the illumos Fault Management Daemon on a sled.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct FmdHostCase {
    /// Unique identifier for this case.
    pub uuid: FmdHostCaseUuid,
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

impl IdOrdItem for FmdHostCase {
    type Key<'a> = FmdHostCaseUuid;

    fn key(&self) -> Self::Key<'_> {
        self.uuid
    }

    id_upcast!();
}

/// A resource affected by a diagnosed fault.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct FmdResource {
    /// Fault Management Resource Identifier
    /// (e.g. "dev:////pci@af,0/pci1022,1483@3,5").
    pub fmri: String,
    /// Unique identifier for this resource entry.
    pub uuid: FmdResourceUuid,
    /// UUID of the case that diagnosed this fault.
    pub case_id: FmdHostCaseUuid,
    /// Whether the resource is marked faulty.
    pub faulty: bool,
    /// Whether the resource is marked unusable.
    pub unusable: bool,
    /// Whether the resource is marked invisible.
    pub invisible: bool,
}

impl IdOrdItem for FmdResource {
    type Key<'a> = FmdResourceUuid;

    fn key(&self) -> Self::Key<'_> {
        self.uuid
    }

    id_upcast!();
}

/// Successfully collected FMD fault data.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
pub struct FmdInventory {
    pub cases: IdOrdMap<FmdHostCase>,
    pub resources: IdOrdMap<FmdResource>,
}

/// Maximum number of FMD cases sled-agent will report for a single sled.
/// Exceeding this returns [`FmdInventoryErrorKind::TooManyCases`] rather than
/// silently truncating: a count this high indicates a pathological state
/// operators should investigate directly via `fmadm`.
pub const FMD_MAX_CASES: u32 = 1000;

/// Maximum number of FMD resources sled-agent will report for a single sled.
/// See [`FMD_MAX_CASES`] for rationale.
pub const FMD_MAX_RESOURCES: u32 = 1000;

/// Classification of an [`FmdInventoryError`].
///
/// `FmdError` is a catch-all for any FMD-side failure: the daemon was
/// unreachable, a case/resource listing failed, or the platform doesn't have
/// FMD at all. The accompanying message disambiguates these cases.
/// `TooManyCases` and `TooManyResources` are first-class because exceeding
/// those bounds is operationally distinct from a transient FMD failure.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FmdInventoryErrorKind {
    /// Catch-all for FMD-side failures.
    FmdError,
    /// Number of FMD cases exceeded [`FMD_MAX_CASES`].
    TooManyCases,
    /// Number of FMD resources exceeded [`FMD_MAX_RESOURCES`].
    TooManyResources,
}

/// An error reported by sled-agent in place of an [`FmdInventory`].
///
/// `kind` is a typed discriminator suitable for filtering / monitoring.
/// `message` is a human-readable description (built via `Display`); it is
/// informational only and should not be parsed.
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
pub struct FmdInventoryError {
    pub kind: FmdInventoryErrorKind,
    pub message: String,
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
        v37::inventory::SvcsEnabledNotOnlineResult,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<FmdInventory, FmdInventoryError>::json_schema"
    )]
    pub fmd: Result<FmdInventory, FmdInventoryError>,
}

impl From<Inventory> for v37::inventory::Inventory {
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
