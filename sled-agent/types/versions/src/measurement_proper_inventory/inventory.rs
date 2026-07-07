// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use iddqd::id_upcast;
use iddqd::{IdOrdItem, IdOrdMap};
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::{DatasetUuid, OmicronZoneUuid};
use schemars::{
    JsonSchema, SchemaGenerator, schema::Schema, schema::SchemaObject,
};
use serde::{Deserialize, Serialize};
use sled_hardware_types::{Baseboard, SledCpuFamily};
use std::collections::BTreeMap;
use std::net::SocketAddrV6;

use crate::v1::inventory::{
    BootPartitionContents, ConfigReconcilerInventoryResult, InventoryDataset,
    InventoryDisk, InventoryZpool, OrphanedDataset,
    RemoveMupdateOverrideInventory, SledRole,
};
use crate::v14;
use crate::v14::inventory::{
    ConfigReconcilerInventoryStatus, HealthMonitorInventory,
    OmicronFileSourceResolverInventory, OmicronSledConfig,
    ReconciledSingleMeasurement,
};

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
    pub health_monitor: HealthMonitorInventory,
    pub reference_measurements: IdOrdMap<SingleMeasurementInventory>,
}

impl TryFrom<Inventory> for v14::inventory::Inventory {
    type Error = external::Error;

    fn try_from(value: Inventory) -> Result<Self, Self::Error> {
        let measurements = value
            .reference_measurements
            .into_iter()
            .map(|m| ReconciledSingleMeasurement {
                file_name: m.path.file_name().unwrap_or("").to_string(),
                path: m.path,
                result: m.result,
            })
            .collect();

        let last_reconciliation = value.last_reconciliation.map(|v| {
            v14::inventory::ConfigReconcilerInventory {
                last_reconciled_config: v.last_reconciled_config,
                external_disks: v.external_disks,
                datasets: v.datasets,
                orphaned_datasets: v.orphaned_datasets,
                zones: v.zones,
                boot_partitions: v.boot_partitions,
                remove_mupdate_override: v.remove_mupdate_override,
                measurements,
            }
        });

        Ok(Self {
            sled_id: value.sled_id,
            sled_agent_address: value.sled_agent_address,
            sled_role: value.sled_role,
            baseboard: value.baseboard,
            usable_hardware_threads: value.usable_hardware_threads,
            usable_physical_ram: value.usable_physical_ram,
            cpu_family: value.cpu_family,
            reservoir_size: value.reservoir_size,
            disks: value.disks,
            zpools: value.zpools,
            datasets: value.datasets,
            ledgered_sled_config: value.ledgered_sled_config,
            reconciler_status: value.reconciler_status,
            last_reconciliation,
            file_source_resolver: value.file_source_resolver,
            health_monitor: value.health_monitor,
        })
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

/// An attempt at resolving a single measurement file to a valid path
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct SingleMeasurementInventory {
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,
    pub result: ConfigReconcilerInventoryResult,
}

impl IdOrdItem for SingleMeasurementInventory {
    type Key<'a> = &'a Utf8PathBuf;
    fn key(&self) -> Self::Key<'_> {
        &self.path
    }
    id_upcast!();
}

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
