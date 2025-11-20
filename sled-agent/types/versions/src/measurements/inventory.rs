// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::net::SocketAddrV6;

use chrono::{DateTime, Utc};
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::api::external;
use omicron_common::{
    api::external::{ByteCount, Generation},
    disk::{DatasetConfig, OmicronPhysicalDiskConfig},
};
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::{DatasetUuid, OmicronZoneUuid};
use omicron_uuid_kinds::{MupdateOverrideUuid, PhysicalDiskUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::{Baseboard, SledCpuFamily};
use std::time::Duration;

use crate::v1;
use crate::v1::inventory::{
    BootPartitionContents, ConfigReconcilerInventoryResult,
    HostPhase2DesiredSlots, InventoryDataset, InventoryDisk, InventoryZpool,
    ManifestInventory, MupdateOverrideInventory, OrphanedDataset,
    RemoveMupdateOverrideInventory, SledRole,
};
use crate::v10;
use crate::v11;
use crate::v11::inventory::OmicronZoneConfig;
use camino::Utf8PathBuf;
use daft::Diffable;
use schemars::SchemaGenerator;
use schemars::schema::{Schema, SchemaObject};
use std::fmt;
use tufaceous_artifact::ArtifactHash;

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
    pub zone_image_resolver: ZoneImageResolverInventory,
}

impl TryFrom<Inventory> for v11::inventory::Inventory {
    type Error = external::Error;

    fn try_from(value: Inventory) -> Result<Self, Self::Error> {
        let ledgered_sled_config =
            value.ledgered_sled_config.map(TryInto::try_into).transpose()?;
        let last_reconciliation =
            value.last_reconciliation.map(TryInto::try_into).transpose()?;
        let zone_image_resolver = value.zone_image_resolver.try_into()?;
        let reconciler_status = value.reconciler_status.try_into()?;
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
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
        })
    }
}

impl TryFrom<Inventory> for v10::inventory::Inventory {
    type Error = external::Error;

    fn try_from(value: Inventory) -> Result<Self, Self::Error> {
        let ledgered_sled_config =
            value.ledgered_sled_config.map(TryInto::try_into).transpose()?;
        let last_reconciliation =
            value.last_reconciliation.map(TryInto::try_into).transpose()?;
        let zone_image_resolver = value.zone_image_resolver.try_into()?;
        let reconciler_status = value.reconciler_status.try_into()?;
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
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
        })
    }
}

/// Inventory representation of zone image resolver status and health.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneImageResolverInventory {
    /// The zone manifest status.
    pub zone_manifest: ManifestInventory,

    /// The zone manifest status.
    pub measurement_manifest: ManifestInventory,

    pub mupdate_override: MupdateOverrideInventory,
}

impl TryFrom<ZoneImageResolverInventory>
    for v1::inventory::ZoneImageResolverInventory
{
    type Error = external::Error;

    fn try_from(
        value: ZoneImageResolverInventory,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            zone_manifest: value.zone_manifest,
            mupdate_override: value.mupdate_override,
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
    pub measurements: IdOrdMap<ReconciledSingleMeasurement>,
    /// The result of removing the mupdate override file on disk.
    ///
    /// `None` if `remove_mupdate_override` was not provided in the sled config.
    pub remove_mupdate_override: Option<RemoveMupdateOverrideInventory>,
}

impl TryFrom<ConfigReconcilerInventory>
    for v11::inventory::ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(value: ConfigReconcilerInventory) -> Result<Self, Self::Error> {
        let last_reconciled_config = value.last_reconciled_config.try_into()?;
        Ok(Self {
            last_reconciled_config,
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions,
            remove_mupdate_override: value.remove_mupdate_override,
        })
    }
}

impl TryFrom<ConfigReconcilerInventory>
    for v10::inventory::ConfigReconcilerInventory
{
    type Error = external::Error;

    fn try_from(value: ConfigReconcilerInventory) -> Result<Self, Self::Error> {
        let last_reconciled_config = value.last_reconciled_config.try_into()?;
        Ok(Self {
            last_reconciled_config,
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions,
            remove_mupdate_override: value.remove_mupdate_override,
        })
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

impl TryFrom<ConfigReconcilerInventoryStatus>
    for v10::inventory::ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        value: ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        match value {
            ConfigReconcilerInventoryStatus::NotYetRun => {
                Ok(v10::inventory::ConfigReconcilerInventoryStatus::NotYetRun)
            }
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Ok(v10::inventory::ConfigReconcilerInventoryStatus::Running {
                config: Box::new((*config).try_into()?),
                started_at,
                running_for,
            }),
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                Ok(v10::inventory::ConfigReconcilerInventoryStatus::Idle {
                    completed_at,
                    ran_for,
                })
            }
        }
    }
}

impl TryFrom<ConfigReconcilerInventoryStatus>
    for v11::inventory::ConfigReconcilerInventoryStatus
{
    type Error = external::Error;

    fn try_from(
        value: ConfigReconcilerInventoryStatus,
    ) -> Result<Self, Self::Error> {
        match value {
            ConfigReconcilerInventoryStatus::NotYetRun => {
                Ok(v11::inventory::ConfigReconcilerInventoryStatus::NotYetRun)
            }
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => Ok(v11::inventory::ConfigReconcilerInventoryStatus::Running {
                config: Box::new((*config).try_into()?),
                started_at,
                running_for,
            }),
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                Ok(v11::inventory::ConfigReconcilerInventoryStatus::Idle {
                    completed_at,
                    ran_for,
                })
            }
        }
    }
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: Generation,
    // Serialize and deserialize disks, datasets, and zones as maps for
    // backwards compatibility. Newer IdOrdMaps should not use IdOrdMapAsMap.
    #[serde(
        with = "iddqd::id_ord_map::IdOrdMapAsMap::<OmicronPhysicalDiskConfig>"
    )]
    pub disks: IdOrdMap<OmicronPhysicalDiskConfig>,
    #[serde(with = "iddqd::id_ord_map::IdOrdMapAsMap::<DatasetConfig>")]
    pub datasets: IdOrdMap<DatasetConfig>,
    #[serde(with = "iddqd::id_ord_map::IdOrdMapAsMap::<OmicronZoneConfig>")]
    pub zones: IdOrdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    #[serde(default = "HostPhase2DesiredSlots::current_contents")]
    pub host_phase_2: HostPhase2DesiredSlots,
    #[serde(default = "OmicronMeasurements::measurements_defaults")]
    pub measurements: OmicronMeasurements,
}

impl TryFrom<OmicronSledConfig> for v11::inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: OmicronSledConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        })
    }
}

impl TryFrom<OmicronSledConfig> for v10::inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: OmicronSledConfig) -> Result<Self, Self::Error> {
        let zones = value
            .zones
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
        })
    }
}

impl TryFrom<v10::inventory::OmicronSledConfig> for OmicronSledConfig {
    type Error = external::Error;

    fn try_from(
        value: v10::inventory::OmicronSledConfig,
    ) -> Result<Self, Self::Error> {
        let zones = value
            .zones
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones,
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2,
            measurements: OmicronMeasurements::measurements_defaults(),
        })
    }
}

/// Where the measurement source is located
///
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OmicronMeasurementSetDesiredContents {
    /// This measurement source is whatever happens to be on the sled's
    /// "install" dataset.
    ///
    /// This is whatever was put in place at the factory or by the latest
    /// MUPdate. The image used here can vary by sled and even over time (if the
    /// sled gets MUPdated again). We expect this to be only used for
    /// emergencies
    InstallDataset,

    /// This measurement source source are the artifacts matching this hash from the TUF
    /// artifact store (aka "TUF repo depot").
    ///
    /// This originates from TUF repos uploaded to Nexus which are then
    /// replicated out to all sleds.
    #[serde(rename_all = "snake_case")]
    Artifacts { hashes: Vec<ArtifactHash> },
}

fn measurement_set_default() -> OmicronMeasurementSetDesiredContents {
    OmicronMeasurementSetDesiredContents::InstallDataset
}

/// Describes the set of reference measurements for a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronMeasurements {
    #[serde(default = "measurement_set_default")]
    pub measurements: OmicronMeasurementSetDesiredContents,
}

impl OmicronMeasurements {
    pub fn measurements_defaults() -> OmicronMeasurements {
        OmicronMeasurements { measurements: measurement_set_default() }
    }
    pub fn display(&self) -> OmicronMeasurementsDisplay<'_> {
        OmicronMeasurementsDisplay { inner: self }
    }
}

/// a displayer for [`OmicronMeasurements`]
pub struct OmicronMeasurementsDisplay<'a> {
    inner: &'a OmicronMeasurements,
}

impl fmt::Display for OmicronMeasurementsDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let OmicronMeasurements { measurements } = self.inner;

        writeln!(f, "measurements: ")?;
        match measurements {
            OmicronMeasurementSetDesiredContents::InstallDataset => {
                writeln!(f, "install dataset")?
            }
            OmicronMeasurementSetDesiredContents::Artifacts { hashes } => {
                for h in hashes {
                    writeln!(f, "artifact: {h}")?;
                }
            }
        }
        Ok(())
    }
}

/// An attempt at resolving a single measurement file to a valid path
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ReconciledSingleMeasurement {
    pub file_name: String,

    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,
    pub result: ConfigReconcilerInventoryResult,
}

impl IdOrdItem for ReconciledSingleMeasurement {
    type Key<'a> = String;
    fn key(&self) -> Self::Key<'_> {
        // XXX uuugghg
        self.file_name.clone()
    }
    id_upcast!();
}

impl ReconciledSingleMeasurement {
    pub fn display(&self) -> ReconciledSingleMeasurementDisplay<'_> {
        ReconciledSingleMeasurementDisplay { inner: self }
    }
}

/// a displayer for [`ReconciledSingleMeasurement`]
pub struct ReconciledSingleMeasurementDisplay<'a> {
    inner: &'a ReconciledSingleMeasurement,
}

impl fmt::Display for ReconciledSingleMeasurementDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconciledSingleMeasurement { file_name, path, result } =
            self.inner;

        write!(f, "{file_name} with path {path}: ")?;
        match result {
            ConfigReconcilerInventoryResult::Ok => writeln!(f, "ok")?,
            ConfigReconcilerInventoryResult::Err { message } => {
                writeln!(f, "error : {message}")?
            }
        }
        Ok(())
    }
}

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
