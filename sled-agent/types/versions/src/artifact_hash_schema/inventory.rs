// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::time::Duration;

use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::api::external::ByteCount;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_common::update::OmicronInstallManifestSource;
use omicron_common::zpool_name::ZpoolName;
use omicron_generation_kinds::Generation;
use omicron_generation_kinds::SledConfigGeneration;
use omicron_ledger::Ledgerable;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use schemars::SchemaGenerator;
use schemars::schema::Schema;
use schemars::schema::SchemaObject;
use serde::Deserialize;
use serde::Serialize;
use sled_hardware_types::BaseboardId;
use sled_hardware_types::SledCpuFamily;
use tufaceous_artifact::ArtifactHash;

use crate::v1;
use crate::v1::disk::DatasetConfig;
use crate::v1::disk::M2Slot;
use crate::v1::disk::OmicronPhysicalDiskConfig;
use crate::v1::inventory::BootImageHeader;
use crate::v1::inventory::ConfigReconcilerInventoryResult;
use crate::v1::inventory::InventoryDataset;
use crate::v1::inventory::InventoryDisk;
use crate::v1::inventory::ManifestNonBootInventory;
use crate::v1::inventory::MupdateOverrideInventory;
use crate::v1::inventory::OrphanedDataset;
use crate::v1::inventory::RemoveMupdateOverrideInventory;
use crate::v1::inventory::SledRole;
use crate::v14;
use crate::v16::inventory::SingleMeasurementInventory;
use crate::v24::inventory::InventoryZpool;
use crate::v40::inventory::FmdInventory;
use crate::v40::inventory::FmdInventoryError;
use crate::v46::inventory::SvcsEnabledNotOnlineResult;
use crate::v49::inventory::OmicronSledUpdateDisposition;
use crate::v51;
use crate::v51::inventory::OmicronZoneType;
use crate::v53;
use crate::v53::inventory::InstanceManagerStatus;

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

impl From<Inventory> for v53::inventory::Inventory {
    fn from(value: Inventory) -> Self {
        Self {
            sled_id: value.sled_id,
            sled_agent_address: value.sled_agent_address,
            sled_role: value.sled_role,
            baseboard_id: value.baseboard_id,
            usable_hardware_threads: value.usable_hardware_threads,
            usable_physical_ram: value.usable_physical_ram,
            cpu_family: value.cpu_family,
            reservoir_size: value.reservoir_size,
            disks: value.disks,
            zpools: value.zpools,
            datasets: value.datasets,
            ledgered_sled_config: value.ledgered_sled_config.map(Into::into),
            reconciler_status: value.reconciler_status.into(),
            last_reconciliation: value.last_reconciliation.map(Into::into),
            instance_manager_status: value.instance_manager_status,
            file_source_resolver: value.file_source_resolver.into(),
            smf_services_enabled_not_online: value
                .smf_services_enabled_not_online,
            reference_measurements: value.reference_measurements,
            fmd: value.fmd,
        }
    }
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

impl From<OmicronSledConfig> for v51::inventory::OmicronSledConfig {
    fn from(value: OmicronSledConfig) -> Self {
        Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones.into_iter().map(Into::into).collect(),
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2.into(),
            measurements: value
                .measurements
                .into_iter()
                .map(Into::into)
                .collect(),
            update_disposition: value.update_disposition,
        }
    }
}

impl From<v51::inventory::OmicronSledConfig> for OmicronSledConfig {
    fn from(value: v51::inventory::OmicronSledConfig) -> Self {
        Self {
            generation: value.generation,
            disks: value.disks,
            datasets: value.datasets,
            zones: value.zones.into_iter().map(Into::into).collect(),
            remove_mupdate_override: value.remove_mupdate_override,
            host_phase_2: value.host_phase_2.into(),
            measurements: value
                .measurements
                .into_iter()
                .map(Into::into)
                .collect(),
            update_disposition: value.update_disposition,
        }
    }
}

/// Describes the set of Omicron-managed zones running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZonesConfig {
    /// generation number of this configuration
    ///
    /// This generation number is owned by the control plane (i.e., RSS or
    /// Nexus, depending on whether RSS-to-Nexus handoff has happened).  It
    /// should not be bumped within Sled Agent.
    ///
    /// Sled Agent rejects attempts to set the configuration to a generation
    /// older than the one it's currently running.
    pub generation: Generation,

    /// list of running zones
    pub zones: Vec<OmicronZoneConfig>,
}

impl From<v51::inventory::OmicronZonesConfig> for OmicronZonesConfig {
    fn from(value: v51::inventory::OmicronZonesConfig) -> Self {
        Self {
            generation: value.generation,
            zones: value.zones.into_iter().map(Into::into).collect(),
        }
    }
}

/// Describes one Omicron-managed zone running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneConfig {
    pub id: OmicronZoneUuid,

    /// The pool on which we'll place this zone's root filesystem.
    ///
    /// Note that the root filesystem is transient -- the sled agent is
    /// permitted to destroy this dataset each time the zone is initialized.
    pub filesystem_pool: Option<ZpoolName>,
    pub zone_type: OmicronZoneType,
    // Use `InstallDataset` if this field is not present in a deserialized
    // blueprint or ledger.
    #[serde(default = "OmicronZoneImageSource::deserialize_default")]
    pub image_source: OmicronZoneImageSource,
}

impl IdOrdItem for OmicronZoneConfig {
    type Key<'a> = OmicronZoneUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

impl From<OmicronZoneConfig> for v51::inventory::OmicronZoneConfig {
    fn from(value: OmicronZoneConfig) -> Self {
        Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type,
            image_source: value.image_source.into(),
        }
    }
}

impl From<v51::inventory::OmicronZoneConfig> for OmicronZoneConfig {
    fn from(value: v51::inventory::OmicronZoneConfig) -> Self {
        Self {
            id: value.id,
            filesystem_pool: value.filesystem_pool,
            zone_type: value.zone_type,
            image_source: value.image_source.into(),
        }
    }
}

/// Where Sled Agent should get the image for a zone.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OmicronZoneImageSource {
    /// This zone's image source is whatever happens to be on the sled's
    /// "install" dataset.
    ///
    /// This is whatever was put in place at the factory or by the latest
    /// MUPdate. The image used here can vary by sled and even over time (if the
    /// sled gets MUPdated again).
    ///
    /// Historically, this was the only source for zone images. In an system
    /// with automated control-plane-driven update we expect to only use this
    /// variant in emergencies where the system had to be recovered via MUPdate.
    InstallDataset,
    /// This zone's image source is the artifact matching this hash from the TUF
    /// artifact store (aka "TUF repo depot").
    ///
    /// This originates from TUF repos uploaded to Nexus which are then
    /// replicated out to all sleds.
    Artifact { hash: ArtifactHash },
}

impl OmicronZoneImageSource {
    // See `OmicronZoneConfig`. This is a separate function instead of being
    // `impl Default` because we don't want to accidentally use this default
    // outside of `serde(default)`.
    pub fn deserialize_default() -> Self {
        OmicronZoneImageSource::InstallDataset
    }
}

impl From<OmicronZoneImageSource> for v1::inventory::OmicronZoneImageSource {
    fn from(value: OmicronZoneImageSource) -> Self {
        match value {
            OmicronZoneImageSource::InstallDataset => Self::InstallDataset,
            OmicronZoneImageSource::Artifact { hash } => {
                Self::Artifact { hash }
            }
        }
    }
}

impl From<v1::inventory::OmicronZoneImageSource> for OmicronZoneImageSource {
    fn from(value: v1::inventory::OmicronZoneImageSource) -> Self {
        match value {
            v1::inventory::OmicronZoneImageSource::InstallDataset => {
                Self::InstallDataset
            }
            v1::inventory::OmicronZoneImageSource::Artifact { hash } => {
                Self::Artifact { hash }
            }
        }
    }
}

/// Describes the desired contents of a host phase 2 slot (i.e., the boot
/// partition on one of the internal M.2 drives).
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostPhase2DesiredContents {
    /// Do not change the current contents.
    ///
    /// We use this value when we've detected a sled has been mupdated (and we
    /// don't want to overwrite phase 2 images until we understand how to
    /// recover from that mupdate) and as the default value when reading an
    /// [`OmicronSledConfig`] that was ledgered before this concept existed.
    CurrentContents,

    /// Set the phase 2 slot to the given artifact.
    ///
    /// The artifact will come from an unpacked and distributed TUF repo.
    Artifact { hash: ArtifactHash },
}

impl From<HostPhase2DesiredContents>
    for v1::inventory::HostPhase2DesiredContents
{
    fn from(value: HostPhase2DesiredContents) -> Self {
        match value {
            HostPhase2DesiredContents::CurrentContents => Self::CurrentContents,
            HostPhase2DesiredContents::Artifact { hash } => {
                Self::Artifact { hash }
            }
        }
    }
}

impl From<v1::inventory::HostPhase2DesiredContents>
    for HostPhase2DesiredContents
{
    fn from(value: v1::inventory::HostPhase2DesiredContents) -> Self {
        match value {
            v1::inventory::HostPhase2DesiredContents::CurrentContents => {
                Self::CurrentContents
            }
            v1::inventory::HostPhase2DesiredContents::Artifact { hash } => {
                Self::Artifact { hash }
            }
        }
    }
}

/// Describes the desired contents for both host phase 2 slots.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct HostPhase2DesiredSlots {
    pub slot_a: HostPhase2DesiredContents,
    pub slot_b: HostPhase2DesiredContents,
}

impl From<HostPhase2DesiredSlots> for v1::inventory::HostPhase2DesiredSlots {
    fn from(value: HostPhase2DesiredSlots) -> Self {
        Self { slot_a: value.slot_a.into(), slot_b: value.slot_b.into() }
    }
}

impl From<v1::inventory::HostPhase2DesiredSlots> for HostPhase2DesiredSlots {
    fn from(value: v1::inventory::HostPhase2DesiredSlots) -> Self {
        Self { slot_a: value.slot_a.into(), slot_b: value.slot_b.into() }
    }
}

/// Represents a single measurement artfact from the TUF artifact
/// store (aka "TUF repo depot"). The fully resolved measurement
/// set is used with trust quorum.
///
/// Measurements may also come from outside the TUF repo depot
/// via the install dataset from MUPdate but are not represented here
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct OmicronSingleMeasurement {
    /// Measurements are the artifacts matching the hashes from the TUF
    /// artifact store (aka "TUF repo depot")
    ///
    /// Measurements may also come from outside the TUF repo depot
    /// via the install dataset from MUPdate but are not explicitly
    /// tracked here
    pub hash: ArtifactHash,
}

impl From<OmicronSingleMeasurement>
    for v14::inventory::OmicronSingleMeasurement
{
    fn from(value: OmicronSingleMeasurement) -> Self {
        Self { hash: value.hash }
    }
}

impl From<v14::inventory::OmicronSingleMeasurement>
    for OmicronSingleMeasurement
{
    fn from(value: v14::inventory::OmicronSingleMeasurement) -> Self {
        Self { hash: value.hash }
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
    for v51::inventory::ConfigReconcilerInventoryStatus
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
    for v51::inventory::ConfigReconcilerInventory
{
    fn from(value: ConfigReconcilerInventory) -> Self {
        Self {
            last_reconciled_config: value.last_reconciled_config.into(),
            external_disks: value.external_disks,
            datasets: value.datasets,
            orphaned_datasets: value.orphaned_datasets,
            zones: value.zones,
            boot_partitions: value.boot_partitions.into(),
            remove_mupdate_override: value.remove_mupdate_override,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct BootPartitionContents {
    #[serde(with = "snake_case_result")]
    #[schemars(schema_with = "SnakeCaseResult::<M2Slot, String>::json_schema")]
    pub boot_disk: Result<M2Slot, String>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<BootPartitionDetails, String>::json_schema"
    )]
    pub slot_a: Result<BootPartitionDetails, String>,
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<BootPartitionDetails, String>::json_schema"
    )]
    pub slot_b: Result<BootPartitionDetails, String>,
}

impl From<BootPartitionContents> for v1::inventory::BootPartitionContents {
    fn from(value: BootPartitionContents) -> Self {
        Self {
            boot_disk: value.boot_disk,
            slot_a: value.slot_a.map(Into::into),
            slot_b: value.slot_b.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct BootPartitionDetails {
    pub header: BootImageHeader,
    pub artifact_hash: ArtifactHash,
    pub artifact_size: usize,
}

impl From<BootPartitionDetails> for v1::inventory::BootPartitionDetails {
    fn from(value: BootPartitionDetails) -> Self {
        Self {
            header: value.header,
            artifact_hash: value.artifact_hash,
            artifact_size: value.artifact_size,
        }
    }
}

/// Inventory representation of zone image resolver and measurement resolver
/// status and health. Previously known as `ZoneImageResolverInventory`
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct OmicronFileSourceResolverInventory {
    /// The zone manifest status.
    pub zone_manifest: ManifestInventory,

    /// The measurement manifest status.
    pub measurement_manifest: ManifestInventory,

    pub mupdate_override: MupdateOverrideInventory,
}

impl From<OmicronFileSourceResolverInventory>
    for v14::inventory::OmicronFileSourceResolverInventory
{
    fn from(value: OmicronFileSourceResolverInventory) -> Self {
        Self {
            zone_manifest: value.zone_manifest.into(),
            measurement_manifest: value.measurement_manifest.into(),
            mupdate_override: value.mupdate_override,
        }
    }
}

/// Inventory representation of a manifest.
///
/// Part of [`OmicronFileSourceResolverInventory`].
///
/// A manifest is used for both zones and reference measurements.
/// A zone manifest is a listing of all the zones present in a system's install
/// dataset. A measurement manifset is a listing of all the reference measurements
/// present in a system's install dataset. This struct contains information
/// about the install dataset gathered from a system.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ManifestInventory {
    /// The full path to the zone manifest file on the boot disk.
    #[schemars(schema_with = "path_schema")]
    pub boot_disk_path: Utf8PathBuf,

    /// The manifest read from the boot disk, and whether the manifest is valid.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<ManifestBootInventory, String>::json_schema"
    )]
    pub boot_inventory: Result<ManifestBootInventory, String>,

    /// Information about the install dataset on non-boot disks.
    pub non_boot_status: IdOrdMap<ManifestNonBootInventory>,
}

impl From<ManifestInventory> for v1::inventory::ManifestInventory {
    fn from(value: ManifestInventory) -> Self {
        Self {
            boot_disk_path: value.boot_disk_path,
            boot_inventory: value.boot_inventory.map(Into::into),
            non_boot_status: value.non_boot_status,
        }
    }
}

/// Inventory representation of zone artifacts on the boot disk.
///
/// Part of [`ManifestInventory`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ManifestBootInventory {
    /// The manifest source.
    ///
    /// In production this is [`OmicronInstallManifestSource::Installinator`], but
    /// in some development and testing flows Sled Agent synthesizes zone
    /// manifests. In those cases, the source is
    /// [`OmicronInstallManifestSource::SledAgent`].
    pub source: OmicronInstallManifestSource,

    /// The artifacts on disk.
    pub artifacts: IdOrdMap<ZoneArtifactInventory>,
}

impl From<ManifestBootInventory> for v1::inventory::ManifestBootInventory {
    fn from(value: ManifestBootInventory) -> Self {
        Self {
            source: value.source,
            artifacts: value.artifacts.into_iter().map(Into::into).collect(),
        }
    }
}

/// Inventory representation of a single zone artifact on a boot disk.
///
/// Part of [`ManifestBootInventory`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneArtifactInventory {
    /// The name of the zone file on disk, for example `nexus.tar.gz`. Zone
    /// files are always ".tar.gz".
    pub file_name: String,

    /// The full path to the zone file.
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,

    /// The expected size of the file, in bytes.
    pub expected_size: u64,

    /// The expected digest of the file's contents.
    pub expected_hash: ArtifactHash,

    /// The status of the artifact.
    ///
    /// This is `Ok(())` if the artifact is present and matches the expected
    /// size and digest, or an error message if it is missing or does not match.
    #[serde(with = "snake_case_result")]
    #[schemars(schema_with = "SnakeCaseResult::<(), String>::json_schema")]
    pub status: Result<(), String>,
}

impl IdOrdItem for ZoneArtifactInventory {
    type Key<'a> = &'a str;
    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
}

impl From<ZoneArtifactInventory> for v1::inventory::ZoneArtifactInventory {
    fn from(value: ZoneArtifactInventory) -> Self {
        Self {
            file_name: value.file_name,
            path: value.path,
            expected_size: value.expected_size,
            expected_hash: value.expected_hash,
            status: value.status,
        }
    }
}

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
