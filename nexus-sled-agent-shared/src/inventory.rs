// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory types shared between Nexus and sled-agent.

use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use daft::Diffable;
use id_map::IdMap;
use id_map::IdMappable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use indent_write::fmt::IndentWriter;
use omicron_common::disk::{DatasetKind, DatasetName, M2Slot};
use omicron_common::ledger::Ledgerable;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_common::update::OmicronZoneManifestSource;
use omicron_common::{
    api::{
        external::{ByteCount, Generation},
        internal::shared::{NetworkInterface, SourceNatConfig},
    },
    disk::{DatasetConfig, DiskVariant, OmicronPhysicalDiskConfig},
    update::ArtifactId,
    zpool_name::ZpoolName,
};
use omicron_uuid_kinds::{
    DatasetUuid, InternalZpoolUuid, MupdateUuid, OmicronZoneUuid,
};
use omicron_uuid_kinds::{MupdateOverrideUuid, PhysicalDiskUuid};
use omicron_uuid_kinds::{SledUuid, ZpoolUuid};
use schemars::schema::{Schema, SchemaObject};
use schemars::{JsonSchema, SchemaGenerator};
use serde::{Deserialize, Serialize};
// Export these types for convenience -- this way, dependents don't have to
// depend on sled-hardware-types.
pub use sled_hardware_types::{Baseboard, SledCpuFamily};
use strum::EnumIter;
use tufaceous_artifact::{ArtifactHash, KnownArtifactKind};

/// Identifies information about disks which may be attached to Sleds.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDisk {
    pub identity: omicron_common::disk::DiskIdentity,
    pub variant: DiskVariant,
    pub slot: i64,
    // Today we only have NVMe disks so we embedded the firmware metadata here.
    // In the future we can track firmware metadata in a unique type if we
    // support more than one disk format.
    pub active_firmware_slot: u8,
    pub next_active_firmware_slot: Option<u8>,
    pub number_of_firmware_slots: u8,
    pub slot1_is_read_only: bool,
    pub slot_firmware_versions: Vec<Option<String>>,
}

/// Identifies information about zpools managed by the control plane
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryZpool {
    pub id: ZpoolUuid,
    pub total_size: ByteCount,
}

/// Identifies information about datasets within Oxide-managed zpools
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryDataset {
    /// Although datasets mandated by the control plane will have UUIDs,
    /// datasets can be created (and have been created) without UUIDs.
    pub id: Option<DatasetUuid>,

    /// This name is the full path of the dataset.
    // This is akin to [sled_storage::dataset::DatasetName::full_name],
    // and it's also what you'd see when running "zfs list".
    pub name: String,

    /// The amount of remaining space usable by the dataset (and children)
    /// assuming there is no other activity within the pool.
    pub available: ByteCount,

    /// The amount of space consumed by this dataset and descendents.
    pub used: ByteCount,

    /// The maximum amount of space usable by a dataset and all descendents.
    pub quota: Option<ByteCount>,

    /// The minimum amount of space guaranteed to a dataset and descendents.
    pub reservation: Option<ByteCount>,

    /// The compression algorithm used for this dataset, if any.
    pub compression: String,
}

impl From<illumos_utils::zfs::DatasetProperties> for InventoryDataset {
    fn from(props: illumos_utils::zfs::DatasetProperties) -> Self {
        Self {
            id: props.id,
            name: props.name,
            available: props.avail,
            used: props.used,
            quota: props.quota,
            reservation: props.reservation,
            compression: props.compression,
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
    pub zone_image_resolver: ZoneImageResolverInventory,
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

impl ConfigReconcilerInventory {
    /// Iterate over all running zones as reported by the last reconciliation
    /// result.
    ///
    /// This includes zones that are both present in `last_reconciled_config`
    /// and whose status in `zones` indicates "successfully running".
    pub fn running_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.zones.iter().filter_map(|(zone_id, result)| match result {
            ConfigReconcilerInventoryResult::Ok => {
                self.last_reconciled_config.zones.get(zone_id)
            }
            ConfigReconcilerInventoryResult::Err { .. } => None,
        })
    }

    /// Iterate over all zones contained in the most-recently-reconciled sled
    /// config and report their status as of that reconciliation.
    pub fn reconciled_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (&OmicronZoneConfig, &ConfigReconcilerInventoryResult)>
    {
        // `self.zones` may contain zone IDs that aren't present in
        // `last_reconciled_config` at all, if we failed to _shut down_ zones
        // that are no longer present in the config. We use `filter_map` to
        // strip those out, and only report on the configured zones.
        self.zones.iter().filter_map(|(zone_id, result)| {
            let config = self.last_reconciled_config.zones.get(zone_id)?;
            Some((config, result))
        })
    }

    /// Given a sled config, produce a reconciler result that sled-agent could
    /// have emitted if reconciliation succeeded.
    ///
    /// This method should only be used by tests and dev tools; real code should
    /// look at the actual `last_reconciliation` value from the parent
    /// [`Inventory`].
    pub fn debug_assume_success(config: OmicronSledConfig) -> Self {
        let external_disks = config
            .disks
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let datasets = config
            .datasets
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let zones = config
            .zones
            .iter()
            .map(|z| (z.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let remove_mupdate_override =
            config.remove_mupdate_override.map(|_| {
                RemoveMupdateOverrideInventory {
                    boot_disk_result: Ok(
                        RemoveMupdateOverrideBootSuccessInventory::Removed,
                    ),
                    non_boot_message: "mupdate override successfully removed \
                                   on non-boot disks"
                        .to_owned(),
                }
            });

        Self {
            last_reconciled_config: config,
            external_disks,
            datasets,
            orphaned_datasets: IdOrdMap::new(),
            zones,
            boot_partitions: BootPartitionContents::debug_assume_success(),
            remove_mupdate_override,
        }
    }

    /// Given a sled config, update an existing reconciler result to simulate an
    /// output that sled-agent could have emitted if reconciliation succeeded.
    ///
    /// This method should only be used by tests and dev tools; real code should
    /// look at the actual `last_reconciliation` value from the parent
    /// [`Inventory`].
    pub fn debug_update_assume_success(&mut self, config: OmicronSledConfig) {
        let external_disks = config
            .disks
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let datasets = config
            .datasets
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let zones = config
            .zones
            .iter()
            .map(|z| (z.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let remove_mupdate_override =
            config.remove_mupdate_override.map(|_| {
                RemoveMupdateOverrideInventory {
                    boot_disk_result: Ok(
                        RemoveMupdateOverrideBootSuccessInventory::Removed,
                    ),
                    non_boot_message: "mupdate override successfully removed \
                                   on non-boot disks"
                        .to_owned(),
                }
            });

        self.last_reconciled_config = config;
        self.external_disks = external_disks;
        self.datasets = datasets;
        self.orphaned_datasets = IdOrdMap::new();
        self.zones = zones;
        self.remove_mupdate_override = remove_mupdate_override;
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

impl BootPartitionContents {
    pub fn slot_details(
        &self,
        slot: M2Slot,
    ) -> &Result<BootPartitionDetails, String> {
        match slot {
            M2Slot::A => &self.slot_a,
            M2Slot::B => &self.slot_b,
        }
    }

    pub fn debug_assume_success() -> Self {
        Self {
            boot_disk: Ok(M2Slot::A),
            slot_a: Ok(BootPartitionDetails {
                header: BootImageHeader {
                    flags: 0,
                    data_size: 1000,
                    image_size: 1000,
                    target_size: 1000,
                    sha256: [0; 32],
                    image_name: "fake from debug_assume_success()".to_string(),
                },
                artifact_hash: ArtifactHash([0x0a; 32]),
                artifact_size: 1000,
            }),
            slot_b: Ok(BootPartitionDetails {
                header: BootImageHeader {
                    flags: 0,
                    data_size: 1000,
                    image_size: 1000,
                    target_size: 1000,
                    sha256: [1; 32],
                    image_name: "fake from debug_assume_success()".to_string(),
                },
                artifact_hash: ArtifactHash([0x0b; 32]),
                artifact_size: 1000,
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct BootPartitionDetails {
    pub header: BootImageHeader,
    pub artifact_hash: ArtifactHash,
    pub artifact_size: usize,
}

// There are several other fields in the header that we either parse and discard
// or ignore completely; see https://github.com/oxidecomputer/boot-image-tools
// for more thorough support.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct BootImageHeader {
    pub flags: u64,
    pub data_size: u64,
    pub image_size: u64,
    pub target_size: u64,
    pub sha256: [u8; 32],
    pub image_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct OrphanedDataset {
    pub name: DatasetName,
    pub reason: String,
    pub id: Option<DatasetUuid>,
    pub mounted: bool,
    pub available: ByteCount,
    pub used: ByteCount,
}

impl IdOrdItem for OrphanedDataset {
    type Key<'a> = &'a DatasetName;

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    id_upcast!();
}

/// Status of removing the mupdate override in the inventory.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct RemoveMupdateOverrideInventory {
    /// The result of removing the mupdate override on the boot disk.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<RemoveMupdateOverrideBootSuccessInventory, String>::json_schema"
    )]
    pub boot_disk_result:
        Result<RemoveMupdateOverrideBootSuccessInventory, String>,

    /// What happened on non-boot disks.
    ///
    /// We aren't modeling this out in more detail, because we plan to not try
    /// and keep ledgered data in sync across both disks in the future.
    pub non_boot_message: String,
}

/// Status of removing the mupdate override on the boot disk.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveMupdateOverrideBootSuccessInventory {
    /// The mupdate override was successfully removed.
    Removed,

    /// No mupdate override was found.
    ///
    /// This is considered a success for idempotency reasons.
    NoOverride,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ConfigReconcilerInventoryResult {
    Ok,
    Err { message: String },
}

impl From<Result<(), String>> for ConfigReconcilerInventoryResult {
    fn from(result: Result<(), String>) -> Self {
        match result {
            Ok(()) => Self::Ok,
            Err(message) => Self::Err { message },
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
        config: OmicronSledConfig,
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

/// Inventory representation of zone image resolver status and health.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneImageResolverInventory {
    /// The zone manifest status.
    pub zone_manifest: ZoneManifestInventory,

    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideInventory,
}

impl ZoneImageResolverInventory {
    /// Returns a new, fake inventory for tests.
    pub fn new_fake() -> Self {
        Self {
            zone_manifest: ZoneManifestInventory::new_fake(),
            mupdate_override: MupdateOverrideInventory::new_fake(),
        }
    }

    /// Returns a displayer for this inventory.
    pub fn display(&self) -> ZoneImageResolverInventoryDisplay<'_> {
        ZoneImageResolverInventoryDisplay { inner: self }
    }
}

/// Displayer for a [`ZoneImageResolverInventory`]
pub struct ZoneImageResolverInventoryDisplay<'a> {
    inner: &'a ZoneImageResolverInventory,
}

impl fmt::Display for ZoneImageResolverInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ZoneImageResolverInventory { zone_manifest, mupdate_override } =
            self.inner;

        writeln!(f, "zone manifest:")?;
        let mut indented = IndentWriter::new("    ", f);
        // Use write! rather than writeln! because zone_manifest.display()
        // always produces a newline at the end.
        write!(indented, "{}", zone_manifest.display())?;
        let f = indented.into_inner();

        writeln!(f, "mupdate override:")?;
        let mut indented = IndentWriter::new("    ", f);
        // Use write! rather than writeln! because mupdate_override.display()
        // always produces a newline at the end.
        write!(indented, "{}", mupdate_override.display())?;

        Ok(())
    }
}

/// Inventory representation of a zone manifest.
///
/// Part of [`ZoneImageResolverInventory`].
///
/// A zone manifest is a listing of all the zones present in a system's install
/// dataset. This struct contains information about the install dataset gathered
/// from a system.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneManifestInventory {
    /// The full path to the zone manifest file on the boot disk.
    #[schemars(schema_with = "path_schema")]
    pub boot_disk_path: Utf8PathBuf,

    /// The manifest read from the boot disk, and whether the manifest is valid.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<ZoneManifestBootInventory, String>::json_schema"
    )]
    pub boot_inventory: Result<ZoneManifestBootInventory, String>,

    /// Information about the install dataset on non-boot disks.
    pub non_boot_status: IdOrdMap<ZoneManifestNonBootInventory>,
}

impl ZoneManifestInventory {
    /// Returns a new, empty inventory for tests.
    pub fn new_fake() -> Self {
        Self {
            boot_disk_path: Utf8PathBuf::from("/fake/path/install/zones.json"),
            boot_inventory: Ok(ZoneManifestBootInventory::new_fake()),
            non_boot_status: IdOrdMap::new(),
        }
    }

    /// Returns a displayer for this inventory.
    pub fn display(&self) -> ZoneManifestInventoryDisplay<'_> {
        ZoneManifestInventoryDisplay { inner: self }
    }
}

/// Displayer for a [`ZoneManifestInventory`]
#[derive(Clone, Debug)]
pub struct ZoneManifestInventoryDisplay<'a> {
    inner: &'a ZoneManifestInventory,
}

impl fmt::Display for ZoneManifestInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f;

        let ZoneManifestInventory {
            boot_disk_path,
            boot_inventory,
            non_boot_status,
        } = self.inner;
        writeln!(f, "path on boot disk: {}", boot_disk_path)?;

        match boot_inventory {
            Ok(boot_inventory) => {
                writeln!(f, "boot disk inventory:")?;
                let mut indented = IndentWriter::new("    ", f);
                // Use write! rather than writeln! because
                // boot_inventory.display() always ends with a newline.
                write!(indented, "{}", boot_inventory.display())?;
                f = indented.into_inner();
            }
            Err(error) => {
                writeln!(
                    f,
                    "error obtaining zone manifest on boot disk: {error}"
                )?;
            }
        }

        if non_boot_status.is_empty() {
            writeln!(f, "no non-boot disks")?;
        } else {
            writeln!(f, "non-boot disk status:")?;
            for non_boot in non_boot_status {
                let mut indented = IndentWriter::new_skip_initial("    ", f);
                writeln!(indented, "  - {}", non_boot.display())?;
                f = indented.into_inner();
            }
        }

        Ok(())
    }
}

/// Inventory representation of zone artifacts on the boot disk.
///
/// Part of [`ZoneManifestInventory`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneManifestBootInventory {
    /// The manifest source.
    ///
    /// In production this is [`OmicronZoneManifestSource::Installinator`], but
    /// in some development and testing flows Sled Agent synthesizes zone
    /// manifests. In those cases, the source is
    /// [`OmicronZoneManifestSource::SledAgent`].
    pub source: OmicronZoneManifestSource,

    /// The artifacts on disk.
    pub artifacts: IdOrdMap<ZoneArtifactInventory>,
}

impl ZoneManifestBootInventory {
    /// Returns a new, empty inventory for tests.
    ///
    /// For a more representative selection of real zones, see `representative`
    /// in `nexus-inventory`.
    pub fn new_fake() -> Self {
        Self {
            source: OmicronZoneManifestSource::Installinator {
                mupdate_id: MupdateUuid::nil(),
            },
            artifacts: IdOrdMap::new(),
        }
    }

    /// Returns a displayer for this inventory.
    pub fn display(&self) -> ZoneManifestBootInventoryDisplay<'_> {
        ZoneManifestBootInventoryDisplay { inner: self }
    }
}

/// Displayer for a [`ZoneManifestBootInventory`].
#[derive(Clone, Debug)]
pub struct ZoneManifestBootInventoryDisplay<'a> {
    inner: &'a ZoneManifestBootInventory,
}

impl fmt::Display for ZoneManifestBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f;

        let ZoneManifestBootInventory { source, artifacts } = self.inner;
        writeln!(f, "manifest generated by {}", source)?;
        if artifacts.is_empty() {
            writeln!(
                f,
                "no artifacts in install dataset \
                 (this should only be seen in simulated systems)"
            )?;
        } else {
            writeln!(f, "artifacts in install dataset:")?;

            for artifact in artifacts {
                let mut indented = IndentWriter::new_skip_initial("    ", f);
                writeln!(indented, "  - {}", artifact.display())?;
                f = indented.into_inner();
            }
        }

        Ok(())
    }
}

/// Inventory representation of a single zone artifact on a boot disk.
///
/// Part of [`ZoneManifestBootInventory`].
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

impl ZoneArtifactInventory {
    /// Returns a displayer for this inventory.
    pub fn display(&self) -> ZoneArtifactInventoryDisplay<'_> {
        ZoneArtifactInventoryDisplay { inner: self }
    }
}

impl IdOrdItem for ZoneArtifactInventory {
    type Key<'a> = &'a str;
    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
}

/// Displayer for [`ZoneArtifactInventory`].
#[derive(Clone, Debug)]
pub struct ZoneArtifactInventoryDisplay<'a> {
    inner: &'a ZoneArtifactInventory,
}

impl fmt::Display for ZoneArtifactInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ZoneArtifactInventory {
            file_name,
            // We don't show the path here because surrounding code typically
            // displays the path. We could make this controllable in the future
            // via a method on `ZoneArtifactInventoryDisplay`.
            path: _,
            expected_size,
            expected_hash,
            status,
        } = self.inner;
        write!(
            f,
            "{file_name} (expected {expected_size} bytes \
             with hash {expected_hash}): ",
        )?;
        match status {
            Ok(()) => write!(f, "ok"),
            Err(message) => write!(f, "error: {message}"),
        }
    }
}

/// Inventory representation of a zone manifest on a non-boot disk.
///
/// Unlike [`ZoneManifestBootInventory`] which is structured since
/// Reconfigurator makes decisions based on it, information about non-boot disks
/// is purely advisory. For simplicity, we store information in an unstructured
/// format.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneManifestNonBootInventory {
    /// The ID of the non-boot zpool.
    pub zpool_id: InternalZpoolUuid,

    /// The full path to the zone manifest JSON on the non-boot disk.
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,

    /// Whether the status is valid.
    pub is_valid: bool,

    /// A message describing the status.
    ///
    /// If `is_valid` is true, then the message describes the list of artifacts
    /// found and their hashes.
    ///
    /// If `is_valid` is false, then this message describes the reason for the
    /// invalid status. This could include errors reading the zone manifest, or
    /// zone file mismatches.
    pub message: String,
}

impl ZoneManifestNonBootInventory {
    /// Returns a displayer for this inventory.
    pub fn display(&self) -> ZoneManifestNonBootInventoryDisplay<'_> {
        ZoneManifestNonBootInventoryDisplay { inner: self }
    }
}

impl IdOrdItem for ZoneManifestNonBootInventory {
    type Key<'a> = InternalZpoolUuid;
    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }
    id_upcast!();
}

/// Displayer for a [`ZoneManifestNonBootInventory`].
#[derive(Clone, Debug)]
pub struct ZoneManifestNonBootInventoryDisplay<'a> {
    inner: &'a ZoneManifestNonBootInventory,
}

impl fmt::Display for ZoneManifestNonBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ZoneManifestNonBootInventory {
            // The zpool ID is part of the path, so displaying it is redundant.
            zpool_id: _,
            path,
            is_valid,
            message,
        } = self.inner;
        write!(
            f,
            "{path} ({}): {message}",
            if *is_valid { "valid" } else { "invalid" },
        )
    }
}

/// Inventory representation of MUPdate override status.
///
/// Part of [`ZoneImageResolverInventory`].
///
/// This is used by Reconfigurator to determine if a MUPdate override has
/// occurred. For more about mixing MUPdate and updates, see RFD 556.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct MupdateOverrideInventory {
    /// The full path to the mupdate override JSON on the boot disk.
    #[schemars(schema_with = "path_schema")]
    pub boot_disk_path: Utf8PathBuf,

    /// The boot disk override, or an error if it could not be parsed.
    ///
    /// This is `None` if the override is not present.
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<Option<MupdateOverrideBootInventory>, String>::json_schema"
    )]
    pub boot_override: Result<Option<MupdateOverrideBootInventory>, String>,

    /// Information about the MUPdate override on non-boot disks.
    pub non_boot_status: IdOrdMap<MupdateOverrideNonBootInventory>,
}

impl MupdateOverrideInventory {
    /// Returns a new, empty inventory for tests.
    pub fn new_fake() -> Self {
        Self {
            boot_disk_path: Utf8PathBuf::from(
                "/fake/path/install/mupdate_override.json",
            ),
            boot_override: Ok(None),
            non_boot_status: IdOrdMap::new(),
        }
    }

    /// Returns a displayer for this inventory.
    pub fn display(&self) -> MupdateOverrideInventoryDisplay<'_> {
        MupdateOverrideInventoryDisplay { inner: self }
    }
}

/// A displayer for [`MupdateOverrideInventory`].
#[derive(Clone, Debug)]
pub struct MupdateOverrideInventoryDisplay<'a> {
    inner: &'a MupdateOverrideInventory,
}

impl fmt::Display for MupdateOverrideInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f;

        let MupdateOverrideInventory {
            boot_disk_path,
            boot_override,
            non_boot_status,
        } = self.inner;

        writeln!(f, "path on boot disk: {boot_disk_path}")?;
        match boot_override {
            Ok(Some(boot_override)) => {
                writeln!(
                    f,
                    "override on boot disk: {}",
                    boot_override.display()
                )?;
            }
            Ok(None) => {
                writeln!(f, "no override on boot disk")?;
            }
            Err(error) => {
                writeln!(f, "error obtaining override on boot disk: {error}")?;
            }
        }

        if non_boot_status.is_empty() {
            writeln!(f, "no non-boot disks")?;
        } else {
            writeln!(f, "non-boot disk status:")?;
            for non_boot in non_boot_status {
                let mut indented = IndentWriter::new_skip_initial("    ", f);
                writeln!(indented, "  - {}", non_boot.display())?;
                f = indented.into_inner();
            }
        }

        Ok(())
    }
}

/// Inventory representation of the MUPdate override on the boot disk.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct MupdateOverrideBootInventory {
    /// The ID of the MUPdate override.
    ///
    /// This is unique and generated by Installinator each time it is run.
    /// During a MUPdate, each sled gets a MUPdate override ID. (The ID is
    /// shared across boot disks and non-boot disks, though.)
    pub mupdate_override_id: MupdateOverrideUuid,
}

impl MupdateOverrideBootInventory {
    /// Returns a displayer for this inventory.
    pub fn display(&self) -> MupdateOverrideBootInventoryDisplay<'_> {
        MupdateOverrideBootInventoryDisplay { inner: self }
    }
}

#[derive(Clone, Debug)]
pub struct MupdateOverrideBootInventoryDisplay<'a> {
    inner: &'a MupdateOverrideBootInventory,
}

impl fmt::Display for MupdateOverrideBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MupdateOverrideBootInventory { mupdate_override_id } = self.inner;
        write!(f, "{}", mupdate_override_id)
    }
}

/// Inventory representation of the MUPdate override on a non-boot disk.
///
/// Unlike [`MupdateOverrideBootInventory`] which is structured since
/// Reconfigurator makes decisions based on it, information about non-boot disks
/// is purely advisory. For simplicity, we store information in an unstructured
/// format.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct MupdateOverrideNonBootInventory {
    /// The non-boot zpool ID.
    pub zpool_id: InternalZpoolUuid,

    /// The path to the mupdate override JSON on the non-boot disk.
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,

    /// Whether the status is valid.
    pub is_valid: bool,

    /// A message describing the status.
    ///
    /// If `is_valid` is true, then the message is a short description saying
    /// that it matches the boot disk, and whether the MUPdate override is
    /// present.
    ///
    /// If `is_valid` is false, then this message describes the reason for the
    /// invalid status. This could include errors reading the MUPdate override
    /// JSON, or a mismatch between the boot and non-boot disks.
    pub message: String,
}

impl MupdateOverrideNonBootInventory {
    /// Returns a displayer for this inventory.
    pub fn display(&self) -> MupdateOverrideNonBootInventoryDisplay<'_> {
        MupdateOverrideNonBootInventoryDisplay { inner: self }
    }
}

impl IdOrdItem for MupdateOverrideNonBootInventory {
    type Key<'a> = InternalZpoolUuid;
    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }
    id_upcast!();
}

/// Displayer for a [`MupdateOverrideNonBootInventory`].
#[derive(Clone, Debug)]
pub struct MupdateOverrideNonBootInventoryDisplay<'a> {
    inner: &'a MupdateOverrideNonBootInventory,
}

impl fmt::Display for MupdateOverrideNonBootInventoryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MupdateOverrideNonBootInventory {
            // The zpool ID is part of the path, so displaying it is redundant.
            zpool_id: _,
            path,
            is_valid,
            message,
        } = self.inner;
        write!(
            f,
            "{path} ({}): {message}",
            if *is_valid { "valid" } else { "invalid" },
        )
    }
}

/// Describes the role of the sled within the rack.
///
/// Note that this may change if the sled is physically moved
/// within the rack.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SledRole {
    /// The sled is a general compute sled.
    Gimlet,
    /// The sled is attached to the network switch, and has additional
    /// responsibilities.
    Scrimlet,
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

impl HostPhase2DesiredContents {
    /// The artifact hash described by `self`, if it has one.
    pub fn artifact_hash(&self) -> Option<ArtifactHash> {
        match self {
            Self::CurrentContents => None,
            Self::Artifact { hash } => Some(*hash),
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

impl HostPhase2DesiredSlots {
    /// Return a `HostPhase2DesiredSlots` with both slots set to
    /// [`HostPhase2DesiredContents::CurrentContents`]; i.e., "make no changes
    /// to the current contents of either slot".
    pub const fn current_contents() -> Self {
        Self {
            slot_a: HostPhase2DesiredContents::CurrentContents,
            slot_b: HostPhase2DesiredContents::CurrentContents,
        }
    }
}

/// Describes the set of Reconfigurator-managed configuration elements of a sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct OmicronSledConfig {
    pub generation: Generation,
    pub disks: IdMap<OmicronPhysicalDiskConfig>,
    pub datasets: IdMap<DatasetConfig>,
    pub zones: IdMap<OmicronZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    #[serde(default = "HostPhase2DesiredSlots::current_contents")]
    pub host_phase_2: HostPhase2DesiredSlots,
}

impl Default for OmicronSledConfig {
    fn default() -> Self {
        Self {
            generation: Generation::new(),
            disks: IdMap::default(),
            datasets: IdMap::default(),
            zones: IdMap::default(),
            remove_mupdate_override: None,
            host_phase_2: HostPhase2DesiredSlots::current_contents(),
        }
    }
}

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

impl OmicronZonesConfig {
    /// Generation 1 of `OmicronZonesConfig` is always the set of no zones.
    pub const INITIAL_GENERATION: Generation = Generation::from_u32(1);
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
    #[serde(default = "deserialize_image_source_default")]
    pub image_source: OmicronZoneImageSource,
}

impl IdMappable for OmicronZoneConfig {
    type Id = OmicronZoneUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

impl OmicronZoneConfig {
    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    pub fn underlay_ip(&self) -> Ipv6Addr {
        self.zone_type.underlay_ip()
    }

    pub fn zone_name(&self) -> String {
        illumos_utils::running_zone::InstalledZone::get_zone_name(
            self.zone_type.kind().zone_prefix(),
            Some(self.id),
        )
    }

    pub fn dataset_name(&self) -> Option<DatasetName> {
        self.zone_type.dataset_name()
    }
}

/// Describes a persistent ZFS dataset associated with an Omicron zone
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct OmicronZoneDataset {
    pub pool_name: ZpoolName,
}

/// Describes what kind of zone this is (i.e., what component is running in it)
/// as well as any type-specific configuration
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OmicronZoneType {
    BoundaryNtp {
        address: SocketAddrV6,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
        /// The service vNIC providing outbound connectivity using OPTE.
        nic: NetworkInterface,
        /// The SNAT configuration for outbound connections.
        snat_cfg: SourceNatConfig,
    },

    /// Type of clickhouse zone used for a single node clickhouse deployment
    Clickhouse {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    /// A zone used to run a Clickhouse Keeper node
    ///
    /// Keepers are only used in replicated clickhouse setups
    ClickhouseKeeper {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    /// A zone used to run a Clickhouse Server in a replicated deployment
    ClickhouseServer {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    CockroachDb {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },

    Crucible {
        address: SocketAddrV6,
        dataset: OmicronZoneDataset,
    },
    CruciblePantry {
        address: SocketAddrV6,
    },
    ExternalDns {
        dataset: OmicronZoneDataset,
        /// The address at which the external DNS server API is reachable.
        http_address: SocketAddrV6,
        /// The address at which the external DNS server is reachable.
        dns_address: SocketAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
    },
    InternalDns {
        dataset: OmicronZoneDataset,
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
        /// The addresses in the global zone which should be created
        ///
        /// For the DNS service, which exists outside the sleds's typical subnet
        /// - adding an address in the GZ is necessary to allow inter-zone
        /// traffic routing.
        gz_address: Ipv6Addr,

        /// The address is also identified with an auxiliary bit of information
        /// to ensure that the created global zone address can have a unique
        /// name.
        gz_address_index: u32,
    },
    InternalNtp {
        address: SocketAddrV6,
    },
    Nexus {
        /// The address at which the internal nexus server is reachable.
        internal_address: SocketAddrV6,
        /// The address at which the external nexus server is reachable.
        external_ip: IpAddr,
        /// The service vNIC providing external connectivity using OPTE.
        nic: NetworkInterface,
        /// Whether Nexus's external endpoint should use TLS
        external_tls: bool,
        /// External DNS servers Nexus can use to resolve external hosts.
        external_dns_servers: Vec<IpAddr>,
    },
    Oximeter {
        address: SocketAddrV6,
    },
}

impl OmicronZoneType {
    /// Returns the [`ZoneKind`] corresponding to this variant.
    pub fn kind(&self) -> ZoneKind {
        match self {
            OmicronZoneType::BoundaryNtp { .. } => ZoneKind::BoundaryNtp,
            OmicronZoneType::Clickhouse { .. } => ZoneKind::Clickhouse,
            OmicronZoneType::ClickhouseKeeper { .. } => {
                ZoneKind::ClickhouseKeeper
            }
            OmicronZoneType::ClickhouseServer { .. } => {
                ZoneKind::ClickhouseServer
            }
            OmicronZoneType::CockroachDb { .. } => ZoneKind::CockroachDb,
            OmicronZoneType::Crucible { .. } => ZoneKind::Crucible,
            OmicronZoneType::CruciblePantry { .. } => ZoneKind::CruciblePantry,
            OmicronZoneType::ExternalDns { .. } => ZoneKind::ExternalDns,
            OmicronZoneType::InternalDns { .. } => ZoneKind::InternalDns,
            OmicronZoneType::InternalNtp { .. } => ZoneKind::InternalNtp,
            OmicronZoneType::Nexus { .. } => ZoneKind::Nexus,
            OmicronZoneType::Oximeter { .. } => ZoneKind::Oximeter,
        }
    }

    /// Does this zone require time synchronization before it is initialized?"
    ///
    /// This function is somewhat conservative - the set of services
    /// that can be launched before timesync has completed is intentionally kept
    /// small, since it would be easy to add a service that expects time to be
    /// reasonably synchronized.
    pub fn requires_timesync(&self) -> bool {
        match self {
            // These zones can be initialized and started before time has been
            // synchronized. For the NTP zones, this should be self-evident --
            // we need the NTP zone to actually perform time synchronization!
            //
            // The DNS zone is a bit of an exception here, since the NTP zone
            // itself may rely on DNS lookups as a dependency.
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::InternalDns { .. } => false,
            _ => true,
        }
    }

    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    pub fn underlay_ip(&self) -> Ipv6Addr {
        match self {
            OmicronZoneType::BoundaryNtp { address, .. }
            | OmicronZoneType::Clickhouse { address, .. }
            | OmicronZoneType::ClickhouseKeeper { address, .. }
            | OmicronZoneType::ClickhouseServer { address, .. }
            | OmicronZoneType::CockroachDb { address, .. }
            | OmicronZoneType::Crucible { address, .. }
            | OmicronZoneType::CruciblePantry { address }
            | OmicronZoneType::ExternalDns { http_address: address, .. }
            | OmicronZoneType::InternalNtp { address }
            | OmicronZoneType::Nexus { internal_address: address, .. }
            | OmicronZoneType::Oximeter { address } => *address.ip(),
            OmicronZoneType::InternalDns {
                http_address: address,
                dns_address,
                ..
            } => {
                // InternalDns is the only variant that carries two
                // `SocketAddrV6`s that are both on the underlay network. We
                // expect these to have the same IP address.
                debug_assert_eq!(address.ip(), dns_address.ip());
                *address.ip()
            }
        }
    }

    /// Identifies whether this is an NTP zone
    pub fn is_ntp(&self) -> bool {
        match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. } => true,

            OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::ClickhouseServer { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::ExternalDns { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// Identifies whether this is a boundary NTP zone
    pub fn is_boundary_ntp(&self) -> bool {
        matches!(self, OmicronZoneType::BoundaryNtp { .. })
    }

    /// Identifies whether this is a Nexus zone
    pub fn is_nexus(&self) -> bool {
        match self {
            OmicronZoneType::Nexus { .. } => true,

            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::ClickhouseServer { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::ExternalDns { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// Identifies whether this a Crucible (not Crucible pantry) zone
    pub fn is_crucible(&self) -> bool {
        match self {
            OmicronZoneType::Crucible { .. } => true,

            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::ClickhouseServer { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::ExternalDns { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. } => false,
        }
    }

    /// This zone's external IP
    pub fn external_ip(&self) -> Option<IpAddr> {
        match self {
            OmicronZoneType::Nexus { external_ip, .. } => Some(*external_ip),
            OmicronZoneType::ExternalDns { dns_address, .. } => {
                Some(dns_address.ip())
            }
            OmicronZoneType::BoundaryNtp { snat_cfg, .. } => Some(snat_cfg.ip),

            OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::ClickhouseServer { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Oximeter { .. } => None,
        }
    }

    /// The service vNIC providing external connectivity to this zone
    pub fn service_vnic(&self) -> Option<&NetworkInterface> {
        match self {
            OmicronZoneType::Nexus { nic, .. }
            | OmicronZoneType::ExternalDns { nic, .. }
            | OmicronZoneType::BoundaryNtp { nic, .. } => Some(nic),

            OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Clickhouse { .. }
            | OmicronZoneType::ClickhouseKeeper { .. }
            | OmicronZoneType::ClickhouseServer { .. }
            | OmicronZoneType::CockroachDb { .. }
            | OmicronZoneType::Crucible { .. }
            | OmicronZoneType::CruciblePantry { .. }
            | OmicronZoneType::InternalDns { .. }
            | OmicronZoneType::Oximeter { .. } => None,
        }
    }

    /// If this kind of zone has an associated dataset, return the dataset's
    /// name. Otherwise, return `None`.
    pub fn dataset_name(&self) -> Option<DatasetName> {
        let (dataset, dataset_kind) = match self {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. }
            | OmicronZoneType::CruciblePantry { .. } => None,
            OmicronZoneType::Clickhouse { dataset, .. } => {
                Some((dataset, DatasetKind::Clickhouse))
            }
            OmicronZoneType::ClickhouseKeeper { dataset, .. } => {
                Some((dataset, DatasetKind::ClickhouseKeeper))
            }
            OmicronZoneType::ClickhouseServer { dataset, .. } => {
                Some((dataset, DatasetKind::ClickhouseServer))
            }
            OmicronZoneType::CockroachDb { dataset, .. } => {
                Some((dataset, DatasetKind::Cockroach))
            }
            OmicronZoneType::Crucible { dataset, .. } => {
                Some((dataset, DatasetKind::Crucible))
            }
            OmicronZoneType::ExternalDns { dataset, .. } => {
                Some((dataset, DatasetKind::ExternalDns))
            }
            OmicronZoneType::InternalDns { dataset, .. } => {
                Some((dataset, DatasetKind::InternalDns))
            }
        }?;

        Some(DatasetName::new(dataset.pool_name, dataset_kind))
    }
}

/// Like [`OmicronZoneType`], but without any associated data.
///
/// This enum is meant to correspond exactly 1:1 with `OmicronZoneType`.
///
/// # String representations of this type
///
/// There are no fewer than six string representations for this type, all
/// slightly different from each other.
///
/// 1. [`Self::zone_prefix`]: Used to construct zone names.
/// 2. [`Self::service_prefix`]: Used to construct SMF service names.
/// 3. [`Self::name_prefix`]: Used to construct `Name` instances.
/// 4. [`Self::report_str`]: Used for reporting and testing.
/// 5. [`Self::artifact_id_name`]: Used to match TUF artifact IDs.
/// 6. [`Self::artifact_in_install_dataset`]: Used to match zone image tarballs
///    in the install dataset. (This method is equivalent to appending `.tar.gz`
///    to the result of [`Self::zone_prefix`].)
///
/// There is no `Display` impl to ensure that users explicitly choose the
/// representation they want. (Please play close attention to this! The
/// functions are all similar but different, and we don't currently have great
/// type safety around the choice.)
///
/// ## Adding new representations
///
/// If you have a new use case for a string representation, please reuse one of
/// the six representations if at all possible. If you must add a new one,
/// please add it here rather than doing something ad-hoc in the calling code
/// so it's more legible.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, EnumIter,
)]
pub enum ZoneKind {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    ClickhouseServer,
    CockroachDb,
    Crucible,
    CruciblePantry,
    ExternalDns,
    InternalDns,
    InternalNtp,
    Nexus,
    Oximeter,
}

impl ZoneKind {
    /// The NTP prefix used for both BoundaryNtp and InternalNtp zones and
    /// services.
    pub const NTP_PREFIX: &'static str = "ntp";

    /// Return a string that is used to construct **zone names**. This string
    /// is guaranteed to be stable over time.
    pub fn zone_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string that identifies **zone image filenames** in the install
    /// dataset.
    ///
    /// This method is exactly equivalent to `format!("{}.tar.gz",
    /// self.zone_prefix())`, but returns `&'static str`s. A unit test ensures
    /// they stay consistent.
    pub fn artifact_in_install_dataset(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => "ntp.tar.gz",
            ZoneKind::Clickhouse => "clickhouse.tar.gz",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper.tar.gz",
            ZoneKind::ClickhouseServer => "clickhouse_server.tar.gz",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb.tar.gz",
            ZoneKind::Crucible => "crucible.tar.gz",
            ZoneKind::CruciblePantry => "crucible_pantry.tar.gz",
            ZoneKind::ExternalDns => "external_dns.tar.gz",
            ZoneKind::InternalDns => "internal_dns.tar.gz",
            ZoneKind::Nexus => "nexus.tar.gz",
            ZoneKind::Oximeter => "oximeter.tar.gz",
        }
    }

    /// Return a string that is used to construct **SMF service names**. This
    /// string is guaranteed to be stable over time.
    pub fn service_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp".
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            // Note "cockroachdb" for historical reasons.
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible",
            // Note "crucible/pantry" for historical reasons.
            ZoneKind::CruciblePantry => "crucible/pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string suitable for use **in `Name` instances**. This string
    /// is guaranteed to be stable over time.
    ///
    /// This string uses dashes rather than underscores, as required by `Name`.
    pub fn name_prefix(self) -> &'static str {
        match self {
            // BoundaryNtp and InternalNtp both use "ntp" here.
            ZoneKind::BoundaryNtp | ZoneKind::InternalNtp => Self::NTP_PREFIX,
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse-keeper",
            ZoneKind::ClickhouseServer => "clickhouse-server",
            // Note "cockroach" for historical reasons.
            ZoneKind::CockroachDb => "cockroach",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible-pantry",
            ZoneKind::ExternalDns => "external-dns",
            ZoneKind::InternalDns => "internal-dns",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string that is used for reporting and error messages. This is
    /// **not guaranteed** to be stable.
    ///
    /// If you're displaying a user-friendly message, prefer this method.
    pub fn report_str(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp => "boundary_ntp",
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            ZoneKind::CockroachDb => "cockroach_db",
            ZoneKind::Crucible => "crucible",
            ZoneKind::CruciblePantry => "crucible_pantry",
            ZoneKind::ExternalDns => "external_dns",
            ZoneKind::InternalDns => "internal_dns",
            ZoneKind::InternalNtp => "internal_ntp",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Return a string used as an artifact name for control-plane zones.
    /// This is **not guaranteed** to be stable.
    ///
    /// These strings match the `ArtifactId::name`s Nexus constructs when
    /// unpacking the composite control-plane artifact in a TUF repo. Currently,
    /// these are chosen by reading the `pkg` value of the `oxide.json` object
    /// inside each zone image tarball.
    pub fn artifact_id_name(self) -> &'static str {
        match self {
            ZoneKind::BoundaryNtp => "ntp",
            ZoneKind::Clickhouse => "clickhouse",
            ZoneKind::ClickhouseKeeper => "clickhouse_keeper",
            ZoneKind::ClickhouseServer => "clickhouse_server",
            ZoneKind::CockroachDb => "cockroachdb",
            ZoneKind::Crucible => "crucible-zone",
            ZoneKind::CruciblePantry => "crucible-pantry-zone",
            ZoneKind::ExternalDns => "external-dns",
            ZoneKind::InternalDns => "internal-dns",
            ZoneKind::InternalNtp => "ntp",
            ZoneKind::Nexus => "nexus",
            ZoneKind::Oximeter => "oximeter",
        }
    }

    /// Map an artifact ID name to the corresponding file name in the install
    /// dataset.
    ///
    /// We don't allow mapping artifact ID names to `ZoneKind` because the map
    /// isn't bijective -- both internal and boundary NTP zones use the same
    /// `ntp` artifact. But the artifact ID name and the name in the install
    /// dataset do form a bijective map.
    pub fn artifact_id_name_to_install_dataset_file(
        artifact_id_name: &str,
    ) -> Option<&'static str> {
        let zone_kind = match artifact_id_name {
            // We arbitrarily select BoundaryNtp to perform the mapping with.
            "ntp" => ZoneKind::BoundaryNtp,
            "clickhouse" => ZoneKind::Clickhouse,
            "clickhouse_keeper" => ZoneKind::ClickhouseKeeper,
            "clickhouse_server" => ZoneKind::ClickhouseServer,
            "cockroachdb" => ZoneKind::CockroachDb,
            "crucible-zone" => ZoneKind::Crucible,
            "crucible-pantry-zone" => ZoneKind::CruciblePantry,
            "external-dns" => ZoneKind::ExternalDns,
            "internal-dns" => ZoneKind::InternalDns,
            "nexus" => ZoneKind::Nexus,
            "oximeter" => ZoneKind::Oximeter,
            _ => return None,
        };

        Some(zone_kind.artifact_in_install_dataset())
    }

    /// Return true if an artifact represents a control plane zone image
    /// of this kind.
    pub fn is_control_plane_zone_artifact(
        self,
        artifact_id: &ArtifactId,
    ) -> bool {
        artifact_id
            .kind
            .to_known()
            .map(|kind| matches!(kind, KnownArtifactKind::Zone))
            .unwrap_or(false)
            && artifact_id.name == self.artifact_id_name()
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
    /// Return the artifact hash used for the zone image, if the zone's image
    /// source is from the artifact store.
    pub fn artifact_hash(&self) -> Option<ArtifactHash> {
        if let OmicronZoneImageSource::Artifact { hash } = self {
            Some(*hash)
        } else {
            None
        }
    }
}

// See `OmicronZoneConfig`. This is a separate function instead of being `impl
// Default` because we don't want to accidentally use this default in Rust code.
fn deserialize_image_source_default() -> OmicronZoneImageSource {
    OmicronZoneImageSource::InstallDataset
}

#[cfg(test)]
mod tests {
    use omicron_common::api::external::Name;
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_name_prefixes() {
        for zone_kind in ZoneKind::iter() {
            let name_prefix = zone_kind.name_prefix();
            name_prefix.parse::<Name>().unwrap_or_else(|e| {
                panic!(
                    "failed to parse name prefix {:?} for zone kind {:?}: {}",
                    name_prefix, zone_kind, e
                );
            });
        }
    }

    #[test]
    fn test_zone_prefix_matches_artifact_in_install_dataset() {
        for zone_kind in ZoneKind::iter() {
            let zone_prefix = zone_kind.zone_prefix();
            let expected_artifact = format!("{zone_prefix}.tar.gz");
            assert_eq!(
                expected_artifact,
                zone_kind.artifact_in_install_dataset()
            );
        }
    }

    #[test]
    fn test_artifact_id_to_install_dataset_file() {
        for zone_kind in ZoneKind::iter() {
            let artifact_id_name = zone_kind.artifact_id_name();
            let expected_file = zone_kind.artifact_in_install_dataset();
            assert_eq!(
                Some(expected_file),
                ZoneKind::artifact_id_name_to_install_dataset_file(
                    artifact_id_name
                )
            );
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
