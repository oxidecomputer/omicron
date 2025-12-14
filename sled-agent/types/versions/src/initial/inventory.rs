// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory types for Sled Agent API versions 1-3.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::api::external::{ByteCount, Generation};
use omicron_common::api::internal::shared::external_ip::v1::SourceNatConfig;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_common::disk::{
    DatasetConfig, DatasetName, DiskVariant, M2Slot, OmicronPhysicalDiskConfig,
};
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_common::update::OmicronZoneManifestSource;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::{
    DatasetUuid, InternalZpoolUuid, MupdateOverrideUuid, OmicronZoneUuid,
    PhysicalDiskUuid, SledUuid, ZpoolUuid,
};
use schemars::schema::{Schema, SchemaObject};
use schemars::{JsonSchema, r#gen::SchemaGenerator};
use serde::{Deserialize, Serialize};
// Export these types for convenience -- this way, dependents don't have to
// depend on sled-hardware-types.
pub use sled_hardware_types::{Baseboard, SledCpuFamily};
use strum::EnumIter;
use tufaceous_artifact::ArtifactHash;

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

/// Inventory representation of zone image resolver status and health.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct ZoneImageResolverInventory {
    /// The zone manifest status.
    pub zone_manifest: ZoneManifestInventory,

    /// The mupdate override status.
    pub mupdate_override: MupdateOverrideInventory,
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

impl IdOrdItem for ZoneArtifactInventory {
    type Key<'a> = &'a str;
    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
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

impl IdOrdItem for ZoneManifestNonBootInventory {
    type Key<'a> = InternalZpoolUuid;
    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }
    id_upcast!();
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

impl IdOrdItem for MupdateOverrideNonBootInventory {
    type Key<'a> = InternalZpoolUuid;
    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }
    id_upcast!();
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
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Diffable,
    EnumIter,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
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

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
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

/// Describes what kind of zone this is (i.e., what component is running in it)
/// as well as any type-specific configuration.
///
/// Note: This version does NOT have `lockstep_port` in the Nexus variant.
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
    /// Note: This variant does NOT have `lockstep_port` (added in v4).
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
