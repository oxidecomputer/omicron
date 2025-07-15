// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing collection of hardware/software inventory
//!
//! This lives in nexus/types because it's used by both nexus/db-model and
//! nexus/inventory.  (It could as well just live in nexus/db-model, but
//! nexus/inventory does not currently know about nexus/db-model and it's
//! convenient to separate these concerns.)

use crate::external_api::params::PhysicalDiskKind;
use crate::external_api::params::UninitializedSledId;
use chrono::DateTime;
use chrono::Utc;
use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
use daft::Diffable;
pub use gateway_client::types::PowerState;
pub use gateway_client::types::RotImageError;
pub use gateway_client::types::SpType;
pub use gateway_types::rot::RotSlot;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use omicron_common::api::external::ByteCount;
pub use omicron_common::api::internal::shared::NetworkInterface;
pub use omicron_common::api::internal::shared::NetworkInterfaceKind;
pub use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::disk::M2Slot;
pub use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::sync::Arc;
use strum::EnumIter;
use tufaceous_artifact::ArtifactHash;

/// Results of collecting hardware/software inventory from various Omicron
/// components
///
/// This type is structured so that it's both easy to collect and easy to insert
/// into the database.  This means items that are represented with separate
/// database tables (like service processors and roots of trust) are represented
/// with separate records, even though they might come from the same source
/// (in this case, a single MGS request).
///
/// We make heavy use of maps, sets, and Arcs here because some of these objects
/// are pointed-to by many other objects in the same Collection.  This approach
/// ensures clear ownership.  It also reflects how things will wind up in the
/// database.
///
/// See the documentation in the database schema for more background.
#[serde_as]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Collection {
    /// unique identifier for this collection
    pub id: CollectionUuid,
    /// errors encountered during collection
    pub errors: Vec<String>,
    /// time the collection started
    pub time_started: DateTime<Utc>,
    /// time the collection eneded
    pub time_done: DateTime<Utc>,
    /// name of the agent doing the collecting (generally, this Nexus's uuid)
    pub collector: String,

    /// unique baseboard ids that were found in this collection
    ///
    /// In practice, these will be inserted into the `hw_baseboard_id` table.
    pub baseboards: BTreeSet<Arc<BaseboardId>>,
    /// unique caboose contents that were found in this collection
    ///
    /// In practice, these will be inserted into the `sw_caboose` table.
    pub cabooses: BTreeSet<Arc<Caboose>>,
    /// unique root of trust page contents that were found in this collection
    ///
    /// In practice, these will be inserted into the `sw_root_of_trust_page`
    /// table.
    pub rot_pages: BTreeSet<Arc<RotPage>>,

    /// all service processors, keyed by baseboard id
    ///
    /// In practice, these will be inserted into the `inv_service_processor`
    /// table.
    #[serde_as(as = "Vec<(_, _)>")]
    pub sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
    /// all host phase 1 flash hashes, keyed first by the phase 1 slot, then the
    /// baseboard id of the sled where they were found
    ///
    /// In practice, these will be inserted into the
    /// `inv_host_phase_1_flash_hash` table.
    #[serde_as(as = "BTreeMap<_, Vec<(_, _)>>")]
    pub host_phase_1_flash_hashes:
        BTreeMap<M2Slot, BTreeMap<Arc<BaseboardId>, HostPhase1FlashHash>>,
    /// all roots of trust, keyed by baseboard id
    ///
    /// In practice, these will be inserted into the `inv_root_of_trust` table.
    #[serde_as(as = "Vec<(_, _)>")]
    pub rots: BTreeMap<Arc<BaseboardId>, RotState>,
    /// all caboose contents found, keyed first by the kind of caboose
    /// (`CabooseWhich`), then the baseboard id of the sled where they were
    /// found
    ///
    /// In practice, these will be inserted into the `inv_caboose` table.
    #[serde_as(as = "BTreeMap<_, Vec<(_, _)>>")]
    pub cabooses_found:
        BTreeMap<CabooseWhich, BTreeMap<Arc<BaseboardId>, CabooseFound>>,
    /// all root of trust page contents found, keyed first by the kind of page
    /// (`RotPageWhich`), then the baseboard id of the sled where they were
    /// found
    ///
    /// In practice, these will be inserted into the `inv_root_of_trust_page`
    /// table.
    #[serde_as(as = "BTreeMap<_, Vec<(_, _)>>")]
    pub rot_pages_found:
        BTreeMap<RotPageWhich, BTreeMap<Arc<BaseboardId>, RotPageFound>>,

    /// Sled Agent information, by *sled* id
    pub sled_agents: IdOrdMap<SledAgent>,

    /// The raft configuration (cluster membership) of the clickhouse keeper
    /// cluster as returned from each available keeper via `clickhouse-admin` in
    /// the `ClickhouseKeeper` zone
    ///
    /// Each clickhouse keeper is uniquely identified by its `KeeperId`
    /// and deployed to a separate omicron zone. The uniqueness of IDs and
    /// deployments is guaranteed by the reconfigurator. DNS is used to find
    /// `clickhouse-admin-keeper` servers running in the same zone as keepers
    /// and retrieve their local knowledge of the raft cluster. Each keeper
    /// reports its own unique ID along with its membership information. We use
    /// this information to decide upon the most up to date state (which will
    /// eventually be reflected to other keepers), so that we can choose how to
    /// reconfigure our keeper cluster if needed.
    ///
    /// All this data is directly reported from the `clickhouse-keeper-admin`
    /// servers in this format. While we could also cache the zone ID
    /// in the `ClickhouseKeeper` zones, return that along with the
    /// `ClickhouseKeeperClusterMembership`,  and map by zone ID here, the
    /// information would be superfluous. It would be filtered out by the
    /// reconfigurator planner downstream. It is not necessary for the planners
    /// to use this since the blueprints already contain the zone ID/ KeeperId
    /// mappings and guarantee unique pairs.
    pub clickhouse_keeper_cluster_membership:
        BTreeSet<ClickhouseKeeperClusterMembership>,

    /// The status of our cockroachdb cluster, keyed by node identifier
    pub cockroach_status:
        BTreeMap<omicron_cockroach_metrics::NodeId, CockroachStatus>,
}

impl Collection {
    pub fn caboose_for(
        &self,
        which: CabooseWhich,
        baseboard_id: &BaseboardId,
    ) -> Option<&CabooseFound> {
        self.cabooses_found
            .get(&which)
            .and_then(|by_bb| by_bb.get(baseboard_id))
    }

    pub fn rot_page_for(
        &self,
        which: RotPageWhich,
        baseboard_id: &BaseboardId,
    ) -> Option<&RotPageFound> {
        self.rot_pages_found
            .get(&which)
            .and_then(|by_bb| by_bb.get(baseboard_id))
    }

    /// Iterate over all the Omicron zones in the sled-agent ledgers of this
    /// collection
    pub fn all_ledgered_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.sled_agents
            .iter()
            .filter_map(|sa| sa.ledgered_sled_config.as_ref())
            .flat_map(|config| config.zones.iter())
    }

    /// Iterate over all the successfully-started Omicron zones (as reported by
    /// each sled-agent's last reconciliation attempt)
    pub fn all_running_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.sled_agents
            .iter()
            .filter_map(|sa| sa.last_reconciliation.as_ref())
            .flat_map(|reconciliation| reconciliation.running_omicron_zones())
    }

    /// Iterate over all the Omicron zones along with their statuses (as
    /// reported by each sled-agent's last reconciliation attempt)
    pub fn all_reconciled_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (&OmicronZoneConfig, &ConfigReconcilerInventoryResult)>
    {
        self.sled_agents
            .iter()
            .filter_map(|sa| sa.last_reconciliation.as_ref())
            .flat_map(|reconciliation| {
                reconciliation.reconciled_omicron_zones()
            })
    }

    /// Iterate over the sled ids of sleds identified as Scrimlets
    pub fn scrimlets(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.sled_agents.iter().filter_map(|sa| {
            (sa.sled_role == SledRole::Scrimlet).then_some(sa.sled_id)
        })
    }

    /// Return the latest clickhouse keeper configuration in this collection, if
    /// there is one.
    pub fn latest_clickhouse_keeper_membership(
        &self,
    ) -> Option<ClickhouseKeeperClusterMembership> {
        self.clickhouse_keeper_cluster_membership
            .iter()
            .max_by_key(|membership| membership.leader_committed_log_index)
            .map(|membership| (membership.clone()))
    }
}

/// A unique baseboard id found during a collection
///
/// Baseboard ids are the keys used to link up information from disparate
/// sources (like a service processor and a sled agent).
///
/// These are normalized in the database.  Each distinct baseboard id is
/// assigned a uuid and shared across the many possible collections that
/// reference it.
///
/// Usually, the part number and serial number are combined with a revision
/// number.  We do not include that here.  If we ever did find a baseboard with
/// the same part number and serial number but a new revision number, we'd want
/// to treat that as the same baseboard as one with a different revision number.
#[derive(
    Clone,
    Debug,
    Diffable,
    Ord,
    Eq,
    Hash,
    PartialOrd,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct BaseboardId {
    /// Oxide Part Number
    pub part_number: String,
    /// Serial number (unique for a given part number)
    pub serial_number: String,
}

impl std::fmt::Display for BaseboardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.part_number, self.serial_number)
    }
}

impl From<crate::external_api::shared::Baseboard> for BaseboardId {
    fn from(value: crate::external_api::shared::Baseboard) -> Self {
        BaseboardId { part_number: value.part, serial_number: value.serial }
    }
}

impl From<UninitializedSledId> for BaseboardId {
    fn from(value: UninitializedSledId) -> Self {
        BaseboardId { part_number: value.part, serial_number: value.serial }
    }
}

impl slog::KV for BaseboardId {
    fn serialize(
        &self,
        _record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str("part_number".into(), &self.part_number)?;
        serializer.emit_str("serial_number".into(), &self.serial_number)
    }
}

/// Caboose contents found during a collection
///
/// These are normalized in the database.  Each distinct `Caboose` is assigned a
/// uuid and shared across many possible collections that reference it.
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct Caboose {
    pub board: String,
    pub git_commit: String,
    pub name: String,
    pub version: String,
    // The sign will generally be present for production RoT and RoT bootloader images.
    // It's currently absent from SP images and could be absent from RoT images as well.
    pub sign: Option<String>,
}

impl From<gateway_client::types::SpComponentCaboose> for Caboose {
    fn from(c: gateway_client::types::SpComponentCaboose) -> Self {
        Caboose {
            board: c.board,
            git_commit: c.git_commit,
            name: c.name,
            version: c.version,
            sign: c.sign,
        }
    }
}

/// Indicates that a particular `Caboose` was found (at a particular time from a
/// particular source, but these are only for debugging)
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct CabooseFound {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub caboose: Arc<Caboose>,
}

/// Describes a service processor found during collection
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct ServiceProcessor {
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub sp_type: SpType,
    pub sp_slot: u16,

    pub baseboard_revision: u32,
    pub hubris_archive: String,
    pub power_state: PowerState,
}

/// Describes the root of trust state found (from a service processor) during
/// collection
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct RotState {
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub active_slot: RotSlot,
    pub persistent_boot_preference: RotSlot,
    pub pending_persistent_boot_preference: Option<RotSlot>,
    pub transient_boot_preference: Option<RotSlot>,
    pub slot_a_sha3_256_digest: Option<String>,
    pub slot_b_sha3_256_digest: Option<String>,
    pub stage0_digest: Option<String>,
    pub stage0next_digest: Option<String>,

    pub slot_a_error: Option<RotImageError>,
    pub slot_b_error: Option<RotImageError>,
    pub stage0_error: Option<RotImageError>,
    pub stage0next_error: Option<RotImageError>,
}

/// Describes a host phase 1 flash hash found from a service processor
/// during collection
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct HostPhase1FlashHash {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub slot: M2Slot,
    pub hash: ArtifactHash,
}

/// Describes which caboose this is (which component, which slot)
#[derive(
    Clone,
    Copy,
    Debug,
    EnumIter,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
)]
pub enum CabooseWhich {
    SpSlot0,
    SpSlot1,
    RotSlotA,
    RotSlotB,
    Stage0,
    Stage0Next,
}

/// Root of trust page contents found during a collection
///
/// These are normalized in the database.  Each distinct `RotPage` is assigned a
/// uuid and shared across many possible collections that reference it.
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct RotPage {
    pub data_base64: String,
}

/// Indicates that a particular `RotPage` was found (at a particular time from a
/// particular source, but these are only for debugging)
#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct RotPageFound {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub page: Arc<RotPage>,
}

/// Describes which root of trust page this is
#[derive(
    Clone,
    Copy,
    Debug,
    EnumIter,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
)]
pub enum RotPageWhich {
    Cmpa,
    CfpaActive,
    CfpaInactive,
    CfpaScratch,
}

/// Trait to convert between the two MGS root of trust page types and a tuple of
/// `([RotPageWhich], [RotPage])`.
///
/// This cannot use the standard `From` trait due to orphan rules: we do not own
/// the `gateway_client` type, and tuples are always considered foreign.
pub trait IntoRotPage {
    fn into_rot_page(self) -> (RotPageWhich, RotPage);
}

impl IntoRotPage for gateway_client::types::RotCmpa {
    fn into_rot_page(self) -> (RotPageWhich, RotPage) {
        (RotPageWhich::Cmpa, RotPage { data_base64: self.base64_data })
    }
}

impl IntoRotPage for gateway_client::types::RotCfpa {
    fn into_rot_page(self) -> (RotPageWhich, RotPage) {
        use gateway_client::types::RotCfpaSlot;
        let which = match self.slot {
            RotCfpaSlot::Active => RotPageWhich::CfpaActive,
            RotCfpaSlot::Inactive => RotPageWhich::CfpaInactive,
            RotCfpaSlot::Scratch => RotPageWhich::CfpaScratch,
        };
        (which, RotPage { data_base64: self.base64_data })
    }
}

/// Firmware reported for a physical NVMe disk.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct NvmeFirmware {
    pub active_slot: u8,
    pub next_active_slot: Option<u8>,
    pub number_of_slots: u8,
    pub slot1_is_read_only: bool,
    pub slot_firmware_versions: Vec<Option<String>>,
}

/// Firmware reported by sled agent for a particular disk format.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum PhysicalDiskFirmware {
    Unknown,
    Nvme(NvmeFirmware),
}

/// A physical disk reported by a sled agent.
///
/// This identifies that a physical disk appears in a Sled.
/// The existence of this object does not necessarily imply that
/// the disk is being actively managed by the control plane.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct PhysicalDisk {
    // XXX: Should this just be InventoryDisk? Do we need a separation between
    // InventoryDisk and PhysicalDisk? The types are structurally the same, but
    // maybe the separation is useful to indicate that a `PhysicalDisk` doesn't
    // always show up in the inventory.
    pub identity: omicron_common::disk::DiskIdentity,
    pub variant: PhysicalDiskKind,
    pub slot: i64,
    pub firmware: PhysicalDiskFirmware,
}

impl From<InventoryDisk> for PhysicalDisk {
    fn from(disk: InventoryDisk) -> PhysicalDisk {
        PhysicalDisk {
            identity: disk.identity,
            variant: disk.variant.into(),
            slot: disk.slot,
            firmware: PhysicalDiskFirmware::Nvme(NvmeFirmware {
                active_slot: disk.active_firmware_slot,
                next_active_slot: disk.next_active_firmware_slot,
                number_of_slots: disk.number_of_firmware_slots,
                slot1_is_read_only: disk.slot1_is_read_only,
                slot_firmware_versions: disk.slot_firmware_versions,
            }),
        }
    }
}

/// A zpool reported by a sled agent.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Zpool {
    pub time_collected: DateTime<Utc>,
    pub id: ZpoolUuid,
    pub total_size: ByteCount,
}

impl Zpool {
    pub fn new(time_collected: DateTime<Utc>, pool: InventoryZpool) -> Zpool {
        Zpool { time_collected, id: pool.id, total_size: pool.total_size }
    }
}

/// A dataset reported by a sled agent.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Dataset {
    /// Although datasets mandated by the control plane will have UUIDs,
    /// datasets can be created (and have been created) without UUIDs.
    pub id: Option<DatasetUuid>,

    /// This name is the full path of the dataset.
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

// TODO: Rather than converting, I think these types can be de-duplicated
impl From<InventoryDataset> for Dataset {
    fn from(disk: InventoryDataset) -> Self {
        Self {
            id: disk.id,
            name: disk.name,
            available: disk.available,
            used: disk.used,
            quota: disk.quota,
            reservation: disk.reservation,
            compression: disk.compression,
        }
    }
}

/// Inventory reported by sled agent
///
/// This is a software notion of a sled, distinct from an underlying baseboard.
/// A sled may be on a PC (in dev/test environments) and have no associated
/// baseboard.  There might also be baseboards with no associated sled (if
/// they have not been formally added to the control plane).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct SledAgent {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub sled_id: SledUuid,
    pub baseboard_id: Option<Arc<BaseboardId>>,
    pub sled_agent_address: SocketAddrV6,
    pub sled_role: SledRole,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,
    pub disks: Vec<PhysicalDisk>,
    pub zpools: Vec<Zpool>,
    pub datasets: Vec<Dataset>,
    pub ledgered_sled_config: Option<OmicronSledConfig>,
    pub reconciler_status: ConfigReconcilerInventoryStatus,
    pub last_reconciliation: Option<ConfigReconcilerInventory>,
    pub zone_image_resolver: ZoneImageResolverInventory,
}

impl IdOrdItem for SledAgent {
    type Key<'a> = SledUuid;
    fn key(&self) -> Self::Key<'_> {
        self.sled_id
    }
    id_upcast!();
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct CockroachStatus {
    pub ranges_underreplicated: Option<u64>,
    pub liveness_live_nodes: Option<u64>,
}
