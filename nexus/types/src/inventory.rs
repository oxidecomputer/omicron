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
use crate::external_api::shared::Baseboard;
use chrono::DateTime;
use chrono::Utc;
pub use gateway_client::types::PowerState;
pub use gateway_client::types::RotImageError;
pub use gateway_client::types::RotSlot;
pub use gateway_client::types::SpType;
use omicron_common::api::external::ByteCount;
pub use omicron_common::api::internal::shared::NetworkInterface;
pub use omicron_common::api::internal::shared::NetworkInterfaceKind;
pub use omicron_common::api::internal::shared::SourceNatConfig;
pub use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
pub use sled_agent_client::types::OmicronZoneConfig;
pub use sled_agent_client::types::OmicronZoneDataset;
pub use sled_agent_client::types::OmicronZoneType;
pub use sled_agent_client::types::OmicronZonesConfig;
pub use sled_agent_client::types::SledRole;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::sync::Arc;
use strum::EnumIter;

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
    pub sled_agents: BTreeMap<SledUuid, SledAgent>,

    /// Omicron zones found, by *sled* id
    pub omicron_zones: BTreeMap<SledUuid, OmicronZonesFound>,
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

    /// Iterate over all the Omicron zones in the collection
    pub fn all_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.omicron_zones.values().flat_map(|z| z.zones.zones.iter())
    }

    /// Iterate over the sled ids of sleds identified as Scrimlets
    pub fn scrimlets(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.sled_agents
            .iter()
            .filter(|(_, inventory)| inventory.sled_role == SledRole::Scrimlet)
            .map(|(sled_id, _)| *sled_id)
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
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub struct BaseboardId {
    /// Oxide Part Number
    pub part_number: String,
    /// Serial number (unique for a given part number)
    pub serial_number: String,
}

impl From<Baseboard> for BaseboardId {
    fn from(value: Baseboard) -> Self {
        BaseboardId { part_number: value.part, serial_number: value.serial }
    }
}

impl From<UninitializedSledId> for BaseboardId {
    fn from(value: UninitializedSledId) -> Self {
        BaseboardId { part_number: value.part, serial_number: value.serial }
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
}

impl From<gateway_client::types::SpComponentCaboose> for Caboose {
    fn from(c: gateway_client::types::SpComponentCaboose) -> Self {
        Caboose {
            board: c.board,
            git_commit: c.git_commit,
            name: c.name,
            version: c.version,
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

/// A physical disk reported by a sled agent.
///
/// This identifies that a physical disk appears in a Sled.
/// The existence of this object does not necessarily imply that
/// the disk is being actively managed by the control plane.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct PhysicalDisk {
    pub identity: omicron_common::disk::DiskIdentity,
    pub variant: PhysicalDiskKind,
    pub slot: i64,
}

impl From<sled_agent_client::types::InventoryDisk> for PhysicalDisk {
    fn from(disk: sled_agent_client::types::InventoryDisk) -> PhysicalDisk {
        PhysicalDisk {
            identity: disk.identity,
            variant: disk.variant.into(),
            slot: disk.slot,
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
    pub fn new(
        time_collected: DateTime<Utc>,
        pool: sled_agent_client::types::InventoryZpool,
    ) -> Zpool {
        Zpool { time_collected, id: pool.id, total_size: pool.total_size }
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
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OmicronZonesFound {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub sled_id: SledUuid,
    pub zones: OmicronZonesConfig,
}
