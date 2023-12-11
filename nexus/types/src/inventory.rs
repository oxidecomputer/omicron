// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing collection of hardware/software inventory
//!
//! This lives in nexus/types because it's used by both nexus/db-model and
//! nexus/inventory.  (It could as well just live in nexus/db-model, but
//! nexus/inventory does not currently know about nexus/db-model and it's
//! convenient to separate these concerns.)

use chrono::DateTime;
use chrono::Utc;
pub use gateway_client::types::PowerState;
pub use gateway_client::types::RotSlot;
pub use gateway_client::types::SpType;
pub use sled_agent_client::types::OmicronZonesConfig;
pub use sled_agent_client::types::SledRole;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use strum::EnumIter;
use uuid::Uuid;

use crate::external_api::shared::Baseboard;
use omicron_common::api::external::ByteCount;
use std::net::SocketAddrV6;

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
#[derive(Debug, Eq, PartialEq)]
pub struct Collection {
    /// unique identifier for this collection
    pub id: Uuid,
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
    pub sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
    /// all roots of trust, keyed by baseboard id
    ///
    /// In practice, these will be inserted into the `inv_root_of_trust` table.
    pub rots: BTreeMap<Arc<BaseboardId>, RotState>,
    /// all caboose contents found, keyed first by the kind of caboose
    /// (`CabooseWhich`), then the baseboard id of the sled where they were
    /// found
    ///
    /// In practice, these will be inserted into the `inv_caboose` table.
    pub cabooses_found:
        BTreeMap<CabooseWhich, BTreeMap<Arc<BaseboardId>, CabooseFound>>,
    /// all root of trust page contents found, keyed first by the kind of page
    /// (`RotPageWhich`), then the baseboard id of the sled where they were
    /// found
    ///
    /// In practice, these will be inserted into the `inv_root_of_trust_page`
    /// table.
    pub rot_pages_found:
        BTreeMap<RotPageWhich, BTreeMap<Arc<BaseboardId>, RotPageFound>>,

    /// Sleds, by *sled* id
    pub sleds: BTreeMap<Uuid, Sled>,

    /// Omicron zones found, by *sled* id
    pub omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,
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
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
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

/// Caboose contents found during a collection
///
/// These are normalized in the database.  Each distinct `Caboose` is assigned a
/// uuid and shared across many possible collections that reference it.
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
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
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct CabooseFound {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub caboose: Arc<Caboose>,
}

/// Describes a service processor found during collection
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
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
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct RotState {
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub active_slot: RotSlot,
    pub persistent_boot_preference: RotSlot,
    pub pending_persistent_boot_preference: Option<RotSlot>,
    pub transient_boot_preference: Option<RotSlot>,
    pub slot_a_sha3_256_digest: Option<String>,
    pub slot_b_sha3_256_digest: Option<String>,
}

/// Describes which caboose this is (which component, which slot)
#[derive(Clone, Copy, Debug, EnumIter, PartialEq, Eq, PartialOrd, Ord)]
pub enum CabooseWhich {
    SpSlot0,
    SpSlot1,
    RotSlotA,
    RotSlotB,
}

/// Root of trust page contents found during a collection
///
/// These are normalized in the database.  Each distinct `RotPage` is assigned a
/// uuid and shared across many possible collections that reference it.
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct RotPage {
    pub data_base64: String,
}

/// Indicates that a particular `RotPage` was found (at a particular time from a
/// particular source, but these are only for debugging)
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct RotPageFound {
    pub time_collected: DateTime<Utc>,
    pub source: String,
    pub page: Arc<RotPage>,
}

/// Describes which root of trust page this is
#[derive(Clone, Copy, Debug, EnumIter, PartialEq, Eq, PartialOrd, Ord)]
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

/// Describes a sled that's part of the control plane
///
/// This is a software notion of a sled, distinct from an underlying baseboard.
/// A sled may be on a PC (in dev/test environments) and have no associated
/// baseboard.  There might also be baseboards with no associated sled (if
/// they have not been formally added to the control plane).
// XXX-dap call this OmicronSled?
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sled {
    pub source: String,
    pub sled_agent_address: SocketAddrV6,
    pub role: SledRole,
    pub baseboard: Option<Arc<BaseboardId>>,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,
}
