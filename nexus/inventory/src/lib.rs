// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron component inventory
//! XXX-dap TODO-doc
//!
//! This is currently inside Nexus, but it's expected to have few dependencies
//! on parts of Nexus (beyond the database crates) and could conceivably be put
//! into other components.

pub use collector::Collector;

use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use gateway_client::types::PowerState;
use gateway_client::types::RotSlot;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

mod builder;
mod collector;

/// Results of collecting inventory from various Omicron components
#[derive(Debug)]
pub struct Collection {
    /// errors encountered during collection
    pub errors: Vec<anyhow::Error>,
    /// time the collection started
    pub time_started: DateTime<Utc>,
    /// time the collection eneded
    pub time_done: DateTime<Utc>,
    /// name of the agent doing the collecting (generally, this Nexus's uuid)
    pub creator: String,
    /// reason for triggering this collection
    pub comment: String,

    pub baseboards: BTreeSet<Arc<BaseboardId>>,
    pub cabooses: BTreeSet<Arc<Caboose>>,
    pub sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
}

#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct BaseboardId {
    pub serial_number: String,
    pub part_number: String,
}

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
            // The MGS API uses an `Option` here because old SP versions did not
            // supply it.  But modern SP versions do.  So we should never hit
            // this `unwrap_or()`.
            version: c.version.unwrap_or(String::from("<unspecified>")),
        }
    }
}

#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct ServiceProcessor {
    pub baseboard: Arc<BaseboardId>,
    pub time_collected: DateTime<Utc>,
    pub source: String,

    pub hubris_archive: String,
    pub power_state: PowerState,
    pub rot: Option<RotState>,

    pub sp_slot0_caboose: Option<Arc<Caboose>>,
    pub sp_slot1_caboose: Option<Arc<Caboose>>,
    pub rot_slot_a_caboose: Option<Arc<Caboose>>,
    pub rot_slot_b_caboose: Option<Arc<Caboose>>,
}

#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct RotState {
    pub active_slot: RotSlot,
    pub persistent_boot_preference: RotSlot,
    pub pending_persistent_boot_preference: Option<RotSlot>,
    pub transient_boot_preference: Option<RotSlot>,
    pub slot_a_sha3_256_digest: Option<String>,
    pub slot_b_sha3_256_digest: Option<String>,
}

impl TryFrom<gateway_client::types::RotState> for RotState {
    type Error = anyhow::Error;
    fn try_from(
        value: gateway_client::types::RotState,
    ) -> Result<Self, Self::Error> {
        match value {
            gateway_client::types::RotState::Enabled {
                active,
                pending_persistent_boot_preference,
                persistent_boot_preference,
                slot_a_sha3_256_digest,
                slot_b_sha3_256_digest,
                transient_boot_preference,
            } => Ok(RotState {
                active_slot: active,
                persistent_boot_preference,
                pending_persistent_boot_preference,
                transient_boot_preference,
                slot_a_sha3_256_digest,
                slot_b_sha3_256_digest,
            }),
            gateway_client::types::RotState::CommunicationFailed {
                message,
            } => Err(anyhow!("communication with SP failed: {}", message)),
        }
    }
}
