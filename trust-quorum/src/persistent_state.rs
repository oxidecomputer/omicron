// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! All state that must be persisted to storage
//!
//! Note that this state is not necessarily directly serialized and saved.

use crate::crypto::LrtqShare;
use crate::messages::{CommitMsg, PrepareMsg};
use crate::{Configuration, Epoch, PlatformId};
use bootstore::schemes::v0::SharePkgCommon as LrtqShareData;
use gfss::shamir::Share;
use omicron_uuid_kinds::{GenericUuid, RackUuid};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// All the persistent state for this protocol
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistentState {
    // Ledger generation
    pub generation: u64,

    // If this node was an LRTQ node, sled-agent will start it with the ledger
    // data it read from disk. This allows us to upgrade from LRTQ.
    pub lrtq: Option<LrtqShareData>,
    pub prepares: BTreeMap<Epoch, PrepareMsg>,
    pub commits: BTreeMap<Epoch, CommitMsg>,

    // Has the node seen a commit for an epoch higher than it's current
    // configuration for which it has not received a `PrepareMsg` for? If at
    // any time this gets set, than the it remains true for the lifetime of the
    // node. The sled corresponding to the node must be factory reset by wiping
    // its storage.
    pub decommissioned: Option<DecommissionedMetadata>,
}

impl PersistentState {
    pub fn empty() -> PersistentState {
        PersistentState {
            generation: 0,
            lrtq: None,
            prepares: BTreeMap::new(),
            commits: BTreeMap::new(),
            decommissioned: None,
        }
    }

    pub fn rack_id(&self) -> Option<RackUuid> {
        self.last_committed_configuration().map(|c| c.rack_id).or_else(|| {
            self.lrtq
                .as_ref()
                .map(|pkg| RackUuid::from_untyped_uuid(pkg.rack_uuid))
        })
    }

    // There is no information other than lrtq so far. Reconfigurations must
    // upgrade.
    pub fn is_lrtq_only(&self) -> bool {
        self.lrtq.is_some() && self.last_committed_epoch().is_none()
    }

    // Are there any committed configurations or lrtq data?
    pub fn is_uninitialized(&self) -> bool {
        self.lrtq.is_none() && self.last_committed_epoch().is_none()
    }

    pub fn last_prepared_epoch(&self) -> Option<Epoch> {
        self.prepares.keys().last().map(|epoch| *epoch)
    }

    pub fn last_committed_epoch(&self) -> Option<Epoch> {
        self.commits.keys().last().map(|epoch| *epoch)
    }

    // Get the configuration for the current epoch from its prepare message
    pub fn configuration(&self, epoch: Epoch) -> Option<&Configuration> {
        self.prepares.get(&epoch).map(|p| &p.config)
    }

    pub fn last_committed_configuration(&self) -> Option<&Configuration> {
        self.last_committed_epoch().map(|epoch| {
            // There *must* be a prepare if we have a commit
            self.configuration(epoch).expect("missing prepare")
        })
    }

    /// Return the key share for lrtq if one exists
    pub fn lrtq_key_share(&self) -> Option<LrtqShare> {
        self.lrtq.as_ref().map(|p| p.share.clone().into())
    }

    // Return the key share for the latest committed trust quorum configuration
    // if one exists
    pub fn key_share(&self) -> Option<Share> {
        self.last_committed_epoch().map(|epoch| {
            self.prepares.get(&epoch).expect("missing prepare").share.clone()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecommissionedMetadata {
    /// The committed epoch, later than its current configuration at which the
    /// node learned that it had been decommissioned.
    pub epoch: Epoch,

    /// Which node this commit information was learned from  
    pub from: PlatformId,
}

/// A subset of information stored in [`PersistentState`] that is useful
/// for validation, testing, and informational purposes.
#[derive(Debug, Clone)]
pub struct PersistentStateSummary {
    pub rack_id: Option<RackUuid>,
    pub is_lrtq_only: bool,
    pub is_uninitialized: bool,
    pub last_prepared_epoch: Option<Epoch>,
    pub last_committed_epoch: Option<Epoch>,
    pub decommissioned: Option<DecommissionedMetadata>,
}

impl From<&PersistentState> for PersistentStateSummary {
    fn from(value: &PersistentState) -> Self {
        PersistentStateSummary {
            rack_id: value.rack_id(),
            is_lrtq_only: value.is_lrtq_only(),
            is_uninitialized: value.is_uninitialized(),
            last_prepared_epoch: value.last_prepared_epoch(),
            last_committed_epoch: value.last_committed_epoch(),
            decommissioned: value.decommissioned.clone(),
        }
    }
}
