// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! All state that must be persisted to storage
//!
//! Note that this state is not necessarily directly serialized and saved.

use crate::messages::{CommitMsg, PrepareMsg};
use crate::{Configuration, KeyShareEd25519, KeyShareGf256};
use crate::{Epoch, PlatformId};
use bootstore::schemes::v0::SharePkgCommon as LrtqShareData;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// All the persistent state for this protocol
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    // Are there any committed configurations or lrtq data?
    pub fn is_uninitialized(&self) -> bool {
        self.lrtq.is_none() && self.last_committed_epoch().is_none()
    }

    // Is there only lrtq data, and no committed configurations yet?
    pub fn is_lrtq_only(&self) -> bool {
        self.lrtq.is_some() && self.last_committed_epoch().is_none()
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
    pub fn lrtq_key_share(&self) -> Option<KeyShareEd25519> {
        self.lrtq.as_ref().map(|p| KeyShareEd25519(p.share.clone()))
    }

    // Return the key share for the latest committed trust quorum configuration
    // if one exists
    pub fn key_share(&self) -> Option<KeyShareGf256> {
        self.last_committed_epoch().map(|epoch| {
            self.prepares.get(&epoch).expect("missing prepare").share.clone()
        })
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecommissionedMetadata {
    /// The committed epoch, later than its current configuration at which the
    /// node learned that it had been decommissioned.
    pub epoch: Epoch,

    /// Which node this commit information was learned from  
    pub from: PlatformId,
}
