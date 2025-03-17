// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::messages::{CommitMsg, PrepareMsg};
use crate::{Configuration, Epoch, PlatformId, RackId, Share, Threshold};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// All the persistent state for this protocol
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistentState {
    // If this node was an LRTQ node, sled-agent will start it with the ledger
    // data it read from disk. This allows us to upgrade from LRTQ.
    pub lrtq_ledger_data: Option<LrtqLedgerData>,
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

    pub fn last_committed_reconfiguration(&self) -> Option<&Configuration> {
        self.last_committed_epoch().map(|epoch| {
            // There *must* be a prepare if we have a commit
            self.configuration(epoch).expect("missing prepare")
        })
    }
}

/// Data loaded from the ledger by sled-agent on instruction from Nexus
///
/// The epoch is always 0, because LRTQ does not allow key-rotation
///
/// Technically, this is persistant state, but it is not altered by this
/// protocol and therefore it is not part of `PersistentState`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct LrtqLedgerData {
    pub rack_uuid: RackId,
    pub threshold: Threshold,
    pub share: Share,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecommissionedMetadata {
    /// The committed epoch, later than its current configuration at which the
    /// node learned that it had been decommissioned.
    pub epoch: Epoch,

    /// Which node this commit information was learned from  
    pub from: PlatformId,
}
