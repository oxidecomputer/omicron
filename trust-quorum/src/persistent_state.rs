// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! All state that must be persisted to storage
//!
//! Note that this state is not necessarily directly serialized and saved.

use crate::crypto::LrtqShare;
use crate::{Configuration, Epoch, PlatformId};
use bootstore::schemes::v0::SharePkgCommon as LrtqShareData;
use gfss::shamir::Share;
use iddqd::IdOrdMap;
use omicron_uuid_kinds::{GenericUuid, RackUuid};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// All the persistent state for this protocol
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistentState {
    // If this node was an LRTQ node, sled-agent will start it with the ledger
    // data it read from disk. This allows us to upgrade from LRTQ.
    pub lrtq: Option<LrtqShareData>,
    pub configs: IdOrdMap<Configuration>,

    // Our own key shares per configuration
    pub shares: BTreeMap<Epoch, Share>,
    pub commits: BTreeSet<Epoch>,

    // Has the node been informed that it is no longer part of the trust quorum?
    //
    // If at any time this gets set, than the it remains true for the lifetime
    // of the node. The sled corresponding to the node must be factory reset by
    // wiping its storage.
    pub expunged: Option<ExpungedMetadata>,
}

impl PersistentState {
    pub fn empty() -> PersistentState {
        PersistentState {
            lrtq: None,
            configs: IdOrdMap::new(),
            shares: BTreeMap::new(),
            commits: BTreeSet::new(),
            expunged: None,
        }
    }

    pub fn rack_id(&self) -> Option<RackUuid> {
        self.latest_config().map(|c| c.rack_id).or_else(|| {
            self.lrtq
                .as_ref()
                .map(|pkg| RackUuid::from_untyped_uuid(pkg.rack_uuid))
        })
    }

    // There is no information other than lrtq so far. Reconfigurations must
    // upgrade.
    pub fn is_lrtq_only(&self) -> bool {
        self.lrtq.is_some() && self.latest_committed_epoch().is_none()
    }

    // Are there any committed configurations or lrtq data?
    pub fn is_uninitialized(&self) -> bool {
        self.lrtq.is_none() && self.latest_committed_epoch().is_none()
    }

    pub fn latest_config(&self) -> Option<&Configuration> {
        self.configs.iter().last()
    }

    pub fn latest_committed_epoch(&self) -> Option<Epoch> {
        self.commits.iter().last().map(|epoch| *epoch)
    }

    // Get the configuration for the current epoch from its prepare message
    pub fn configuration(&self, epoch: Epoch) -> Option<&Configuration> {
        self.configs.get(&epoch)
    }

    pub fn latest_committed_configuration(&self) -> Option<&Configuration> {
        self.latest_committed_epoch().map(|epoch| {
            // There *must* be a configuration if we have a commit
            self.configuration(epoch).expect("missing prepare")
        })
    }

    pub fn latest_committed_config_and_share(
        &self,
    ) -> Option<(&Configuration, &Share)> {
        self.latest_committed_epoch().map(|epoch| {
            // There *must* be a configuration and share if we have a commit
            (
                self.configs.get(&epoch).expect("latest config exists"),
                self.shares.get(&epoch).expect("latest share exists"),
            )
        })
    }

    /// Return the key share for lrtq if one exists
    pub fn lrtq_key_share(&self) -> Option<LrtqShare> {
        self.lrtq.as_ref().map(|p| p.share.clone().into())
    }

    // Do we have a configuration and share for this epoch?
    pub fn has_prepared(&self, epoch: Epoch) -> bool {
        self.configs.contains_key(&epoch) && self.shares.contains_key(&epoch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpungedMetadata {
    /// The committed epoch, later than its current configuration at which the
    /// node learned that it had been expunged.
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
    pub latest_config: Option<Epoch>,
    pub latest_committed_config: Option<Epoch>,
    pub latest_share: Option<Epoch>,
    pub expunged: Option<ExpungedMetadata>,
}

impl From<&PersistentState> for PersistentStateSummary {
    fn from(value: &PersistentState) -> Self {
        PersistentStateSummary {
            rack_id: value.rack_id(),
            is_lrtq_only: value.is_lrtq_only(),
            is_uninitialized: value.is_uninitialized(),
            latest_config: value.latest_config().map(|c| c.epoch),
            latest_committed_config: value.latest_committed_epoch(),
            latest_share: value.shares.keys().last().map(|e| *e),
            expunged: value.expunged.clone(),
        }
    }
}
