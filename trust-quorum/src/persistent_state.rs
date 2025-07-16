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
use subtle::ConstantTimeEq;

/// All the persistent state for this protocol
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistentState {
    // If this node was an LRTQ node, sled-agent will start it with the ledger
    // data it read from disk. This allows us to upgrade from LRTQ.
    pub lrtq: Option<LrtqShareData>,
    pub configs: IdOrdMap<Configuration>,
    pub shares: BTreeMap<Epoch, Share>,
    pub commits: BTreeSet<Epoch>,

    // Has the node been informed that it is no longer part of the trust quorum?
    //
    // If at any time this gets set, than the it remains true for the lifetime
    // of the node. The sled corresponding to the node must be factory reset by
    // wiping its storage.
    pub expunged: Option<ExpungedMetadata>,
}

// We manually implement `PartialEq` here because of the constant
// time comparison required for comparing key shares. We don't want to
// implement `PartialEq` on those key shares directly because we want
// to limit how we do comparisons in the `gfss` library. This helps us
// guarantee we keep any optimization barriers in place as described in
// the [Choice](https://docs.rs/subtle/latest/subtle/struct.Choice.html)
// documentation for [subtle](https://docs.rs/subtle/latest/subtle/index.html)
impl PartialEq for PersistentState {
    fn eq(&self, other: &Self) -> bool {
        // Destructure to ensure compilation fails if we add fields to
        // `PersistentState` and forget to update this method.
        let PersistentState {
            lrtq: lrtq1,
            configs: configs1,
            shares: shares1,
            commits: commits1,
            expunged: expunged1,
        } = &self;
        let PersistentState {
            lrtq: lrtq2,
            configs: configs2,
            shares: shares2,
            commits: commits2,
            expunged: expunged2,
        } = &other;

        lrtq1 == lrtq2
            && configs1 == configs2
            && shares1.len() == shares2.len()
            && shares1.iter().zip(shares2.iter()).all(
                |((epoch1, share1), (epoch2, share2))| {
                    epoch1 == epoch2
                        && share1
                            .x_coordinate
                            .ct_eq(&share2.x_coordinate)
                            .into()
                        && share1.y_coordinates.len()
                            == share2.y_coordinates.len()
                        && share1
                            .y_coordinates
                            .iter()
                            .zip(share2.y_coordinates.iter())
                            .all(|(y1, y2)| y1.ct_eq(y2).into())
                },
            )
            && commits1 == commits2
            && expunged1 == expunged2
    }
}

impl Eq for PersistentState {}

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
        self.latest_committed_configuration().map(|c| c.rack_id).or_else(|| {
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

    /// Return the key share for lrtq if one exists
    pub fn lrtq_key_share(&self) -> Option<LrtqShare> {
        self.lrtq.as_ref().map(|p| p.share.clone().into())
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
