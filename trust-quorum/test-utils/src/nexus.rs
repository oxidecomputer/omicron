// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus related types for trust-quorum testing

use daft::Diffable;
use iddqd::id_ord_map::RefMut;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use trust_quorum::{
    Epoch, LrtqUpgradeMsg, PlatformId, ReconfigureMsg, Threshold,
};

// The operational state of nexus for a given configuration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Diffable)]
pub enum NexusOp {
    Committed,
    Aborted,
    Preparing,
    LrtqCommitted,
}

/// A single nexus configuration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Diffable)]
pub struct NexusConfig {
    pub op: NexusOp,
    pub epoch: Epoch,
    pub last_committed_epoch: Option<Epoch>,
    pub coordinator: PlatformId,
    pub members: BTreeSet<PlatformId>,
    // This is our `K` parameter
    pub threshold: Threshold,

    // This is our `Z` parameter.
    //
    // Nexus can commit when it has seen K+Z prepare acknowledgements
    //
    // Only nexus needs to know this value since it alone determines when a
    // commit may occur.
    pub commit_crash_tolerance: u8,

    pub prepared_members: BTreeSet<PlatformId>,
    pub committed_members: BTreeSet<PlatformId>,
}

impl NexusConfig {
    pub fn new(
        epoch: Epoch,
        last_committed_epoch: Option<Epoch>,
        coordinator: PlatformId,
        members: BTreeSet<PlatformId>,
        threshold: Threshold,
    ) -> NexusConfig {
        // We want a few extra nodes beyond `threshold` to ack before we commit.
        // This is the number of nodes that can go offline while still allowing
        // an unlock to occur.
        let commit_crash_tolerance = match members.len() - threshold.0 as usize
        {
            0..=1 => 0,
            2..=4 => 1,
            5..=7 => 2,
            _ => 3,
        };
        NexusConfig {
            op: NexusOp::Preparing,
            epoch,
            last_committed_epoch,
            coordinator,
            members,
            threshold,
            commit_crash_tolerance,
            prepared_members: BTreeSet::new(),
            committed_members: BTreeSet::new(),
        }
    }

    // An LRTQ config can only be the initial config.
    //
    // We create it so that we can test upgrading out of it.
    pub fn new_lrtq(
        coordinator: PlatformId,
        members: BTreeSet<PlatformId>,
    ) -> NexusConfig {
        let threshold = Threshold((members.len() / 2 + 1) as u8);
        NexusConfig {
            // We start committed, since we aren't actually running the LRTQ protocol
            op: NexusOp::LrtqCommitted,
            epoch: Epoch(1),
            last_committed_epoch: None,
            coordinator,
            members,
            threshold,
            commit_crash_tolerance: 0,
            prepared_members: BTreeSet::new(),
            committed_members: BTreeSet::new(),
        }
    }

    pub fn to_reconfigure_msg(&self, rack_id: RackUuid) -> ReconfigureMsg {
        ReconfigureMsg {
            rack_id,
            epoch: self.epoch,
            last_committed_epoch: self.last_committed_epoch,
            members: self.members.clone(),
            threshold: self.threshold,
        }
    }

    pub fn to_lrtq_upgrade_msg(&self, rack_id: RackUuid) -> LrtqUpgradeMsg {
        LrtqUpgradeMsg {
            rack_id,
            epoch: self.epoch,
            members: self.members.clone(),
            threshold: self.threshold,
        }
    }

    // Are there enough prepared members to commit?
    pub fn can_commit(&self) -> bool {
        self.prepared_members.len()
            >= (self.threshold.0 + self.commit_crash_tolerance) as usize
    }
}

impl IdOrdItem for NexusConfig {
    type Key<'a> = Epoch;

    fn key(&self) -> Self::Key<'_> {
        self.epoch
    }

    id_upcast!();
}

/// A model of Nexus's view of the world during the test
#[derive(Debug, Clone, Diffable)]
pub struct NexusState {
    // No reason to change the rack_id
    pub rack_id: RackUuid,

    pub configs: IdOrdMap<NexusConfig>,
}

impl NexusState {
    #[allow(clippy::new_without_default)]
    pub fn new() -> NexusState {
        // We end up replaying events in tqdb, and can't use a random rack
        // uuid.
        NexusState { rack_id: RackUuid::nil(), configs: IdOrdMap::new() }
    }

    // Create a `ReconfigureMsg` for the latest nexus config
    pub fn reconfigure_msg_for_latest_config(
        &self,
    ) -> (&PlatformId, ReconfigureMsg) {
        let config = self.configs.iter().last().expect("at least one config");
        (&config.coordinator, config.to_reconfigure_msg(self.rack_id))
    }

    // Create an `LrtqUpgradeMsg` for the latest nexus config
    pub fn lrtq_upgrade_msg_for_latest_config(
        &self,
    ) -> (&PlatformId, LrtqUpgradeMsg) {
        let config = self.configs.iter().last().expect("at least one config");
        (&config.coordinator, config.to_lrtq_upgrade_msg(self.rack_id))
    }

    pub fn latest_config(&self) -> &NexusConfig {
        self.configs.iter().last().expect("at least one config")
    }

    pub fn latest_config_mut(&mut self) -> RefMut<'_, NexusConfig> {
        self.configs.iter_mut().last().expect("at least one config")
    }

    pub fn last_committed_config(&self) -> Option<&NexusConfig> {
        // IdOrdMap doesn't allow reverse iteration.
        // We therefore iterate through all configs to find the latest committed one.
        // We could track this out of band but that leaves more room for error.
        let mut found: Option<&NexusConfig> = None;
        for c in &self.configs {
            if c.op == NexusOp::Committed {
                found = Some(c)
            }
        }
        found
    }

    pub fn needs_upgrade_from_lrtq(&self) -> bool {
        // If we don't have any committed TQ configurations and
        // the configuration for epoch 1 is LRTQ then we must
        // call `Node::coordinate_upgrade_from_lrtq` rather than
        // `Node::coordinate_reconfiguration`.
        self.last_committed_config().is_none()
            && self
                .configs
                .get(&Epoch(1))
                .map(|c| c.op == NexusOp::LrtqCommitted)
                .unwrap_or(false)
    }
}

#[derive(
    Debug,
    Clone,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Diffable,
)]
pub enum NexusReply {
    AckedPreparesFromCoordinator { epoch: Epoch, acks: BTreeSet<PlatformId> },
    CommitAck { from: PlatformId, epoch: Epoch },
}
