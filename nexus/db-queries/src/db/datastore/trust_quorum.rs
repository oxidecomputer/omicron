// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum related queries

use nexus_db_model::HwBaseboardId;

use super::DataStore;
use crate::context::OpContext;

// This should almost certainly be the same type currently in `sled_agent_types`.
// That type should move to `nexus_sled_agent_shared` in the current code,
// and eventually into `sled_agent_types_migrations` since `nexus_sled_agent_shared` will
// no longer be needed.
use nexus_types::inventory::BaseboardId;
use omicron_uuid_kinds::RackUuid;
use std::collections::{BTreeMap, BTreeSet};

// Todo, put this somewhere sane
pub enum TrustQuorumState {
    Preparing,
    Committed,
    Aborted,
}

// TODO: Use trust quorum type
pub struct Epoch(u64);

// TODO: Move this type from sled-agent-types to nexus-sled-agent-shared ?
// Really it will come from migrations

// TODO: Move this (to nexus-types?)
pub struct ActiveTrustQuorum {
    rack_id: RackUuid,
    lrtq_members: BTreeSet<BaseboardId>,
    epoch: u64,
    state: TrustQuorumState,
    last_committed_epoch: Option<Epoch>,
    threshold: u8, // TODO: use TQ type
    commit_crash_tolerance: u8,
    coordinator: BaseboardId,
    // TODO: This should come from trust-quorum
    encrypted_rack_secrets: Option<EncryptedRackSecrets>,
    members: BTreeSet<BaseboardId>,
    acked_prepares: BTreeSet<BaseboardId>,
    acked_commits: BTreeSet<BaseboardId>,
}

impl ActiveTrustQuorum {
    pub fn is_upgrading_from_lrtq(&self) -> bool {
        !self.lrtq_members.is_empty() && self.last_committed_epoch.is_none()
    }
}

impl DataStore {}
