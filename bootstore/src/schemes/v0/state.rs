// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State for the V0 protocol state machine

use super::fsm_output::ApiOutput;
use super::state_learned::LearnedState;
use super::state_learning::LearningState;
use super::state_uninitialized::UninitializedState;
use super::{fsm_output::Output, state_initial_member::InitialMemberState};
use crate::{
    trust_quorum::{
        create_pkgs, LearnedSharePkgV0, RackSecret, SharePkgV0,
        TrustQuorumError,
    },
    Sha3_256Digest,
};
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// A number of clock ticks from some unknown epoch
pub type Ticks = usize;

// An index into an encrypted share
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareIdx(pub usize);

/// Configuration of the FSM
#[derive(Debug, Clone)]
pub struct Config {
    pub learn_timeout: Ticks,
    pub rack_init_timeout: Ticks,
    pub rack_secret_request_timeout: Ticks,
}

/// In memory state shared by all 4 FSM states
pub struct FsmCommonData {
    // Unique IDs of this peer
    pub id: Baseboard,

    // Configuration of the FSM
    pub config: Config,

    // Unique IDs of known peers
    pub peers: BTreeSet<Baseboard>,

    // The current time in ticks since the creation of the FSM
    pub clock: Ticks,

    // An api caller can issue a request for a `RackSecret` with
    // `load_rack_secret`. Sometimes we don't have the `RackSecret` recomputed
    // and so we have to mark the request as pending until we retrieve enough
    // shares to recompute it and return it to the caller. We keep track
    // of the pending request here as it's valid in all FSM states except
    // `State::Uninitialized`.
    //
    // The value stored inside the `Option` is the start time of the request.
    //
    // A caller should only issue this once. If it's issued more times, we'll
    // overwrite the start time to extend the timeout.
    pub pending_api_rack_secret_request: Option<Ticks>,
}

impl FsmCommonData {
    pub fn new(id: Baseboard, config: Config) -> FsmCommonData {
        FsmCommonData {
            id,
            config,
            peers: BTreeSet::new(),
            clock: 0,
            pending_api_rack_secret_request: None,
        }
    }

    // If there's a pending api request for the rack secret:
    //
    // Return the secret if there is one passed in, otherwise return an error.
    // Return `None` if no request is pending
    pub fn resolve_pending_api_request(
        &mut self,
        rack_secret: Option<&RackSecret>,
    ) -> Option<Result<ApiOutput, ApiError>> {
        match (rack_secret, &mut self.pending_api_rack_secret_request) {
            (None, Some(_)) => {
                self.pending_api_rack_secret_request = None;
                Some(Err(ApiError::FailedToReconstructRackSecret))
            }
            (Some(rack_secret), Some(_)) => {
                self.pending_api_rack_secret_request = None;
                Some(Ok(ApiOutput::RackSecret(rack_secret.clone())))
            }
            (_, None) => None,
        }
    }

    // Send a request for a share to each peer
    pub fn broadcast_share_requests(
        &self,
        rack_uuid: Uuid,
        known_shares: Option<&BTreeMap<Baseboard, Vec<u8>>>,
    ) -> Output {
        let known_peers = if let Some(shares) = known_shares {
            shares.keys().cloned().collect()
        } else {
            BTreeSet::new()
        };

        // All broadcast requests share the same id
        let request_id = Uuid::new_v4();
        let envelopes = self
            .peers
            .difference(&known_peers)
            .cloned()
            .map(|to| Envelope {
                to,
                msg: Request {
                    id: request_id,
                    type_: RequestType::GetShare { rack_uuid },
                }
                .into(),
            })
            .collect();

        Output { persist: false, envelopes, api_output: None }
    }

    // Round-robin peer selection
    pub fn next_peer(&self, current: &Baseboard) -> Option<Baseboard> {
        if let Some(index) = self.peers.iter().position(|x| x == current) {
            let next_index = (index + 1) % peers.len();
            self.peers.iter().nth(next_index).cloned()
        } else {
            self.peers.first().cloned()
        }
    }
}

/// Metadata associated with a request that is keyed by `Baseboard`
#[derive(Debug)]
pub struct RequestMetadata {
    pub request_id: Uuid,
    pub start: Ticks,
}

/// Whether we are in the process of learning enough shares to recompute the
/// rack secret or if we have already recomputed it.
#[derive(Debug)]
pub enum RackSecretState {
    // TODO: Zeroize or wrap in a Secert
    Shares(BTreeMap<Baseboard, Vec<u8>>),
    Secret(RackSecret),
}

/// Initialization state for tracking rack init on the coordinator node (the one
/// local to RSS)
#[derive(Debug)]
pub struct RackInitState {
    pub start: Ticks,
    pub total_members: usize,
    pub acks: BTreeSet<Baseboard>,
}

impl RackInitState {
    pub fn timer_expired(&self, now: Ticks, timeout: Ticks) -> bool {
        now.saturating_sub(self.start) > timeout
    }

    pub fn is_complete(&self) -> bool {
        self.acks.len() == self.total_members
    }
}

/// State stored on the M.2s.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentState {
    // The generation number for ledger writing purposes on both M.2s
    pub version: u32,
    pub fsm_state: PersistentFsmState,
}

// Based on the state machine of how `PersistentState` gets updated, we can
// deterministically infer a version.
impl From<PersistentFsmState> for PersistentState {
    fn from(fsm_state: PersistentFsmState) -> Self {
        use PersistentFsmState::*;
        let version = match &fsm_state {
            Uninitialized => 0,
            InitialMember { distributed_shares, .. } => {
                (1 + distributed_shares.len()).try_into().unwrap()
            }
            Learning => 1,
            Learned => 2,
        };
        PersistentState { version, fsm_state }
    }
}

/// A subset of `State` that must be persisted to the M.2s
#[derive(Debug, Serialize, Deserialize)]
pub enum PersistentFsmState {
    Uninitialized,
    InitialMember {
        pkg: SharePkgV0,
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    },
    Learning,
    Learned {
        pkg: LearnedSharePkgV0,
    },
}

impl From<State> for PersistentFsmState {
    fn from(value: State) -> Self {
        match value {
            State::Uninitialized => Self::Uninitialized,
            State::InitialMember(InitialMemberState {
                pkg,
                distributed_shares,
                ..
            }) => Self::InitialMember { pkg, distributed_shares },
            State::Learning(_) => Self::Learning,
            State::Learned(LearnedState { pkg, .. }) => Self::Learned { pkg },
        }
    }
}

impl From<PersistentFsmState> for State {
    fn from(value: PersistentFsmState) -> Self {
        use PersistentFsmState::*;
        match value {
            Uninitialized => Self::Uninitialized,
            InitialMember { pkg, distributed_shares } => Self::InitialMember(
                InitialMemberState::new(pkg, distributed_shares),
            ),
            Learning => Self::Learning(None),
            Learned { pkg } => Self::Learned(LearnedState::new(pkg)),
        }
    }
}

/// The possible states of a peer FSM
///
/// The following are valid state transitions
///
///   * Uninitialized -> InitialMember
///   * Uninitialized -> Learning -> Learned
///
#[derive(Debug)]
pub enum State {
    /// The peer does not yet know whether it is an initial member of the group
    /// or a learner.
    ///
    /// All peers start out in this state prior to RSS running
    Uninitialized(UninitializedState),

    /// The peer is an initial member of the group setup via RSS
    ///
    /// This is a terminal state of the peer, mutually exclusive with
    /// `Learned`
    InitialMember(InitialMemberState),

    /// The peer has been instructed to learn a share
    ///
    /// This peer was added to the cluster after rack initialization completed.
    Learning(LearningState),

    /// The peer has learned its share
    ///
    /// This is a terminal state of the peer, mutually exclusive with
    /// `InitialMember`
    Learned(LearnedState),
}

impl State {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::InitialMember(_) => "initial_member",
            Self::Learning(_) => "learning",
            Self::Learned(_) => "learned",
        }
    }
}
