// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State for the V0 protocol state machine

use crate::trust_quorum::{
    create_pkgs, LearnedSharePkgV0, RackSecret, SharePkgV0, TrustQuorumError,
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

/// An attempt by *this* peer to learn a key share
///
/// When an attempt is started, a peer is selected from those known to the FSM,
/// and a `RequestType::Learn` message is sent to that peer. This peer is recorded
/// along with the current clock as `start`. If `Config.learn_timeout` ticks
/// have fired since `start` based on the current FSM clock, without this
/// FSM having received a `ResponseType::Pkg`, then the `LearnAttempt` will be
/// cancelled, and the next peer in order known to the FSM will be contacted in
/// a new attempt.
#[derive(Debug, Clone)]
pub struct LearnAttempt {
    pub peer: Baseboard,
    pub start: Ticks,
}

impl LearnAttempt {
    pub fn expired(&self, now: Ticks, timeout: Ticks) -> bool {
        now.saturating_sub(self.start) >= timeout
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
        now.saturating_sub(self.start) >= timeout
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
    Uninitialized,

    /// The peer is an initial member of the group setup via RSS
    ///
    /// This is a terminal state of the peer, mutually exclusive with
    /// `Learned`
    InitialMember(InitialMemberState),

    /// The peer has been instructed to learn a share
    ///
    /// This peer was added to the cluster after rack initialization completed.
    Learning(Option<LearnAttempt>),

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

#[derive(Debug)]
pub struct LearnedState {
    pub pkg: LearnedSharePkgV0,
    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    pub rack_secret_state: Option<RackSecretState>,
}

impl LearnedState {
    pub fn new(pkg: LearnedSharePkgV0) -> Self {
        LearnedState { pkg, rack_secret_state: None }
    }
}

#[derive(Debug)]
pub struct InitialMemberState {
    pub pkg: SharePkgV0,

    /// Shares given to other sleds. We mark them as used so that we don't
    /// hand them out twice. If the same sled asks us for a share, because
    /// it crashes or there is a network blip, we will return the same
    /// share each time.
    ///
    /// Note that this is a fairly optimistic strategy as the requesting
    /// sled can always go ask another sled after a network blip. However,
    /// this guarantees that a single sled never hands out more than one of
    /// its shares to any given sled.
    ///
    /// We can't do much better than this without some sort of centralized
    /// distributor which is part of the reconfiguration mechanism in later
    /// versions of the trust quourum protocol.
    pub distributed_shares: BTreeMap<Baseboard, ShareIdx>,

    // Acknowledgements tracked during rack initialization if this node is
    // the one acting as coordinator (local to RSS).
    pub rack_init_state: Option<RackInitState>,

    // Pending learn requests from other peers mapped to their start time.
    //
    // When a peer attempts to learn a share, we may not be able to give it
    // one because we cannot yet recompute the rack secret. We queue requests
    // here and respond when we can recompute the rack secret. If we timeout before
    // we can recompute the rack secret, we respond with a timeout Error.
    //
    // Note that if we get a new `RequestType::Learn` from a peer that is already
    // pending, we will reset the start time.
    pub pending_learn_requests: BTreeMap<Baseboard, RequestMetadata>,

    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    pub rack_secret_state: Option<RackSecretState>,
}

impl InitialMemberState {
    pub fn new(
        pkg: SharePkgV0,
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    ) -> Self {
        InitialMemberState {
            pkg,
            distributed_shares,
            rack_init_state: None,
            pending_learn_requests: BTreeMap::new(),
            rack_secret_state: None,
        }
    }
}
