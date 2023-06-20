// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State for the V0 protocol state machine

use super::fsm::StateHandler;
use super::fsm_output::{ApiError, ApiOutput};
use super::messages::{Envelope, Request, RequestType};
use super::state_learned::LearnedState;
use super::state_learning::LearningState;
use super::state_uninitialized::UninitializedState;
use super::{fsm_output::Output, state_initial_member::InitialMemberState};
use crate::{
    trust_quorum::{LearnedSharePkgV0, RackSecret, SharePkgV0},
    Sha3_256Digest,
};
use derive_more::From;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A number of clock ticks from some unknown epoch
pub type Ticks = usize;

// An index into an encrypted share
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareIdx(pub usize);

/// Configuration of the FSM
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub learn_timeout: Ticks,
    pub rack_init_timeout: Ticks,
    pub rack_secret_request_timeout: Ticks,
}

/// In memory state shared by all 4 FSM states
#[derive(Debug)]
pub struct FsmCommonData {
    /// Unique IDs of this peer
    pub id: Baseboard,

    /// Configuration of the FSM
    pub config: Config,

    /// Unique IDs of connected peers
    pub peers: BTreeSet<Baseboard>,

    /// The current time in ticks since the creation of the FSM
    pub clock: Ticks,

    /// Any shares we are in the process of retrieving or a computed rack
    /// secret. If no collection is ongoing and we are not holding a computed
    /// rack secret, then the state is `Empty`. The state is always empty for
    /// `State::Uninitialized` and `State::Learning`
    pub rack_secret_state: RackSecretState,
}

impl FsmCommonData {
    pub fn new(id: Baseboard, config: Config) -> FsmCommonData {
        FsmCommonData {
            id,
            config,
            peers: BTreeSet::new(),
            clock: 0,
            rack_secret_state: RackSecretState::Empty,
        }
    }

    // Round-robin peer selection
    pub fn next_peer(&self, current: &Baseboard) -> Option<Baseboard> {
        if let Some(index) = self.peers.iter().position(|x| x == current) {
            let next_index = (index + 1) % self.peers.len();
            self.peers.iter().nth(next_index).cloned()
        } else {
            self.peers.first().cloned()
        }
    }

    // Send a `GetShare` request to the newly connected peer if necessary
    pub fn on_connect(&self, peer: Baseboard, rack_uuid: Uuid) -> Output {
        if let RackSecretState::Retrieving { request_id, from, .. } =
            &self.rack_secret_state
        {
            // We don't have a share from the peer and we weren't previously
            // connected to the peer.
            if !from.contains(&peer) && !self.peers.contains(&peer) {
                let request = Request {
                    id: *request_id,
                    type_: RequestType::GetShare { rack_uuid },
                };
                return Output {
                    persist: false,
                    envelopes: vec![Envelope { to: peer, msg: request.into() }],
                    api_output: None,
                };
            }
        }
        Output::none()
    }
}

/// Metadata associated with a request that is keyed by `Baseboard`
#[derive(Debug)]
pub struct RequestMetadata {
    pub request_id: Uuid,
    pub start: Ticks,
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Shares(Vec<Vec<u8>>);

impl Shares {
    fn insert(&mut self, share: Vec<u8>) {
        self.0.push(share);
    }
}

impl Debug for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shares").finish()
    }
}

/// The state of rack secret share retrieval
///
/// Primarily this is whether we are in the process of retrieving enough shares to
/// recompute the rack secret or if we have already recomputed it.
#[derive(Debug)]
pub enum RackSecretState {
    /// We are not currently retrieving any shares and we don't have a computed
    /// rack secret
    Empty,

    /// We only store the shares in memory until some expiry deadline.
    /// However, we extend this deadline each time a share is learned to enable
    /// computation of the rack secret. If no shares are added for a while, and
    /// we still don't have enough shares to compute the rack secret then we
    /// drop all the shares.
    Retrieving {
        // We use a separate BTreeSet to track who we retrieved shares form so
        // that we can wrap the shares with a `Secret`
        request_id: Uuid,
        from: BTreeSet<Baseboard>,
        shares: Shares,
        expiry: Ticks,
        threshold: usize,
        share_digests: Vec<Sha3_256Digest>,
    },
    /// We only store the computed secret in memory for so long after it has
    /// been computed
    Computed { secret: RackSecret, expiry: Ticks },
}

impl RackSecretState {
    pub fn load(
        &mut self,
        rack_uuid: Uuid,
        peers: &BTreeSet<Baseboard>,
        local_id: &Baseboard,
        local_share: &Vec<u8>,
        new_expiry: Ticks,
        threshold: usize,
        share_digests: &Vec<Sha3_256Digest>,
    ) -> Output {
        match self {
            RackSecretState::Empty => {
                // Start the share retrieval process
                let (request_id, output) =
                    self.broadcast_share_requests(rack_uuid, peers);
                *self = RackSecretState::Retrieving {
                    request_id,
                    from: BTreeSet::from([local_id.clone()]),
                    shares: Shares(vec![local_share.clone()]),
                    expiry: new_expiry,
                    threshold,
                    share_digests: share_digests.clone(),
                };
                output
            }
            RackSecretState::Retrieving { expiry, .. } => {
                // We already have a retrieval in progress.
                // Just extend the expiry
                *expiry = new_expiry;
                Output::none()
            }
            RackSecretState::Computed { secret, .. } => {
                // We have the secret, so return it.
                Output {
                    persist: false,
                    envelopes: vec![],
                    api_output: Some(Ok(ApiOutput::RackSecret(secret.clone()))),
                }
            }
        }
    }

    // Did the share retrieval process timeout ?
    pub fn on_tick(&mut self, now: Ticks) -> Output {
        match self {
            RackSecretState::Retrieving { expiry, .. } => {
                if *expiry < now {
                    *self = RackSecretState::Empty;
                    return ApiError::RackSecretLoadTimeout.into();
                }
            }
            RackSecretState::Computed { expiry, .. } => {
                if *expiry < now {
                    // This is just cleanup of the secret from memory
                    // No reason to report this
                    *self = RackSecretState::Empty;
                }
            }
            RackSecretState::Empty => (),
        }
        Output::none()
    }

    fn validate_share(
        from: &Baseboard,
        share: &Vec<u8>,
        share_digests: &Vec<Sha3_256Digest>,
    ) -> Result<(), ApiError> {
        let computed_hash = Sha3_256Digest(
            Sha3_256::digest(share).as_slice().try_into().unwrap(),
        );

        if !share_digests.contains(&computed_hash) {
            Err(ApiError::InvalidShare { from: from.clone() })
        } else {
            Ok(())
        }
    }

    // Send a request for a share to each peer
    pub fn broadcast_share_requests(
        &self,
        rack_uuid: Uuid,
        peers: &BTreeSet<Baseboard>,
    ) -> (Uuid, Output) {
        // All broadcast requests share the same id
        let request_id = Uuid::new_v4();
        let envelopes = peers
            .iter()
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

        (request_id, Output { persist: false, envelopes, api_output: None })
    }

    /// We received a share
    ///
    /// Verify that the share is valid and recompute the secret if there are
    /// enough shares.
    pub fn on_share(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        share: Vec<u8>,
        rack_secret_expiry: Ticks,
    ) -> Output {
        match self {
            RackSecretState::Empty => ApiError::UnexpectedResponse {
                from,
                state: "-",
                request_id,
                msg: "share",
            }
            .into(),
            RackSecretState::Retrieving {
                from: from_all,
                shares,
                threshold,
                share_digests,
                ..
            } => {
                // Ignore the share if we already have one from this peer
                if from_all.contains(&from) {
                    return Output::none();
                }

                // Compute and validate hash of the received key share
                if let Err(api_error) =
                    Self::validate_share(&from, &share, &share_digests)
                {
                    return api_error.into();
                }

                // Keep track of our new valid share
                shares.insert(share);
                from_all.insert(from);

                // If we have enough shares, try to recompute the rack secret
                // If we fail here, this is a major problem so we report it and
                // clear our rack secret state, as we won't be able to succeed.
                // This error should be effectively impossible to ever hit.
                if shares.0.len() == *threshold {
                    match RackSecret::combine_shares(&shares.0) {
                        Ok(secret) => {
                            *self = RackSecretState::Computed {
                                secret: secret.clone(),
                                expiry: rack_secret_expiry,
                            };
                            ApiOutput::RackSecret(secret).into()
                        }
                        Err(_) => {
                            *self = RackSecretState::Empty;
                            ApiError::FailedToReconstructRackSecret.into()
                        }
                    }
                } else {
                    // We don't have enough shares
                    Output::none()
                }
            }
            RackSecretState::Computed { secret, .. } => {
                // We already have the state, go ahead and return it.
                ApiOutput::RackSecret(secret.clone()).into()
            }
        }
    }
}

/// Initialization state for tracking rack init on the coordinator node (the one
/// local to RSS)
#[derive(Debug)]
pub enum RackInitState {
    // This only exists on the RSS sled
    Running {
        start: Ticks,
        total_members: usize,
        acks: BTreeSet<Baseboard>,

        // Any unacked messages will be re-sent when the destination peer
        // connects.
        unacked_envelopes: Vec<Envelope>,
    },
    Initialized,
    Timeout,
}

impl RackInitState {
    // Return true if we just completed rack init, false otherwise
    pub fn on_ack(&mut self, from: Baseboard) -> bool {
        if let RackInitState::Running {
            total_members,
            acks,
            unacked_envelopes,
            ..
        } = self
        {
            unacked_envelopes.retain(|envelope| envelope.to != from);
            acks.insert(from);
            if acks.len() == *total_members {
                *self = RackInitState::Initialized;
                return true;
            }
        }
        false
    }
    pub fn timer_expired(&self, now: Ticks, timeout: Ticks) -> bool {
        if let RackInitState::Running { start, .. } = self {
            now.saturating_sub(*start) > timeout
        } else {
            false
        }
    }

    pub fn is_initialized(&self) -> bool {
        matches!(self, RackInitState::Initialized)
    }

    pub fn unacked_peers(&self) -> BTreeSet<Baseboard> {
        if let RackInitState::Running { unacked_envelopes, .. } = self {
            unacked_envelopes
                .iter()
                .map(|envelope| envelope.to.clone())
                .collect()
        } else {
            BTreeSet::new()
        }
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
            Learned { .. } => 2,
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
            State::Uninitialized(_) => Self::Uninitialized,
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
            Uninitialized => Self::Uninitialized(UninitializedState {}),
            InitialMember { pkg, distributed_shares } => Self::InitialMember(
                InitialMemberState::new(pkg, distributed_shares),
            ),
            Learning => Self::Learning(LearningState { attempt: None }),
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
#[derive(Debug, From)]
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

impl StateHandler for State {
    fn handle_request(
        self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (State, Output) {
        match self {
            State::Uninitialized(state) => {
                state.handle_request(common, from, request_id, request)
            }
            State::InitialMember(state) => {
                state.handle_request(common, from, request_id, request)
            }
            State::Learning(state) => {
                state.handle_request(common, from, request_id, request)
            }
            State::Learned(state) => {
                state.handle_request(common, from, request_id, request)
            }
        }
    }

    fn handle_response(
        self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: super::messages::ResponseType,
    ) -> (State, Output) {
        match self {
            State::Uninitialized(state) => {
                state.handle_response(common, from, request_id, response)
            }
            State::InitialMember(state) => {
                state.handle_response(common, from, request_id, response)
            }
            State::Learning(state) => {
                state.handle_response(common, from, request_id, response)
            }
            State::Learned(state) => {
                state.handle_response(common, from, request_id, response)
            }
        }
    }

    fn tick(self, common: &mut FsmCommonData) -> (State, Output) {
        match self {
            State::Uninitialized(state) => state.tick(common),
            State::InitialMember(state) => state.tick(common),
            State::Learning(state) => state.tick(common),
            State::Learned(state) => state.tick(common),
        }
    }

    fn on_connect(
        &mut self,
        common: &mut FsmCommonData,
        peer: Baseboard,
    ) -> Output {
        match self {
            State::Uninitialized(state) => state.on_connect(common, peer),
            State::InitialMember(state) => state.on_connect(common, peer),
            State::Learning(state) => state.on_connect(common, peer),
            State::Learned(state) => state.on_connect(common, peer),
        }
    }

    fn on_disconnect(
        &mut self,
        common: &mut FsmCommonData,
        peer: Baseboard,
    ) -> Output {
        match self {
            State::Uninitialized(state) => state.on_disconnect(common, peer),
            State::InitialMember(state) => state.on_disconnect(common, peer),
            State::Learning(state) => state.on_disconnect(common, peer),
            State::Learned(state) => state.on_disconnect(common, peer),
        }
    }
}

impl State {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Uninitialized(_) => "uninitialized",
            Self::InitialMember(_) => "initial_member",
            Self::Learning(_) => "learning",
            Self::Learned(_) => "learned",
        }
    }
}
