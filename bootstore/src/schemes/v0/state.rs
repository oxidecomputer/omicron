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
#[derive(Debug)]
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
            let next_index = (index + 1) % self.peers.len();
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

impl RackSecretState {
    /// If there are enough shares *and* we are currently collecting shares
    /// *and* the new share validates, then try to reconstruct and return the
    /// *`RackSecret`.
    ///
    /// We return the result of the share combination so that callers can
    /// handle what is essentially a fatal error if secret reconstruction fails.
    ///
    /// If we are not currently collecting shares, or the share does not
    /// validate we return an `Err(Output)` that can be directly returned to our
    /// caller's caller.
    pub fn combine_shares_if_necessary(
        state: &mut Option<RackSecretState>,
        from: Baseboard,
        request_id: Uuid,
        threshold: usize,
        share: Vec<u8>,
        share_digests: &Vec<Sha3_256Digest>,
    ) -> Result<Result<RackSecret, vsss_rs::Error>, Output> {
        match state {
            None => Err(ApiError::UnexpectedResponse {
                from,
                state: "-",
                request_id,
                msg: "share",
            }
            .into()),
            Some(RackSecretState::Secret(_)) => {
                // We already have the rack secret, just drop the extra share
                Err(Output::none())
            }
            Some(RackSecretState::Shares(shares)) => {
                // Compute and validate hash of the received key share
                if let Err(api_error) =
                    validate_share(&from, &share, share_digests)
                {
                    return Err(api_error.into());
                }

                // Add the share to our current set
                shares.insert(from, share);

                if shares.len() == threshold as usize {
                    let to_combine: Vec<_> = shares.values().cloned().collect();
                    Ok(RackSecret::combine_shares(&to_combine))
                } else {
                    Err(Output::none())
                }
            }
        }
    }
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

fn validate_share(
    from: &Baseboard,
    share: &Vec<u8>,
    share_digests: &Vec<Sha3_256Digest>,
) -> Result<(), ApiError> {
    let computed_hash =
        Sha3_256Digest(Sha3_256::digest(share).as_slice().try_into().unwrap());

    if !share_digests.contains(&computed_hash) {
        Err(ApiError::InvalidShare { from: from.clone() })
    } else {
        Ok(())
    }
}
