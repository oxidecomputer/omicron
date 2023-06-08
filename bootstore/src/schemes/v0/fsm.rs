// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::fsm_output::{ApiError, ApiOutput, Output};
use super::messages::{
    Envelope, Error, Msg, Request, RequestType, Response, ResponseType,
};
use super::state::{
    Config, FsmCommonData, InitialMemberState, LearnAttempt, LearnedState,
    PersistentFsmState, PersistentState, RackInitState, RackSecretState,
    RequestMetadata, ShareIdx, State, Ticks,
};
use crate::trust_quorum::{
    create_pkgs, LearnedSharePkgV0, RackSecret, SharePkgV0, TrustQuorumError,
};
use crate::Sha3_256Digest;
use secrecy::ExposeSecret;
use sha3::{Digest, Sha3_256};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Handler methods implemented by each FSM state
pub trait StateHandler {
    fn handle_request(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (state, Output);

    fn handle_response(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> (State, Output);

    fn tick(mut self, common: &mut FsmCommonData) -> (State, Output);
}

/// The state machine for a [`$crate::Peer`]
///
/// This FSM assumes a network layer above it that can map peer IDs
/// ( [`Baseboard`]s) to TCP sockets. When an attempt is made to send a
/// message to a given peer the network layer will send it over an established
/// connection if one exists. If there is no connection already to that peer,
/// but the prefix is present and known, the network layer will attempt to
/// establish a connection and then transmit the message. If the peer is not
/// known then it must have just had its network prefix removed. In this case
/// the message will be dropped and the FSM will be told to remove the peer.
#[derive(Debug)]
pub struct Fsm {
    common: FsmCommonData,

    // Use an option to allow taking and mutating `State` independently of `Fsm`
    state: Option<State>,
}

impl Fsm {
    pub fn new(id: Baseboard, config: Config, state: State) -> Fsm {
        Fsm { common: FsmCommonData::new(id, config), state: Some(state) }
    }

    /// This call is triggered locally as a result of RSS running
    /// It may only be called once, which is enforced by checking to see if
    /// we already are in `State::Uninitialized`.
    pub fn init_rack(
        &mut self,
        state: &mut State,
        rack_uuid: Uuid,
        initial_membership: BTreeSet<Baseboard>,
    ) -> Output {
        let State::Uninitialized = state else {
            return ApiError::RackAlreadyInitialized.into();
        };
        let total_members = initial_membership.len();
        match create_pkgs(rack_uuid, initial_membership.clone()) {
            Ok(pkgs) => {
                let request_id = Uuid::new_v4();
                let envelopes = pkgs
                    .expose_secret()
                    .into_iter()
                    .zip(initial_membership)
                    .filter_map(|(pkg, peer)| {
                        if peer == self.id {
                            // Don't send a message to ourself
                            *state = State::InitialMember(InitialMemberState {
                                pkg: pkg.clone(),
                                distributed_shares: BTreeMap::new(),
                                rack_init_state: Some(RackInitState {
                                    start: self.clock,
                                    total_members,
                                    acks: BTreeSet::from([peer]),
                                }),
                                pending_learn_requests: BTreeMap::new(),
                                rack_secret_state: None,
                            });
                            None
                        } else {
                            Some(Envelope {
                                to: peer,
                                msg: Request {
                                    id: request_id,
                                    type_: RequestType::Init(pkg.clone()),
                                }
                                .into(),
                            })
                        }
                    })
                    .collect();
                Output { persist: true, envelopes, api_output: None }
            }
            Err(e) => ApiError::RackInitFailed(e).into(),
        }
    }

    /// This call is triggered locally after RSS runs, in order to retrieve the
    /// `RackSecret` so that it can be used as input key material.
    ///
    /// if the rack secret has not already been loaded, then share retrieval
    /// will begin.
    pub fn load_rack_secret(&mut self, state: &mut State) -> Output {
        match state {
            // We don't allow retrieval before initialization
            State::Uninitialized => ApiError::RackNotInitialized.into(),
            State::Learning(_) => {
                self.pending_api_rack_secret_request = Some(self.clock);
                Output::none()
            }
            State::InitialMember(InitialMemberState {
                pkg,
                rack_secret_state,
                ..
            }) => {
                match rack_secret_state {
                    None => {
                        self.pending_api_rack_secret_request = Some(self.clock);
                        // Add our own share during initialization
                        *rack_secret_state =
                            Some(RackSecretState::Shares(BTreeMap::from([(
                                self.id.clone(),
                                pkg.share.clone(),
                            )])));
                        Output::none()
                    }
                    Some(RackSecretState::Shares(_)) => {
                        // Refresh the start time to extend the request timeout
                        self.pending_api_rack_secret_request = Some(self.clock);
                        Output::none()
                    }
                    Some(RackSecretState::Secret(rack_secret)) => {
                        // We already know the secret, so return it.
                        Output {
                            persist: false,
                            envelopes: vec![],
                            api_output: Some(Ok(ApiOutput::RackSecret(
                                rack_secret.clone(),
                            ))),
                        }
                    }
                }
            }
            State::Learned(LearnedState { pkg, rack_secret_state }) => {
                match rack_secret_state {
                    None => {
                        self.pending_api_rack_secret_request = Some(self.clock);
                        // Add our own share during initialization
                        *rack_secret_state =
                            Some(RackSecretState::Shares(BTreeMap::from([(
                                self.id.clone(),
                                pkg.share.clone(),
                            )])));
                        Output::none()
                    }
                    Some(RackSecretState::Shares(_)) => {
                        // Refresh the start time to extend the request timeout
                        self.pending_api_rack_secret_request = Some(self.clock);
                        Output::none()
                    }
                    Some(RackSecretState::Secret(rack_secret)) => {
                        // We already know the secret, so return it.
                        Output {
                            persist: false,
                            envelopes: vec![],
                            api_output: Some(Ok(ApiOutput::RackSecret(
                                rack_secret.clone(),
                            ))),
                        }
                    }
                }
            }
        }
    }

    /// An abstraction of a timer tick.
    ///
    /// Ticks mutate state and can result in message retries.
    ///
    /// Each tick represents some abstract duration of time.
    /// Timeouts are represented by number of ticks in this module, which
    /// allows for deterministic property based tests in a straightforward manner.
    /// On each tick, the current duration since start is passed in. We only
    /// deal in relative time needed for timeouts, and not absolute time. This
    /// strategy allows for deterministic property based tests.
    pub fn tick(&mut self) -> Output {
        self.common.clock += 1;
        let state = self.state.take().unwrap();
        let (new_state, output) = state.tick(&mut self.common);
        self.state = Some(new_state);
        output
    }

    /// A connection has been established an a peer has been learned.
    /// This peer may or may not already be known by the FSM.
    pub fn insert_peer(&mut self, peer: Baseboard) {
        self.peers.insert(peer);
    }

    /// When a the upper layer sees the advertised prefix for a peer go away,
    /// and not just a dropped connection, it will inform the FSM to remove the peer.
    ///
    /// This is a useful mechanism to prevent generating requests for failed sleds.
    pub fn remove_peer(&mut self, peer: Baseboard) {
        self.peers.remove(&peer);
    }

    /// Handle a message from a peer.
    ///
    /// Return whether persistent state needs syncing to disk and a set of
    /// messages to send to other peers. Persistant state must be saved by
    /// the caller and safely persisted before messages are sent, or the next
    /// message is handled here.
    pub fn handle(&mut self, from: Baseboard, msg: Msg) -> Output {
        match msg {
            Msg::Req(req) => self.handle_request(from, req),
            Msg::Rsp(rsp) => self.handle_response(from, rsp),
        }
    }

    // Handle a `Request` message
    fn handle_request(&mut self, from: Baseboard, request: Request) -> Output {
        let state = self.state.take().unwrap();
        let (new_state, output) = state.handle_request(
            &mut self.common,
            from,
            request.id,
            request.type_,
        );
        self.state = Some(new_state);
        output
    }

    fn handle_response(
        &mut self,
        from: Baseboard,
        response: Response,
    ) -> Output {
        let state = self.state.take().unwrap();
        let (new_state, output) = state.handle_response(
            &mut self.common,
            from,
            response.request_id,
            response.type_,
        );
        self.state = Some(new_state);
        output
    }
}

// Send a request for a share to each peer
// TODO: Hang this off `FsmCommonData` ?
pub fn broadcast_share_requests(
    common: &FsmCommonData,
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
    let envelopes = common
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
pub fn next_peer(
    current: &Baseboard,
    peers: &BTreeSet<Baseboard>,
) -> Option<Baseboard> {
    if let Some(index) = peers.iter().position(|x| x == current) {
        let next_index = (index + 1) % peers.len();
        peers.iter().nth(next_index).cloned()
    } else {
        peers.first().cloned()
    }
}
