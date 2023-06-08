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
    InitialMemberState, LearnAttempt, LearnedState, PersistentFsmState,
    PersistentState, RackInitState, RackSecretState, RequestMetadata, ShareIdx,
    State, Ticks,
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
    // Unique IDs of this peer
    id: Baseboard,

    // Configuration of the FSM
    config: Config,

    // Unique IDs of known peers
    peers: BTreeSet<Baseboard>,

    // The current time in ticks since the creation of the FSM
    clock: Ticks,

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
    pending_api_rack_secret_request: Option<Ticks>,
}

/// Configuration of the FSM
#[derive(Debug, Clone)]
pub struct Config {
    pub retry_timeout: Ticks,
    pub learn_timeout: Ticks,
    pub rack_init_timeout: Ticks,
}

impl Fsm {
    pub fn new(id: Baseboard, config: Config) -> Fsm {
        Fsm {
            id,
            config,
            peers: BTreeSet::new(),
            clock: 0,
            pending_api_rack_secret_request: None,
        }
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
    pub fn tick(&mut self, state: &mut State) -> Output {
        self.clock += 1;

        match state {
            State::Learning(Some(attempt)) => {
                if attempt.expired(self.clock, self.config.learn_timeout) {
                    if let Some(peer) = next_peer(&attempt.peer, &self.peers) {
                        attempt.peer = peer.clone();
                        attempt.start = self.clock;
                        return Output::request(peer, RequestType::Learn);
                    }
                    // No peers to learn from
                    *state = State::Learning(None);
                }
            }
            State::Learning(None) => {
                if let Some(peer) = self.peers.first() {
                    *state = State::Learning(Some(LearnAttempt {
                        peer: peer.clone(),
                        start: self.clock,
                    }));
                    return Output::request(peer.clone(), RequestType::Learn);
                }
                // No peers to learn from
            }
            State::InitialMember(InitialMemberState {
                pkg,
                rack_init_state,
                ..
            }) => {
                if let Some(rack_init_state2) = rack_init_state {
                    if rack_init_state2.timer_expired(
                        self.clock,
                        self.config.rack_init_timeout,
                    ) {
                        let unacked_peers = pkg
                            .initial_membership
                            .difference(&rack_init_state2.acks)
                            .cloned()
                            .collect();
                        *rack_init_state = None;
                        return ApiError::RackInitTimeout { unacked_peers }
                            .into();
                    }
                }
            }
            _ => (),
        }
        Output::none()
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
    pub fn handle(
        &mut self,
        state: &mut State,
        from: Baseboard,
        msg: Msg,
    ) -> Output {
        match msg {
            Msg::Req(req) => self.handle_request(state, from, req),
            Msg::Rsp(rsp) => self.handle_response(state, from, rsp),
        }
    }

    // Handle a `Request` message
    fn handle_request(
        &mut self,
        state: &mut State,
        from: Baseboard,
        request: Request,
    ) -> Output {
        match request.type_ {
            RequestType::Init(new_pkg) => {
                self.on_init(state, from, request.id, new_pkg)
            }
            RequestType::InitLearner => {
                self.on_init_learner(state, from, request.id)
            }
            RequestType::GetShare { rack_uuid } => {
                self.on_get_share(state, from, request.id, rack_uuid)
            }
            RequestType::Learn => self.on_learn(state, from, request.id),
        }
    }

    // Handle a `RequestType::Init` message
    fn on_init(
        &mut self,
        state: &mut State,
        from: Baseboard,
        request_id: Uuid,
        new_pkg: SharePkgV0,
    ) -> Output {
        match state {
            State::Uninitialized => {
                *state = State::InitialMember(InitialMemberState::new(
                    new_pkg,
                    BTreeMap::new(),
                ));
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                )
            }
            State::InitialMember(InitialMemberState { pkg, .. }) => {
                if new_pkg == *pkg {
                    // Idempotent response given same pkg
                    Output::respond(from, request_id, ResponseType::InitAck)
                } else {
                    let rack_uuid = pkg.rack_uuid;
                    Output::respond(
                        from,
                        request_id,
                        Error::AlreadyInitialized { rack_uuid }.into(),
                    )
                }
            }
            State::Learning(_) => {
                Output::respond(from, request_id, Error::AlreadyLearning.into())
            }
            State::Learned(LearnedState { pkg, .. }) => {
                let rack_uuid = pkg.rack_uuid;
                Output::respond(
                    from,
                    request_id,
                    Error::AlreadyLearned { rack_uuid }.into(),
                )
            }
        }
    }

    // Handle a `RequestType::InitLearner` message
    fn on_init_learner(
        &mut self,
        state: &mut State,
        from: Baseboard,
        request_id: Uuid,
    ) -> Output {
        match state {
            State::Uninitialized => {
                *state = State::Learning(None);
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                )
            }
            State::InitialMember(InitialMemberState { pkg, .. }) => {
                let rack_uuid = pkg.rack_uuid;
                Output::respond(
                    from,
                    request_id,
                    Error::AlreadyInitialized { rack_uuid }.into(),
                )
            }
            State::Learning(_) | State::Learned(_) => {
                // Idempotent
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                )
            }
        }
    }

    // Handle a `RequestType::GetShare` message
    fn on_get_share(
        &self,
        state: &State,
        from: Baseboard,
        request_id: Uuid,
        rack_uuid: Uuid,
    ) -> Output {
        match state {
            State::Uninitialized => {
                Output::respond(from, request_id, Error::NotInitialized.into())
            }
            State::Learning(_) => {
                Output::respond(from, request_id, Error::StillLearning.into())
            }
            State::InitialMember(InitialMemberState { pkg, .. }) => {
                Output::respond(
                    from,
                    request_id,
                    ResponseType::Share(pkg.share.clone()),
                )
            }
            State::Learned(LearnedState { pkg, .. }) => Output::respond(
                from,
                request_id,
                ResponseType::Share(pkg.share.clone()),
            ),
        }
    }

    // Handle a `RequestType::Learn` message
    fn on_learn(
        &mut self,
        state: &mut State,
        from: Baseboard,
        request_id: Uuid,
    ) -> Output {
        match state {
            State::Uninitialized => {
                Output::respond(from, request_id, Error::NotInitialized.into())
            }
            State::Learning(_) => {
                Output::respond(from, request_id, Error::StillLearning.into())
            }
            State::Learned(_) => Output::respond(
                from,
                request_id,
                Error::CannotSpareAShare.into(),
            ),
            State::InitialMember(InitialMemberState {
                pkg,
                distributed_shares,
                rack_secret_state,
                pending_learn_requests,
                ..
            }) => {
                match rack_secret_state {
                    Some(RackSecretState::Secret(rack_secret)) => {
                        // We already know the rack secret so respond to the
                        // peer.
                        send_share_response(
                            from,
                            request_id,
                            &pkg,
                            distributed_shares,
                            rack_secret,
                        )
                    }
                    Some(RackSecretState::Shares(shares)) => {
                        // Register the request and try to collect enough
                        // shares to unlock the rack secret. When we have
                        // enough we will respond to the caller.
                        pending_learn_requests.insert(
                            from,
                            RequestMetadata { request_id, start: self.clock },
                        );
                        self.broadcast_share_requests(
                            pkg.rack_uuid,
                            Some(shares),
                        )
                    }
                    None => {
                        // Register the request and try to collect enough
                        // shares to unlock the rack secret. When we have
                        // enough we will respond to the caller.
                        pending_learn_requests.insert(
                            from,
                            RequestMetadata { request_id, start: self.clock },
                        );
                        self.broadcast_share_requests(pkg.rack_uuid, None)
                    }
                }
            }
        }
    }

    fn handle_response(
        &mut self,
        state: &mut State,
        from: Baseboard,
        response: Response,
    ) -> Output {
        match response.type_ {
            ResponseType::InitAck => {
                self.on_init_ack(state, from, response.request_id)
            }
            ResponseType::Share(share) => self.on_share(state, from, share),
            /*ResponseType::Pkg(pkg) => {
                // TODO: If we are in `Learning` state then set the state to `Learned`
                // with this pkg. Otherwise, discard the message and log it.
            }
            ResponseType::Error(error) => {
                // TODO: If the error is relevant to any outstanding requests
                // then handle them appropriately. For instance, if it's related
                // to initialization then inform the caller. If it's a `Learn` request
                // then cancel the attempt and try the next peer.
                // We almost certainly need sequence numbers to track any
                // outstanding requests.
            }
            */
            _ => unimplemented!(),
        }
    }

    // Handle a `ResponseType::InitAck` message
    fn on_init_ack(
        &mut self,
        state: &mut State,
        from: Baseboard,
        request_id: Uuid,
    ) -> Output {
        match state {
            State::InitialMember(InitialMemberState {
                rack_init_state,
                ..
            }) => {
                let Some(rack_init_state2) = rack_init_state else {
                    // We're done initializing
                    return Output::none();
                };
                rack_init_state2.acks.insert(from);
                if rack_init_state2.acks.len() == rack_init_state2.total_members
                {
                    *rack_init_state = None;
                    Output::rack_init_complete()
                } else {
                    Output::none()
                }
            }
            // TODO: Log message sent to wrong peer?
            _ => Output::none(),
        }
    }

    // Handle a `ResponseType::Share` message
    fn on_share(
        &mut self,
        state: &mut State,
        from: Baseboard,
        share: Vec<u8>,
    ) -> Output {
        match state {
            State::Uninitialized | State::Learning(_) => {
                ApiError::UnexpectedShareReceived { from, state: state.name() }
                    .into()
            }
            State::InitialMember(state) => {
                self.on_share_as_initial_member(from, share, state)
            }
            State::Learned(LearnedState { pkg, rack_secret_state }) => {
                unimplemented!()
            }
        }
    }

    /// Handle a `ResponseType::Share` message in `State::InitialMember`
    fn on_share_as_initial_member(
        &mut self,
        from: Baseboard,
        share: Vec<u8>,
        state: &mut InitialMemberState,
    ) -> Output {
        let InitialMemberState {
            pkg,
            distributed_shares,
            pending_learn_requests,
            rack_secret_state,
            ..
        } = state;

        match rack_secret_state {
            None => {
                ApiError::UnexpectedShareReceived { from, state: state.name() }
                    .into()
            }
            Some(RackSecretState::Secret(_)) => {
                // We already have the rack secret, just drop the extra share
                Output::none()
            }
            Some(RackSecretState::Shares(shares)) => {
                // 4. If we have enough shares compute the rack secret
                // 5. Resolve any outstanding requests that require the rack secret

                /// Compute and validate hash of the received key share
                if let Err(api_error) =
                    validate_share(&from, &share, &pkg.share_digests)
                {
                    return api_error.into();
                }

                // Add the share to our current set
                shares.insert(from, share);

                // Do we have enough shares to compute the rack secret?
                if shares.len() == pkg.threshold as usize {
                    let to_combine: Vec<_> = shares.values().cloned().collect();
                    match RackSecret::combine_shares(&to_combine) {
                        Ok(rack_secret) => {
                            // If we have any pending peer learn requests, we
                            // can now resolve them.
                            let (persist, envelopes) = resolve_learn_requests(
                                &rack_secret,
                                &pkg,
                                distributed_shares,
                                pending_learn_requests,
                            );
                            // If we have a pending API request for the rack secret we can
                            // resolve it now.
                            let api_output = if self
                                .pending_api_rack_secret_request
                                .is_some()
                            {
                                self.pending_api_rack_secret_request = None;
                                Some(Ok(ApiOutput::RackSecret(
                                    rack_secret.clone(),
                                )))
                            } else {
                                None
                            };

                            *rack_secret_state =
                                Some(RackSecretState::Secret(rack_secret));

                            // TODO: resolve any API requests for the rack secret
                            Output { persist, envelopes, api_output }
                        }
                        Err(e) => {
                            // Resolve all pending learn requests with an error
                            let envelopes = resolve_learn_requests_with_error(
                                Error::FailedToReconstructRackSecret,
                                pending_learn_requests,
                            );
                            // If we have a pending API request for the rack secret we can
                            // resolve it now.
                            let api_output = if self
                                .pending_api_rack_secret_request
                                .is_some()
                            {
                                self.pending_api_rack_secret_request = None;
                                Some(Err(
                                    ApiError::FailedToReconstructRackSecret,
                                ))
                            } else {
                                None
                            };
                            Output {
                                persist: false,
                                envelopes,
                                api_output: api_output,
                            }
                        }
                    }
                } else {
                    // Still waiting to receive more shares
                    Output::none()
                }
            }
        }
    }

    // Send a request for a share to each peer
    fn broadcast_share_requests(
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
}

// We've just recomputed the rack secret. Now resolve any pending learn
// requests. Return whether we need to persist state and any envelopes to
// send to peers.
fn resolve_learn_requests(
    rack_secret: &RackSecret,
    pkg: &SharePkgV0,
    distributed_shares: &mut BTreeMap<Baseboard, ShareIdx>,
    pending_learn_requests: &mut BTreeMap<Baseboard, RequestMetadata>,
) -> (bool, Vec<Envelope>) {
    let mut persist = false;
    let mut envelopes = Vec::with_capacity(pending_learn_requests.len());
    match pkg.decrypt_shares(rack_secret) {
        Ok(shares) => {
            while let Some((to, metadata)) = pending_learn_requests.pop_first()
            {
                if let Some(idx) = distributed_shares.get(&to) {
                    // The share was already handed out to this
                    // peer. Give back the same one.
                    let share = shares.expose_secret()[idx.0].clone();
                    let msg = Response {
                        request_id: metadata.request_id,
                        type_: ResponseType::Share(share),
                    };
                    envelopes.push(Envelope { to, msg: msg.into() });
                } else {
                    // We need to pick a share to hand out and
                    // persist that fact. We find the highest currently used
                    // index and add 1 or we select index 0.
                    let idx = distributed_shares
                        .values()
                        .max()
                        .cloned()
                        .map(|idx| idx.0 + 1)
                        .unwrap_or(0);

                    match shares.expose_secret().get(idx) {
                        Some(share) => {
                            distributed_shares
                                .insert(to.clone(), ShareIdx(idx));
                            let msg = Response {
                                request_id: metadata.request_id,
                                type_: ResponseType::Share(share.clone()),
                            };
                            envelopes.push(Envelope { to, msg: msg.into() });
                            // We just handed out a new share
                            // Ensure we perist this state
                            persist = true
                        }
                        None => {
                            let msg = Response {
                                request_id: metadata.request_id,
                                type_: Error::CannotSpareAShare.into(),
                            };
                            envelopes.push(Envelope { to, msg: msg.into() });
                        }
                    }
                }
            }
        }
        Err(_) => {
            envelopes = resolve_learn_requests_with_error(
                Error::FailedToDecryptShares,
                pending_learn_requests,
            );
        }
    }

    (persist, envelopes)
}

/// An error prevented successful resolution of learn requests
fn resolve_learn_requests_with_error(
    error: Error,
    pending_learn_requests: &mut BTreeMap<Baseboard, RequestMetadata>,
) -> Vec<Envelope> {
    let mut envelopes = Vec::with_capacity(pending_learn_requests.len());
    while let Some((to, metadata)) = pending_learn_requests.pop_first() {
        let msg =
            Response { request_id: metadata.request_id, type_: error.into() };
        envelopes.push(Envelope { to, msg: msg.into() })
    }
    envelopes
}

// Send a `ResponseType::Share` message once we have recomputed the rack secret
fn send_share_response(
    from: Baseboard,
    request_id: Uuid,
    pkg: &SharePkgV0,
    distributed_shares: &mut BTreeMap<Baseboard, ShareIdx>,
    rack_secret: &RackSecret,
) -> Output {
    match pkg.decrypt_shares(rack_secret) {
        Ok(shares) => {
            if let Some(idx) = distributed_shares.get(&from) {
                // The share was already handed out to this
                // peer. Give back the same one.
                let share = shares.expose_secret()[idx.0].clone();
                Output::respond(from, request_id, ResponseType::Share(share))
            } else {
                // We need to pick a share to hand out and
                // persist that fact. We find the highest currently used
                // index and add 1 or we select index 0.
                let idx = distributed_shares
                    .values()
                    .max()
                    .cloned()
                    .map(|idx| idx.0 + 1)
                    .unwrap_or(0);

                match shares.expose_secret().get(idx) {
                    Some(share) => {
                        distributed_shares.insert(from.clone(), ShareIdx(idx));
                        Output::persist_and_respond(
                            from,
                            request_id,
                            ResponseType::Share(share.clone()),
                        )
                    }
                    None => Output::respond(
                        from,
                        request_id,
                        Error::CannotSpareAShare.into(),
                    ),
                }
            }
        }
        Err(_) => Output::respond(
            from,
            request_id,
            Error::FailedToDecryptShares.into(),
        ),
    }
}

// Round-robin peer selection
fn next_peer(
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
