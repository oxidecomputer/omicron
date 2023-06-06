// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::messages::{
    Envelope, Error, Msg, Request, RequestType, Response, ResponseType,
};
use crate::trust_quorum::{LearnedSharePkgV0, RackSecret, SharePkgV0};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// A number of clock ticks from some unknown epoch
type Ticks = usize;

// An index into an encrypted share
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareIdx(usize);

/// Output from a a call to [`Fsm::Handle`]
pub struct Output {
    /// Possible state that needs persisting before any messages are sent
    pub persist: bool,

    /// Messages wrapped with a destination
    pub envelopes: Vec<Envelope>,

    // A reply to the user of the FSM API
    pub api_output: Option<ApiOutput>,
}

impl Output {
    pub fn none() -> Output {
        Output { persist: false, envelopes: vec![], api_output: None }
    }

    // Send a request directly to a peer
    fn request(to: Baseboard, type_: RequestType) -> Output {
        let request = Request { id: Uuid::new_v4(), type_ };
        Output {
            persist: false,
            envelopes: vec![Envelope { to, msg: request.into() }],
            api_output: None,
        }
    }

    // Return a response directly to a peer that doesn't require persistence
    fn respond(to: Baseboard, request_id: Uuid, type_: ResponseType) -> Output {
        let response = Response { request_id, type_ };
        Output {
            persist: false,
            envelopes: vec![Envelope { to, msg: response.into() }],
            api_output: None,
        }
    }

    // Indicate to the caller that state must be perisisted and then a response
    // returned to the peer.
    fn persist_and_respond(
        to: Baseboard,
        request_id: Uuid,
        type_: ResponseType,
    ) -> Output {
        let response = Response { request_id, type_ };
        Output {
            persist: true,
            envelopes: vec![Envelope { to, msg: response.into() }],
            api_output: None,
        }
    }

    fn rack_init_timeout(unacked_peers: BTreeSet<Baseboard>) -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::RackInitTimeout { unacked_peers }),
        }
    }
}

/// The caller of the API (aka the peer/network layer will sometimes need to get
/// messages delivered to it, such as when rack initialization has completed. We
/// provide this information in `Output::api_output`.
pub enum ApiOutput {
    RackInitComplete,
    RackInitTimeout { unacked_peers: BTreeSet<Baseboard> },
}

/// State stored on the M.2s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    // The generation number for ledger writing purposes on both M.2s
    pub version: u32,
    pub fsm_state: State,
}

/// The possible states of a peer FSM
///
/// The following are valid state transitions
///
///   * Uninitialized -> InitialMember
///   * Uninitialized -> Learning -> Learned
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum State {
    /// The peer does not yet know whether it is an initial member of the group
    /// or a learner.
    Uninitialized,

    /// The peer is an initial member of the group setup via RSS
    InitialMember {
        pkg: SharePkgV0,

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
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    },

    /// The peer has been instructed to learn a share
    Learning,

    /// The peer has learned its share
    Learned { pkg: LearnedSharePkgV0 },
}

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
    peer: Baseboard,
    start: Ticks,
}

impl LearnAttempt {
    fn expired(&self, now: Ticks, timeout: Ticks) -> bool {
        now.saturating_sub(self.start) >= timeout
    }
}

/// Metadata associated with a request that is keyed by `Baseboard`
pub struct RequestMetadata {
    request_id: Uuid,
    start: Ticks,
}

/// Whether we are in the process of learning enough shares to recompute the
/// rack secret or if we have already recomputed it.
pub enum RackSecretState {
    // TODO: Zeroize or wrap in a Secert
    Shares(BTreeMap<Baseboard, Vec<u8>>),
    Secret(RackSecret),
}

/// Initialization state for tracking rack init on the coordinator node (the one
/// local to RSS)
pub struct RackInitState {
    start: Ticks,
    total_members: usize,
    acks: BTreeSet<Baseboard>,
}

impl RackInitState {
    pub fn timer_expired(&self, now: Ticks, timeout: Ticks) -> bool {
        now.saturating_sub(self.start) >= timeout
    }
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
pub struct Fsm {
    // Unique IDs of this peer
    id: Baseboard,

    // Configuration of the FSM
    config: Config,

    // Unique IDs of known peers
    peers: BTreeSet<Baseboard>,

    // The current time in ticks since the creation of the FSM
    clock: Ticks,

    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    rack_secret_state: Option<RackSecretState>,

    // Pending learn requests from other peers mapped to their start time.
    //
    // When a peer attempts to learn a share, we may not be able to give it
    // one because we cannot yet recompute the rack secret. We queue requests
    // here and respond when we can recompute the rack secret. If we timeout before
    // we can recompute the rack secret, we respond with a timeout Error.
    //
    // Note that if we get a new `RequestType::Learn` from a peer that is already
    // pending, we will reset the start time.
    //
    // TODO: If we add message sequence numbers we must include them in the
    // value here and we should discard the old message still.
    pending_learn_requests: BTreeMap<Baseboard, RequestMetadata>,

    // Acknowledgements tracked during rack initialization if this node is
    // the one acting as coordinator (local to RSS).
    rack_init_state: Option<RackInitState>,

    // Our own attempt to learn our share
    //
    // While this is only valid in the `Learning` state, we do not want to
    // persist it, because it can possibly be updated frequently and use up
    // write cycles on the M.2 SSDs.
    learn_attempt: Option<LearnAttempt>,
}

/// Configuration of the FSM
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
            rack_secret_state: None,
            pending_learn_requests: BTreeMap::new(),
            rack_init_state: None,
            learn_attempt: None,
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
    pub fn tick(&mut self, state: &mut PersistentState) -> Output {
        self.clock += 1;

        match &state.fsm_state {
            State::Learning => match &self.learn_attempt {
                None => {
                    if let Some(peer) = self.peers.first() {
                        self.send_learn_request(peer.clone());
                    }
                    // No peers to learn from
                }
                Some(attempt) => {
                    if attempt.expired(self.clock, self.config.learn_timeout) {
                        if let Some(peer) =
                            next_peer(&attempt.peer, &self.peers)
                        {
                            self.send_learn_request(peer);
                        }
                        // No peers to learn from
                        self.learn_attempt = None;
                    }
                }
            },
            State::InitialMember { pkg, .. } => {
                if let Some(rack_init_state) = &mut self.rack_init_state {
                    if rack_init_state.timer_expired(
                        self.clock,
                        self.config.rack_init_timeout,
                    ) {
                        let unacked_peers = pkg
                            .initial_membership
                            .difference(&rack_init_state.acks)
                            .cloned()
                            .collect();
                        return Output::rack_init_timeout(unacked_peers);
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
        state: &mut PersistentState,
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
        state: &mut PersistentState,
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
        state: &mut PersistentState,
        from: Baseboard,
        request_id: Uuid,
        new_pkg: SharePkgV0,
    ) -> Output {
        match &mut state.fsm_state {
            State::Uninitialized => {
                state.fsm_state = State::InitialMember {
                    pkg: new_pkg,
                    distributed_shares: BTreeMap::new(),
                };
                state.version += 1;
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                )
            }
            State::InitialMember { pkg, .. } => {
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
            State::Learning => {
                Output::respond(from, request_id, Error::AlreadyLearning.into())
            }
            State::Learned { pkg } => {
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
        state: &mut PersistentState,
        from: Baseboard,
        request_id: Uuid,
    ) -> Output {
        match &mut state.fsm_state {
            State::Uninitialized => {
                state.fsm_state = State::Learning;
                state.version += 1;
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                )
            }
            State::InitialMember { pkg, .. } => {
                let rack_uuid = pkg.rack_uuid;
                Output::respond(
                    from,
                    request_id,
                    Error::AlreadyInitialized { rack_uuid }.into(),
                )
            }
            State::Learning | State::Learned { .. } => {
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
        state: &mut PersistentState,
        from: Baseboard,
        request_id: Uuid,
        rack_uuid: Uuid,
    ) -> Output {
        match &state.fsm_state {
            State::Uninitialized => {
                Output::respond(from, request_id, Error::NotInitialized.into())
            }
            State::Learning => {
                Output::respond(from, request_id, Error::StillLearning.into())
            }
            State::InitialMember { pkg, .. } => Output::respond(
                from,
                request_id,
                ResponseType::Share(pkg.share.clone()),
            ),
            State::Learned { pkg } => Output::respond(
                from,
                request_id,
                ResponseType::Share(pkg.share.clone()),
            ),
        }
    }

    // Handle a `RequestType::Learn` message
    fn on_learn(
        &mut self,
        state: &mut PersistentState,
        from: Baseboard,
        request_id: Uuid,
    ) -> Output {
        match &mut state.fsm_state {
            State::Uninitialized => {
                Output::respond(from, request_id, Error::NotInitialized.into())
            }
            State::Learning => {
                Output::respond(from, request_id, Error::StillLearning.into())
            }
            State::Learned { .. } => Output::respond(
                from,
                request_id,
                Error::CannotSpareAShare.into(),
            ),
            State::InitialMember { pkg, distributed_shares } => {
                match &self.rack_secret_state {
                    Some(RackSecretState::Secret(rack_secret)) => {
                        // We already know the rack secret so respond to the
                        // peer.
                        send_share_response(
                            &mut state.version,
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
                        self.pending_learn_requests.insert(
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
                        self.pending_learn_requests.insert(
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
        state: &mut PersistentState,
        from: Baseboard,
        response: Response,
    ) -> Output {
        match response.type_ {
            ResponseType::InitAck => {
                self.on_init_ack(state, from, response.request_id)
            }
            /*ResponseType::Share(share) => {
                // TODO: Add the share to `self.rack_secrert_state`
                // If we have enough of them then we respond to any pending
                // requests that require a recomputation of the rack secret.
                // We must also verify the share is valid here.
                // TODO: Should we send a local message for response error
                // logging or log directly from the FSM?
            }
            ResponseType::Pkg(pkg) => {
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

    fn on_init_ack(
        &mut self,
        state: &mut PersistentState,
        from: Baseboard,
        request_id: Uuid,
    ) -> Output {
        if let Some(rack_init_state) = &mut self.rack_init_state {
            rack_init_state.acks.insert(from);
            if rack_init_state.acks.len() == rack_init_state.total_members {
                return Output {
                    persist: false,
                    envelopes: vec![],
                    api_output: Some(ApiOutput::RackInitComplete),
                };
            }
        }
        //TODO: Log receipt of a message that was sent to the wrong peer?
        Output::none()
    }

    // Send a `RequestType::Learn` message
    fn send_learn_request(&mut self, peer: Baseboard) -> Output {
        self.learn_attempt =
            Some(LearnAttempt { peer: peer.clone(), start: self.clock });
        Output::request(peer, RequestType::Learn)
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

// Send a `ResponseType::Share` message once we have recomputed the rack secret
fn send_share_response(
    persistent_state_version: &mut u32,
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
                        *persistent_state_version += 1;
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
