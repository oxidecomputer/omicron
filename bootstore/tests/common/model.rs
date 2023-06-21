// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model state for bootstore peers

use bootstore::schemes::v0::{
    Config, LearnAttempt, Msg, Request, RequestType, Response, ResponseType,
    Ticks,
};
use derive_more::From;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

// A simplified version of `state::RackSecretState`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelRackSecretState {
    Empty,
    Retrieving {
        // We have received shares from these peers so far
        received: BTreeSet<Baseboard>,
        // When does this request timeout?
        expiry: Ticks,
        // How many shares should we receive before we move into `Computed`
        threshold: usize,
    },
    Computed {
        // How long will we stay in computed before we drop the secret and go
        // back to `Empty`
        expiry: Ticks,
    },
}

type Expiry = Ticks;

// A simplified version of `state::State`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelState {
    Uninitialized,
    InitialMember {
        pending_learn_requests: BTreeMap<Baseboard, Expiry>,
        already_learned: BTreeSet<Baseboard>,
    },
    Learning {
        attempt: Option<LearnAttempt>,
    },
    Learned,
}

// A simplified version of a peer `FSM`
#[derive(Debug, Clone)]
pub struct PeerModel {
    pub clock: Ticks,
    pub config: Config,
    pub threshold: usize,
    pub id: Baseboard,
    pub state: ModelState,
    pub rack_secret_state: ModelRackSecretState,
    pub connected: BTreeSet<Baseboard>,
}

impl PeerModel {
    pub fn new(id: Baseboard, threshold: usize, config: Config) -> PeerModel {
        PeerModel {
            clock: 0,
            config,
            threshold,
            id,
            rack_secret_state: ModelRackSecretState::Empty,
            state: ModelState::Uninitialized,
            connected: BTreeSet::new(),
        }
    }

    // Copied from `FsmCommonData::next_peer`
    pub fn next_peer(&self, current: &Baseboard) -> Option<Baseboard> {
        if let Some(index) = self.connected.iter().position(|x| x == current) {
            let next_index = (index + 1) % self.connected.len();
            self.connected.iter().nth(next_index).cloned()
        } else {
            self.connected.first().cloned()
        }
    }

    // A simplified version of a real message handler for a peer
    // We don't track share values or return shares to learners here
    // We just return enough information so that the proptest can verify
    // the real output of a message handler is correct.
    pub fn handle_msg(&mut self, from: &Baseboard, msg: Msg) -> ExpectedOutput {
        match msg {
            Msg::Req(Request { type_: request, .. }) => {
                self.handle_request(from, request)
            }
            Msg::Rsp(Response { type_: response, .. }) => {
                self.handle_response(from, response)
            }
        }
    }

    fn handle_request(
        &mut self,
        from: &Baseboard,
        request: RequestType,
    ) -> ExpectedOutput {
        match request {
            RequestType::GetShare { .. } => match self.state {
                ModelState::InitialMember { .. } | ModelState::Learned => {
                    ExpectedMsg::Share.into()
                }
                _ => ExpectedMsg::Error.into(),
            },
            RequestType::Learn { .. } => match &mut self.state {
                ModelState::InitialMember {
                    pending_learn_requests,
                    already_learned,
                } => {
                    let new_expiry =
                        self.clock + self.config.rack_secret_request_timeout;
                    let learn_expiry = self.clock + self.config.learn_timeout;
                    let threshold = self.threshold;
                    match &mut self.rack_secret_state {
                        state @ ModelRackSecretState::Empty => {
                            // Transition our model state to "Retrieving"
                            *state = ModelRackSecretState::Retrieving {
                                // Make sure to add ourself
                                received: BTreeSet::from([self.id.clone()]),
                                expiry: new_expiry,
                                threshold,
                            };
                            pending_learn_requests
                                .insert(from.clone(), learn_expiry);
                            // We expect a share to be sent to all connected peers
                            ExpectedMsg::BroadcastGetShare(
                                self.connected.clone(),
                            )
                            .into()
                        }
                        ModelRackSecretState::Retrieving { expiry, .. } => {
                            // We extend the timeout on a reload
                            *expiry = new_expiry;
                            pending_learn_requests
                                .insert(from.clone(), learn_expiry);
                            ExpectedOutput::none()
                        }
                        ModelRackSecretState::Computed { .. } => {
                            // For now we assume an initial member always
                            // has enough shares to hand out. This is a basic
                            // limitation of the V0 protocol and it should be
                            // upheld for the amount of learners and initial
                            // members used in a test.
                            //
                            // We persist when we hand out a package, to mark
                            // it as used.
                            let persist = if !already_learned.contains(from) {
                                already_learned.insert(from.clone());
                                true
                            } else {
                                false
                            };
                            ExpectedOutput {
                                persist,
                                msg: Some(ExpectedMsg::Pkg),
                                api_output: None,
                            }
                        }
                    }
                }
                _ => ExpectedMsg::Error.into(),
            },
            RequestType::Init(_) => {
                // Rack init is always done as part of test setup, so this
                // will alawys return an error
                ExpectedMsg::Error.into()
            }
        }
    }

    fn handle_response(
        &mut self,
        from: &Baseboard,
        response: ResponseType,
    ) -> ExpectedOutput {
        match response {
            ResponseType::InitAck => ExpectedOutput::none(),
            ResponseType::Share(_) => {
                let expiry =
                    self.clock + self.config.rack_secret_request_timeout;

                let pending_and_learned = match &mut self.state {
                    ModelState::InitialMember {
                        pending_learn_requests,
                        already_learned,
                    } => Some((pending_learn_requests, already_learned)),
                    ModelState::Learned => None,
                    _ => {
                        return ExpectedApiError::UnexpectedResponse.into();
                    }
                };

                // We are in `InitialMember` or `Learning` state at this point
                match &mut self.rack_secret_state {
                    ModelRackSecretState::Empty => {
                        ExpectedApiError::UnexpectedResponse.into()
                    }
                    ModelRackSecretState::Retrieving {
                        received,
                        threshold,
                        ..
                    } => {
                        received.insert(from.clone());
                        if received.len() == *threshold {
                            self.rack_secret_state =
                                ModelRackSecretState::Computed { expiry };
                            // Are we resolving any pending learn requests?
                            let (persist, msg) =
                                if let Some((pending, already_learned)) =
                                    pending_and_learned
                                {
                                    let mut persist = false;
                                    for p in pending.keys() {
                                        if !already_learned.contains(p) {
                                            persist = true;
                                            already_learned.insert(p.clone());
                                        }
                                    }
                                    let msg = Some(ExpectedMsg::BroadcastPkg(
                                        pending.keys().cloned().collect(),
                                    ));
                                    pending.clear();
                                    (persist, msg)
                                } else {
                                    (false, None)
                                };
                            ExpectedOutput {
                                persist,
                                msg,
                                api_output: Some(Ok(
                                    ExpectedApiOutput::RackSecretLoaded,
                                )),
                            }
                        } else {
                            ExpectedOutput::none()
                        }
                    }
                    ModelRackSecretState::Computed { .. } => {
                        // Most likely a late response, just ignore it as
                        // we do in the real code.
                        ExpectedOutput::none()
                    }
                }
            }
            ResponseType::Pkg(_) => match &mut self.state {
                state @ ModelState::Learning { .. } => {
                    *state = ModelState::Learned;
                    ExpectedOutput {
                        persist: true,
                        msg: None,
                        api_output: None,
                    }
                }
                _ => ExpectedApiError::UnexpectedResponse.into(),
            },
            ResponseType::Error(_) => {
                ExpectedApiError::ErrorResponseReceived.into()
            }
        }
    }
}

// A simplified version of `fsm_output::Output`
#[derive(Debug, From, PartialEq, Eq)]
pub struct ExpectedOutput {
    pub persist: bool,
    pub msg: Option<ExpectedMsg>,
    pub api_output: Option<Result<ExpectedApiOutput, ExpectedApiError>>,
}

impl ExpectedOutput {
    fn none() -> ExpectedOutput {
        ExpectedOutput { persist: false, msg: None, api_output: None }
    }
}

impl From<ExpectedApiError> for ExpectedOutput {
    fn from(err: ExpectedApiError) -> Self {
        ExpectedOutput { persist: false, msg: None, api_output: Some(Err(err)) }
    }
}

impl From<ExpectedApiOutput> for ExpectedOutput {
    fn from(value: ExpectedApiOutput) -> Self {
        ExpectedOutput {
            persist: false,
            msg: None,
            api_output: Some(Ok(value)),
        }
    }
}

impl From<ExpectedMsg> for ExpectedOutput {
    fn from(msg: ExpectedMsg) -> Self {
        ExpectedOutput { persist: false, msg: Some(msg), api_output: None }
    }
}

// A simplified version of `fsm_output::ApiOutput`
#[derive(Debug, PartialEq, Eq)]
pub enum ExpectedApiOutput {
    RackSecretLoaded,
}

// A simplified version of `fsm_output::ApiError`
#[derive(Debug, PartialEq, Eq)]
pub enum ExpectedApiError {
    RackSecretTimeout,
    PeerAlreadyInitialized,
    RackNotInitialized,
    StillLearning,
    UnexpectedResponse,
    ErrorResponseReceived,
}

// Messages expected to be sent for certain model callbacks
//
// We ignore `Init` and `InitAck` messages as rack init is always successful as
// part of test setup.
#[derive(Debug, PartialEq, Eq)]
pub enum ExpectedMsg {
    GetShare,
    Learn { destination: Baseboard },
    Share,
    Pkg,
    // To keep things simple for now, we just return a generic error
    Error,

    // A special message to indicate a broadcast to all connected peers
    BroadcastGetShare(BTreeSet<Baseboard>),

    // A special messsage to indicate all the learners informed with a pkg
    BroadcastPkg(BTreeSet<Baseboard>),
}

// Our global system model
#[derive(Debug)]
pub struct Model {
    config: Config,
    rack_uuid: Option<Uuid>,
    peers: BTreeMap<Baseboard, PeerModel>,
    initial_members: BTreeSet<Baseboard>,
    threshold: usize,
}

impl Model {
    pub fn new(
        config: Config,
        initial_members: BTreeSet<Baseboard>,
        learners: BTreeSet<Baseboard>,
    ) -> Model {
        // This is the calculation done in bootstore/src/trust_quorum/share_pkg.rs
        let threshold = initial_members.len() / 2 + 1;
        let peers = initial_members
            .iter()
            .chain(learners.iter())
            .cloned()
            .map(|id| (id.clone(), PeerModel::new(id, threshold, config)))
            .collect();

        Model { config, rack_uuid: None, peers, initial_members, threshold }
    }

    pub fn get_peer(&self, peer_id: &Baseboard) -> &PeerModel {
        self.peers.get(peer_id).unwrap()
    }

    // Handle a message for a given peer
    pub fn handle_msg(
        &mut self,
        source: &Baseboard,
        dest: &Baseboard,
        msg: Msg,
    ) -> ExpectedOutput {
        self.peers.get_mut(dest).unwrap().handle_msg(source, msg)
    }

    // Handle Action::RackInit
    pub fn on_rack_init(&mut self, rack_uuid: Uuid) {
        if self.rack_uuid.is_none() {
            self.rack_uuid = Some(rack_uuid);
            for peer_id in &self.initial_members {
                self.peers.get_mut(peer_id).unwrap().state =
                    ModelState::InitialMember {
                        pending_learn_requests: BTreeMap::new(),
                        already_learned: BTreeSet::new(),
                    };
            }
        }
    }

    // Inform `source` it's connected to `dest` as a result of `Action::Connect`
    //
    // We can send one of two messages depending upon the current state of peer:
    //  * `Learn` if we are learning and `dest` is the only connected peer
    //  * `GetShare` if we are retrieving shares and have not learned one from
    // this peer
    pub fn on_connect(
        &mut self,
        source: &Baseboard,
        dest: Baseboard,
    ) -> Option<ExpectedMsg> {
        // We setup mutual connections
        let peer = self.peers.get_mut(&source).unwrap();

        // Determine if any new learn requests need to be sent. This is the case
        // if this peer is learning and this is the only connection available.
        if let ModelState::Learning { attempt: None } = &peer.state {
            peer.connected.insert(dest.clone());
            peer.state = ModelState::Learning {
                attempt: Some(LearnAttempt {
                    peer: dest.clone(),
                    start: peer.clock,
                }),
            };
            return Some(ExpectedMsg::Learn { destination: dest });
        }

        // Determine if any `GetShare` messages need to be sent. This is
        // the case if a peer is currently retrieving shares and hasn't
        // received one from the newly connected peer.
        if let ModelRackSecretState::Retrieving { received, .. } =
            &peer.rack_secret_state
        {
            if !peer.connected.contains(&dest) && !received.contains(&dest) {
                peer.connected.insert(dest);
                return Some(ExpectedMsg::GetShare);
            }
        }

        peer.connected.insert(dest);
        None
    }

    // Inform `source` it's disconnected from `dest` as a result of
    // `Action::Disconnect`
    //
    // If we are currently learning from the disconnected peer, we should go
    // onto the next peer if there is one and send a learn request.
    pub fn on_disconnect(
        &mut self,
        source: &Baseboard,
        dest: &Baseboard,
    ) -> Option<ExpectedMsg> {
        let peer = self.peers.get_mut(&source).unwrap();
        peer.connected.remove(dest);
        let next_peer = peer.next_peer(dest);
        if let ModelState::Learning { attempt: Some(attempt) } = &mut peer.state
        {
            if &attempt.peer == dest {
                if let Some(new_dest) = next_peer {
                    attempt.peer = new_dest.clone();
                    attempt.start = peer.clock;
                    return Some(ExpectedMsg::Learn { destination: new_dest });
                };
            }
        }
        None
    }

    // Handle a tick callback for a given peer
    //
    // There can only be two possible, mutually exclusive, expected outputs:
    //  1. A timeout waiting for shares to recompute the rack secret
    //  2. A learn request being sent to a peer if the prior one timed out
    pub fn on_tick(&mut self, peer_id: &Baseboard) -> ExpectedOutput {
        let peer = self.peers.get_mut(peer_id).unwrap();
        peer.clock += 1;

        // Are any pending learn requests expired? We just drop these, as the
        // learner will timeout and try another peer.
        if let ModelState::InitialMember { pending_learn_requests, .. } =
            &mut peer.state
        {
            pending_learn_requests.retain(|_, expiry| *expiry >= peer.clock);
        }

        // Are we a learner and did our attempt timeout?
        if let ModelState::Learning { attempt: Some(attempt) } = &peer.state {
            if (attempt.start + self.config.learn_timeout) < peer.clock {
                // Our attempt expired, go on to the next peer
                if let Some(dest) = peer.next_peer(&attempt.peer) {
                    peer.state = ModelState::Learning {
                        attempt: Some(LearnAttempt {
                            peer: dest.clone(),
                            start: peer.clock,
                        }),
                    };
                    return ExpectedMsg::Learn { destination: dest }.into();
                } else {
                    // Clear our attempt, as there is no next peer
                    peer.state = ModelState::Learning { attempt: None };
                }
            }
            // We are still learning, so we cannot be computing a rack secret
            return ExpectedOutput::none();
        }

        // Are we retrieivng shares, and did our request timeout?
        match peer.rack_secret_state {
            ModelRackSecretState::Retrieving { expiry, .. } => {
                if expiry < peer.clock {
                    peer.rack_secret_state = ModelRackSecretState::Empty;
                    return ExpectedApiError::RackSecretTimeout.into();
                }
            }
            ModelRackSecretState::Computed { expiry } => {
                if expiry < peer.clock {
                    peer.rack_secret_state = ModelRackSecretState::Empty
                }
            }
            ModelRackSecretState::Empty => (),
        }

        ExpectedOutput::none()
    }

    pub fn load_rack_secret(&mut self, peer_id: &Baseboard) -> ExpectedOutput {
        let threshold = self.threshold;
        let peer = self.peers.get_mut(peer_id).unwrap();
        let new_expiry = peer.clock + self.config.rack_secret_request_timeout;

        match &peer.state {
            ModelState::Uninitialized => {
                return ExpectedApiError::RackNotInitialized.into();
            }
            ModelState::Learning { .. } => {
                return ExpectedApiError::StillLearning.into();
            }
            // We are capable of loading a rack secret in these states
            ModelState::Learned | ModelState::InitialMember { .. } => (),
        }

        match &mut peer.rack_secret_state {
            state @ ModelRackSecretState::Empty => {
                // Transition our model state to "Retrieving"
                *state = ModelRackSecretState::Retrieving {
                    // Make sure to add ourself
                    received: BTreeSet::from([peer_id.clone()]),
                    expiry: new_expiry,
                    threshold,
                };
                // We expect a share to be sent to all connected peers
                ExpectedMsg::BroadcastGetShare(peer.connected.clone()).into()
            }
            ModelRackSecretState::Retrieving { expiry, .. } => {
                // We extend the timeout on a reload
                *expiry = new_expiry;
                ExpectedOutput::none()
            }
            ModelRackSecretState::Computed { .. } => {
                ExpectedApiOutput::RackSecretLoaded.into()
            }
        }
    }

    pub fn init_learner(&mut self, peer_id: &Baseboard) -> ExpectedOutput {
        let peer = self.peers.get_mut(peer_id).unwrap();
        match &mut peer.state {
            state @ ModelState::Uninitialized => {
                let dest = peer.connected.first();
                let attempt = dest.map(|dest| LearnAttempt {
                    peer: dest.clone(),
                    start: peer.clock,
                });
                *state = ModelState::Learning { attempt };
                let msg = dest.map(|dest| ExpectedMsg::Learn {
                    destination: dest.clone(),
                });
                ExpectedOutput { persist: true, msg, api_output: None }
            }
            _ => ExpectedApiError::PeerAlreadyInitialized.into(),
        }
    }
}
