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
    Envelope, Msg, Request, RequestType, Response, ResponseType,
};
use super::state::{
    Config, FsmCommonData, RackInitState, RackSecretState, State,
};
use super::{
    InitialMemberState, LearnedState, LearningState, UninitializedState,
};
use crate::trust_quorum::create_pkgs;
use secrecy::ExposeSecret;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Handler methods implemented by each FSM state
pub trait StateHandler {
    fn handle_request(
        self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (State, Output);

    fn handle_response(
        self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> (State, Output);

    fn tick(self, common: &mut FsmCommonData) -> (State, Output);
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
    // An invariant of the Fsm is that `state` is always `Some` to API callers.
    state: Option<State>,
}

impl Fsm {
    /// Create a new FSM. This is useful when starting from a persistent state
    /// on disk.
    pub fn new(id: Baseboard, config: Config, state: State) -> Fsm {
        Fsm { common: FsmCommonData::new(id, config), state: Some(state) }
    }

    /// Create a new FSM in `State::Uninitialized`
    pub fn new_uninitialized(id: Baseboard, config: Config) -> Fsm {
        let state = Some(State::Uninitialized(UninitializedState {}));
        Fsm { common: FsmCommonData::new(id, config), state }
    }

    pub fn state_name(&self) -> &'static str {
        self.state.as_ref().unwrap().name()
    }

    pub fn state(&self) -> &State {
        self.state.as_ref().unwrap()
    }

    pub fn common_data(&self) -> &FsmCommonData {
        &self.common
    }

    /// This call is triggered locally as a result of RSS running
    /// It may only be called once, which is enforced by checking to see if
    /// we already are in `State::Uninitialized`.
    pub fn init_rack(
        &mut self,
        rack_uuid: Uuid,
        initial_membership: BTreeSet<Baseboard>,
    ) -> Output {
        let State::Uninitialized(_) = self.state.as_ref().unwrap() else {
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
                        if peer == self.common.id {
                            // Don't send a message to ourself
                            self.state = Some(State::InitialMember(
                                InitialMemberState {
                                    pkg: pkg.clone(),
                                    distributed_shares: BTreeMap::new(),
                                    rack_init_state: Some(RackInitState {
                                        start: self.common.clock,
                                        total_members,
                                        acks: BTreeSet::from([peer]),
                                    }),
                                    pending_learn_requests: BTreeMap::new(),
                                    rack_secret_state: None,
                                },
                            ));
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

    /// Initialize a node added after rack initialization
    pub fn init_learner(&mut self) -> Output {
        let State::Uninitialized(_) = self.state.as_ref().unwrap() else {
            return ApiError::PeerAlreadyInitialized.into();
        };

        let mut state = LearningState { attempt: None };
        let output = state.new_attempt(&mut self.common);
        self.state = Some(State::Learning(state).into());
        output
    }

    /// This call is triggered locally after RSS runs, in order to retrieve the
    /// `RackSecret` so that it can be used as input key material.
    ///
    /// if the rack secret has not already been loaded, then share retrieval
    /// will begin.
    pub fn load_rack_secret(&mut self) -> Output {
        match self.state.as_mut().unwrap() {
            // We don't allow retrieval before initialization
            State::Uninitialized(_) => ApiError::RackNotInitialized.into(),
            State::Learning(_) => ApiError::StillLearning.into(),
            State::InitialMember(InitialMemberState {
                pkg,
                rack_secret_state,
                ..
            }) => {
                match rack_secret_state {
                    None => {
                        self.common.pending_api_rack_secret_request =
                            Some(self.common.clock);
                        // Add our own share during initialization
                        *rack_secret_state =
                            Some(RackSecretState::Shares(BTreeMap::from([(
                                self.common.id.clone(),
                                pkg.share.clone(),
                            )])));
                        Output::none()
                    }
                    Some(RackSecretState::Shares(_)) => {
                        // Refresh the start time to extend the request timeout
                        self.common.pending_api_rack_secret_request =
                            Some(self.common.clock);
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
                        self.common.pending_api_rack_secret_request =
                            Some(self.common.clock);
                        // Add our own share during initialization
                        *rack_secret_state =
                            Some(RackSecretState::Shares(BTreeMap::from([(
                                self.common.id.clone(),
                                pkg.share.clone(),
                            )])));
                        Output::none()
                    }
                    Some(RackSecretState::Shares(_)) => {
                        // Refresh the start time to extend the request timeout
                        self.common.pending_api_rack_secret_request =
                            Some(self.common.clock);
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
        self.common.peers.insert(peer);
    }

    /// When a the upper layer sees the advertised prefix for a peer go away,
    /// and not just a dropped connection, it will inform the FSM to remove the peer.
    ///
    /// This is a useful mechanism to prevent generating requests for failed sleds.
    pub fn remove_peer(&mut self, peer: Baseboard) {
        self.common.peers.remove(&peer);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemes::v0::state_uninitialized::UninitializedState;

    fn test_config() -> Config {
        Config {
            learn_timeout: 5,
            rack_init_timeout: 5,
            rack_secret_request_timeout: 5,
            retry_timeout: 2,
        }
    }

    fn initial_members() -> BTreeSet<Baseboard> {
        [("a", "1"), ("b", "1"), ("c", "1"), ("d", "1"), ("e", "1")]
            .iter()
            .map(|(id, model)| {
                Baseboard::new_pc(id.to_string(), model.to_string())
            })
            .collect()
    }

    /// Test that Fsm::init_rack correctly mutates state and returns correct
    /// output when no members ack receipt of the initialization.
    #[test]
    fn init_rack_timeout_no_members_acked() {
        let initial_members = initial_members();
        let config = test_config();
        let mut fsm = Fsm::new(
            initial_members.first().unwrap().clone(),
            config.clone(),
            State::Uninitialized(UninitializedState {}),
        );

        // Tick in uninitialized state has no output
        let output = fsm.tick();
        assert_eq!(output, Output::none());
        assert_eq!("uninitialized", fsm.state_name());

        // Initializing a rack results in a state change of *this* FSM to `InitialMember`
        // and results in an envelope destined for each of the other peers.
        let rack_uuid = Uuid::new_v4();
        let output = fsm.init_rack(rack_uuid, initial_members.clone());
        assert_eq!("initial_member", fsm.state_name());

        // We changed states, requiring persistence
        assert_eq!(output.persist, true);
        assert_eq!(output.envelopes.len(), 4);
        assert_eq!(output.api_output, None);

        // We don't timeout until we reach timeout number of ticks + 1.
        // This guarantees at least a minimum timeout if registration occurs
        // right before a tick.
        for _ in 0..config.rack_init_timeout {
            assert_eq!(Output::none(), fsm.tick());
        }

        // We should timeout on the next tick, reporting that none of the peers
        // acked.
        let output = fsm.tick();
        assert_eq!(output.persist, false);
        assert_eq!(output.envelopes.len(), 0);

        let api_err = output.api_output.unwrap().unwrap_err();
        let mut expected = initial_members.clone();
        expected.pop_first().unwrap();
        assert_eq!(
            api_err,
            ApiError::RackInitTimeout { unacked_peers: expected }
        );
    }

    /// Ack one of the members and ensure the timeout occurs with the rest unacked
    #[test]
    fn partial_rack_init_timeout() {
        let initial_members = initial_members();
        let config = test_config();
        let mut fsm = Fsm::new(
            initial_members.first().unwrap().clone(),
            config.clone(),
            State::Uninitialized(UninitializedState {}),
        );
        let rack_uuid = Uuid::new_v4();
        let mut output = fsm.init_rack(rack_uuid, initial_members.clone());

        // Unpack the request to ack
        let Envelope{
            to,
            msg: Msg::Req(Request{id, type_: RequestType::Init(_)})
        } = output.envelopes.pop().unwrap() else {
            panic!("expected a request");
        };
        let from = to;
        let request_id = id;

        // Skip over ticks that won't trigger timeout
        for _ in 0..config.rack_init_timeout {
            assert_eq!(Output::none(), fsm.tick());
        }

        // Handle the response
        let response = Response { request_id, type_: ResponseType::InitAck };
        let output = fsm.handle_response(from.clone(), response);
        assert_eq!(Output::none(), output);

        // Construct the expected unacked sleds map
        // Remove ourself, and the acked sled
        let mut expected = initial_members.clone();
        expected.pop_first().unwrap();
        expected.remove(&from);

        // Now timeout. There should only be unacked sleds not including the responder.
        let api_err = fsm.tick().api_output.unwrap().unwrap_err();
        assert_eq!(
            api_err,
            ApiError::RackInitTimeout { unacked_peers: expected }
        );
    }

    /// Ack all members and ensure rack init completes
    #[test]
    fn rack_init_completes() {
        let initial_members = initial_members();
        let config = test_config();
        let mut fsm = Fsm::new(
            initial_members.first().unwrap().clone(),
            config.clone(),
            State::Uninitialized(UninitializedState {}),
        );
        let rack_uuid = Uuid::new_v4();
        let output = fsm.init_rack(rack_uuid, initial_members.clone());

        let responses: Vec<_> = output
            .envelopes
            .into_iter()
            .map(|envelope| {
                if let Msg::Req(Request { id, .. }) = envelope.msg {
                    (
                        envelope.to,
                        Response {
                            request_id: id,
                            type_: ResponseType::InitAck,
                        },
                    )
                } else {
                    panic!("expected a request");
                }
            })
            .collect();

        // Handle responses
        let num_responses = responses.len();
        for (i, (from, response)) in responses.into_iter().enumerate() {
            let output = fsm.handle_response(from, response);
            if i != num_responses - 1 {
                // We don't expect any output as rack init is not complete
                assert_eq!(Output::none(), output);
            } else {
                // Rack initialization completes on processing the last response and
                // we inform the caller.
                assert_eq!(
                    ApiOutput::RackInitComplete,
                    output.api_output.unwrap().unwrap()
                );
            }
        }
    }
}
