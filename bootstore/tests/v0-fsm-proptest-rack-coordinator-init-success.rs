// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for bootstore scheme v0 protocol logic
//!
//! These tests create a single `Fsm` as the system under test (SUT), where that
//! FSM is the RSS node and where `Fsm::rack_init` always succeeds. Success may
//! happend immediately if all nodes are connected, or when initial members
//! connect a short time after  `Fsm::rack_init` is called.
//!
//! After rack init succeeds various api calls will be generated to exercise
//! the Fsm.

mod common;

use assert_matches::assert_matches;
use bootstore::schemes::v0::{
    ApiError, ApiOutput, Config, Envelope, Fsm, Msg, MsgError, Request,
    RequestType, Response, ResponseType, Share,
};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use uuid::Uuid;

const MIN_INITIAL_MEMBERS: usize = 3;
const MAX_INITIAL_MEMBERS: usize = 12;
const TICK_TIMEOUT: Duration = Duration::from_millis(250);

use common::generators::{
    arb_config, arb_initial_member_ids, arb_learner_id, arb_msg_error,
    MAX_ACTIONS, TICKS_PER_ACTION,
};

/// Actions run during the rack init phase of the test
#[derive(Debug, Clone)]
pub enum RackInitAction {
    /// A symmetric connection to the SUT FSM
    Connect(Baseboard),

    /// Call the `Fsm::init_rack` on `rss_sled`
    RackInit,
}

/// Actions run after rack init succeeds
#[derive(Debug, Clone)]
pub enum Action {
    LoadRackSecret,
    Connect(Baseboard),
    Disconnect(Baseboard),
    Ticks(usize),
    GetShare(Baseboard),
    // Trigger an error response by using an invalid rack_uuid in the request
    GetShareFail(Baseboard),
    Learn(Baseboard),
    // Generate an error response from another peer
    ErrorResponse(Baseboard, MsgError),
}

#[derive(Debug)]
pub struct TestInput {
    pub initial_members: BTreeSet<Baseboard>,
    pub config: Config,
    pub rack_uuid: Uuid,
    pub rack_init_sequence: Vec<RackInitAction>,
    pub actions: Vec<Action>,
}

fn arb_action(
    initial_members: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Action> {
    // We skip the first peer, which is the SUT
    let peers: Vec<_> = initial_members.iter().skip(1).cloned().collect();
    let selected_peer = any::<prop::sample::Index>()
        .prop_map(move |index| index.get(&peers).clone());
    let err_response = (selected_peer.clone(), arb_msg_error())
        .prop_map(|(from, err)| Action::ErrorResponse(from, err));
    prop_oneof![
        50 => (TICKS_PER_ACTION).prop_map(Action::Ticks),
        10 => selected_peer.clone().prop_map(Action::Connect),
        10 => selected_peer.clone().prop_map(Action::Disconnect),
        5 => Just(Action::LoadRackSecret),
        3 => selected_peer.clone().prop_map(Action::GetShare),
        3 => selected_peer.prop_map(Action::GetShareFail),
        5 => arb_learner_id().prop_map(Action::Learn),
        3 => err_response
    ]
}

/// A generated initialization sequence that runs before other actions This
/// sequence can be quite arbitrary. Notably, we ensure that rack init never
/// fails because we never advance time so requests timeout. There will only
/// be one `RackInit` variant in the returned strategy Vec, somewhere between
/// the connections.
fn arb_rack_init_sequence(
    initial_members: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Vec<RackInitAction>> {
    // The first node is our SUT
    let peers: Vec<_> = initial_members.iter().skip(1).cloned().collect();
    (any::<prop::sample::Index>(), Just(peers).prop_shuffle()).prop_map(
        |(index, peers)| {
            let rack_init_index = index.index(peers.len());
            let mut rack_init_sequence = Vec::with_capacity(peers.len() + 1);
            for (i, peer) in peers.into_iter().enumerate() {
                if i == rack_init_index {
                    rack_init_sequence.push(RackInitAction::RackInit);
                }
                rack_init_sequence.push(RackInitAction::Connect(peer));
            }
            rack_init_sequence
        },
    )
}

/// Create the test input to this test
fn arb_test_input() -> impl Strategy<Value = TestInput> {
    arb_initial_member_ids(MIN_INITIAL_MEMBERS, MAX_INITIAL_MEMBERS)
        .prop_flat_map(|initial_members| {
            // An intermediate tuple strategy
            (
                Just(initial_members.clone()),
                arb_config(),
                Just(Uuid::new_v4()),
                arb_rack_init_sequence(initial_members.clone()),
                proptest::collection::vec(
                    arb_action(initial_members),
                    1..=MAX_ACTIONS,
                ),
            )
        })
        .prop_map(
            |(
                initial_members,
                config,
                rack_uuid,
                rack_init_sequence,
                actions,
            )| {
                TestInput {
                    initial_members,
                    config,
                    rack_uuid,
                    rack_init_sequence,
                    actions,
                }
            },
        )
}

// A tracked request issued to the SUT
pub struct TestRequest {
    pub start: Instant,
    pub acks: BTreeSet<Baseboard>,
}

impl TestRequest {
    pub fn new(start: Instant) -> TestRequest {
        TestRequest { start, acks: BTreeSet::new() }
    }
}

pub struct TestState {
    // The Fsm under test
    sut: Fsm,

    // The unique id of the initialized rack
    rack_uuid: Uuid,

    // The generated configuration
    config: Config,

    // IDs of all initial members
    initial_members: BTreeSet<Baseboard>,

    // Any peers connected to the SUT Fsm
    connected_peers: BTreeSet<Baseboard>,

    // The current time at the SUT Fsm
    now: Instant,

    // Has `Fsm::init_rack` been called yet?
    rack_init_started: bool,

    // Shares distributed as part of rack_init
    shares: BTreeMap<Baseboard, Share>,

    // `Fsm::load_rack_secret` requests triggered by an `Action::LoadRackSecret`
    load_rack_secret_requests: BTreeMap<Uuid, TestRequest>,

    // Generated Learn requests triggered by `Action::Learn`
    learn_requests: BTreeMap<Uuid, TestRequest>,

    // Learners that have already learned their shares
    already_learned: BTreeSet<Baseboard>,

    // Rack secret threshold
    threshold: usize,

    // The number of "extra" shares per initial member sled
    encrypted_shares_per_sled: usize,
}

impl TestState {
    pub fn new(
        initial_members: BTreeSet<Baseboard>,
        config: Config,
        rack_uuid: Uuid,
    ) -> TestState {
        let sut_id = initial_members.first().cloned().unwrap();
        let threshold = initial_members.len() / 2 + 1;
        // 255 is the maximum number of shares that can be created.
        // This number matches the code in `share_pkg::create_pkgs`.
        let encrypted_shares_per_sled = (255 / initial_members.len()) - 1;
        TestState {
            sut: Fsm::new_uninitialized(sut_id, config.clone()),
            rack_uuid,
            config,
            initial_members,
            connected_peers: BTreeSet::new(),
            now: Instant::now(),
            rack_init_started: false,
            shares: BTreeMap::new(),
            load_rack_secret_requests: BTreeMap::new(),
            learn_requests: BTreeMap::new(),
            already_learned: BTreeSet::new(),
            threshold,
            encrypted_shares_per_sled,
        }
    }

    pub fn init_rack(&mut self, actions: Vec<RackInitAction>) {
        // Ensure Rack Init completes successfully
        for action in actions {
            let envelopes = match action {
                RackInitAction::Connect(peer_id) => {
                    self.connected_peers.insert(peer_id.clone());
                    let result = self.sut.on_connected(self.now, peer_id);
                    let envelopes = self.sut.drain_envelopes().collect();
                    self.check_init_connect_output(result, &envelopes);
                    envelopes
                }
                RackInitAction::RackInit => {
                    self.rack_init_started = true;
                    let result = self.sut.init_rack(
                        self.now,
                        self.rack_uuid,
                        self.initial_members.clone(),
                    );
                    let envelopes = self.sut.drain_envelopes().collect();
                    self.check_rack_init_output(result, &envelopes);
                    envelopes
                }
            };
            self.ack_init_msgs(envelopes);
        }
    }

    fn check_rack_init_output(
        &self,
        result: Result<(), ApiError>,
        envelopes: &Vec<Envelope>,
    ) {
        assert!(result.is_ok());
        assert_eq!(self.connected_peers.len(), envelopes.len());
        for envelope in envelopes {
            assert!(self.connected_peers.contains(&envelope.to));
            assert_matches!(
                &envelope.msg,
                &Msg::Req(Request { type_: RequestType::Init(_), .. })
            );
        }
    }

    fn check_init_connect_output(
        &self,
        result: Result<(), ApiError>,
        envelopes: &Vec<Envelope>,
    ) {
        assert!(result.is_ok());
        if self.rack_init_started {
            assert_eq!(envelopes.len(), 1);
            assert_matches!(
                &envelopes.first().unwrap().msg,
                &Msg::Req(Request { type_: RequestType::Init(_), .. })
            );
        } else {
            assert_eq!(envelopes.len(), 0);
        }
    }

    fn ack_init_msgs(&mut self, envelopes: Vec<Envelope>) {
        let total = envelopes.len();
        for (i, envelope) in envelopes.into_iter().enumerate() {
            let Envelope {
                to,
                msg: Msg::Req(Request { id, type_: RequestType::Init(pkg)})
            } = envelope else {
                panic!("Already verified the RequestType");
            };
            self.shares.insert(to.clone(), Share(pkg.share.clone()));
            let ack = Response { request_id: id, type_: ResponseType::InitAck }
                .into();
            let output = self.sut.handle_msg(self.now, to, ack);
            if i == total - 1
                && self.connected_peers.len() == self.initial_members.len() - 1
            {
                assert_matches!(output, Ok(Some(ApiOutput::RackInitComplete)));
            } else {
                assert_matches!(output, Ok(None));
            }
        }
    }

    pub fn learn(&mut self, peer_id: Baseboard) {
        let request_id = Uuid::new_v4();
        self.learn_requests.insert(request_id, TestRequest::new(self.now));
        let req = Request { id: request_id, type_: RequestType::Learn }.into();
        let output = self.sut.handle_msg(self.now, peer_id.clone(), req);
        assert_eq!(output, Ok(None));
        let envelopes = self.sut.drain_envelopes().collect();
        self.expect_get_share_broadcast(&envelopes);
        self.deliver_share_responses(envelopes);
    }

    pub fn load_rack_secret(&mut self) {
        let request_id = self.sut.load_rack_secret(self.now).unwrap();
        self.load_rack_secret_requests
            .insert(request_id, TestRequest::new(self.now));
        let envelopes = self.sut.drain_envelopes().collect();
        self.expect_get_share_broadcast(&envelopes);
        self.deliver_share_responses(envelopes);
    }

    fn expect_get_share_broadcast(&self, envelopes: &Vec<Envelope>) {
        assert_eq!(self.connected_peers.len(), envelopes.len());
        for envelope in envelopes {
            assert!(self.connected_peers.contains(&envelope.to));
            assert_matches!(
                &envelope.msg,
                &Msg::Req(Request {
                    type_: RequestType::GetShare { rack_uuid },
                    ..
                }) if rack_uuid == self.rack_uuid
            );
        }
    }

    fn deliver_share_responses(&mut self, envelopes: Vec<Envelope>) {
        for envelope in envelopes {
            let share = self.shares.get(&envelope.to).unwrap().clone();
            let request_id = envelope.msg.request_id();
            let rsp =
                Response { request_id, type_: ResponseType::Share(share) }
                    .into();
            let output =
                self.sut.handle_msg(self.now, envelope.to.clone(), rsp);

            // Is this a `LoadRackSecret` request or a `Learn` request?
            if let Some(test_req) =
                self.load_rack_secret_requests.get_mut(&request_id)
            {
                test_req.acks.insert(envelope.to);
                // We don't count the SUT, which has its own share
                if test_req.acks.len() == self.threshold - 1 {
                    assert_matches!(
                        output,
                        Ok(Some(ApiOutput::RackSecret { .. }))
                    );
                    self.load_rack_secret_requests.remove(&request_id);
                } else {
                    assert_matches!(output, Ok(None));
                    assert!(self.sut.drain_envelopes().next().is_none());
                }
            } else if let Some(test_req) =
                self.learn_requests.get_mut(&request_id)
            {
                test_req.acks.insert(envelope.to);
                // We don't count the SUT, which has its own share
                if test_req.acks.len() == self.threshold - 1 {
                    self.learn_requests.remove(&request_id);
                    // There should be a `LearnedSharePkg` delivered to the
                    // learner
                    let mut iter = self.sut.drain_envelopes();
                    let rsp = iter.next().unwrap();
                    let expected_id = request_id;

                    // Have we handed out all our shares?
                    if self.already_learned.len()
                        == self.encrypted_shares_per_sled
                        && !self.already_learned.contains(&rsp.to)
                    {
                        assert_matches!(
                            rsp.msg,
                            Msg::Rsp(Response {
                                request_id,
                                type_: ResponseType::Error(MsgError::CannotSpareAShare),
                            }) if request_id == expected_id
                        );
                        assert_eq!(output, Ok(None));
                    } else {
                        assert_matches!(
                            rsp.msg,
                            Msg::Rsp(Response {
                                request_id,
                                type_: ResponseType::Pkg(_),
                            }) if request_id == expected_id
                        );
                        if self.already_learned.contains(&rsp.to) {
                            // We don't persist (inform the api) if the share
                            // has already been handed out
                            assert_eq!(output, Ok(None));
                        } else {
                            assert_matches!(
                                output,
                                Ok(Some(ApiOutput::ShareDistributedToLearner))
                            );
                            self.already_learned.insert(rsp.to);
                        }
                    }
                    assert!(iter.next().is_none());
                } else {
                    // We don't have a threshold yet
                    assert_matches!(output, Ok(None));
                    assert!(self.sut.drain_envelopes().next().is_none());
                }
            } else {
                // These are extra shares (after the threshold is reached)
                assert_matches!(output, Ok(None));
                assert!(self.sut.drain_envelopes().next().is_none());
            }
        }
    }

    pub fn connect(&mut self, peer_id: Baseboard) {
        let result = self.sut.on_connected(self.now, peer_id.clone());
        let envelopes = self.sut.drain_envelopes().collect();
        self.check_connect_output(&peer_id, result, &envelopes);
        self.connected_peers.insert(peer_id);
        self.deliver_share_responses(envelopes);
    }

    fn check_connect_output(
        &self,
        peer_id: &Baseboard,
        result: Result<(), ApiError>,
        envelopes: &Vec<Envelope>,
    ) {
        assert!(result.is_ok());
        for envelope in envelopes {
            let request_id = envelope.msg.request_id();
            assert_eq!(peer_id, &envelope.to);
            assert_matches!(
                &envelope.msg,
                &Msg::Req(Request { type_: RequestType::GetShare { .. }, .. })
            );
            // An outstanding request must exist and the peer must
            // not have acked for an envelope to be sent.
            if let Some(test_req) =
                self.load_rack_secret_requests.get(&request_id)
            {
                assert!(!test_req.acks.contains(&envelope.to));
            } else {
                assert!(!self.learn_requests[&request_id]
                    .acks
                    .contains(&envelope.to));
            }
        }
    }

    pub fn disconnect(&mut self, peer_id: Baseboard) {
        self.sut.on_disconnected(&peer_id);
        self.connected_peers.remove(&peer_id);

        // There should be no envelopes sent on a disconnect
        assert_eq!(None, self.sut.drain_envelopes().next());
    }

    pub fn tick(&mut self, ticks: usize) {
        for _ in 0..ticks {
            self.now += TICK_TIMEOUT;
            if let Err(errors) = self.sut.tick(self.now) {
                // The only possible error is a timeout
                // Ensure the request exists and it should have timed out
                for (request_id, error) in errors {
                    assert_eq!(error, ApiError::RackSecretLoadTimeout);
                    let test_req = &self.load_rack_secret_requests[&request_id];
                    let expiry = test_req.start
                        + self.config.rack_secret_request_timeout;
                    assert!(expiry < self.now);
                    // Remove the tracking req from test state
                    self.load_rack_secret_requests.remove(&request_id);
                }
            }
        }
    }

    pub fn get_share(&mut self, peer_id: Baseboard) {
        let id = Uuid::new_v4();
        let req = Request {
            id,
            type_: RequestType::GetShare { rack_uuid: self.rack_uuid },
        }
        .into();
        let res = self.sut.handle_msg(self.now, peer_id.clone(), req);
        assert_eq!(res, Ok(None));
        let mut iter = self.sut.drain_envelopes();
        let envelope = iter.next().unwrap();
        assert_matches!(envelope, Envelope {
            to,
            msg: Msg::Rsp(Response {
                request_id,
                type_: ResponseType::Share(_)
            })
        } if to == peer_id && id == request_id);

        // There's only one envelope
        assert!(iter.next().is_none());
    }

    pub fn get_share_fail(&mut self, peer_id: Baseboard) {
        let id = Uuid::new_v4();
        let bad_rack_uuid = Uuid::new_v4();
        let req = Request {
            id,
            type_: RequestType::GetShare { rack_uuid: bad_rack_uuid },
        }
        .into();
        let res = self.sut.handle_msg(self.now, peer_id.clone(), req);
        assert_eq!(res, Ok(None));
        let mut iter = self.sut.drain_envelopes();
        let envelope = iter.next().unwrap();
        assert_matches!(envelope, Envelope {
            to,
            msg: Msg::Rsp(Response {
                request_id,
                type_: ResponseType::Error(
                    MsgError::RackUuidMismatch { expected , got  }
                )
            })
        } if to == peer_id &&
             id == request_id &&
             expected == self.rack_uuid &&
             got == bad_rack_uuid
        );

        // There's only one envelope
        assert!(iter.next().is_none());
    }

    pub fn handle_error_response(&mut self, peer_id: Baseboard, err: MsgError) {
        let rsp =
            Response { request_id: Uuid::new_v4(), type_: err.into() }.into();
        let output = self.sut.handle_msg(self.now, peer_id, rsp);
        assert_matches!(output, Err(ApiError::ErrorResponseReceived { .. }));
        assert!(self.sut.drain_envelopes().next().is_none());
    }
}

proptest! {
    #![proptest_config(
        ProptestConfig {max_shrink_iters: 100000, ..ProptestConfig::default()})]
    #[test]
    fn run(input in arb_test_input()) {
        let mut state = TestState::new(
            input.initial_members,
            input.config,
            input.rack_uuid
        );

        // Ensure Rack Init completes successfully
        state.init_rack(input.rack_init_sequence);

        for action in input.actions {
            //println!("{:?}", action);
            match action {
                Action::LoadRackSecret => state.load_rack_secret(),
                Action::Connect(peer_id) => state.connect(peer_id),
                Action::Disconnect(peer_id) => state.disconnect(peer_id),
                Action::Ticks(ticks) => state.tick(ticks),
                Action::GetShare(peer_id) => state.get_share(peer_id),
                Action::GetShareFail(peer_id) => state.get_share_fail(peer_id),
                Action::Learn(peer_id) => state.learn(peer_id),
                Action::ErrorResponse(peer_id, err) => {
                    state.handle_error_response(peer_id, err)
                }
            }
        }
    }
}

fn main() {
    run();
}
