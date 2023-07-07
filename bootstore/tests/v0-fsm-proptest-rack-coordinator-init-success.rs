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
    ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Request, RequestType,
    Response, ResponseType, Share,
};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use uuid::Uuid;

const MIN_INITIAL_MEMBERS: usize = 3;
const MAX_INITIAL_MEMBERS: usize = 12;
const TICK_TIMEOUT: Duration = Duration::from_millis(30000);

use common::generators::{
    arb_config, arb_initial_member_ids, MAX_ACTIONS, TICKS_PER_ACTION,
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
    prop_oneof![
        50 => (TICKS_PER_ACTION).prop_map(Action::Ticks),
        10 => selected_peer.clone().prop_map(Action::Connect),
        10 => selected_peer.prop_map(Action::Disconnect),
        5 => Just(Action::LoadRackSecret)
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

    // Rack secret threshold
    threshold: usize,
}

impl TestState {
    pub fn new(
        initial_members: BTreeSet<Baseboard>,
        config: Config,
        rack_uuid: Uuid,
    ) -> TestState {
        let sut_id = initial_members.first().cloned().unwrap();
        let threshold = initial_members.len() / 2 + 1;
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
            threshold,
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

    pub fn load_rack_secret(&mut self) {
        let request_uuid = self.sut.load_rack_secret(self.now).unwrap();
        self.load_rack_secret_requests
            .insert(request_uuid, TestRequest::new(self.now));
        let envelopes = self.sut.drain_envelopes().collect();
        self.check_load_rack_secret_output(&envelopes);
        self.deliver_share_responses(envelopes);
    }

    fn check_load_rack_secret_output(&self, envelopes: &Vec<Envelope>) {
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
            let acks = &mut self
                .load_rack_secret_requests
                .get_mut(&request_id)
                .unwrap()
                .acks;
            acks.insert(envelope.to.clone());
            let output = self.sut.handle_msg(self.now, envelope.to, rsp);
            // We don't count the SUT, which has its own share
            if acks.len() == self.threshold - 1 {
                assert_matches!(output, Ok(Some(ApiOutput::RackSecret { .. })));
            } else {
                assert_matches!(output, Ok(None));
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
        if self.connected_peers.contains(peer_id) {
            assert!(envelopes.is_empty());
        } else {
            for envelope in envelopes {
                let request_id = envelope.msg.request_id();
                assert_eq!(peer_id, &envelope.to);
                assert_matches!(
                    &envelope.msg,
                    &Msg::Req(Request {
                        type_: RequestType::GetShare { .. },
                        ..
                    })
                );
                // An outstanding request must exist and the peer must
                // not have acked for an envelope to be sent.
                assert!(!self.load_rack_secret_requests[&request_id]
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
            }
        }
    }
}

fn main() {
    run();
}
