// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based test for bootstore scheme v0 protocol logic
//!
//! This test creates a single `Fsm` as the system under test (SUT), where that
//! FSM is the RSS node and where `Fsm::rack_init` always succeeds. Success may
//! happen immediately if all nodes are connected, or when initial members
//! connect a short time after `Fsm::rack_init` is called.
//!
//! After rack init succeeds various api calls will be generated to exercise
//! the Fsm.

mod common;

use assert_matches::assert_matches;
use bootstore::schemes::v0::{
    ApiError, ApiOutput, Envelope, Fsm, FsmConfig, Msg, MsgError, RackUuid,
    Request, RequestType, Response, ResponseType, Share,
};
use proptest::prelude::*;
use sled_hardware_types::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use common::generators::{
    arb_action, arb_config, arb_initial_member_ids, Action, MAX_ACTIONS,
    MAX_INITIAL_MEMBERS, MIN_INITIAL_MEMBERS,
};
use common::{CommonTestState, TestRequest};

/// Actions run during the rack init phase of the test
#[derive(Debug, Clone)]
pub enum RackInitAction {
    /// A symmetric connection to the SUT FSM
    Connect(Baseboard),

    /// Call the `Fsm::init_rack` on `rss_sled`
    RackInit,
}

#[derive(Debug)]
pub struct TestInput {
    pub initial_members: BTreeSet<Baseboard>,
    pub config: FsmConfig,
    pub rack_uuid: RackUuid,
    pub rack_init_sequence: Vec<RackInitAction>,
    pub actions: Vec<Action>,
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

/// Create the input to this test
fn arb_test_input() -> impl Strategy<Value = TestInput> {
    arb_initial_member_ids(MIN_INITIAL_MEMBERS, MAX_INITIAL_MEMBERS)
        .prop_flat_map(|initial_members| {
            let is_learner = false;
            // An intermediate tuple strategy
            (
                Just(initial_members.clone()),
                arb_config(),
                Just(Uuid::new_v4().into()),
                arb_rack_init_sequence(initial_members.clone()),
                proptest::collection::vec(
                    arb_action(initial_members, is_learner),
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

pub struct TestState {
    common: CommonTestState,

    // Has `Fsm::init_rack` been called yet?
    rack_init_started: bool,

    // Shares distributed as part of rack_init
    shares: BTreeMap<Baseboard, Share>,

    // Generated Learn requests triggered by `Action::Learn`
    learn_requests: BTreeMap<Uuid, TestRequest>,

    // Learners that have already learned their shares
    already_learned: BTreeSet<Baseboard>,

    // The number of "extra" shares per initial member sled
    encrypted_shares_per_sled: usize,
}

impl TestState {
    pub fn new(
        initial_members: BTreeSet<Baseboard>,
        config: FsmConfig,
        rack_uuid: RackUuid,
    ) -> TestState {
        let sut_id = initial_members.first().cloned().unwrap();
        // 255 is the maximum number of shares that can be created.
        // This number matches the code in `share_pkg::create_pkgs`.
        let encrypted_shares_per_sled = (255 / initial_members.len()) - 1;
        let sut = Fsm::new_uninitialized(sut_id, config);
        let common =
            CommonTestState::new(sut, initial_members, config, rack_uuid);
        TestState {
            common,
            rack_init_started: false,
            shares: BTreeMap::new(),
            learn_requests: BTreeMap::new(),
            already_learned: BTreeSet::new(),
            encrypted_shares_per_sled,
        }
    }

    pub fn init_rack(&mut self, actions: Vec<RackInitAction>) {
        // Ensure Rack Init completes successfully
        for action in actions {
            let envelopes = match action {
                RackInitAction::Connect(peer_id) => {
                    self.common.connected_peers.insert(peer_id.clone());
                    let result =
                        self.common.sut.on_connected(self.common.now, peer_id);
                    let envelopes = self.common.sut.drain_envelopes().collect();
                    self.check_init_connect_output(result, &envelopes);
                    envelopes
                }
                RackInitAction::RackInit => {
                    self.rack_init_started = true;
                    let result = self.common.sut.init_rack(
                        self.common.now,
                        self.common.rack_uuid,
                        self.common.initial_members.clone(),
                    );
                    let envelopes = self.common.sut.drain_envelopes().collect();
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
        assert_eq!(self.common.connected_peers.len(), envelopes.len());
        for envelope in envelopes {
            assert!(self.common.connected_peers.contains(&envelope.to));
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
                msg: Msg::Req(Request { id, type_: RequestType::Init(pkg) }),
            } = envelope
            else {
                panic!("Already verified the RequestType");
            };
            self.shares.insert(to.clone(), Share(pkg.common.share.clone()));
            let ack = Response { request_id: id, type_: ResponseType::InitAck }
                .into();
            let output = self.common.sut.handle_msg(self.common.now, to, ack);
            if i == total - 1
                && self.common.connected_peers.len()
                    == self.common.initial_members.len() - 1
            {
                assert_matches!(output, Ok(Some(ApiOutput::RackInitComplete)));
            } else {
                assert_matches!(output, Ok(None));
            }
        }
    }

    pub fn learn(&mut self, peer_id: Baseboard) {
        let request_id = Uuid::new_v4();
        self.learn_requests
            .insert(request_id, TestRequest::new(self.common.now));
        let req = Request { id: request_id, type_: RequestType::Learn }.into();
        let output = self.common.sut.handle_msg(self.common.now, peer_id, req);
        assert_eq!(output, Ok(None));
        let envelopes = self.common.sut.drain_envelopes().collect();
        self.common.expect_get_share_broadcast(&envelopes);
        self.deliver_share_responses(envelopes);
    }

    fn deliver_share_responses(&mut self, envelopes: Vec<Envelope>) {
        for envelope in envelopes {
            let share = self.shares.get(&envelope.to).unwrap().clone();
            let request_id = envelope.msg.request_id();
            let rsp =
                Response { request_id, type_: ResponseType::Share(share) }
                    .into();
            let output = self.common.sut.handle_msg(
                self.common.now,
                envelope.to.clone(),
                rsp,
            );

            // Is this a `LoadRackSecret` request or a `Learn` request?
            if let Some(test_req) =
                self.common.load_rack_secret_requests.get_mut(&request_id)
            {
                test_req.acks.insert(envelope.to);
                // We don't count the SUT, which has its own share
                if test_req.acks.len() == self.common.threshold - 1 {
                    assert_matches!(
                        output,
                        Ok(Some(ApiOutput::RackSecret { .. }))
                    );
                    self.common.load_rack_secret_requests.remove(&request_id);
                } else {
                    assert_matches!(output, Ok(None));
                    assert!(self.common.sut.drain_envelopes().next().is_none());
                }
            } else if let Some(test_req) =
                self.learn_requests.get_mut(&request_id)
            {
                test_req.acks.insert(envelope.to);
                // We don't count the SUT, which has its own share
                if test_req.acks.len() == self.common.threshold - 1 {
                    self.learn_requests.remove(&request_id);
                    // There should be a `LearnedSharePkg` delivered to the
                    // learner
                    let mut iter = self.common.sut.drain_envelopes();
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
                                type_: ResponseType::LearnPkg(_),
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
                    assert!(self.common.sut.drain_envelopes().next().is_none());
                }
            } else {
                // These are extra shares (after the threshold is reached)
                assert_matches!(output, Ok(None));
                assert!(self.common.sut.drain_envelopes().next().is_none());
            }
        }
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
                self.common.load_rack_secret_requests.get(&request_id)
            {
                assert!(!test_req.acks.contains(&envelope.to));
            } else {
                assert!(!self.learn_requests[&request_id]
                    .acks
                    .contains(&envelope.to));
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
                Action::LoadRackSecret => {
                    let envelopes = state.common.load_rack_secret();
                    state.deliver_share_responses(envelopes);
                }
                Action::Connect(peer_id) => {
                    let (result, envelopes) = state.common.connect(peer_id.clone());
                    state.check_connect_output(&peer_id, result, &envelopes);
                    state.deliver_share_responses(envelopes);
                }
                Action::Disconnect(peer_id) => state.common.disconnect(peer_id),
                Action::Ticks(ticks) => state.common.tick(ticks),
                Action::GetShare(peer_id) => state.common.get_share(peer_id),
                Action::GetShareFail(peer_id) => state.common.get_share_fail(peer_id),
                Action::Learn(peer_id) => state.learn(peer_id),
                Action::ErrorResponse(peer_id, err) => {
                    state.common.handle_error_response(peer_id, err)
                }
            }
        }
    }
}

fn main() {
    run();
}
