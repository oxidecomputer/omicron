// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based test for bootstore scheme v0 protocol logic
//!
//! This test creates a single `Fsm` as the system under test (SUT), where that
//! FSM is a learner node.

mod common;

use assert_matches::assert_matches;
use bootstore::schemes::v0::{
    ApiError, ApiOutput, Envelope, Fsm, FsmConfig, LearnedSharePkg, Msg,
    MsgError, RackUuid, Request, RequestType, Response, ResponseType, Share,
    SharePkg, State, create_pkgs,
};
use bootstore::trust_quorum::RackSecret;
use common::CommonTestState;
use proptest::prelude::*;
use secrecy::ExposeSecret;
use sled_hardware_types::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use common::generators::{
    Action, MAX_ACTIONS, MAX_INITIAL_MEMBERS, MIN_INITIAL_MEMBERS,
    TICK_TIMEOUT, arb_action, arb_config, arb_initial_member_ids,
    arb_learner_id,
};

/// Actions run during the learning phase of the test
#[derive(Debug, Clone)]
pub enum LearnAction {
    Timeout(Baseboard),
    CannotSpareAShare(Baseboard),
    Success(Baseboard),
}

#[derive(Debug, Clone)]
pub struct TestInput {
    pub sut_id: Baseboard,
    pub initial_members: BTreeSet<Baseboard>,
    pub config: FsmConfig,
    pub rack_uuid: RackUuid,
    pub learn_sequence: Vec<LearnAction>,
    pub actions: Vec<Action>,
}

/// We randomize the order of learners, and only allow one node to distribute
/// shares, thus ensuring that the learner itself will rotate and exercise the
/// full learning path.
fn arb_learn_sequence(
    initial_members: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Vec<LearnAction>> {
    // The peer that successfully responds to the learner
    let peers: Vec<_> = initial_members.iter().cloned().collect();
    (
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        Just(peers).prop_shuffle(),
    )
        .prop_map(|(success_index, timeout_index, peers)| {
            let success_index = success_index.index(peers.len());
            let timeout_index = timeout_index.index(peers.len());
            let mut seq = vec![];
            for (i, peer) in peers.into_iter().enumerate() {
                if i == success_index {
                    seq.push(LearnAction::Success(peer));
                    break;
                } else if i >= timeout_index {
                    seq.push(LearnAction::Timeout(peer));
                } else {
                    seq.push(LearnAction::CannotSpareAShare(peer));
                }
            }
            seq
        })
}

/// Create the input to this test
fn arb_test_input() -> impl Strategy<Value = TestInput> {
    arb_initial_member_ids(MIN_INITIAL_MEMBERS, MAX_INITIAL_MEMBERS)
        .prop_flat_map(|initial_members| {
            let is_learner = true;
            // An intermediate tuple strategy
            (
                arb_learner_id(),
                Just(initial_members.clone()),
                arb_config(),
                Just(Uuid::new_v4().into()),
                arb_learn_sequence(initial_members.clone()),
                proptest::collection::vec(
                    arb_action(initial_members, is_learner),
                    1..=MAX_ACTIONS,
                ),
            )
        })
        .prop_map(
            |(
                sut_id,
                initial_members,
                config,
                rack_uuid,
                learn_sequence,
                actions,
            )| {
                TestInput {
                    sut_id,
                    initial_members,
                    config,
                    rack_uuid,
                    learn_sequence,
                    actions,
                }
            },
        )
}

pub struct TestState {
    common: CommonTestState,

    // Packages that we pretend were generated by a rack coordinator
    pkgs: BTreeMap<Baseboard, SharePkg>,
}

impl TestState {
    pub fn new(
        sut_id: Baseboard,
        initial_members: BTreeSet<Baseboard>,
        config: FsmConfig,
        rack_uuid: RackUuid,
        pkgs: BTreeMap<Baseboard, SharePkg>,
    ) -> TestState {
        let sut = Fsm::new_uninitialized(sut_id, config);
        let common =
            CommonTestState::new(sut, initial_members, config, rack_uuid);
        TestState { common, pkgs }
    }

    pub fn learn_share_pkg(&mut self, actions: Vec<LearnAction>) {
        // First send a `Learn` request to the SUT to start the process
        assert!(self.common.sut.init_learner(self.common.now).is_ok());

        // Check that the state of the SUT is correct when no peers are connected
        self.check_learning_state();

        for action in actions {
            match action {
                LearnAction::Timeout(peer_id) => {
                    self.trigger_learn_timeout(peer_id);
                }
                LearnAction::CannotSpareAShare(peer_id) => {
                    self.cannot_spare_a_share(peer_id);
                }
                LearnAction::Success(peer_id) => self.learn_success(peer_id),
            }
        }
    }

    fn check_learning_state(&mut self) {
        assert_eq!(self.common.sut.state(), &State::Learning);
        // We aren't connected to any peers, so no messages should have been sent yet
        assert!(self.common.sut.drain_envelopes().next().is_none());
    }

    // The number of ticks required to trigger a `learn` timeout
    fn ticks_until_learn_timeout(&self) -> usize {
        usize::try_from(
            (self.common.config.learn_timeout.as_millis()
                / TICK_TIMEOUT.as_millis())
                + 1,
        )
        .unwrap()
    }

    // Firts connect to `peer_id` so that a learn request is issued. Then
    // successfully ack it and ensure the state transition to `State::Learned`
    // occurs.
    fn learn_success(&mut self, peer_id: Baseboard) {
        self.disconnect_all_peers_and_clear_pending_learn_request();
        let request_id =
            self.connect_and_expect_a_learn_request(peer_id.clone());

        // Let's just hand out the first decrypted share after we reconstruct the rack
        // secret
        let mut shares = vec![];
        for i in 0..self.common.threshold {
            shares
                .push(self.pkgs.values().nth(i).unwrap().common.share.clone());
        }
        let rack_secret = RackSecret::combine_shares(&shares).unwrap();
        let first_pkg = self.pkgs.values().nth(0).unwrap();
        // Decrypt and discover the share to distributed
        let share =
            first_pkg.decrypt_shares(&rack_secret).unwrap().expose_secret()[0]
                .clone();

        let mut common = first_pkg.common.clone();
        common.share = share;

        let learned_pkg = LearnedSharePkg { common };

        let rsp = Response {
            request_id,
            type_: ResponseType::LearnPkg(learned_pkg.clone()),
        }
        .into();
        let output = self.common.sut.handle_msg(self.common.now, peer_id, rsp);
        assert_eq!(output, Ok(Some(ApiOutput::LearningCompleted)));
        assert!(self.common.sut.drain_envelopes().next().is_none());
        assert_eq!(
            self.common.sut.state(),
            &State::Learned { pkg: learned_pkg }
        );
    }

    // We can only guarantee that a `Learn` request for a peer will
    // be sent if it is the only connected peer. We guaranteed this by
    // disconnecting all peers and then ticking through a timeout so that no
    // outstanding request remains.
    fn disconnect_all_peers_and_clear_pending_learn_request(&mut self) {
        for peer in &self.common.connected_peers {
            self.common.sut.on_disconnected(&peer);
        }
        self.common.connected_peers = BTreeSet::new();
        for _ in 0..self.ticks_until_learn_timeout() {
            self.common.now += TICK_TIMEOUT;
            assert!(self.common.sut.tick(self.common.now).is_ok());
            assert!(self.common.sut.drain_envelopes().next().is_none());
        }
    }

    // Connect to trigger a learn request, then drain envelopes and ensure
    // one is sent. Return the request_id;
    fn connect_and_expect_a_learn_request(
        &mut self,
        peer_id: Baseboard,
    ) -> Uuid {
        self.common.connected_peers.insert(peer_id.clone());
        assert!(
            self.common
                .sut
                .on_connected(self.common.now, peer_id.clone())
                .is_ok()
        );
        let mut iter = self.common.sut.drain_envelopes();
        let envelope = iter.next().unwrap();
        assert_matches!(envelope,
        Envelope {
            to,
            msg: Msg::Req(Request {  type_:  RequestType::Learn,  ..})
        } if to == peer_id
        );
        assert!(iter.next().is_none());
        envelope.msg.request_id()
    }

    fn connect_and_expect_no_messages(&mut self, peer_id: Baseboard) {
        self.common.connected_peers.insert(peer_id.clone());
        assert!(self.common.sut.on_connected(self.common.now, peer_id).is_ok());
        assert!(self.common.sut.drain_envelopes().next().is_none());
    }

    // First connect to peer_id so that a `Learn` request is issued. Then mock an
    // error response.
    //
    // The error is reported to the Fsm API output for logging, but no new learn
    // request is sent, as that optimization is not implemented. Instead another
    // learn request will be sent on the next timeout. We expect learn timeouts
    // to be on the order of 5s, so this does not seem unreasonable in order to
    // keep the code simple.
    fn cannot_spare_a_share(&mut self, peer_id: Baseboard) {
        self.disconnect_all_peers_and_clear_pending_learn_request();
        let expected_request_id =
            self.connect_and_expect_a_learn_request(peer_id.clone());

        // Respond with an error
        let rsp = Response {
            request_id: expected_request_id,
            type_: ResponseType::Error(MsgError::CannotSpareAShare),
        }
        .into();
        let output =
            self.common.sut.handle_msg(self.common.now, peer_id.clone(), rsp);
        assert_matches!(
            output,
            Err(ApiError::ErrorResponseReceived {
                from,
                state,
                request_id,
                error
            }) => {
                assert_eq!(from, peer_id);
                assert_eq!(state, "learning");
                assert_eq!(request_id, expected_request_id);
                assert_eq!(error, MsgError::CannotSpareAShare)
        });

        self.disconnect_all_peers_and_clear_pending_learn_request();
    }

    // First connect to peer_id so that a `Learn` request is issued. Then call
    // `Fsm::tick` enough times that the learner times out and moves onto the
    // next peer.
    fn trigger_learn_timeout(&mut self, peer_id: Baseboard) {
        // There should be a single learn request destined for `peer_id` if this
        // is the first connection when the SUT is in `State::Learning`
        if self.common.connected_peers.is_empty() {
            let _request_id = self.connect_and_expect_a_learn_request(peer_id);
        } else {
            self.connect_and_expect_no_messages(peer_id);
        }

        // We have at least one connected peer, so on timeout another `Learn`
        // message will be sent. Before timeout, though no messages will be
        // sent.
        for _ in 0..self.ticks_until_learn_timeout() - 1 {
            self.common.now += TICK_TIMEOUT;
            assert!(self.common.sut.tick(self.common.now).is_ok());
            assert!(self.common.sut.drain_envelopes().next().is_none());
        }
        // Trigger the timeout
        self.common.now += TICK_TIMEOUT;
        assert!(self.common.sut.tick(self.common.now).is_ok());
        let mut iter = self.common.sut.drain_envelopes();
        assert_matches!(iter.next().unwrap(),
        Envelope {
            to,
            msg: Msg::Req(Request {  type_:  RequestType::Learn,  ..})
        } if self.common.connected_peers.contains(&to)
        );
    }

    // A learner cannot respond to learn attemps successfully
    pub fn learn_attempt_must_fail(&mut self, peer_id: Baseboard) {
        let expected_request_id = Uuid::new_v4();
        let req =
            Request { id: expected_request_id, type_: RequestType::Learn }
                .into();
        let output =
            self.common.sut.handle_msg(self.common.now, peer_id.clone(), req);
        assert_eq!(output, Ok(None));
        let mut iter = self.common.sut.drain_envelopes();
        assert_matches!(iter.next().unwrap(),
            Envelope {
                to,
                msg: Msg::Rsp(Response {request_id, type_: ResponseType::Error(MsgError::CannotSpareAShare)})
            } => {
                assert_eq!(request_id, expected_request_id);
                assert_eq!(to, peer_id);
        });
        assert!(iter.next().is_none());
    }

    fn deliver_share_responses(&mut self, envelopes: Vec<Envelope>) {
        for envelope in envelopes {
            let share = Share(
                self.pkgs.get(&envelope.to).unwrap().common.share.clone(),
            );
            let request_id = envelope.msg.request_id();
            let rsp =
                Response { request_id, type_: ResponseType::Share(share) }
                    .into();
            let output = self.common.sut.handle_msg(
                self.common.now,
                envelope.to.clone(),
                rsp,
            );

            // Do we have a `LoadRackSecret` request?
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
                panic!("Share sent without an outstanding load rack request");
            }
        }
    }
}

proptest! {
    #![proptest_config(
        ProptestConfig {max_shrink_iters: 100000, ..ProptestConfig::default()})]
    #[test]
    fn run(input in arb_test_input()) {

        let pkgs: BTreeMap<Baseboard, SharePkg>
            = create_pkgs(input.rack_uuid.0, input.initial_members.clone())
                .unwrap()
                .expose_secret()
                .clone()
                .iter()
                .zip(input.initial_members.clone())
                .map(|(pkg, peer_id)| (peer_id, pkg.clone()))
                .collect();

        let mut state = TestState::new(
            input.sut_id,
            input.initial_members,
            input.config,
            input.rack_uuid,
            pkgs
        );

        // Complete the learn sequence until the learner has received its
        // `LearnedSharePkg`
        state.learn_share_pkg(input.learn_sequence);

        for action in input.actions {
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
                Action::Learn(peer_id) => state.learn_attempt_must_fail(peer_id),
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
