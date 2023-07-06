// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for bootstore scheme v0 protocol logic
//!
//! These tests create a single `Fsm` as the system under test (SUT), interact
//! with the Fsm via generated API calls and messages, and verify that the
//! underlying behavior of the Fsm is as expected.

mod common;

use assert_matches::assert_matches;
use bootstore::schemes::v0::{
    create_pkgs, ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Request,
    RequestType, Response, ResponseType, SharePkg, State,
};
use proptest::prelude::*;
use secrecy::ExposeSecret;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use uuid::Uuid;

use common::generators::{arb_test_input, Action};

const TICK_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, PartialEq, Eq)]
enum RackInitStatus {
    // The RSS node is *not* the SUT node and we pretend it created the packages
    // stored here.
    RssNotSut(Vec<SharePkg>),

    // The SUT is the RSS node and is running
    SutAsRssRunning,
    // The SUT is the RSS node and has completed successfully
    SutAsRssSucceeded,
    // The SUT is the RSS node and has failed
    //
    // This is a state at which any further commands are undefined. The test
    // should  just stop and pass at this point.
    SutAsRssFailed,
}

/// State for the running test
pub struct TestState {
    // The ID of the Fsm under test
    sut_id: Baseboard,

    // The Fsm under test
    sut: Fsm,

    // The ID of the RSS sled
    rss_id: Baseboard,

    // The unique id of the initialized rack
    rack_uuid: Uuid,

    // IDs of all initial members
    initial_members: BTreeSet<Baseboard>,

    // IDs of all learners
    learners: BTreeSet<Baseboard>,

    // Any peers connected to the SUT Fsm
    connected_peers: BTreeSet<Baseboard>,

    // The current time at the SUT Fsm
    now: Instant,

    // We only allow rack init to run once if SUT Fsm is the RSS sled.
    //
    //
    // A failed rack init requires a complete rack reset, and so we just pass
    // the test if a rack init fails for any reason when the SUT is the RSS sled.
    rack_init_status: Option<RackInitStatus>,
}

impl TestState {
    pub fn new(
        sut_id: Baseboard,
        rss_id: Baseboard,
        initial_members: BTreeSet<Baseboard>,
        learners: BTreeSet<Baseboard>,
        config: Config,
        rack_uuid: Uuid,
    ) -> TestState {
        let sut = Fsm::new_uninitialized(sut_id.clone(), config);
        TestState {
            sut_id,
            sut,
            rss_id,
            rack_uuid,
            initial_members,
            learners,
            connected_peers: BTreeSet::new(),
            now: Instant::now(),
            rack_init_status: None,
        }
    }

    pub fn on_action(&mut self, action: Action) -> Result<(), TestCaseError> {
        match action {
            Action::RackInit => self.on_rack_init(),
            Action::Connect(peers) => self.on_connect(peers),
            Action::Disconnect(peers) => self.on_disconnect(peers),
            Action::Ticks(ticks) => self.on_ticks(ticks),
            Action::LoadRackSecret => self.on_load_rack_secret(),
        }
    }

    fn on_load_rack_secret(&mut self) -> Result<(), TestCaseError> {
        match self.sut.load_rack_secret(self.now) {
            Ok(()) => {
                if self.rss_id == self.sut_id {
                    // SUT is either an initializing coordinator, or rack init
                    // completed successfully
                    let valid = match &self.rack_init_status {
                        Some(RackInitStatus::SutAsRssRunning)
                        | Some(RackInitStatus::SutAsRssSucceeded) => true,
                        _ => false,
                    };
                    prop_assert!(valid);
                    // We allow rack secret loading during init so that the coordinator is symmetric
                    // with other nodes.
                    for envelope in self.sut.drain_envelopes() {
                        prop_assert!(self
                            .connected_peers
                            .contains(&envelope.to));
                        expect_init_or_get_share_request(envelope.msg);
                    }
                } else {
                    // SUT != RSS
                    assert_matches!(
                        self.rack_init_status,
                        Some(RackInitStatus::RssNotSut(_))
                    );
                    for envelope in self.sut.drain_envelopes() {
                        prop_assert!(self
                            .connected_peers
                            .contains(&envelope.to));
                        expect_get_share_request(envelope.msg);
                    }
                }
                assert_matches!(self.sut.state(), State::InitialMember { .. });
            }
            Err(err) => match err {
                ApiError::NotInitialized => {
                    prop_assert!(self.rack_init_status.is_none());
                    assert_matches!(self.sut.state(), State::Uninitialized);
                }
                _ => panic!("Unexpected error: {}", err),
            },
        }

        Ok(())
    }

    fn on_ticks(&mut self, ticks: usize) -> Result<(), TestCaseError> {
        for _ in 0..ticks {
            self.now += TICK_TIMEOUT;
            match self.sut.tick(self.now) {
                Ok(()) => {
                    self.check_successful_tick_postconditions()?;
                }
                Err(errors) => self.check_tick_errors(errors)?,
            }
        }
        Ok(())
    }

    fn on_rack_init(&mut self) -> Result<(), TestCaseError> {
        self.check_rack_init_preconditions()?;
        if self.sut_id != self.rss_id {
            if self.rack_init_status.is_none() {
                // This is the first rack init from RSS to SUT
                let pkgs =
                    create_pkgs(self.rack_uuid, self.initial_members.clone())
                        .unwrap()
                        .expose_secret()
                        .clone();
                // We arbitrarily choose the first share for rack init
                let pkg = pkgs[0].clone();
                self.rack_init_status = Some(RackInitStatus::RssNotSut(pkgs));
                let req = rss_to_sut_init_request(pkg);
                self.run_and_check_rack_init_msg_result(req)?;
            }
        } else {
            // SUT == RSS: Go ahead and call `Fsm::init_rack()`
            self.run_and_check_init_rack_api_call()?;
            self.rack_init_status = Some(RackInitStatus::SutAsRssRunning);
        }
        self.check_rack_init_postconditions()?;
        Ok(())
    }

    fn on_connect(
        &mut self,
        mut peers: BTreeSet<Baseboard>,
    ) -> Result<(), TestCaseError> {
        // Filter out the SUT
        let _ = peers.remove(&self.sut_id);
        let new_peers: BTreeSet<_> =
            peers.difference(&self.connected_peers).cloned().collect();
        for peer in &new_peers {
            let _ = self.sut.on_connected(self.now, peer.clone());

            // TODO: We will be expecting diff messages once we handle more actions
            match self.rack_init_status {
                Some(RackInitStatus::SutAsRssRunning)
                | Some(RackInitStatus::SutAsRssSucceeded) => {
                    // We allow rack secret loading during init so that the coordinator is symmetric
                    // with other nodes.
                    for envelope in self.sut.drain_envelopes() {
                        prop_assert_eq!(peer, &envelope.to);
                        expect_init_or_get_share_request(envelope.msg);
                    }
                }
                Some(RackInitStatus::RssNotSut(_)) => {
                    for envelope in self.sut.drain_envelopes() {
                        prop_assert_eq!(peer, &envelope.to);
                        expect_get_share_request(envelope.msg);
                    }
                }
                _ => (),
            }
        }
        self.connected_peers.extend(new_peers);
        Ok(())
    }

    fn on_disconnect(
        &mut self,
        mut peers: BTreeSet<Baseboard>,
    ) -> Result<(), TestCaseError> {
        // Filter out the SUT
        for peer in &peers {
            self.sut.on_disconnected(peer);
            self.connected_peers.remove(peer);

            // There should be no envelopes sent on a disconnect
            prop_assert_eq!(None, self.sut.drain_envelopes().next());
        }
        Ok(())
    }

    // Call `Fsm::init_rack` at the SUT and validate the output
    fn run_and_check_init_rack_api_call(
        &mut self,
    ) -> Result<(), TestCaseError> {
        if let Err(e) = self.sut.init_rack(
            self.now,
            self.rack_uuid,
            self.initial_members.clone(),
        ) {
            prop_assert!(self.rack_init_status.is_some())
        } else {
            // There should be an init message for each connected peer
            self.expect_init_broadcast()?;
        }
        Ok(())
    }

    // Handle a msg with `RequestType::Init` at the SUT and check that a rack
    // init via a message from RSS is correct
    fn run_and_check_rack_init_msg_result(
        &mut self,
        req: Msg,
    ) -> Result<(), TestCaseError> {
        match self.sut.handle_msg(self.now, self.rss_id.clone(), req) {
            Ok(Some(output)) => {
                prop_assert_eq!(output, ApiOutput::PeerInitialized);
                let mut iter = self.sut.drain_envelopes();
                // There should be an `InitAck` response
                expect_init_ack_response(&self.rss_id, iter.next().unwrap())?;
                // There shouldn't be any more messages
                prop_assert_eq!(None, iter.next());
            }
            Ok(None) => {
                // We generate the same idempotent request, so we should see the same
                // InitAck message.
                let mut iter = self.sut.drain_envelopes();
                // There should be an `InitAck` response
                expect_init_ack_response(&self.rss_id, iter.next().unwrap())?;
                // There shouldn't be any more messages
                prop_assert_eq!(None, iter.next());
            }
            Err(err) => {
                panic!("No error expected: {}", err);
            }
        }
        Ok(())
    }

    fn check_rack_init_preconditions(&self) -> Result<(), TestCaseError> {
        if self.rack_init_status.is_none() {
            prop_assert_eq!(self.sut.state(), &State::Uninitialized);
        }
        Ok(())
    }

    fn check_rack_init_postconditions(&self) -> Result<(), TestCaseError> {
        if self.initial_members.contains(&self.sut_id) {
            prop_assert_eq!(self.sut.state().name(), "initial_member");
        }
        Ok(())
    }

    fn check_successful_tick_postconditions(
        &mut self,
    ) -> Result<(), TestCaseError> {
        match self.rack_init_status {
            None => {
                // There should be no messages output if we are uninitialized
                prop_assert_eq!(self.sut.state(), &State::Uninitialized);
                prop_assert_eq!(None, self.sut.drain_envelopes().next());
            }
            Some(RackInitStatus::SutAsRssRunning) => {
                // There should be an existing InitRack request in the `RequestManager`
                prop_assert!(self.sut.is_rack_initializing());
            }
            _ => (),
        }

        Ok(())
    }

    fn check_tick_errors(
        &mut self,
        errors: BTreeMap<Uuid, ApiError>,
    ) -> Result<(), TestCaseError> {
        // TODO: Deal with other types of errors after we add test support
        for error in errors {
            if matches!(error, (_, ApiError::RackInitTimeout { .. })) {
                // There should no longer be an outstanding `InitRack` request
                // in the `RequestManager`
                prop_assert!(!self.sut.is_rack_initializing());
                prop_assert!(self.sut.rack_init_failed());
                self.rack_init_status = Some(RackInitStatus::SutAsRssFailed);
            }
        }
        Ok(())
    }

    fn expect_init_broadcast(&mut self) -> Result<(), TestCaseError> {
        let sent_to: BTreeSet<Baseboard> = self
            .sut
            .drain_envelopes()
            .map(|envelope| {
                assert_matches!(
                    envelope.msg,
                    Msg::Req(Request { type_: RequestType::Init(_), .. })
                );
                envelope.to
            })
            .collect();

        prop_assert_eq!(
            &sent_to,
            &self.connected_peers.difference(&self.learners).cloned().collect()
        );
        Ok(())
    }
}

fn expect_get_share_request(msg: Msg) {
    assert_matches!(
        msg,
        Msg::Req(Request { type_: RequestType::GetShare { .. }, .. })
    );
}

fn expect_init_or_get_share_request(msg: Msg) {
    assert_matches!(
        msg,
        Msg::Req(
            Request { type_: RequestType::Init(_), .. }
                | Request { type_: RequestType::GetShare { .. }, .. }
        )
    );
}

fn expect_init_request(
    to: &Baseboard,
    envelope: Envelope,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(&envelope.to, to);
    assert_matches!(
        envelope.msg,
        Msg::Req(Request { type_: RequestType::Init(_), .. })
    );
    Ok(())
}

fn expect_init_ack_response(
    to: &Baseboard,
    envelope: Envelope,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(&envelope.to, to);
    assert_matches!(
        envelope.msg,
        Msg::Rsp(Response { type_: ResponseType::InitAck, .. })
    );
    Ok(())
}

// A request for rack init sent to the SUT from RSS
fn rss_to_sut_init_request(pkg: SharePkg) -> Msg {
    Request {
        id: Uuid::new_v4(),
        // We always just give the SUT
        type_: RequestType::Init(pkg),
    }
    .into()
}

const MAX_INITIAL_MEMBERS: usize = 12;
const MAX_LEARNERS: usize = 10;

proptest! {
    #![proptest_config(ProptestConfig {max_shrink_iters: 100000, ..ProptestConfig::default()})]
    #[test]
    fn run(input in arb_test_input(MAX_INITIAL_MEMBERS, MAX_LEARNERS)) {
        let mut state = TestState::new(
            input.sut_id,
            input.rss_id,
            input.initial_members.clone(),
            input.learners,
            input.config,
            input.rack_uuid,
        );

        for (i, action) in input.actions.into_iter().enumerate() {
            if let Some(RackInitStatus::SutAsRssFailed) = state.rack_init_status {
                // Just assume the test succeeded. We can't do anything after
                // a faiilure to initialize. In a real system the operator would perform an RSS
                // reset.
                // We want to minimize this path in our tests, but we still want
                // to test it. That's why we wait `Action` generators appropriately.
                eprintln!("Rack init failed: proptest bailed after {i} iterations");
                return Ok(());
            }
            //println!("{:#?}", action);
            state.on_action(action)?;
        }
    }
}
