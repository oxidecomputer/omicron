// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for bootstore scheme v0 protocol logic
//!
//! These tests create a single `Fsm` as the system under test (SUT), interact
//! with the Fsm via generated API calls and messages, and verify that the
//! underlying behavior of the Fsm is as expected.

mod common;

use bootstore::schemes::v0::{
    create_pkgs, ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Request,
    RequestType, Response, ResponseType, SharePkg, State,
};

use proptest::prelude::*;
use secrecy::ExposeSecret;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use uuid::Uuid;

use common::generators::{arb_test_input, Action};

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
    current_time: Instant,

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
            current_time: Instant::now(),
            rack_init_status: None,
        }
    }

    pub fn on_action(&mut self, action: Action) -> Result<(), TestCaseError> {
        match action {
            Action::RackInit => self.on_rack_init(),
            _ => Ok(()),
        }
    }

    pub fn on_rack_init(&mut self) -> Result<(), TestCaseError> {
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

                // We only send the init request if the SUT peer is an initial member
                if self.initial_members.contains(&self.sut_id) {
                    let req = rss_to_sut_init_request(pkg);
                    self.run_and_check_rack_init_msg_result(req)?;
                }
            }
        }
        self.check_rack_init_postconditions()?;
        Ok(())
    }

    // Check that a rack init via a message from RSS is correct
    // We only run this if the SUT peer is an initial_member
    // TODO: Should we have separate proptests for the following? :
    // * RSS == SUT
    // * SUT != RSS && initial_members.contains(SUT)
    // * learners.contains(SUT)
    fn run_and_check_rack_init_msg_result(
        &mut self,
        req: Msg,
    ) -> Result<(), TestCaseError> {
        match self.sut.handle_msg(self.current_time, self.rss_id.clone(), req) {
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
        Ok(())
    }

    fn all_other_initial_members<'a>(
        &'a self,
        excluded: &'a Baseboard,
    ) -> impl Iterator<Item = &Baseboard> + 'a {
        self.initial_members.iter().filter(move |id| *id != excluded)
    }
}

fn expect_init_ack_response(
    to: &Baseboard,
    envelope: Envelope,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(&envelope.to, to);
    let matches = matches!(
        envelope.msg,
        Msg::Rsp(Response { type_: ResponseType::InitAck, .. })
    );
    prop_assert!(matches);
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

        for action in input.actions {
            // println!("{:#?}", action);
            state.on_action(action)?;
        }
    }
}
