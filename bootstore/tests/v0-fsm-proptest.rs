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
    ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Request, RequestType,
    Response, ResponseType,
};

use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use uuid::Uuid;

use common::generators::{arb_test_input, Action};

/// State for the running test
pub struct TestState {
    // The ID of the Fsm under test
    sut_id: Baseboard,

    // IDs of all initial members
    initial_members: BTreeSet<Baseboard>,

    // IDs of all learners
    learners: BTreeSet<Baseboard>,

    // Any peers connected to the SUT Fsm
    connected_peers: BTreeSet<Baseboard>,

    // The current time at the SUT Fsm
    current_time: Instant,

    // Is the rack already initialized?
    //
    // If initialization fails, we have to wipe all the sleds and start over. For the
    // purposes of this test we therefore assume that initialization always succeeds
    // if the initialization action runs.
    rack_init_complete: bool,
}

impl TestState {
    pub fn new(
        initial_members: BTreeSet<Baseboard>,
        learners: BTreeSet<Baseboard>,
        config: Config,
    ) -> TestState {
        TestState {
            sut_id: initial_members.first().unwrap().clone(),
            initial_members,
            learners,
            connected_peers: BTreeSet::new(),
            current_time: Instant::now(),
            rack_init_complete: false,
        }
    }

    fn all_other_initial_members<'a>(
        &'a self,
        excluded: &'a Baseboard,
    ) -> impl Iterator<Item = &Baseboard> + 'a {
        self.initial_members.iter().filter(move |id| *id != excluded)
    }
}

const MAX_INITIAL_MEMBERS: usize = 12;
const MAX_LEARNERS: usize = 10;

proptest! {
    #![proptest_config(ProptestConfig {max_shrink_iters: 100000, ..ProptestConfig::default()})]
    #[test]
    fn run(input in arb_test_input(MAX_INITIAL_MEMBERS, MAX_LEARNERS)) {
        let mut state = TestState::new(
            input.initial_members.clone(),
            input.learners,
            input.config,
        );

        // Before we run our generated actions, we want to ensure all sleds are
        // connected to the rss_sled and successfully rack init. This is a requirement
        // monitored by humans on the real rack, so let's just do it.
/*        let rss_sled = input.initial_members.first().clone().unwrap();
        let flows = state.all_other_initial_members(&rss_sled).cloned().map(|dest| {
            (rss_sled.clone(), dest)
        }).collect();
        state.on_action(Action::Connect(flows))?;
        state.on_action(Action::RackInit {
            rss_sled: rss_sled.clone(),
            rack_uuid: input.rack_uuid,
            initial_members: input.initial_members.clone()
        })?;
*/
        for action in input.actions {
            println!("{:#?}", action);
            //state.on_action(action)?;
        }
    }
}
