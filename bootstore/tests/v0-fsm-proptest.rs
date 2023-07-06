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

enum RackInitStatus {
    Running,
    Succeeded,
    Failed,
}

/// State for the running test
pub struct TestState {
    // The ID of the Fsm under test
    sut_id: Baseboard,

    // The Fsm under test
    sut: Fsm,

    // The ID of the RSS sled
    rss_id: Baseboard,

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
    ) -> TestState {
        let sut = Fsm::new_uninitialized(sut_id.clone(), config);
        TestState {
            sut_id,
            sut,
            rss_id,
            initial_members,
            learners,
            connected_peers: BTreeSet::new(),
            current_time: Instant::now(),
            rack_init_status: None,
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
            input.sut_id,
            input.rss_id,
            input.initial_members.clone(),
            input.learners,
            input.config,
        );

        for action in input.actions {
            // println!("{:#?}", action);
            //state.on_action(action)?;
        }
    }
}
