// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for bootstore scheme v0 protocol logic
//!
//! These tests operate by setting up a testbed of FSMs and triggering them
//! to exchange messages via API calls. The API calls are triggered via test
//! generated `Action`s. We check properties to validate correctness at various
//! stages of action processing such as after an FSM API gets called, and after
//! all generated messages have been delivered to the subset of reachable peers.

use assert_matches::assert_matches;
use bootstore::schemes::v0::{Config, Fsm};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use uuid::Uuid;

// Generate an individual Baseboard used as a peer id
fn arb_baseboard() -> impl Strategy<Value = Baseboard> {
    "[a-z]".prop_map(|id| Baseboard::Pc {
        identifier: id.to_string(),
        model: "0".to_string(),
    })
}

// Generate a set of peer IDs
fn arb_peer_ids() -> impl Strategy<Value = BTreeSet<Baseboard>> {
    proptest::collection::btree_set(arb_baseboard(), 3..=12)
}

// Generate an FSM configuration
//
// Timeouts are in "Ticks", which maps to a fixed tick timer set by higher
// level software. The actual timing is unimportant for the protocol logic, we
// are just concerned that the behavior is correct in regards to some abstract
// clock.
//
// We ensure that `retry_timeout` is always less than the other timeouts.
fn arb_config() -> impl Strategy<Value = Config> {
    (5..=10_usize, 5..=20_usize, 1..=3_usize).prop_map(
        |(learn_timeout, rack_init_timeout, retry_timeout)| Config {
            learn_timeout,
            rack_init_timeout,
            rack_secret_request_timeout: rack_init_timeout,
            retry_timeout,
        },
    )
}

// Generate test actions to drive the property based tests
//fn arb_action() -> impl Strategy<Value = Action> {}

// A test action to drive the test forward.
pub enum Action {
    Initialize {
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        membership: BTreeSet<Baseboard>,
    },
}

proptest! {
    #[test]
    fn peer_ids(ids in arb_peer_ids()) {
        let rack_uuid = Uuid::new_v4();

    }
}
