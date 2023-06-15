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
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

// Generate an individual Baseboard used as a peer id
fn arb_baseboard() -> impl Strategy<Value = Baseboard> {
    "[a-z]".prop_map(|id| Baseboard::Pc {
        identifier: id.to_string(),
        model: "0".to_string(),
    })
}

// Generate a set of peer IDs
fn arb_peer_ids(
    min: usize,
    max: usize,
) -> impl Strategy<Value = BTreeSet<Baseboard>> {
    proptest::collection::btree_set(arb_baseboard(), min..=max)
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

// Generate a single test action to drive the property based tests
fn arb_action(
    rack_uuid: Uuid,
    config: Config,
    initial_members: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Action> {
    // Choose an RSS sled randomly
    any::<prop::sample::Selector>().prop_map(move |selector| {
        Action::Initialize {
            rss_sled: selector.select(&initial_members).clone(),
            rack_uuid,
            initial_members: initial_members.clone(),
        }
    })
}

// Generate a vector of arbitrary actions to drive the property based tests
//
// This vector of actions is the top level generator for our tests.
fn arb_actions(
    max_initial_members: usize,
) -> impl Strategy<Value = (Vec<Action>, BTreeSet<Baseboard>, Config)> {
    let min_initial_members = 3;
    (arb_peer_ids(min_initial_members, max_initial_members), arb_config())
        .prop_flat_map(|(initial_members, config)| {
            let rack_uuid = Uuid::new_v4();
            (
                proptest::collection::vec(
                    arb_action(rack_uuid, config, initial_members.clone()),
                    1..=5,
                ),
                Just(initial_members),
                Just(config),
            )
        })
}

// A test action to drive the test forward.
#[derive(Debug)]
pub enum Action {
    Initialize {
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        initial_members: BTreeSet<Baseboard>,
    },
}

// Create a set of FSMs used in a test
fn create_peers(
    peer_ids: BTreeSet<Baseboard>,
    config: Config,
) -> BTreeMap<Baseboard, Fsm> {
    peer_ids
        .into_iter()
        .map(|id| (id.clone(), Fsm::new_uninitialized(id, config)))
        .collect()
}

fn assert_invariants(
    peers: &BTreeMap<Baseboard, Fsm>,
) -> Result<(), TestCaseError> {
    Ok(())
}

proptest! {
    #[test]
//fn blah() {
    fn run((actions, initial_members, config) in arb_actions(12)) {
        let mut peers = create_peers(initial_members, config);
        for action in actions {
            match action {
                Action::Initialize { rss_sled, rack_uuid, initial_members } => {
                    let output = peers
                        .get_mut(&rss_sled)
                        .unwrap()
                        .init_rack(rack_uuid, initial_members);
//                    println!("{:#?}", output);
                }
            }
            assert_invariants(&peers);
        }
    }
}
