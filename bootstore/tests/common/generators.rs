// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Proptest generators

use super::actions::Action;
use bootstore::schemes::v0::{Config, Envelope, Fsm, Msg, Ticks};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use uuid::Uuid;

/// Input to the `run` method of our proptests
#[derive(Debug)]
pub struct TestInput {
    pub actions: Vec<Action>,
    pub initial_members: BTreeSet<Baseboard>,
    pub config: Config,
    pub rack_uuid: Uuid,
}

/// Generate top-level test input
pub fn arb_test_input(
    max_initial_members: usize,
) -> impl Strategy<Value = TestInput> {
    let min_initial_members = 3;
    (arb_peer_ids(min_initial_members, max_initial_members), arb_config())
        .prop_flat_map(|(initial_members, config)| {
            // We have to generate an intermediate tuple of strategies
            let rack_uuid = Uuid::new_v4();
            (
                proptest::collection::vec(
                    arb_action(rack_uuid, config, initial_members.clone()),
                    1..=20,
                ),
                Just(initial_members),
                Just(config),
                Just(rack_uuid),
            )
        })
        // then we map the tuple into a structure
        .prop_map(|(actions, initial_members, config, rack_uuid)| TestInput {
            actions,
            initial_members,
            config,
            rack_uuid,
        })
}

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
    any::<prop::sample::Selector>().prop_map(move |selector| Action::RackInit {
        rss_sled: selector.select(&initial_members).clone(),
        rack_uuid,
        initial_members: initial_members.clone(),
    })
}
