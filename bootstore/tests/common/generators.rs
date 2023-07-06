// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Proptest generators

use bootstore::schemes::v0::Config;
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::time::Duration;
use uuid::Uuid;

// Ranges for timeout generation
const LEARN_TIMEOUT_SECS: RangeInclusive<u64> = 5..=10;
const RACK_SECRET_TIMEOUT_SECS: RangeInclusive<u64> = 20..=50;
const TICKS_PER_ACTION: RangeInclusive<usize> = 1..=5;
const MAX_ACTIONS: usize = 1000;

/// Input to the `run` method of our proptests
#[derive(Debug)]
pub struct TestInput {
    pub actions: Vec<Action>,
    pub initial_members: BTreeSet<Baseboard>,
    pub learners: BTreeSet<Baseboard>,
    pub config: Config,
    pub rack_uuid: Uuid,

    // The sled where rss runs and we should rack init
    pub rss_id: Baseboard,

    // The sled of the system under test
    pub sut_id: Baseboard,
}

/// A test action to drive the test forward
#[derive(Debug, Clone)]
pub enum Action {
    /// Call the `Fsm::init_rack` on `rss_sled`
    RackInit,

    Ticks(usize),

    // Connections are relative to the SUT Fsm
    Connect(BTreeSet<Baseboard>),

    // Disconnections are relative to the SUT Fsm
    Disconnect(BTreeSet<Baseboard>),

    /// Call `Fsm::load_rack_secret`.
    LoadRackSecret,
    // Initialize a learner
    // InitLearner(Baseboard),
}

/// Generate top-level test input
pub fn arb_test_input(
    max_initial_members: usize,
    max_learners: usize,
) -> impl Strategy<Value = TestInput> {
    let min_initial_members = 3;
    (
        arb_initial_member_ids(min_initial_members, max_initial_members),
        arb_learner_ids(max_learners),
        arb_config(),
    )
        .prop_flat_map(|(initial_members, learners, config)| {
            // We have to generate an intermediate tuple of strategies
            let rack_uuid = Uuid::new_v4();
            let rss_selector = any::<prop::sample::Selector>();
            let sut_selector = any::<prop::sample::Selector>();
            (
                proptest::collection::vec(
                    arb_action(initial_members.clone(), learners.clone()),
                    1..=MAX_ACTIONS,
                ),
                Just(initial_members),
                Just(learners),
                Just(config),
                Just(rack_uuid),
                rss_selector,
                sut_selector,
            )
        })
        // then we map the tuple into a structure
        .prop_map(
            |(
                actions,
                initial_members,
                learners,
                config,
                rack_uuid,
                rss_selector,
                sut_selector,
            )| {
                let rss_id = rss_selector.select(&initial_members).clone();
                let sut_id = sut_selector.select(&initial_members).clone();
                TestInput {
                    actions,
                    initial_members,
                    learners,
                    config,
                    rack_uuid,
                    rss_id,
                    sut_id,
                }
            },
        )
}

// Generate an individual Baseboard used as a peer id
fn arb_baseboard() -> impl Strategy<Value = Baseboard> {
    "[a-z]".prop_map(|id| Baseboard::Pc {
        identifier: id.to_string(),
        model: "0".to_string(),
    })
}

// Generate a set of peer IDs for initial members
fn arb_initial_member_ids(
    min: usize,
    max: usize,
) -> impl Strategy<Value = BTreeSet<Baseboard>> {
    proptest::collection::btree_set(arb_baseboard(), min..=max)
}

// Generate a set of peer IDs for learners
fn arb_learner_ids(max: usize) -> impl Strategy<Value = BTreeSet<Baseboard>> {
    proptest::collection::btree_set(
        "learner-[a-z][a-z]".prop_map(|id| Baseboard::Pc {
            identifier: id.to_string(),
            model: "0".to_string(),
        }),
        0..max,
    )
}

// Generate
fn arb_peer_subset(
    peers: Vec<Baseboard>,
) -> impl Strategy<Value = BTreeSet<Baseboard>> + Clone {
    prop::collection::vec(any::<prop::sample::Index>(), 1..=peers.len())
        .prop_map(move |indexes| {
            indexes.into_iter().map(|index| index.get(&peers).clone()).collect()
        })
}

// Generate an FSM configuration
//
// Timeouts are in "Ticks", which maps to a fixed tick timer set by higher
// level software. The actual timing is unimportant for the protocol logic, we
// are just concerned that the behavior is correct in regards to some abstract
// clock.
fn arb_config() -> impl Strategy<Value = Config> {
    (LEARN_TIMEOUT_SECS, RACK_SECRET_TIMEOUT_SECS).prop_map(
        |(learn_timeout, rack_secret_request_timeout)| Config {
            learn_timeout: Duration::from_secs(learn_timeout),
            rack_init_timeout: Duration::from_secs(rack_secret_request_timeout),
            rack_secret_request_timeout: Duration::from_secs(
                rack_secret_request_timeout,
            ),
        },
    )
}

// Generate a single test action to drive the property based tests
fn arb_action(
    initial_members: BTreeSet<Baseboard>,
    learners: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Action> {
    let peers: Vec<_> =
        initial_members.iter().chain(learners.iter()).cloned().collect();
    let peer_subset = arb_peer_subset(peers);
    prop_oneof![
        40 => (TICKS_PER_ACTION).prop_map(Action::Ticks),
        100 => peer_subset.clone().prop_map(Action::Connect),
        50 => peer_subset.prop_map(Action::Disconnect),
        1 => Just(Action::RackInit),
        15 => Just(Action::LoadRackSecret),
    ]
}
