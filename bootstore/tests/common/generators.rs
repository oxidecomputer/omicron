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
const TICKS_PER_ACTION: RangeInclusive<usize> = 1..=20;
const TICK_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_ACTIONS: usize = 1000;

/// Input to the `run` method of our proptests
#[derive(Debug)]
pub struct TestInput {
    pub actions: Vec<Action>,
    pub initial_members: BTreeSet<Baseboard>,
    pub learners: BTreeSet<Baseboard>,
    pub config: Config,
    pub rack_uuid: Uuid,
}

/// A test action to drive the test forward
#[derive(Debug, Clone)]
pub enum Action {
    /// Call the `Fsm::init_rack` on `rss_sled`
    ///
    /// This may or may not be the SUT Fsm
    /// If it is the SUT FSM then the SUT FSM will act as coordinator
    /// If it is not the SUT FSM, then we will send an `Init` request
    /// to the SUT FSM if the SUT FSM is connected to the `rss_sled`.
    /// If it is not connected this is a noop.
    RackInit {
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        initial_members: BTreeSet<Baseboard>,
    },
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
            (
                proptest::collection::vec(
                    arb_action(
                        rack_uuid,
                        initial_members.clone(),
                        learners.clone(),
                    ),
                    1..=MAX_ACTIONS,
                ),
                Just(initial_members),
                Just(learners),
                Just(config),
                Just(rack_uuid),
            )
        })
        // then we map the tuple into a structure
        .prop_map(
            |(actions, initial_members, learners, config, rack_uuid)| {
                TestInput {
                    actions,
                    initial_members,
                    learners,
                    config,
                    rack_uuid,
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
    rack_uuid: Uuid,
    initial_members: BTreeSet<Baseboard>,
    learners: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Action> {
    let peers: Vec<_> =
        initial_members.iter().chain(learners.iter()).cloned().collect();
    let peer_subset = arb_peer_subset(peers);
    let initial_members2 = initial_members.clone();
    prop_oneof![
            100 => (TICKS_PER_ACTION).prop_map(Action::Ticks),
            5 => peer_subset.clone().prop_map(Action::Connect),
            5 => peer_subset.prop_map(Action::Disconnect),
            // Choose an RSS sled randomly
            1 => any::<prop::sample::Selector>().prop_map(move |selector| {
                Action::RackInit {
                    rss_sled: selector.select(&initial_members2).clone(),
                    rack_uuid,
                    initial_members: initial_members2.clone(),
                }
            }),
            15 => Just(Action::LoadRackSecret),
    /*        10 => any::<prop::sample::Selector>().prop_map(move |selector| {
                // If there are no learners just issue a tick
                selector.try_select(&learners).map_or(
                    Action::Ticks(1),
                    |peer| Action::InitLearner(peer.clone())
                )
            })
            */
        ]
}
