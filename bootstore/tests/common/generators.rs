// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Proptest generators

use super::network::FlowId;
use bootstore::schemes::v0::{Config, Ticks};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use uuid::Uuid;

// Ranges for timeout generation
const LEARN_TIMEOUT: RangeInclusive<Ticks> = 5..=10;
const RACK_SECRET_TIMEOUT: RangeInclusive<Ticks> = 20..=50;
const TICKS_PER_ACTION: RangeInclusive<Ticks> = 1..=20;
const MSG_DELIVERY_DELAY: RangeInclusive<Ticks> = 0..=20;
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

// Certain operations take different amounts of time to complete, and messages
// take some duration to deliver. We can parameterize how long operations
// and message delivery take without being overly prescriptive by adopting a
// certain tick behavior at each point in the test run.
//
// While we could get complex and map different delivery times to different
// network flows, and have operations take different amounts of time at
// different sleds, we keep things relatively simple for now by having the tick
// behavior affect all flows and sleds equally.
#[derive(Debug, Clone)]
pub struct Delays {
    // The time to send a message from source to destination
    pub msg_delivery: Ticks,
}

impl Default for Delays {
    fn default() -> Self {
        Delays { msg_delivery: 1 }
    }
}

/// A test action to drive the test forward
#[derive(Debug, Clone)]
pub enum Action {
    /// Call the `Fsm::init_rack` on `rss_sled`
    RackInit {
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        initial_members: BTreeSet<Baseboard>,
    },
    ChangeDelays(Delays),
    Ticks(Ticks),
    Connect(Vec<FlowId>),
    Disconnect(Vec<FlowId>),

    /// Call `Fsm::load_rack_secret` on the given sled
    LoadRackSecret(Baseboard),

    // Initialize a learner
    InitLearner(Baseboard),
}

/// Generate top-level test input
pub fn arb_test_input(
    max_initial_members: usize,
    max_learners: usize,
) -> impl Strategy<Value = TestInput> {
    let min_initial_members = 3;
    (
        arb_peer_ids(min_initial_members, max_initial_members),
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
fn arb_peer_ids(
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

// Generate an FSM configuration
//
// Timeouts are in "Ticks", which maps to a fixed tick timer set by higher
// level software. The actual timing is unimportant for the protocol logic, we
// are just concerned that the behavior is correct in regards to some abstract
// clock.
fn arb_config() -> impl Strategy<Value = Config> {
    (LEARN_TIMEOUT, RACK_SECRET_TIMEOUT).prop_map(
        |(learn_timeout, rack_secret_request_timeout)| Config {
            learn_timeout,
            rack_init_timeout: rack_secret_request_timeout,
            rack_secret_request_timeout,
        },
    )
}

// Generate a set of flows from one peer to another *different* peer
fn arb_flows(
    initial_members: Vec<Baseboard>,
) -> impl Strategy<Value = Vec<FlowId>> + Clone {
    prop::collection::vec(
        any::<prop::sample::Index>(),
        2..=initial_members.len(),
    )
    .prop_shuffle()
    .prop_map(move |indexes| {
        indexes
            .chunks_exact(2)
            .filter_map(|indexes| {
                let source =
                    &initial_members[indexes[0].index(initial_members.len())];
                let dest =
                    &initial_members[indexes[1].index(initial_members.len())];

                // Don't create flows from a peer to itself
                if source != dest {
                    Some((source.clone(), dest.clone()))
                } else {
                    None
                }
            })
            .collect()
    })
}

// Generate arbitrary `Delays`
fn arb_delays() -> impl Strategy<Value = Delays> {
    MSG_DELIVERY_DELAY.prop_map(|msg_delivery| Delays { msg_delivery })
}

// Generate a single test action to drive the property based tests
fn arb_action(
    rack_uuid: Uuid,
    initial_members: BTreeSet<Baseboard>,
    learners: BTreeSet<Baseboard>,
) -> impl Strategy<Value = Action> {
    let peers: Vec<_> =
        initial_members.iter().chain(learners.iter()).cloned().collect();
    let flows = arb_flows(peers);
    let initial_members2 = initial_members.clone();
    prop_oneof![
        100 => (TICKS_PER_ACTION).prop_map(Action::Ticks),
        5 => flows.clone().prop_map(Action::Connect),
        5 => flows.prop_map(Action::Disconnect),
        20 => arb_delays().prop_map(Action::ChangeDelays),
        // Choose an RSS sled randomly
        1 => any::<prop::sample::Selector>().prop_map(move |selector| {
            Action::RackInit {
                rss_sled: selector.select(&initial_members2).clone(),
                rack_uuid,
                initial_members: initial_members2.clone(),
            }
        }),
        15 => any::<prop::sample::Selector>().prop_map(move |selector| {
            Action::LoadRackSecret(selector.select(&initial_members).clone())
        }),
        10 => any::<prop::sample::Selector>().prop_map(move |selector| {
            // If there are no learners just issue a tick
            selector.try_select(&learners).map_or(
                Action::Ticks(1),
                |peer| Action::InitLearner(peer.clone())
            )
        })
    ]
}
