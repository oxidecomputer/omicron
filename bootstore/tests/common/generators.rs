// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Proptest generators

use bootstore::schemes::v0::{FsmConfig, MsgError};
use proptest::prelude::*;
use sled_hardware_types::Baseboard;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::time::Duration;
use uuid::Uuid;

// Ranges for timeout generation
const LEARN_TIMEOUT_SECS: RangeInclusive<u64> = 5..=10;
const RACK_SECRET_TIMEOUT_SECS: RangeInclusive<u64> = 5..=20;
#[allow(dead_code)]
const TICKS_PER_ACTION: RangeInclusive<usize> = 1..=5;
pub const MAX_ACTIONS: usize = 1000;
pub const MIN_INITIAL_MEMBERS: usize = 3;
pub const MAX_INITIAL_MEMBERS: usize = 12;
pub const TICK_TIMEOUT: Duration = Duration::from_millis(250);

/// Actions run after rack init succeeds
#[derive(Debug, Clone)]
pub enum Action {
    LoadRackSecret,
    Connect(Baseboard),
    Disconnect(Baseboard),
    Ticks(usize),
    GetShare(Baseboard),
    // Trigger an error response by using an invalid rack_uuid in the request
    GetShareFail(Baseboard),
    Learn(Baseboard),
    // Generate an error response from another peer
    ErrorResponse(Baseboard, MsgError),
}

pub fn arb_action(
    initial_members: BTreeSet<Baseboard>,
    is_learner: bool,
) -> impl Strategy<Value = Action> {
    // We skip the first peer, which is the SUT if we aren't testing a learner
    let peers: Vec<_> = if is_learner {
        initial_members.iter().cloned().collect()
    } else {
        initial_members.iter().skip(1).cloned().collect()
    };
    let selected_peer = any::<prop::sample::Index>()
        .prop_map(move |index| index.get(&peers).clone());
    let err_response = (selected_peer.clone(), arb_msg_error())
        .prop_map(|(from, err)| Action::ErrorResponse(from, err));
    prop_oneof![
        50 => (TICKS_PER_ACTION).prop_map(Action::Ticks),
        10 => selected_peer.clone().prop_map(Action::Connect),
        10 => selected_peer.clone().prop_map(Action::Disconnect),
        5 => Just(Action::LoadRackSecret),
        3 => selected_peer.clone().prop_map(Action::GetShare),
        3 => selected_peer.prop_map(Action::GetShareFail),
        5 => arb_learner_id().prop_map(Action::Learn),
        3 => err_response
    ]
}

// Generate an individual Baseboard used as a peer id
pub fn arb_baseboard() -> impl Strategy<Value = Baseboard> {
    "[a-z]"
        .prop_map(|id| Baseboard::Pc { identifier: id, model: "0".to_string() })
}

// Generate a set of peer IDs for initial members
pub fn arb_initial_member_ids(
    min: usize,
    max: usize,
) -> impl Strategy<Value = BTreeSet<Baseboard>> {
    proptest::collection::btree_set(arb_baseboard(), min..=max)
}

// Generate a set of peer IDs for learners
#[allow(unused)]
pub fn arb_learner_ids(
    max: usize,
) -> impl Strategy<Value = BTreeSet<Baseboard>> {
    proptest::collection::btree_set(arb_learner_id(), 0..max)
}

pub fn arb_learner_id() -> impl Strategy<Value = Baseboard> {
    "learner-[a-z][a-z]"
        .prop_map(|id| Baseboard::Pc { identifier: id, model: "0".to_string() })
}

// Generate an FSM configuration
pub fn arb_config() -> impl Strategy<Value = FsmConfig> {
    (LEARN_TIMEOUT_SECS, RACK_SECRET_TIMEOUT_SECS).prop_map(
        |(learn_timeout, rack_secret_request_timeout)| FsmConfig {
            learn_timeout: Duration::from_secs(learn_timeout),
            rack_init_timeout: Duration::from_secs(rack_secret_request_timeout),
            rack_secret_request_timeout: Duration::from_secs(
                rack_secret_request_timeout,
            ),
        },
    )
}

// Generate a `MsgError`
#[allow(dead_code)]
pub fn arb_msg_error() -> impl Strategy<Value = MsgError> {
    prop_oneof![
        Just(MsgError::AlreadyInitialized),
        Just(MsgError::NotInitialized),
        Just(MsgError::StillLearning),
        Just(MsgError::CannotSpareAShare),
        Just(MsgError::RackUuidMismatch {
            expected: Uuid::new_v4().into(),
            got: Uuid::new_v4().into()
        })
    ]
}
