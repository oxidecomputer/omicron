// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Proptest generators

use bootstore::schemes::v0::{Config, MsgError};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::time::Duration;
use uuid::Uuid;

// Ranges for timeout generation
const LEARN_TIMEOUT_SECS: RangeInclusive<u64> = 5..=10;
const RACK_SECRET_TIMEOUT_SECS: RangeInclusive<u64> = 5..=20;
pub const TICKS_PER_ACTION: RangeInclusive<usize> = 1..=5;
pub const MAX_ACTIONS: usize = 1000;

// Generate an individual Baseboard used as a peer id
pub fn arb_baseboard() -> impl Strategy<Value = Baseboard> {
    "[a-z]".prop_map(|id| Baseboard::Pc {
        identifier: id.to_string(),
        model: "0".to_string(),
    })
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
    "learner-[a-z][a-z]".prop_map(|id| Baseboard::Pc {
        identifier: id.to_string(),
        model: "0".to_string(),
    })
}

// Generate an FSM configuration
pub fn arb_config() -> impl Strategy<Value = Config> {
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

// Generate a `MsgError`
pub fn arb_msg_error() -> impl Strategy<Value = MsgError> {
    prop_oneof![
        Just(MsgError::AlreadyInitialized),
        Just(MsgError::NotInitialized),
        Just(MsgError::StillLearning),
        Just(MsgError::CannotSpareAShare),
        Just(MsgError::RackUuidMismatch {
            expected: Uuid::new_v4(),
            got: Uuid::new_v4()
        })
    ]
}
