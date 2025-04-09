// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based tests for the behavior of a reconfiguration coordinator

use assert_matches::assert_matches;
use omicron_test_utils::dev::{LogContext, test_setup_log};
use omicron_uuid_kinds::RackUuid;
use proptest::prelude::*;
use slog::Logger;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use test_strategy::{Arbitrary, proptest};
use trust_quorum::{
    Epoch, Node, PersistentState, PlatformId, PrepareMsg, ReconfigureMsg,
    Threshold,
};

// A high-level set of actions to drive the state of a peer forward.
pub enum Action {
    StartReconfigurationNextEpoch,
    StartReconfigurationStaleEpoch,
    Tick(Instant),
    AckPreparesCurrentEpoch(BTreeSet<PlatformId>),
    AckPreparesOldEpoch(BTreeSet<PlatformId>),
    CommitApiCall,
    CancelApiCall,
}

// The test itself maintains an internal state that includes not only the state
// of the `Node`, but also enough state of the trust quorum system to enable
// tests via `Action`s.
struct TestState {
    // The coordinator node which is the system under test (SUT)
    pub node: Node,

    // The current time - pretend time is global, instantaneous, and universal for
    // this test.
    pub now: Instant,

    // Prepare messages seen by each node that is not the SUT
    pub prepares: BTreeMap<Epoch, BTreeMap<PlatformId, PrepareMsg>>,

    // Commit messages seen by each node that is not the SUT
    pub commits: BTreeMap<Epoch, BTreeSet<PlatformId>>,
}

impl TestState {
    pub fn new(log: Logger, coordinator_id: PlatformId) -> TestState {
        TestState {
            node: Node::new(log, coordinator_id, PersistentState::empty()),
            now: Instant::now(),
            prepares: BTreeMap::new(),
            commits: BTreeMap::new(),
        }
    }
}

fn arb_member() -> impl Strategy<Value = PlatformId> {
    (0..255u8)
        .prop_map(|serial| PlatformId::new("test".into(), serial.to_string()))
}

fn arb_initial_members() -> impl Strategy<Value = BTreeSet<PlatformId>> {
    proptest::collection::btree_set(arb_member(), 3..10)
}

#[derive(Arbitrary, Debug)]
pub struct TestInput {
    #[strategy(arb_initial_members())]
    initial_members: BTreeSet<PlatformId>,
    #[strategy(2..#initial_members.len() as u8)]
    threshold: u8,
}

#[proptest]
fn test_coordinator_behavior_from_empty_state(input: TestInput) {
    let logctx = test_setup_log("coordinator_behavior_from_empty_state");
    println!("input = {input:#?}");
    let coordinator_id = input.initial_members.first().unwrap().clone();
    let mut state = TestState::new(logctx.log.clone(), coordinator_id);
    let mut outbox = Vec::new();

    // Create an initial configuration at epoch 1 given the generated initial
    // members.
    let msg = ReconfigureMsg {
        rack_id: RackUuid::new_v4(),
        epoch: Epoch(1),
        last_committed_epoch: None,
        members: input.initial_members,
        threshold: Threshold(input.threshold),
        retry_timeout: Duration::from_millis(100),
    };

    let res = state
        .node
        .coordinate_reconfiguration(state.now, &mut outbox, msg)
        .unwrap();
    //    println!("res = {res:#?}");
    println!("outbox = {outbox:#?}");

    logctx.cleanup_successful();
}
