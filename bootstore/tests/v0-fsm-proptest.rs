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

mod common;

use assert_matches::assert_matches;
use bootstore::schemes::v0::{Config, Envelope, Fsm, Msg, Ticks};
use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use common::actions::Action;
use common::generators::arb_actions;

// Named endpoints on a network
type Source = Baseboard;
type Destination = Baseboard;

/// A flow of messages from a source to a destination
type FlowId = (Source, Destination);

/// A simulation of a test network
///
/// Every time a peer handles a message it may send messages. We add these
/// messages to the network and distribute them during the test as a result
/// of actions. We allow dropping of messages and interleaving of messages in
/// different flows (source/dest pairs in one direction). However, since we are modeling a single
/// TCP connection for each flow we do not interleave messages within the same flow.
#[derive(Debug, Default)]
pub struct Network {
    // Messages queued, but not yet sent.
    unsent: BTreeMap<FlowId, Msg>,

    // Messages sent and "floating" in the network
    sent: BTreeMap<FlowId, Msg>,
}

/// State for the running test
///
/// `TestState` contains the real system under test (SUT) state of the peers, as
/// well as helper types and model state that allow making assertions about what
/// we expect the SUT to be at any given point in test execution.
pub struct TestState {
    // All peers in the test
    peers: BTreeMap<Baseboard, Fsm>,

    // A model of the network used for sending and receiving messages across
    // peers
    network: Network,

    // We assume all clocks tick at approximately the same rate, with a delta
    // small enough not to matter for the tests. We don't care about real time
    // synchronization at all. Therefore this clock, can serve as a global clock
    // and we can ensure that on each tick, a tick callback on every FSM fires.
    clock: Ticks,

    // A copy of the configuration at all peers
    config: Config,
}

impl TestState {
    pub fn new(peer_ids: BTreeSet<Baseboard>, config: Config) -> TestState {
        let peers = peer_ids
            .into_iter()
            .map(|id| (id.clone(), Fsm::new_uninitialized(id, config)))
            .collect();

        TestState { peers, network: Network::default(), clock: 0, config }
    }

    pub fn peer(&mut self, id: &Baseboard) -> &mut Fsm {
        self.peers.get_mut(id).unwrap()
    }

    /// Process a test action
    pub fn on_action(&mut self, action: Action) {
        match action {
            Action::Initialize { rss_sled, rack_uuid, initial_members } => {
                let output =
                    self.peer(&rss_sled).init_rack(rack_uuid, initial_members);
                println!("{:#?}", output);
            }
        }
    }
}

proptest! {
    #[test]
    fn run((actions, initial_members, config) in arb_actions(12)) {
        let mut state = TestState::new(initial_members, config);
        for action in actions {
            state.on_action(action);
        }
    }
}
