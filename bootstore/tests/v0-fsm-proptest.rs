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
use bootstore::schemes::v0::{
    ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Output, Request,
    RequestType, Response, ResponseType, Ticks,
};

use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use common::actions::Action;
use common::generators::arb_actions;
use common::network::Network;

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

    // Is the rack already initialized?
    //
    // If initialization fails, we have to wipe all the sleds and start over. For the
    // purposes of this test we therefore assume that initialization always succeeds
    // if the initialization action runs.
    rack_init_complete: bool,
}

impl TestState {
    pub fn new(peer_ids: BTreeSet<Baseboard>, config: Config) -> TestState {
        let peers = peer_ids
            .into_iter()
            .map(|id| (id.clone(), Fsm::new_uninitialized(id, config)))
            .collect();

        TestState {
            peers,
            network: Network::default(),
            clock: 0,
            config,
            rack_init_complete: false,
        }
    }

    /// Process a test action
    pub fn on_action(&mut self, action: Action) -> Result<(), TestCaseError> {
        match action {
            Action::Initialize { rss_sled, rack_uuid, initial_members } => {
                let output =
                    self.peer(&rss_sled).init_rack(rack_uuid, initial_members);
                self.check_rack_init_api_output(&rss_sled, &output)?;

                // Send the `Initialize` messages to all peers
                self.network.send(&rss_sled, output.envelopes);
                self.network.deliver_all();

                // Handle the `Initialize` message sent to each peer
                while let Some((destination, mut sourced_msgs)) =
                    self.network.delivered().pop_first()
                {
                    // There should only be one `Initialize` message sent to each peer
                    prop_assert_eq!(sourced_msgs.len(), 1);
                    let (source, msg) = sourced_msgs.pop().unwrap();
                    let output = self.peer(&destination).handle(source, msg);
                    self.check_handle_initialize_req_output(
                        &destination,
                        &output,
                    );

                    // Queue the acknowledgement to the rss_sled in the network
                    self.network.send(&destination, output.envelopes);
                }

                // Deliver all the `InitAck` messages to the rss_sleds inbox
                self.network.deliver_all();

                while let Some((destination, mut sourced_msgs)) =
                    self.network.delivered().pop_first()
                {
                    // The only destination should be the rss_sled
                    prop_assert_eq!(&rss_sled, &destination);
                    // TODO: We only deal with initial members now. When
                    // we have peers that will join later, we'll have to
                    // check the actual initial members.
                    prop_assert_eq!(sourced_msgs.len(), self.peers.len() - 1);

                    let num_responses = sourced_msgs.len();
                    for (i, (source, msg)) in
                        sourced_msgs.into_iter().enumerate()
                    {
                        let output =
                            self.peer(&destination).handle(source, msg);
                        if i == num_responses - 1 {
                            // Rack initialization completes on processing the
                            // last response and we inform the caller.
                            let expected: Output =
                                ApiOutput::RackInitComplete.into();
                            prop_assert_eq!(expected, output);
                        } else {
                            // Nothing happens until the rss_sled receives the
                            // last `InitAck` response.
                            prop_assert_eq!(Output::none(), output);
                        }
                    }
                }

                self.rack_init_complete = true;
                Ok(())
            }
        }
    }

    fn peer(&mut self, id: &Baseboard) -> &mut Fsm {
        self.peers.get_mut(id).unwrap()
    }

    fn all_other_peers<'a>(
        &'a self,
        excluded: &'a Baseboard,
    ) -> impl Iterator<Item = &Baseboard> + 'a {
        self.peers.keys().filter(move |id| *id != excluded)
    }

    // Validate that the output of a rack_init API request made to a given
    // peer makes sense
    fn check_rack_init_api_output(
        &self,
        rss_sled: &Baseboard,
        output: &Output,
    ) -> Result<(), TestCaseError> {
        if self.rack_init_complete {
            let expected: Output = ApiError::RackAlreadyInitialized.into();
            prop_assert_eq!(&expected, output);
            Ok(())
        } else {
            prop_assert!(output.persist);
            prop_assert_eq!(&output.api_output, &None);
            for (peer, envelope) in
                self.all_other_peers(rss_sled).zip(&output.envelopes)
            {
                prop_assert_eq!(peer, &envelope.to);
            }
            Ok(())
        }
    }

    // Verify that the output of a peer receiving an `Initialize` request
    // is appropriate.
    fn check_handle_initialize_req_output(
        &self,
        peer_id: &Baseboard,
        output: &Output,
    ) -> Result<(), TestCaseError> {
        // TODO: FILL ME IN
        Ok(())
    }
}

proptest! {
    #[test]
    fn run((actions, initial_members, config) in arb_actions(12)) {
        let mut state = TestState::new(initial_members, config);
        for action in actions {
            state.on_action(action)?;
        }
    }
}
