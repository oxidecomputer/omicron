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
    ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Output, RackSecretState,
    Request, RequestType, Response, ResponseType, State, Ticks,
};

use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use common::actions::{Action, Delays};
use common::generators::arb_test_input;
use common::model::{ExpectedOutput, Model, ModelRackSecretState, PeerModel};
use common::network::Network;

/// State for the running test
///
/// `TestState` contains the real system under test (SUT) state of the peers, as
/// well as helper types and model state that allow making assertions about what
/// we expect the SUT to be at any given point in test execution.
pub struct TestState {
    // All peers in the test
    peers: BTreeMap<Baseboard, Fsm>,

    // IDs of all initial members
    initial_members: BTreeSet<Baseboard>,

    // IDs of all learners
    learners: BTreeSet<Baseboard>,

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

    // Generated delays
    delays: Delays,

    // Model of system behavior
    //
    // Used for asserting that output and messages match what we expect
    model: Model,
}

impl TestState {
    pub fn new(
        initial_members: BTreeSet<Baseboard>,
        learners: BTreeSet<Baseboard>,
        config: Config,
        delays: Delays,
    ) -> TestState {
        let peers = initial_members
            .iter()
            .chain(learners.iter())
            .cloned()
            .map(|id| (id.clone(), Fsm::new_uninitialized(id, config)))
            .collect();

        let model =
            Model::new(config, initial_members.clone(), learners.clone());

        TestState {
            peers,
            initial_members,
            learners,
            network: Network::default(),
            clock: 0,
            config,
            rack_init_complete: false,
            delays,
            model,
        }
    }

    /// Process a test action
    ///
    /// This is the primary test callback and it performs the following tasks:
    ///  * Update the model state
    ///  * Update the actual state of affected peer FSMs
    ///  * Perform any other state updates such as network deliveries
    ///  * Ensure that any output from the peer FSMs is correct such that:
    ///    * It's valid for the given action
    ///    * It matches what the model expected
    ///  * Ensure global invariants of the system under test and test state
    pub fn on_action(&mut self, action: Action) -> Result<(), TestCaseError> {
        let expected = self.model.on_action(action.clone());
        match action {
            Action::RackInit { rss_sled, rack_uuid, initial_members } => {
                self.on_rack_init(rss_sled, rack_uuid, initial_members)?;
            }
            Action::Connect(flows) => {
                let mut sourced_envelopes =
                    BTreeMap::<Baseboard, Vec<Envelope>>::new();
                for (source, dest) in flows {
                    self.network.connected(source.clone(), dest.clone());

                    let output = self.peer_mut(&source).connected(dest.clone());
                    prop_assert_eq!(output.api_output, None);
                    prop_assert_eq!(output.persist, false);
                    sourced_envelopes
                        .entry(source.clone())
                        .or_default()
                        .extend(output.envelopes);

                    let output = self.peer_mut(&dest).connected(source);
                    prop_assert_eq!(output.api_output, None);
                    prop_assert_eq!(output.persist, false);
                    sourced_envelopes
                        .entry(dest)
                        .or_default()
                        .extend(output.envelopes);
                }

                self.check_envelopes_sent_on_connect(
                    sourced_envelopes,
                    expected,
                )?;
            }
            Action::Disconnect(flows) => {
                // TODO: Assert that output makes sense and dispatch it
                for (source, dest) in flows {
                    self.network.disconnected(source.clone(), dest.clone());
                    let _output =
                        self.peer_mut(&source).disconnected(dest.clone());
                    let _output = self.peer_mut(&dest).disconnected(source);
                }
            }
            Action::Ticks(ticks) => {
                for _ in 0..ticks {
                    self.clock += 1;
                    self.network.advance(self.clock);
                    let delivery_time = self.clock + self.delays.msg_delivery;
                    while let Some((destination, mut sourced_msgs)) =
                        self.network.delivered().pop_first()
                    {
                        for (source, msg) in sourced_msgs.drain(..) {
                            // Handle the delivered message
                            let output = self
                                .peer_mut(&destination)
                                .handle(source.clone(), msg.clone());

                            // Verify that the handler did the right thing
                            self.check_handled_msg_output(
                                &destination,
                                &source,
                                &msg,
                                &output,
                            )?;

                            // Send any resulting messages over the network
                            self.network.send(
                                &source,
                                output.envelopes,
                                delivery_time,
                            );
                        }
                    }

                    // Handle ticks at each peer. This can only result in
                    // timeouts. We never send messages on ticks, as there are
                    // no conventional retries. Instead of retries, we send
                    // any necessary messages on new connections, since we are
                    // modeling the network as TCP streams.
                    for (peer_id, fsm) in &mut self.peers {
                        let output = fsm.tick();
                        check_for_timeouts(
                            self.clock, peer_id, output, &expected,
                        )?;
                    }
                }
            }
            Action::ChangeDelays(delays) => {
                self.delays = delays;
            }
            Action::LoadRackSecret(peer) => {
                let output = self.peer_mut(&peer).load_rack_secret();
                self.check_load_rack_secret_api_output(
                    &peer, &output, expected,
                )?;
                let msg_delivery_time = self.clock + self.delays.msg_delivery;
                self.network.send(&peer, output.envelopes, msg_delivery_time);
            }
        }
        self.check_invariants()?;
        Ok(())
    }

    // Handle an `Action::RackInit`
    //
    // We always ensure the first rack init succeeds and so just run it to
    // completion instantly.
    //
    // The justification for this is that in a real system if rack init fails,
    // we have to start from scratch anyway by wiping all bootstores.
    fn on_rack_init(
        &mut self,
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        initial_members: BTreeSet<Baseboard>,
    ) -> Result<(), TestCaseError> {
        let msg_delivery_time = self.clock + self.delays.msg_delivery;

        let output =
            self.peer_mut(&rss_sled).init_rack(rack_uuid, initial_members);
        self.check_rack_init_api_output(&rss_sled, &output)?;

        if self.rack_init_complete {
            // We already initialized the rack and verified an error
            // in `self.check_rack_init_api_output` above.
            // Just return here, as there is nothing left to do and the following code
            // is only meant for test startup.
            return Ok(());
        }

        // Send the `Initialize` messages to all peers
        self.network.send(&rss_sled, output.envelopes, msg_delivery_time);
        self.network.deliver_all();

        // Handle the `Initialize` message sent to each peer
        while let Some((destination, mut sourced_msgs)) =
            self.network.delivered().pop_first()
        {
            // There should only be one `Initialize` message sent to each peer
            prop_assert_eq!(sourced_msgs.len(), 1);
            let (source, msg) = sourced_msgs.pop_front().unwrap();
            let output = self.peer_mut(&destination).handle(source, msg);
            self.check_handle_initialize_req_output(&destination, &output)?;

            // Queue the acknowledgement to the rss_sled in the network
            self.network.send(
                &destination,
                output.envelopes,
                msg_delivery_time,
            );
        }

        // Deliver all the `InitAck` messages to the rss_sleds inbox
        self.network.deliver_all();

        while let Some((destination, sourced_msgs)) =
            self.network.delivered().pop_first()
        {
            // The only destination should be the rss_sled
            prop_assert_eq!(&rss_sled, &destination);
            // TODO: We only deal with initial members now. When
            // we have peers that will join later, we'll have to
            // check the actual initial members.
            prop_assert_eq!(sourced_msgs.len(), self.initial_members.len() - 1);

            let num_responses = sourced_msgs.len();
            for (i, (source, msg)) in sourced_msgs.into_iter().enumerate() {
                let output = self.peer_mut(&destination).handle(source, msg);
                if i == num_responses - 1 {
                    // Rack initialization completes on processing the
                    // last response and we inform the caller.
                    let expected: Output = ApiOutput::RackInitComplete.into();
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

    fn peer(&self, id: &Baseboard) -> &Fsm {
        self.peers.get(id).unwrap()
    }

    fn peer_mut(&mut self, id: &Baseboard) -> &mut Fsm {
        self.peers.get_mut(id).unwrap()
    }

    fn all_other_initial_members<'a>(
        &'a self,
        excluded: &'a Baseboard,
    ) -> impl Iterator<Item = &Baseboard> + 'a {
        self.initial_members.iter().filter(move |id| *id != excluded)
    }

    // After each action is run we ensure that global invariants hold
    //
    // Some of these invariants are to make sure our test environment is correct
    // and not specific to the FSMs themselves.
    fn check_invariants(&self) -> Result<(), TestCaseError> {
        let clock = self.peers.first_key_value().unwrap().1.common_data().clock;
        let mut connected_peers_per_sled = BTreeMap::new();
        for (peer_id, peer) in &self.peers {
            // Ensure that the clock at each sled is identical
            prop_assert_eq!(clock, peer.common_data().clock);
            connected_peers_per_sled
                .insert(peer_id.clone(), peer.common_data().peers.clone());

            // Ensure that we don't hold onto any expired secret state
            // We expire at expiry + 1 to allow *at least* the number of ticks to complete
            match &peer.common_data().rack_secret_state {
                RackSecretState::Empty => (),
                RackSecretState::Retrieving { expiry, .. } => {
                    prop_assert!(clock <= *expiry);
                }
                RackSecretState::Computed { expiry, .. } => {
                    prop_assert!(clock <= *expiry);
                }
            }
        }

        // Ensure that sleds see mutual connectivity.
        for (source, connected_to) in &connected_peers_per_sled {
            for dest in connected_to {
                prop_assert!(connected_peers_per_sled
                    .get(dest)
                    .unwrap()
                    .contains(source));
            }
        }

        Ok(())
    }

    // Verify that the envelopes sent by peers when they become connected to
    // other peers match what the model expects.
    fn check_envelopes_sent_on_connect(
        &self,
        envelopes: BTreeMap<Baseboard, Vec<Envelope>>,
        expected: Vec<ExpectedOutput>,
    ) -> Result<(), TestCaseError> {
        for (_, envelopes) in &envelopes {
            ensure_all_envelopes_contain_get_share(&envelopes)?;
        }
        // Collect all the actual unique (source, dest) pairs
        let actual = envelopes.into_iter().fold(
            BTreeSet::new(),
            |mut acc, (source, envelopes)| {
                for envelope in envelopes {
                    acc.insert((source.clone(), envelope.to));
                }
                acc
            },
        );

        // Collect all the expected unique (source, dest) pairs
        let expected: BTreeSet<_> = expected
            .into_iter()
            .filter_map(|e| {
                if let ExpectedOutput::GetShare { source, destination } = e {
                    Some((source, destination))
                } else {
                    None
                }
            })
            .collect();

        prop_assert_eq!(expected, actual);
        Ok(())
    }

    // Validate that the output from handling a message at a given peer makes sense
    fn check_handled_msg_output(
        &self,
        destination: &Baseboard,
        source: &Baseboard,
        msg: &Msg,
        output: &Output,
    ) -> Result<(), TestCaseError> {
        match msg {
            Msg::Req(Request {
                id,
                type_: RequestType::GetShare { rack_uuid },
            }) => {
                prop_assert_eq!(output.envelopes.len(), 1);
                // Ensure that the response is sent to the requester
                // and is of the righ type
                let envelope = output.envelopes.first().unwrap();
                prop_assert_eq!(&envelope.to, source);
                let is_share_rsp = matches!(
                    envelope.msg,
                    Msg::Rsp(Response {
                        request_id: id,
                        type_: ResponseType::Share(_)
                    })
                );
                prop_assert!(is_share_rsp);
                Ok(())
            }
            Msg::Req(Request { id, type_: RequestType::Learn }) => {
                // TODO: Cover learn requests
                Ok(())
            }
            Msg::Req(_) => {
                // We don't care about Init or InitLearner requests
                Ok(())
            }
            Msg::Rsp(Response {
                request_id,
                type_: ResponseType::Error(error),
            }) => {
                // TODO: Ensure error response is valid when we can
                Ok(())
            }
            Msg::Rsp(_) => {
                // Responses can always be delivered late and so it's sometimes
                // impossible to compare them to either model or real peer
                // FSM state. The only non-error messages we care about for
                // this test are `Share` responses, and we already ensure that
                // they get sent over the network when a `GetShare` request is
                // received. As such we don't test them here, given the problems
                // with knowing when the response is appropriate.
                Ok(())
            }
        }
    }

    // Validate that the output of `Fsm::load_rack_secret` made to a given peer
    // makes sense.
    fn check_load_rack_secret_api_output(
        &self,
        peer_id: &Baseboard,
        output: &Output,
        expected: Vec<ExpectedOutput>,
    ) -> Result<(), TestCaseError> {
        let peer_state = self.peer(peer_id).state();
        let peer_common_data = self.peer(peer_id).common_data();

        // Ensure that our model state, and what the model expects matches
        // our system under test.
        let model = self.model.get_peer(peer_id);
        match &model.rack_secret_state {
            ModelRackSecretState::Empty => {
                prop_assert_eq!(output.envelopes.len(), 0);
                prop_assert!(output.api_output.is_none());
                if !matches!(
                    peer_common_data.rack_secret_state,
                    RackSecretState::Empty
                ) {
                    prop_assert!(false, "Rack secret state should be empty");
                }
            }
            ModelRackSecretState::Retrieving { received, .. } => {
                prop_assert!(output.api_output.is_none());
                if let RackSecretState::Retrieving { from, .. } =
                    &peer_common_data.rack_secret_state
                {
                    // We have learned shares from the same peers as the model
                    prop_assert_eq!(from, received);
                } else {
                    prop_assert!(
                        false,
                        "rack secret state not retrieving shares"
                    );
                }

                // Ensure that all messages are `RequestType::GetShare`
                ensure_all_envelopes_contain_get_share(&output.envelopes)?;

                // Ensure the requests when to the same destinations as the
                // model expected
                let expected_dest: BTreeSet<_> = expected
                    .iter()
                    .filter_map(|e| {
                        if let ExpectedOutput::GetShare {
                            source,
                            destination,
                            ..
                        } = e
                        {
                            Some(destination.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let actual_dest: BTreeSet<_> =
                    output.envelopes.iter().cloned().map(|e| e.to).collect();
                prop_assert_eq!(expected_dest, actual_dest);
            }
            ModelRackSecretState::Computed { .. } => {
                let returned_rack_secret = matches!(
                    output.api_output,
                    Some(Ok(ApiOutput::RackSecret(_)))
                );
                prop_assert!(returned_rack_secret);
                if !matches!(
                    peer_common_data.rack_secret_state,
                    RackSecretState::Computed { .. },
                ) {
                    prop_assert!(
                        false,
                        "Rack secret state should already be computed"
                    );
                }
                prop_assert_eq!(output.envelopes.len(), 0);
            }
        }

        Ok(())
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
                self.all_other_initial_members(rss_sled).zip(&output.envelopes)
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
        // We don't currently send extra rack init messages in this test
        prop_assert_eq!(self.peer(peer_id).state_name(), "initial_member");
        prop_assert!(output.persist);
        prop_assert_eq!(&output.api_output, &None);
        prop_assert_eq!(output.envelopes.len(), 1);
        let response_is_ack = matches!(
            output.envelopes[0].msg,
            Msg::Rsp(Response { request_id: _, type_: ResponseType::InitAck })
        );
        prop_assert!(response_is_ack);
        Ok(())
    }
}

// Ensure that the model state's expected timeouts matches actual timeouts
// reported by peers.
fn check_for_timeouts(
    clock: Ticks,
    peer_id: &Baseboard,
    output: Output,
    expected: &Vec<ExpectedOutput>,
) -> Result<(), TestCaseError> {
    let expected_timeout = expected
        .iter()
        .find(|e| {
            if let ExpectedOutput::RackSecretTimeout(
                expected_peer_id,
                timeout_tick,
            ) = e
            {
                if *timeout_tick == clock && peer_id == expected_peer_id {
                    true
                } else {
                    false
                }
            } else {
                false
            }
        })
        .is_some();

    let actual_timeout =
        matches!(output.api_output, Some(Err(ApiError::RackSecretLoadTimeout)));

    prop_assert_eq!(expected_timeout, actual_timeout);
    Ok(())
}

// Ensure all messages are of `RequestType::GetShare`
fn ensure_all_envelopes_contain_get_share(
    envelopes: &Vec<Envelope>,
) -> Result<(), TestCaseError> {
    for envelope in envelopes {
        let is_get_share = matches!(
            envelope.msg,
            Msg::Req(Request { type_: RequestType::GetShare { .. }, .. })
        );
        prop_assert!(is_get_share);
    }
    Ok(())
}

const MAX_INITIAL_MEMBERS: usize = 12;
const MAX_LEARNERS: usize = 10;

proptest! {
    #![proptest_config(ProptestConfig {max_shrink_iters: 10000, ..ProptestConfig::default()})]
    #[test]
    fn run(input in arb_test_input(MAX_INITIAL_MEMBERS, MAX_LEARNERS)) {
        let mut state = TestState::new(
            input.initial_members.clone(),
            input.learners,
            input.config,
            Delays::default()
        );

        // Before we run our generated actions, we want to ensure all sleds are
        // connected to the rss_sled and successfully rack init. This is a requirement
        // monitored by humans on the real rack, so let's just do it.
        let rss_sled = input.initial_members.first().clone().unwrap();
        let flows = state.all_other_initial_members(&rss_sled).cloned().map(|dest| {
            (rss_sled.clone(), dest)
        }).collect();
        state.on_action(Action::Connect(flows))?;
        state.on_action(Action::RackInit {
            rss_sled: rss_sled.clone(),
            rack_uuid: input.rack_uuid,
            initial_members: input.initial_members.clone()
        })?;

        for action in input.actions {
            //println!("{:#?}", action);
            state.on_action(action)?;
        }
    }
}
