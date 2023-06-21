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

use bootstore::schemes::v0::{
    ApiError, ApiOutput, Config, Envelope, Fsm, Msg, Output, RackSecretState,
    Request, RequestType, Response, ResponseType, Ticks,
};

use proptest::prelude::*;
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use common::generators::{arb_test_input, Action, Delays};
use common::model::{ExpectedMsg, ExpectedOutput, Model, ModelState};
use common::network::Network;

use crate::common::model::{ExpectedApiError, ExpectedApiOutput};

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

    // A model of the network used for sending and receiving messages across
    // peers
    network: Network,

    // We assume all clocks tick at approximately the same rate, with a delta
    // small enough not to matter for the tests. We don't care about real time
    // synchronization at all. Therefore, this clock can serve as a global clock
    // and we can ensure that on each tick a tick callback on every FSM fires.
    clock: Ticks,

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
            network: Network::default(),
            clock: 0,
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
        let delivery_time = self.clock + self.delays.msg_delivery;
        match action {
            Action::RackInit { rss_sled, rack_uuid, initial_members } => {
                self.model.on_rack_init(rack_uuid);
                self.on_rack_init(rss_sled, rack_uuid, initial_members)?;
            }
            Action::Connect(flows) => {
                for (source, dest) in flows {
                    self.network.connected(source.clone(), dest.clone());

                    // Feed the model and fsm for source->dest
                    let expected = self.model.on_connect(&source, dest.clone());
                    let output = self.peer_mut(&source).connected(dest.clone());
                    check_connect_output(expected, &output)?;
                    self.network.send(&source, output.envelopes, delivery_time);

                    // Feed the model and fsm for dest->source
                    let expected = self.model.on_connect(&dest, source.clone());
                    let output = self.peer_mut(&dest).connected(source);
                    check_connect_output(expected, &output)?;
                    self.network.send(&dest, output.envelopes, delivery_time);
                }
            }
            Action::Disconnect(flows) => {
                for (source, dest) in flows {
                    self.network.disconnected(source.clone(), dest.clone());

                    // source -> dest
                    let expected = self.model.on_disconnect(&source, &dest);
                    let output =
                        self.peer_mut(&source).disconnected(dest.clone());
                    check_disconnect_output(expected, &output)?;
                    self.network.send(&source, output.envelopes, delivery_time);

                    // dest -> source
                    let expected = self.model.on_disconnect(&dest, &source);
                    let output = self.peer_mut(&dest).disconnected(source);
                    check_disconnect_output(expected, &output)?;
                    self.network.send(&dest, output.envelopes, delivery_time);
                }
            }
            Action::Ticks(ticks) => {
                for _ in 0..ticks {
                    self.clock += 1;
                    self.network.advance(self.clock);
                    let delivery_time = self.clock + self.delays.msg_delivery;

                    // We arbitrarily decide to deliver messages before ticks
                    while let Some((destination, mut sourced_msgs)) =
                        self.network.delivered().pop_first()
                    {
                        for (source, msg) in sourced_msgs.drain(..) {
                            // Handle the delivered message in the model
                            let expected = self.model.handle_msg(
                                &source,
                                &destination,
                                msg.clone(),
                            );

                            // Handle the delivered message
                            let output = self
                                .peer_mut(&destination)
                                .handle(source.clone(), msg.clone());

                            // Verify that the handler did the right thing
                            check_handled_msg_output(expected, &output)?;

                            // Send any resulting messages over the network
                            self.network.send(
                                &destination,
                                output.envelopes,
                                delivery_time,
                            );
                        }
                    }

                    // Handle ticks at each peer. This can only result in
                    // timeouts or learn requests.
                    for (peer_id, fsm) in &mut self.peers {
                        let expected = self.model.on_tick(peer_id);
                        let output = fsm.tick();
                        check_tick_output(expected, &output)?;
                        self.network.send(
                            peer_id,
                            output.envelopes,
                            delivery_time,
                        );
                    }
                }
            }
            Action::ChangeDelays(delays) => {
                self.delays = delays;
            }
            Action::LoadRackSecret(peer) => {
                let expected = self.model.load_rack_secret(&peer);
                let output = self.peer_mut(&peer).load_rack_secret();
                check_load_rack_secret_output(expected, &output)?;

                let msg_delivery_time = self.clock + self.delays.msg_delivery;
                self.network.send(&peer, output.envelopes, msg_delivery_time);
            }
            Action::InitLearner(peer) => {
                let expected = self.model.init_learner(&peer);
                let output = self.peer_mut(&peer).init_learner();
                check_init_learner_output(expected, &output)?;

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

            // Keep track of connectivity for later check
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

            // Ensure the peer is in the same state as the model
            match self.model.get_peer(peer_id).state {
                ModelState::Uninitialized => {
                    prop_assert_eq!(peer.state_name(), "uninitialized")
                }
                ModelState::InitialMember { .. } => {
                    prop_assert_eq!(peer.state_name(), "initial_member")
                }
                ModelState::Learning { .. } => {
                    prop_assert_eq!(peer.state_name(), "learning")
                }
                ModelState::Learned => {
                    prop_assert_eq!(peer.state_name(), "learned")
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

fn check_disconnect_output(
    expected: Option<ExpectedMsg>,
    output: &Output,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(&output.api_output, &None);
    prop_assert_eq!(output.persist, false);
    if let Some(ExpectedMsg::Learn { destination }) = expected {
        prop_assert_eq!(1, output.envelopes.len());
        let Envelope { to, msg } = output.envelopes.first().unwrap();
        prop_assert_eq!(&destination, to);
        let matches =
            matches!(msg, Msg::Req(Request { type_: RequestType::Learn, .. }));
        prop_assert!(matches);
    } else {
        prop_assert_eq!(0, output.envelopes.len());
    }
    Ok(())
}

// Verify that the envelopes sent by peers when they become connected to
// other peers match what the model expects.
//
// There should only be at most either a single `Learn` or `GetShare`
// request
fn check_connect_output(
    expected: Option<ExpectedMsg>,
    output: &Output,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(&output.api_output, &None);
    prop_assert_eq!(output.persist, false);
    match expected {
        Some(ExpectedMsg::GetShare) => {
            prop_assert_eq!(output.envelopes.len(), 1);
            let matches = matches!(
                output.envelopes.first().unwrap().msg,
                Msg::Req(Request { type_: RequestType::GetShare { .. }, .. })
            );
            prop_assert!(matches);
        }
        Some(ExpectedMsg::Learn { destination }) => {
            prop_assert_eq!(output.envelopes.len(), 1);
            let Envelope { to, msg } = output.envelopes.first().unwrap();
            prop_assert_eq!(&destination, to);
            let matches = matches!(
                msg,
                Msg::Req(Request { type_: RequestType::Learn, .. })
            );
            prop_assert!(matches);
        }
        Some(_) => prop_assert!(false, "ExpectedMsg makes no sense"),
        None => {
            prop_assert_eq!(output.envelopes.len(), 0);
        }
    }

    Ok(())
}

// Validate that the output from handling a message at a given peer makes sense
fn check_handled_msg_output(
    expected: ExpectedOutput,
    output: &Output,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(expected.persist, output.persist);

    // Verify that any expected api output matches actual output
    match expected.api_output {
        None => prop_assert!(output.api_output.is_none()),
        Some(Ok(ExpectedApiOutput::RackSecretLoaded)) => {
            let matches =
                matches!(output.api_output, Some(Ok(ApiOutput::RackSecret(_))));
            prop_assert!(matches);
        }
        Some(Err(ExpectedApiError::UnexpectedResponse)) => {
            let matches = matches!(
                output.api_output,
                Some(Err(ApiError::UnexpectedResponse { .. }))
            );
            prop_assert!(matches);
        }
        Some(Err(ExpectedApiError::ErrorResponseReceived)) => {
            let matches = matches!(
                output.api_output,
                Some(Err(ApiError::ErrorResponseReceived { .. }))
            );
            prop_assert!(matches);
        }
        Some(Err(_)) => {
            prop_assert!(false, "ExpectedApiError makes no sense")
        }
    }

    // Verify that any expected messages match actual messages in envelopes
    if expected.msg.is_none() {
        prop_assert!(output.envelopes.is_empty());
        return Ok(());
    }

    match expected.msg.unwrap() {
        ExpectedMsg::BroadcastGetShare(to) => {
            // Ensure that a `GetShare` request is sent to all connected peers
            prop_assert_eq!(to.len(), output.envelopes.len());
            for envelope in &output.envelopes {
                prop_assert!(to.contains(&envelope.to));
                let matches = matches!(
                    envelope.msg,
                    Msg::Req(Request {
                        type_: RequestType::GetShare { .. },
                        ..
                    })
                );
                prop_assert!(matches);
            }
        }
        ExpectedMsg::Pkg => {
            prop_assert_eq!(output.envelopes.len(), 1);
            let matches = matches!(
                output.envelopes.first().unwrap().msg,
                Msg::Rsp(Response { type_: ResponseType::Pkg(_), .. })
            );
            prop_assert!(matches);
        }
        ExpectedMsg::Error => {
            prop_assert_eq!(output.envelopes.len(), 1);
            let matches = matches!(
                output.envelopes.first().unwrap().msg,
                Msg::Rsp(Response { type_: ResponseType::Error(_), .. })
            );
            prop_assert!(matches);
        }
        ExpectedMsg::Share => {
            prop_assert_eq!(output.envelopes.len(), 1);
            let matches = matches!(
                output.envelopes.first().unwrap().msg,
                Msg::Rsp(Response { type_: ResponseType::Share(_), .. })
            );
            prop_assert!(matches);
        }
        ExpectedMsg::BroadcastPkg(to) => {
            // Ensure that a `Pkg` response is sent to all pending learners
            prop_assert_eq!(to.len(), output.envelopes.len());
            for envelope in &output.envelopes {
                prop_assert!(to.contains(&envelope.to));
                let matches = matches!(
                    envelope.msg,
                    Msg::Rsp(Response { type_: ResponseType::Pkg(_), .. })
                );
                prop_assert!(matches);
            }
        }
        _ => prop_assert!(false, "ExpectedMsg makes no sense"),
    }

    Ok(())
}

fn check_init_learner_output(
    expected: ExpectedOutput,
    output: &Output,
) -> Result<(), TestCaseError> {
    let already_init: ExpectedOutput =
        ExpectedApiError::PeerAlreadyInitialized.into();
    if already_init == expected {
        prop_assert!(output.envelopes.is_empty());
        let matches = matches!(
            output.api_output,
            Some(Err(ApiError::PeerAlreadyInitialized))
        );
        prop_assert!(matches);
    } else {
        // We expect a learn request or no request of there are no connected peers
        prop_assert!(output.persist);
        prop_assert_eq!(&output.api_output, &None);
        match expected.msg {
            Some(ExpectedMsg::Learn { destination }) => {
                prop_assert_eq!(1, output.envelopes.len());
                let Envelope { to, msg } = output.envelopes.first().unwrap();
                prop_assert_eq!(&destination, to);
                let matches = matches!(
                    msg,
                    Msg::Req(Request { type_: RequestType::Learn, .. })
                );
                prop_assert!(matches);
            }
            Some(_) => prop_assert!(false, "ExpectedMsg makes no sense"),
            None => prop_assert!(output.envelopes.is_empty()),
        }
    }
    Ok(())
}

// Validate that the output of `Fsm::load_rack_secret` made to a given peer
// makes sense.
fn check_load_rack_secret_output(
    expected: ExpectedOutput,
    output: &Output,
) -> Result<(), TestCaseError> {
    match expected.api_output {
        Some(Ok(ExpectedApiOutput::RackSecretLoaded)) => {
            let matches =
                matches!(output.api_output, Some(Ok(ApiOutput::RackSecret(_))));
            prop_assert!(matches);
        }
        Some(Err(ExpectedApiError::RackNotInitialized)) => {
            let matches = matches!(
                output.api_output,
                Some(Err(ApiError::RackNotInitialized))
            );
            prop_assert!(matches);
        }
        Some(Err(ExpectedApiError::StillLearning)) => {
            let matches = matches!(
                output.api_output,
                Some(Err(ApiError::RackNotInitialized))
            );
            prop_assert!(matches);
        }
        Some(Err(_)) => {
            prop_assert!(false, "ExpectedApiError makes no sense");
        }
        None => {
            if let Some(ExpectedMsg::BroadcastGetShare(to)) = expected.msg {
                // Ensure that a `GetShare` is sent to all connected peers
                prop_assert_eq!(to.len(), output.envelopes.len());
                for envelope in &output.envelopes {
                    prop_assert!(to.contains(&envelope.to));
                    let matches = matches!(
                        envelope.msg,
                        Msg::Req(Request {
                            type_: RequestType::GetShare { .. },
                            ..
                        })
                    );
                    prop_assert!(matches);
                }
            } else {
                prop_assert!(output.envelopes.is_empty());
            }
        }
    }
    Ok(())
}

fn check_tick_output(
    expected: ExpectedOutput,
    output: &Output,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(output.persist, false);

    match expected.msg {
        Some(ExpectedMsg::Learn { destination }) => {
            prop_assert_eq!(&output.api_output, &None);
            prop_assert_eq!(1, output.envelopes.len());
            let Envelope { to, msg } = output.envelopes.first().unwrap();
            prop_assert_eq!(&destination, to);
            let matches = matches!(
                msg,
                Msg::Req(Request { type_: RequestType::Learn, .. })
            );
            prop_assert!(matches);
        }
        Some(_) => prop_assert!(false, "ExpectedMsg makes no sense"),
        None => {
            prop_assert!(output.envelopes.is_empty());

            // We can only get a RackSecretTimeout if there are no expected
            // messages
            if let Some(Err(ExpectedApiError::RackSecretTimeout)) =
                expected.api_output
            {
                let matches = matches!(
                    output.api_output,
                    Some(Err(ApiError::RackSecretLoadTimeout))
                );
                prop_assert!(matches);
            }
        }
    }

    Ok(())
}

const MAX_INITIAL_MEMBERS: usize = 12;
const MAX_LEARNERS: usize = 10;

proptest! {
    #![proptest_config(ProptestConfig {max_shrink_iters: 100000, ..ProptestConfig::default()})]
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
//            println!("{:#?}", action);
            state.on_action(action)?;
        }
    }
}
