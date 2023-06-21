// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model state for bootstore peers

use std::collections::{BTreeMap, BTreeSet};

use bootstore::schemes::v0::{Config, Ticks};
use sled_hardware::Baseboard;
use uuid::Uuid;

use super::{generators::Action, network::FlowId};

// A simplified version of `state::RackSecretState`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelRackSecretState {
    Empty,
    Retrieving {
        // We have received shares from these peers so far
        received: BTreeSet<Baseboard>,
        // When does this request timeout?
        expiry: Ticks,
        // How many shares should we receive before we move into `Computed`
        threshold: usize,
    },
    Computed {
        // How long will we stay in computed before we drop the secret and go
        // back to `Empty`
        expiry: Ticks,
    },
}

impl ModelRackSecretState {
    pub fn is_empty(&self) -> bool {
        self == &ModelRackSecretState::Empty
    }
}

// A simplified version of `state::State`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelState {
    Uninitialized,
    InitialMember,
    Learning,
    Learned,
}

// A simplified version of a peer `FSM`
#[derive(Debug, Clone)]
pub struct PeerModel {
    pub state: ModelState,
    pub rack_secret_state: ModelRackSecretState,
}

impl PeerModel {
    pub fn new() -> PeerModel {
        PeerModel {
            rack_secret_state: ModelRackSecretState::Empty,
            state: ModelState::Uninitialized,
        }
    }
}

// When model state is updated by processing an `Action` it will return any
// expected output that it should see from the real bootstore when it processes
// the same `Action`.
pub enum ExpectedOutput {
    RackSecretLoaded(Baseboard),
    // We return which peer had a timeout and the tick value at which it would
    // have timed out.
    RackSecretTimeout(Baseboard, Ticks),
    GetShare { source: Baseboard, destination: Baseboard },
}

// Our global system model
pub struct Model {
    config: Config,
    clock: Ticks,
    rack_uuid: Option<Uuid>,
    peers: BTreeMap<Baseboard, PeerModel>,
    initial_members: BTreeSet<Baseboard>,
    learners: BTreeSet<Baseboard>,
    connected: BTreeSet<FlowId>,
    threshold: usize,
}

impl Model {
    pub fn new(
        config: Config,
        initial_members: BTreeSet<Baseboard>,
        learners: BTreeSet<Baseboard>,
    ) -> Model {
        // This is the calculation done in bootstore/src/trust_quorum/share_pkg.rs
        let threshold = initial_members.len() / 2 + 1;
        let peers = initial_members
            .iter()
            .chain(learners.iter())
            .cloned()
            .map(|id| (id, PeerModel::new()))
            .collect();

        Model {
            config,
            clock: 0,
            rack_uuid: None,
            peers,
            initial_members,
            learners,
            connected: BTreeSet::new(),
            threshold,
        }
    }

    pub fn get_peer_mut(&mut self, peer_id: &Baseboard) -> &mut PeerModel {
        // We ensure models always exist for each peer via test generation, so
        // unwrap is always safe.
        self.peers.get_mut(peer_id).unwrap()
    }

    pub fn get_peer(&self, peer_id: &Baseboard) -> &PeerModel {
        self.peers.get(peer_id).unwrap()
    }

    // On each action we update our model state so it corresponds with
    // the real state of the system under test after the bootstore actions
    // get handled.
    pub fn on_action(&mut self, action: Action) -> Vec<ExpectedOutput> {
        match action {
            Action::RackInit { rack_uuid, .. } => {
                if self.rack_uuid.is_none() {
                    self.rack_uuid = Some(rack_uuid);
                    for peer_id in &self.initial_members {
                        self.peers.get_mut(peer_id).unwrap().state =
                            ModelState::InitialMember;
                    }
                }
                vec![]
            }
            Action::Connect(flows_vec) => {
                // Make sure flows go in both directions
                let mut flows = BTreeSet::new();
                for (source, dest) in flows_vec {
                    flows.insert((source.clone(), dest.clone()));
                    flows.insert((dest, source));
                }

                // Determine if any `GetShare` messages need to be sent. This is
                // the case if a peer is currently retrieving shares and hasn't
                // received one from the newly connected peer.
                let expected = flows
                    .difference(&self.connected)
                    .filter_map(|(source, destination)| {
                        let peer = self.peers.get(source).unwrap();
                        if let ModelRackSecretState::Retrieving {
                            received,
                            ..
                        } = &peer.rack_secret_state
                        {
                            if !received.contains(destination) {
                                return Some(ExpectedOutput::GetShare {
                                    source: source.clone(),
                                    destination: destination.clone(),
                                });
                            }
                        }
                        None
                    })
                    .collect();
                self.connected.extend(flows);
                expected
            }
            Action::Disconnect(flows) => {
                for (source, dest) in flows {
                    // We ensure that connections are known to both sides in
                    // all cases
                    self.connected.remove(&(source.clone(), dest.clone()));
                    self.connected.remove(&(dest, source));
                }
                vec![]
            }
            Action::Ticks(ticks) => {
                let mut expected = vec![];
                self.clock += ticks;
                // Check for any expired rack secret loads
                for (peer_id, model) in &mut self.peers {
                    match model.rack_secret_state {
                        ModelRackSecretState::Retrieving { expiry, .. } => {
                            if expiry < self.clock {
                                expected.push(
                                    ExpectedOutput::RackSecretTimeout(
                                        peer_id.clone(),
                                        expiry + 1,
                                    ),
                                );
                                model.rack_secret_state =
                                    ModelRackSecretState::Empty;
                            }
                        }
                        ModelRackSecretState::Computed { expiry } => {
                            if expiry < self.clock {
                                model.rack_secret_state =
                                    ModelRackSecretState::Empty
                            }
                        }
                        ModelRackSecretState::Empty => (),
                    }
                }
                expected
            }
            Action::ChangeDelays(_) => {
                // Nothing to do here
                vec![]
            }
            Action::LoadRackSecret(peer_id) => {
                // We do this outside the match because of borrow checking rules
                let expected =
                    if self.get_peer(&peer_id).rack_secret_state.is_empty() {
                        // We expect a share to be sent to all connected peers
                        self.connected
                            .iter()
                            .filter_map(|(source, destination)| {
                                if source == &peer_id {
                                    Some(ExpectedOutput::GetShare {
                                        source: source.clone(),
                                        destination: destination.clone(),
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        vec![]
                    };
                let new_expiry =
                    self.clock + self.config.rack_secret_request_timeout;
                let threshold = self.threshold;

                match &mut self.get_peer_mut(&peer_id).rack_secret_state {
                    state @ ModelRackSecretState::Empty => {
                        // Transition our model state to "Retrieving"
                        *state = ModelRackSecretState::Retrieving {
                            // Make sure to add ourself
                            received: BTreeSet::from([peer_id.clone()]),
                            expiry: new_expiry,
                            threshold,
                        };

                        // Return any expected `GetShare` messages
                        expected
                    }
                    ModelRackSecretState::Retrieving { expiry, .. } => {
                        // We extend the timeout on a reload
                        *expiry = new_expiry;
                        expected
                    }
                    ModelRackSecretState::Computed { .. } => {
                        vec![ExpectedOutput::RackSecretLoaded(peer_id)]
                    }
                }
            }
        }
    }
}
