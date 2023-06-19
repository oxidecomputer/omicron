// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model state for bootstore peers

use std::collections::BTreeSet;

use bootstore::schemes::v0::Config;
use sled_hardware::Baseboard;

use super::{actions::Action, network::FlowId};

// A simplified version of `State::RackSecretState`
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

// A simplified version of a peer `FSM`
#[derive(Debug, Clone)]
pub struct PeerModel {
    rack_secret_state: ModelRackSecretState,
}

impl PeerModel {
    pub fn new() -> PeerModel {
        PeerModel { rack_secret_state: ModelRackSecretState::Empty }
    }
}

// When model state is updated by processing an `Action` it will return any
// expected output that it should see from the real bootstore when it processes
// the same `Action`.
pub enum ExpectedOutput {
    RackSecretLoaded(Baseboard),
    RackSecretTimeout(Baseboard),
    GetShare { source: Baseboard, destination: Baseboard },
}

// Our global system model
pub struct Model {
    config: Config,
    clock: Ticks,
    rack_uuid: Option<RackUuid>,
    peers: BTreeMap<Baseboard, PeerModel>,
    connected: BTreeSet<FlowId>,
}

impl Model {
    pub fn new(config: Config, initial_members: BTreeSet<Baseboard>) -> Model {
        let peers = initial_members
            .into_iter()
            .map(|id| (id, PeerModel::new()))
            .collect();

        Model {
            config,
            clock: 0,
            rack_uuid: None,
            peers,
            connected: BTreeSet::new(),
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
                }
                vec![]
            }
            Action::Connect(flows) => {
                // Determine if any `GetShare` messages need to be sent. This
                // is the case if we a peer is currently retrieving shares and
                // hasn't received one from the newly connected peer.
                let expected = flows
                    .difference(&self.connected)
                    .filter_map(|(source, destination)| {
                        let peer = self.peers.get(source);
                        if let Some(ModelRackSecretState::Retrieving {
                            received,
                            ..
                        }) = peer.rack_secret_state
                        {
                            if !received.contains(dest) {
                                return Some(ExpectedOutput::GetShareRetry {
                                    source,
                                    destination,
                                });
                            }
                        }
                        None
                    })
                    .collect();
                self.connected.extend(&flows);
                expected
            }
            Action::Disconnect(flows) => {
                for flow in flows {
                    self.connected.remove(&flow);
                }
                vec![]
            }
            Action::Ticks(ticks) => {
                let expected = vec![];
                self.clock += ticks;
                // Check for any expired rack secret loads
                for (peer_id, model) in self.peers {
                    match model.rack_secret_state {
                        ModelRackSecretState::Retrieving { expiry, .. } => {
                            if expiry < self.clock {
                                expected.push(
                                    ExpectedOutput::RackSecretTimeout(
                                        peer.clone(),
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
                let model = self.get_peer_mut(&peer_id);
                match model.rack_secret_state {
                    ModelRackSecretState::Empty => {
                        // We expect a share  to be sent to all connected peers
                        let expiry = self.clock
                            + self.config.rack_secret_request_timeout;
                        *model.rack_secret_state =
                            ModelRackSecretState::Retrieving {
                                received: BTreeSet::new(),
                                expiry,
                                threshold,
                            };
                    }
                    ModelRackSecretState::Retrieving { .. } => vec![],
                    ModelRackSecretState::Computed { .. } => {
                        vec![ExpectedOutput::RackSecretLoaded(peer_id)]
                    }
                }
            }
        }
    }
}
